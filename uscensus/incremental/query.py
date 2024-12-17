from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from collections.abc import Iterable

import pandas as pd

from uscensus.incremental.model import USCensusBaseModel
from uscensus.util.webcache import afetch, fetch

if TYPE_CHECKING:
    from uscensus.incremental.model import GeographyLevel, Variable
    from uscensus.incremental.wrappers import Dataset

_logger = logging.getLogger(__name__)


def _base_field(field_name: str) -> str:
    if field_name[-1] == 'A':
        field_name = field_name[:-1]
    if field_name[-1] == 'M':
        field_name = field_name[:-1] + 'E'
    try:
        if match := re.fullmatch(r'(.*?)_\d{4}to(\d{4})_(\d+)SS', field_name):
            table, year, variable = match.groups()
            field_name = f'{table}_{year}_{variable}E'
    except TypeError as e:
        raise ValueError(f'Invalid value {field_name}') from e
    return field_name


PredicateScalarValue = int | float | str | tuple[int, int]


def _format_predicate_value(value: PredicateScalarValue) -> str:
    if isinstance(value, tuple):
        return f'{value[0]}:{value[1]}'
    return str(value)


def _format_predicate_values(values: list[PredicateScalarValue]) -> str:
    return ','.join(_format_predicate_value(value) for value in values)


def _all_numeric(values: Iterable[PredicateScalarValue], type: type[int] | type[float]) -> bool:
    def is_convertible(value):
        try:
            if isinstance(value, tuple):
                type(value[0])
                type(value[1])
                return True
            type(value)
            return True
        except Exception:
            _logger.error('unable to convert %s to %s',
                          value, type.__name__, exc_info=True)
            return False
    return all(is_convertible(value) for value in values)


def _all_string(values: Iterable[PredicateScalarValue]) -> bool:
    return all(type(value) in [str, int, float] for value in values)


class QueryBuilderBase(ABC):
    def __init__(self, dataset: Dataset) -> None:
        if not dataset.api_url:
            raise ValueError('No API URL found')
        self.dataset = dataset
        self.geo_for_level: str = ''
        self.geo_for_values: list[str] = []
        self.geo_in: dict[str, list[str]] = {}
        self.predicates: dict[str, list[PredicateScalarValue]] = {}

    @abstractmethod
    def _make_params(self) -> dict[str, str]:
        """Assemble the QueryBuilderBase subclass contents into
        request query parameters.

        """
    @abstractmethod
    def _make_dataframe(self, data: dict) -> pd.DataFrame:
        """Convert the API JSON response into a DataFrame."""

    def set_geo_for(self, geo_for: str, *values: str) -> QueryBuilderBase:
        """Set the Census geographies for which to retrieve data."""
        geo_level = self.dataset.geography.levels.get(geo_for)
        if not geo_level:
            raise ValueError(f'Invalid "for" geography "{geo_for}"')
        if '*' in values and len(values) > 1:
            raise ValueError(
                'Cannot specify wildcard "for" geography with other values')

        self.geo_for_level = geo_for
        self.geo_for_values = list(values)
        return self

    def add_geo_in(self, geo_in: str, *values: str) -> QueryBuilderBase:
        """Indicate the Census geographies containing the "for"
        geography, if required.

        """
        if '*' in values and len(values) > 1:
            raise ValueError(
                'Cannot specify wildcard "in" predicate with other values')
        self.geo_in[geo_in] = list(values)
        return self

    def add_predicate(self,
                      field: str,
                      *values: str | int | float | tuple[int, int]) -> QueryBuilderBase:
        """Set a desired predicate to qualify the query.

        The values may be scalar to set discrete predicate values or,
        for a numerical typed variable, a 2-tuple to set a range.

        NOTE: "for" and "in" predicates should be set using the special-purpose
        `set_geo_for` and `set_geo_in` methods.

        """
        if field == 'for' or field == 'in':
            raise ValueError('Set "for" and "in" using set_geo_* methods.')
        if (base_field := _base_field(field)) not in self.dataset.variables:
            base_field = field
        variable = self.dataset.variables.get(base_field)
        _logger.info('Values: %s', values)
        if not variable:
            raise ValueError(f'Unknown predicate "{field}"')
        if variable.predicateType == 'int' and not _all_numeric(values, int):
            raise TypeError(
                f'Predicate "{field}" requires int values: "{values}"')
        if variable.predicateType == 'float' and not _all_numeric(
                values, float):
            raise TypeError(
                f'Predicate "{field}" requires float values: "{values}"')
        if variable.predicateType == 'string' and not _all_string(values):
            raise TypeError(
                f'Predicate "{field}" requires str values: "{values}"')
        if variable.predicateType == 'ucgid' and not _all_string(values):
            raise TypeError(
                f'Predicate "{field}" requires UCGID values: "{values}"')

        self.predicates[field] = list(values)
        return self

    def _validate_geo(self) -> None:
        """Ensure that all required geographic information has been
        set and is consistent.

        """
        # Is `for` required but missing?
        if not self.geo_for_values and not self.dataset.geography.has_default:
            raise ValueError('Geography is required')
        # Is `for` using a default?
        if not self.geo_for_level:
            return

        geo_level = self._determine_geo_level()

        # Check invariants for values for `in` constraints that don't
        # depend on order.
        for level_id, values in self.geo_in.items():
            if level_id not in geo_level.requires:
                raise ValueError(f'Unexpected "in" geography "{level_id}"')
            if '*' in values and level_id not in geo_level.wildcard:
                raise ValueError(
                    f'Unexpected wildcard in "in" geography "{level_id}"')
            if len(values) > 1 and self.geo_for_values != ['*']:
                raise ValueError(
                    'Multiple "in" geographies with non-wildcard "for"')

        # Validate that multi-valued `in` items don't violate
        # constraints implied by geo level ordering. Note that we
        # can't determine this until both `in` and `for` are set.
        if len(geo_level.requires) > 1:
            # Get the in constraints in required geo order
            ordered_values = [self.geo_in[level_id]
                              for level_id in geo_level.requires
                              if level_id in self.geo_in]
            for idx, cur_level_values in enumerate(ordered_values):
                following_level_values = ordered_values[idx+1:]
                if following_level_values:
                    if (len(cur_level_values) > 1 and
                        not all(x == ['*'] for x in following_level_values)):
                        raise ValueError(
                            'Cannot specify non-wildcard "in" constraint '
                            'below multi-valued level')
                    if ('*' in cur_level_values and
                        not all(x == ['*'] for x in following_level_values)):
                        raise ValueError(
                            'Cannot specify non-wildcard "in" constraint below '
                            'level with wildcard')

    def _determine_geo_level(self) -> GeographyLevel:
        # Find this `for` geography's level descriptor
        errors: list[ValueError] = []
        geo_levels = self.dataset.geography.levels.get(self.geo_for_level)
        if not geo_levels:
            raise ValueError(f'No levels found for geography {self.geo_for_level}')
        for geo_level in geo_levels:
            try:
                self._validate_geo_level(geo_level)
                return geo_level
            except ValueError as e:
                errors.append(e)
        raise ExceptionGroup('No matching geo level', errors)
        

    def _validate_geo_level(self, geo_level: GeographyLevel) -> None:
        # Does it have any required `in` geographies?
        for level_id in geo_level.requires:
            # Are they missing?
            if level_id not in self.geo_in:
                # Are they optional when `for` is a wildcard?
                if ('*' in self.geo_for_values and
                    geo_level.optionalWithWCFor == level_id):
                    continue
                raise ValueError(
                    f'Missing required "in" geography "{level_id}" for '
                    f'level {geo_level}')

    def _make_common_params(self) -> dict[str, str]:
        """Assemble the QueryBuilderBase contents into request query
        parameters.

        """
        self._validate_geo()
        params = {
            'for': f'{self.geo_for_level}:{",".join(self.geo_for_values)}',
        }
        if self.geo_in:
            params['in'] = ' '.join((f'{level}:{",".join(values)}'
                                     for level, values in self.geo_in.items()))
        for predicate, value in self.predicates.items():
            params[predicate] = _format_predicate_values(value)
        return params

    def _prepare_fetch_args(self) -> dict:
        return {
            'url': self.dataset.api_url,
            'session':  self.dataset.client,
            'params': self._make_common_params() | self._make_params(),
        }

    def query(self) -> pd.DataFrame:
        """Issue the query represented by the `QueryBuilderBase` and
        return the results as a pandas DataFrame.

        """
        resp = fetch(**self._prepare_fetch_args())
        return self._make_dataframe(resp.json())

    async def aquery(self) -> pd.DataFrame:
        """Issue the query represented by the `QueryBuilderBase` and
        return the results as a pandas DataFrame.

        """
        self._validate_geo()
        resp = await afetch(**self._prepare_fetch_args())
        resp.raise_for_status()
        return self._make_dataframe(resp.model_dump_json())



class QueryBuilder(QueryBuilderBase):
    """Builds a data API query as described in
    https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf.

    """

    def __init__(self, dataset: Dataset) -> None:
        super().__init__(dataset)
        self.fields: list[str] = []
        self.group: str | None = None

    def set_fields(self, *fields: str) -> QueryBuilder:
        """Set the data fields (variables) to request from the API."""
        suggestedWeights = set()
        requestedWeights = set()
        for field in fields:
            base_field = _base_field(field)
            if base_field not in self.dataset.variables:
                base_field = field
            variable = self.dataset.variables.get(base_field)
            if not variable:
                raise ValueError(f'Unknown field "{field}"')
            if variable.predicateOnly:
                raise ValueError(f'Field "{field}" is predicate-only')
            if self.dataset.c_isMicrodata:
                if variable.isWeight:
                    requestedWeights.add(base_field)
                elif variable.suggestedWeight:
                    suggestedWeights.add(variable.suggestedWeight)
        if suggestedWeights - requestedWeights:
            raise ValueError(
                'Suggested weights not requested for microdata endpoint')

        self.fields = list(fields)
        return self

    def set_group(self, group: str) -> QueryBuilder:
        """Set the data group (table) to request from the API."""
        if group not in self.dataset.groups:
            raise ValueError(f'Unknown group {group}')

        self.group = group
        return self

    def _make_params(self):
        # required = self.dataset.required_variables
        # _logger.info(f'required variables: {required}')
        # missing = set(required) - set(self.predicates) \
        #     - set(self.fields)
        # if missing:
        #     raise ValueError(f'Request missing required variables: {missing}')
        group = [f'group({self.group})'] if self.group else []
        return {
            'get': ','.join(self.fields + group)
        }

    def _make_dataframe(self, data: dict) -> pd.DataFrame:
        ret = pd.DataFrame(data=data[1:], columns=data[0])

        # Fix up data types
        fields = list(self.fields)
        if self.group:
            group = self.dataset.groups[self.group]
            fields += list(group.variables.keys())
        for field in fields:
            base_field = _base_field(field)
            if variable := self.dataset.variables.get(base_field):
                predicate_type = variable.predicateType
                if predicate_type in ('int', 'float'):
                    ret[field] = pd.to_numeric(ret[field], errors='coerce')
        return ret


class RecodeRange(USCensusBaseModel):
    mn: int | float
    mx: int | float


class RecodeValue(USCensusBaseModel):
    b: str
    d: list[list[str | int | RecodeRange]]


def _check_valid_recode_value(
        value: str | int | RecodeRange,
        valid_values: set[int]) -> bool:
    if isinstance(value, int):
        if value not in valid_values:
            return False
    elif isinstance(value, str):
        try:
            if int(value) not in valid_values:
                return False
        except ValueError:
            return False
    elif isinstance(value, RecodeRange):
        for val in range(int(value.mn), int(value.mx) + 1):
            if val not in valid_values:
                return False
    else:
        raise ValueError(
            f'Unexpected recode value type: {type(value).__name__}')
    return True


def _check_valid_recode_values(
        base_var: str,
        variable: Variable,
        category_defs: Iterable[list[str | int | RecodeRange]]) -> bool:
    """Ensure that the values in a category definition satisfy the
    variable definition.

    """
    if not variable.values:
        return True
    valid_values = set()
    if variable.values.item:
        valid_values.update(int(x) for x in variable.values.item)
    if variable.values.range:
        for recode_range in variable.values.range:
            valid_values.update(range(int(recode_range.min),
                                      int(recode_range.max) + 1))
    for category_def in category_defs:
        for value in category_def:
            if not _check_valid_recode_value(value, valid_values):
                raise ValueError(
                    f'Invalid recode value {value} for "{base_var}"')
    return True


class TabulationQueryBuilder(QueryBuilderBase):
    """Builds a microdata tabulation query as described in
    https://www2.census.gov/data/api-documentation/microdata-api-user-guide.pdf.

    """

    def __init__(self, dataset: Dataset) -> None:
        if not dataset.c_isMicrodata:
            raise TypeError(
                'Cannot make tabulation query for non-microdata dataset')
        super().__init__(dataset)
        self.weight: str | None = None
        self.avg: str | None = None
        self.rows: list[str] = []
        self.cols: list[str] = []
        self.recodes: dict[str, RecodeValue] = {}

    def set_weight(self, weight: str) -> TabulationQueryBuilder:
        """Select the weight variable to use for the tabulation.

        Valid weights can be retrieved from the dataset's
        `weight_variables` property or the requested variables'
        `suggestedWeight` values.

        """
        variable = self.dataset.variables.get(weight)
        if not variable:
            raise ValueError(f'Unknown field "{weight}"')
        if not variable.isWeight:
            raise ValueError(
                f'Requested weight {weight} is not a weight variable')
        self.weight = weight
        return self

    def set_avg(self, avg: str) -> TabulationQueryBuilder:
        """Return average values the specified continuous-valued field
        instead of returning a sum of sample units.

        """
        variable = self.dataset.variables.get(avg)
        if not variable:
            raise ValueError(f'Unknown field "{avg}"')
        if variable.values and not variable.values.range:
            raise ValueError(
                f'Cannot request average of categorical field {avg}')
        self.avg = avg
        return self

    def set_rows(self, *rows: str):
        """Set the rows for the custom tabulation."""
        for row in rows:
            if row not in self.recodes:
                variable = self.dataset.variables.get(row)
                if not variable:
                    raise ValueError(f'Unknown field "{row}"')
        self.rows = list(rows)
        return self

    def set_cols(self, *cols: str):
        """Set the columns for the custom tabulation."""
        for col in cols:
            if col not in self.recodes:
                variable = self.dataset.variables.get(col)
                if not variable:
                    raise ValueError(f'Unknown field "{col}"')

        self.cols = list(cols)
        return self

    def add_recode(self, new_var: str, base_var: str,
                   *category_defs: list[str | int | RecodeRange]):
        """Specify how to recode an existing variable with a new name."""
        variable = self.dataset.variables.get(base_var)
        if not variable:
            raise ValueError(f'Unknown field "{base_var}"')
        _check_valid_recode_values(base_var, variable, category_defs)

        self.recodes[new_var] = RecodeValue(b=base_var, d=list(category_defs))
        return self

    def _make_params(self):
        ret = {}

        missing = set(self.dataset.required_variables or {}) - \
            set(self.predicates) - set(self.cols) - set(self.rows)
        if missing:
            raise ValueError(f'Failed to set required predicates: {missing}')

        tabulate_args = []
        if self.weight:
            tabulate_args.append(f'weight({self.weight})')
        if self.avg:
            tabulate_args.append(f'avg({self.avg})')
        ret['tabulate'] = ','.join(tabulate_args)

        if not self.cols and not self.rows:
            raise ValueError('At least one of col or row must be specified')
        if 'for' in self.cols or 'for' in self.rows and not self.geo_for_level:
            raise ValueError(
                'Disaggregation by geography requested without specifying geography')
        for row in self.rows:
            ret[f'row+{row}'] = ''
        for col in self.cols:
            ret[f'col+{col}'] = ''

        for new_var, recode in self.recodes.items():
            if new_var not in self.cols and new_var not in self.rows:
                raise ValueError(f'Unused recode "{new_var}"')
            ret[f'recode+{new_var}'] = recode.to_json()

        return ret

    def _make_dataframe(self, data: dict) -> pd.DataFrame:
        # The returned dataset column headers are the row variable
        # names along with JSON dictionaries with each combination of
        # column variables' names and values.
        #
        # First let's turn everything into strings.
        cols = [json.dumps(x)
                if isinstance(x, dict) else x
                for x in data[0]]
        ret = pd.DataFrame(data=data[1:], columns=cols)
        # And set the row index if rows were specified.
        if self.rows:
            ret = ret.set_index(self.rows)
        # If columns were specified, create a MultiIndex for the
        # values after turning each combo into a tuple in the order of
        # self.cols.  We could possibly do something here to show
        # categorical variable names instead of codes, but there are
        # quite a few mixed ones (enumerated special values plus a
        # range),
        if self.cols:
            col_headers = ret.columns.map(json.loads)
            col_tuples = []
            req_cols = list(self.cols)
            if self.avg:
                req_cols.append(self.avg)
            for header in col_headers:
                col_tuples.append(tuple(header[col] for col in req_cols))
            ret.columns = pd.MultiIndex.from_tuples(col_tuples, names=req_cols)
        return ret
