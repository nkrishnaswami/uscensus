from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
import logging
import re
from typing import TYPE_CHECKING

from dataclasses_json import dataclass_json
import pandas as pd

if TYPE_CHECKING:
    from uscensus.incremental.wrappers import Dataset
    from uscensus.incremental.model import Variable
from uscensus.util.webcache import fetch


_logger = logging.getLogger(__name__)


def _base_field(field_name: str) -> str:
    if field_name[-1] == 'A':
        field_name = field_name[:-1]
    if field_name[-1] == 'M':
        field_name = field_name[:-1] + 'E'
    if match := re.match(r'(.*?)_\d{4}to(\d{4})_(\d+)SS', field_name):
        table, year, variable = match.groups()
        field_name = f'{table}_{year}_{variable}E'
    return field_name


def _format_predicate_values(values: str | int | float | tuple | list) -> str:
    if isinstance(values, list):
        return ','.join(values)
    if isinstance(values, tuple):
        return f'{values[0]}:{values[1]}'
    return str(values)


class QueryBuilderBase(ABC):
    def __init__(self, dataset: Dataset) -> None:
        if not dataset.api_url:
            raise ValueError('No API URL found')
        self.dataset = dataset
        self.geo_for_level : str = ''
        self.geo_for_value : str = ''
        self.geo_in : dict[str, str] = {}
        self.predicates : dict[str, int|float|str] = {}

    def set_geo_for(self, geo_for: str, value: str | list[str]) -> QueryBuilderBase:
        """Set the Census geography for which to retrieve data."""
        geo_level = self.dataset.geography.levels.get(geo_for)
        if not geo_level:
            raise ValueError(f'Invalid "for" geography "{geo_for}"')

        self.geo_for_level = geo_for
        if isinstance(value, list):
            value = ','.join(value)
        self.geo_for_value = value
        return self


    def set_geo_in(self, geo_in: dict[str, str]) -> QueryBuilderBase:
        """Indicate the Census geographies containing the "for"
        geography, if required.

        """
        #for geo_in_level in geo_in:
        #    if not geo_level:

        self.geo_in = dict(geo_in)
        return self

    def set_predicates(self,
                       predicates: dict[str, str | int | float |
                                        list[str | int | float] |
                                        tuple[int | float, int | float]]
                       ) -> QueryBuilderBase:
        """Set any desired predicates to qualify the query.

        The values of the `predicates` dictionary may be scalar,
        lists to set individual or multiple predicate values, or a 2-tuple
        to set a range for a numerical typed variable.

        NOTE: "for" and "in" predicates should be set using the special-purpose
        `set_geo_for` and `set_geo_in` methods.

        """
        def satisfies_numeric(value, type):
            return (isinstance(value, type) or
                    isinstance(value, list) or
                    isinstance(value, tuple))
        def satisfies_string(value):
            return (isinstance(value, str) or
                    isinstance(value, list))
        for field, value in predicates.items():
            if field == 'for' or field == 'in':
                raise ValueError('Set "for" and "in" using set_geo_* methods.')
            if (base_field := _base_field(field)) not in self.dataset.variables:
                base_field = field
            variable = self.dataset.variables.get(base_field)
            if not variable:
                raise ValueError(f'Unknown predicate "{field}"')
            if variable.predicateType == 'int' and not satisfies_numeric(value, int):
                raise TypeError(f'Predicate "{field}" requires int value: "{value}"')
            if variable.predicateType == 'float' and not satisfies_numeric(value, float):
                raise TypeError(f'Predicate "{field}" requires float value: "{value}"')
            if variable.predicateType == 'string' and not satisfies_string(value):
                raise TypeError(f'Predicate "{field}" requires str value: "{value}"')
            if variable.predicateType == 'ucgid' and not satisfies_string(value):
                raise TypeError(f'Predicate "{field}" requires UCGID value: "{value}"')

        self.predicates = dict(predicates)
        return self

    def _validate_geo(self) -> None:
        """Ensure that all required geographic information has been
        set.

        """
        
        if not self.geo_for_value and not self.dataset.geography.has_default:
            raise ValueError('Geography is required')
        if not self.geo_for_level:
            return
        geo_level = self.dataset.geography.levels[self.geo_for_level]
        for level_id in geo_level.requires:
            if level_id not in self.geo_in:
                if (self.geo_for_value == '*' and
                    geo_level.optionalWithWCFor == level_id):
                    continue
                raise ValueError(f'Missing required "in" geography "{level_id}"')
        for level_id, value in self.geo_in.items():
            if level_id not in geo_level.requires:
                raise ValueError(f'Unexpected "in" geography "{level_id}"')
            if value == '*' and level_id not in geo_level.wildcard:
                raise ValueError(f'Unexpected wildcard in "in" geography "{level_id}"')

    @abstractmethod
    def _make_params(self) -> dict[str, str]:
        """Assemble the QueryBuilderBase subclass contents into
        request query parameters.

        """
        pass

    def _make_common_params(self) -> dict[str, str]:
        """Assemble the QueryBuilderBase contents into request query
        parameters.

        """
        self._validate_geo()
        params = {
            'for': f'{self.geo_for_level}:{self.geo_for_value}',
        }
        if self.geo_in:
            params['in'] = ' '.join((f'{level}:{value}'
                                     for level, value in self.geo_in.items()))
        for predicate, value in self.predicates.items():
            params[predicate] = _format_predicate_values(value)
        return params

    @abstractmethod
    def _make_dataframe(self, data: dict) -> pd.DataFrame:
        """Convert the API JSON response into a DataFrame.

        """
        pass

    def query(self) -> pd.DataFrame:
        """Issue the query represented by the `QueryBuilderBase` and
        return the results as a pandas DataFrame.

        """
        resp = fetch(
            self.dataset.api_url,
            self.dataset.client,
            params=self._make_common_params() | self._make_params())
        resp.raise_for_status()
        return self._make_dataframe(resp.json())

    async def aquery(self) -> pd.DataFrame:
        """Issue the query represented by the `QueryBuilderBase` and
        return the results as a pandas DataFrame.

        """
        self._validate_geo()
        resp = await afetch(
            self.dataset.api_url,
            self.dataset.client,
            params=self._make_common_params() | self._make_params())
        resp.raise_for_status()
        return self._make_dataframe(resp.json())


class QueryBuilder(QueryBuilderBase):
    """Builds a data API query as described in
    https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf

    """
    def __init__(self, dataset: Dataset):
        super().__init__(dataset)
        self.fields : list[str] = []
        self.groups : list[str] = []

    def set_fields(self, fields: list[str]) -> QueryBuilder:
        """Set the data fields (variables) to request from the API.

        """
        suggestedWeights = set()
        requestedWeights = set()
        for field in fields:
            base_field = _base_field(field)
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

    def set_groups(self, groups: list[str]) -> QueryBuilder:
        """Set the data groups (tables) to request from the API."""
        for group in groups:
            if group not in self.dataset.groups:
                raise ValueError(f'Unknown group {group}')

        self.groups = list(groups)
        return self


    def _make_params(self):
        required = {name for name, variable in self.dataset.variables.items()
                    if variable.required}
        missing = required - set(self.predicates) - set(self.fields)
        if missing:
            raise ValueError(f'Request missing required variables: {missing}')
        return {
            'get': ','.join(
                self.fields + [f'group({group})' for group in self.groups])
        }

    def _make_dataframe(self, data: dict) -> pd.DataFrame:
        ret = pd.DataFrame(data=data[1:], columns=data[0])

        # Fix up data types
        fields = list(self.fields)
        for group_id in self.groups:
            group = self.dataset.groups[group_id]
            fields += list(group.variables.keys())
        for field in fields:
            base_field = _base_field(field)
            predicate_type = self.dataset.variables[base_field].predicateType
            if predicate_type in ('int', 'float'):
                ret[field] = pd.to_numeric(ret[field])
        return ret


@dataclass_json
@dataclass(eq=True, frozen=True)
class RecodeRange:
    mn: int | float
    mx: int | float


@dataclass_json
@dataclass(eq=True, frozen=True)
class RecodeValue:
    b: str
    d: list[list[str | int | RecodeRange]]


def _check_valid_recode_value(
        value: str | int | RecodeRange,
        valid_values: set(int)):
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
        for val in range(value.mn, value.mx + 1):
            if val not in valid_values:
                return False
    else:
        raise ValueError(f'Unexpected recode value type: {type(value).__name__}')
    return True


def _check_valid_recode_values(
        base_var: str,
        variable: Variable,
        category_defs: list[list[str | int | RecodeRange]]) -> bool:
    """Ensure that the values in a category definition satisfy the
    variable definition.

    """
    if not variable.values:
        return True
    valid_values = set()
    if variable.values.item:
        valid_values.update(int(x) for x in variable.values.item)
    if variable.values.range:
        for range in variable.values.range:
            valid_values.update(range(int(range.min),
                                      int(range.max) + 1))
    definition_values = set()
    for category_def in category_defs:
        for value in category_def:
            if not _check_valid_recode_value(value, valid_values):
                raise ValueError(f'Invalid recode value {value} for "{base_var}"')
    return True



class TabulationQueryBuilder(QueryBuilderBase):
    """Builds a microdata tabulation query as described in
    https://www2.census.gov/data/api-documentation/microdata-api-user-guide.pdf

    """
    def __init__(self, dataset: Dataset):
        if not dataset.c_isMicrodata:
            raise TypeError('Cannot make tabulation query for non-microdata dataset')
        super().__init__(dataset)
        self.weight: str = None
        self.avg: str = None
        self.rows: list[str] = []
        self.cols: list[str] = []
        self.recodes: dict[str, Recode] = {}

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
            raise ValueError(f'Requested weight {weight} is not a weight variable')
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
            raise ValueError(f'Cannot request average of categorical field {avg}')
        self.avg = avg
        return self

    def set_rows(self, rows: str | list[str]):
        """Set the rows for the custom tabulation.

        """
        if isinstance(rows, str):
            rows = [rows]
        for row in rows:
            if row not in self.recodes:
                variable = self.dataset.variables.get(row)
                if not variable:
                    raise ValueError(f'Unknown field "{row}"')
        self.rows = rows
        return self

    def set_cols(self, cols: str | list[str]):
        """Set the columns for the custom tabulation.

        """
        if isinstance(cols, str):
            cols = [cols]
        for col in cols:
            if col not in self.recodes:
                variable = self.dataset.variables.get(col)
                if not variable:
                    raise ValueError(f'Unknown field "{col}"')

        self.cols = cols
        return self

    def add_recode(self, new_var: str, base_var: str,
                   *category_defs: list[str | int | RecodeRange]):
        """Specify how to recode an existing variable with a new name.

        """
        variable = self.dataset.variables.get(base_var)
        if not variable:
            raise ValueError(f'Unknown field "{base_var}"')
        _check_valid_recode_values(base_var, variable, category_defs)

        self.recodes[new_var] = RecodeValue(b=base_var, d=list(category_defs))
        return self

    def _make_params(self):
        ret = {}

        required = {name for name, variable in self.dataset.variables.items()
                    if variable.required}
        missing = required - set(self.predicates) - set(self.cols) - set(self.rows)
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
        if 'for' in self.cols or 'for' in self.rows and not self.geo_for_value:
            raise ValueError('Disaggregation by geography requested without specifying geography')
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
                col_tuples.append((tuple(header[col] for col in req_cols)))
            ret.columns = pd.MultiIndex.from_tuples(col_tuples, names=req_cols)
        return ret
