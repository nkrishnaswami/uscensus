from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from uscensus.incremental.wrappers import Dataset
from uscensus.util.webcache import fetch


def _base_field(field_name: str) -> str:
    if field_name[-1] == 'A':
        field_name = field_name[:-1]
    if field_name[-1] == 'M':
        field_name = field_name[:-1] + 'E'
    return field_name


class QueryBuilder:
    def __init__(self, dataset: Dataset) -> None:
        if not dataset.api_url:
            raise ValueError('No API URL found')
        self.dataset = dataset
        self.fields : list[str] = []
        self.groups : list[str] = []
        self.geo_for_level : str = ''
        self.geo_for_value : str = ''
        self.geo_in : dict[str, str] = {}
        self.predicates : dict[str, int|float|str] = {}

    def set_fields(self, fields: list[str]) -> QueryBuilder:
        """Set the data fields (variables) to request from the API."""
        for field in fields:
            base_field = _base_field(field)
            variable = self.dataset.variables.get(base_field)
            if not variable:
                raise ValueError(f'Unknown field "{field}"')
            if variable.predicateOnly:
                raise ValueError(f'Field "{field}" is predicate-only')

        self.fields = list(fields)
        return self

    def set_groups(self, groups: list[str]) -> QueryBuilder:
        """Set the data groups (tables) to request from the API."""
        for group in groups:
            if group not in self.dataset.groups:
                raise ValueError(f'Unknown group {group}')

        self.groups = list(groups)
        return self


    def set_geo_for(self, geo_for: str, value: str) -> QueryBuilder:
        """Set the Census geography for which to retrieve data."""
        geo_level = self.dataset.geography.levels.get(geo_for)
        if not geo_level:
            raise ValueError(f'Invalid "for" geography "{geo_for}"')

        self.geo_for_level = geo_for
        self.geo_for_value = value
        return self


    def set_geo_in(self, geo_in: dict[str, str]) -> QueryBuilder:
        """Indicate the Census geographies containing the "for"
        geography, if required.

        """
        #for geo_in_level in geo_in:
        #    if not geo_level:

        self.geo_in = dict(geo_in)
        return self

    def set_predicates(self, predicates: dict[str, str|int|float]) -> QueryBuilder:
        """Set any desired predicates to qualify the query.

        NOTE: "for" and "in" predicates should be set using the special-purpose
        `set_geo_for` and `set_geo_in` methods.

        """
        for field, value in predicates.items():
            if field == 'for' or field == 'in':
                raise ValueError('Set "for" and "in" using set_geo_* methods.')
            base_field = _base_field(field)
            variable = self.dataset.variables.get(base_field)
            if not variable:
                raise ValueError(f'Unknown predicate "{field}"')
            if variable.predicateType == 'int' and not isinstance(value, int):
                raise TypeError(f'Predicate "{field}" requires int value: "{value}"')
            if variable.predicateType == 'float' and not isinstance(value, float):
                raise TypeError(f'Predicate "{field}" requires float value: "{value}"')
            if variable.predicateType == 'string' and not isinstance(value, str):
                raise TypeError(f'Predicate "{field}" requires str value: "{value}"')
            if variable.predicateType == 'ucgid' and not isinstance(value, str):
                raise TypeError(f'Predicate "{field}" requires UCGID value: "{value}"')

        self.predicates = dict(predicates)
        return self

    def _validate_geo(self) -> None:
        """Ensure that all required geographic information has been
        set.

        """
        if not self.dataset.geography.has_default and not self.geo_for_value:
            raise ValueError('Geography is required')
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

    def _make_params(self) -> dict[str, str]:
        """Assemble the QueryBuilder contents into request query
        parameters.

        """
        params = {
            'get': ','.join(self.fields +
                            [f'group({group})' for group in self.groups]),
            'for': f'{self.geo_for_level}:{self.geo_for_value}',
        }
        if self.geo_in:
            params['in'] = ' '.join((f'{level}:{value}'
                                     for level, value in self.geo_in.items()))
        for predicate, value in self.predicates.items():
            params[predicate] = str(value)
        return params

    def query(self) -> pd.DataFrame:
        """Issue the query represented by the `QueryBuilder` and
        return the results as a pandas DataFrame.

        """
        self._validate_geo()
        resp = fetch(
            self.dataset.api_url,
            self.dataset.client,
            params=self._make_params())
        resp.raise_for_status()
        data = resp.json()
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
