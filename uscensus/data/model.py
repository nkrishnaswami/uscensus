import logging
import re
from typing import Any, Dict, List

import pandas as pd
import httpx

from ..util.webcache import fetch
from ..util.textindex.textindex import TextIndex, VariableFields


_logger = logging.getLogger(__name__)


class CensusDataEndpoint(object):
    """A single census endpoint, with metadata about queryable variables
    and geography
    """

    def __init__(self,
                 key: str,
                 ds: Dict[str, Any],
                 session: httpx.Client,
                 variableindex: TextIndex):
        """Initialize a Census API dataset wrapper.

        Arguments:
          * key: user's API key.
          * ds: census dataset descriptor metadata.
          * session: httpx.Client to use for retrieving data.
          * variableindex: the Index in which to store variable data
        """
        self.key = key                         # API key
        self.session = session                 # httpx.Client
        self.title = ds['title']               # title
        self.description = ds['description']   # long description
        self.__doc__ = self.description
        # dataset descriptors, (general to specific)
        self.dataset = tuple(ds['c_dataset'])
        # vintage, if dataset is year-specific
        self.vintage = ds.get('c_vintage')
        # API endpoint URL
        for distribution in ds.get('distribution') or []:
            if distribution.get('format') == 'API':
                self.endpoint = distribution['accessURL']
        # short ID
        self.id = self.endpoint.replace('https://api.census.gov/data/', '')
        # list of valid geographies
        if geo_url := ds.get('c_geographyLink'):
            self.geographies_ = (fetch(geo_url, self.session).json() or {})
            geo_cols = [
                'scheme',
                'name',
                'predicate_type',
                'referenceDate',
                'requires',
                'optionalWithWCFor',
                'wildcard']
            self.geographies = pd.DataFrame([], columns=geo_cols)
            for scheme in self.geographies_:
                tmp = pd.DataFrame(
                    self.geographies_[scheme], columns=geo_cols)
                tmp['scheme'] = scheme
                self.geographies = pd.concat((self.geographies, tmp))

        # list of valid variables
        if variables_url := ds.get('c_variablesLink'):
            data = (fetch(variables_url, self.session).json() or {})
            self.variables_ = data.get('variables', {})
            self.variables = pd.DataFrame(
                self.variables_, index=[
                    'label', 'concept', 'predicateType', 'group',
                    'limit', 'predicateOnly', 'attributes',
                ]).T
            # index the variables
            self.variableindex = variableindex
            self.variableindex.add(self._generateVariableRows())

        # keep track of concepts for indexing
        self.concepts = set(self.variables['concept']
                            .dropna().sort_values().values)

        # list of keywords
        self.keywords = ds.get('keyword', [])
        # list of tags
        self.tags: List[str] = []
        if 'c_tagsLink' in ds:
            # list of tags
            # Note: as of 2021-04-12, these are all broken
            try:
                data = (fetch(ds['c_tagsLink'], self.session).json() or {})
                self.tags = data.get('tags', [])
            except httpx.HTTPError as e:
                _logger.warning(f"Unable to fetch {ds['c_tagsLink']}: {e}")

        # list of groups
        self.groups_ = {}
        if 'c_groupsLink' in ds:
            # list of groups
            for row in (fetch(ds['c_groupsLink'], self.session).json()
                        or {}).get('groups', []):
                self.groups_[row['name']] = {
                    'descriptions': row['description']
                }
                if row['variables']:
                    data = fetch(row['variables'], self.session).json() or {}
                    self.groups_[row['name']]['variables'] = list(
                        data.get('variables', {}).keys())
        self.groups = pd.DataFrame(self.groups_).T

    def searchVariables(self, query, **constraints):
        """Return for variables matching a query string.

        Keywords are `variable` (ID), `label` (name) and `concept`
        (grouping of variables).

        """
        return pd.DataFrame(
            self.variableindex.query(
                query,
                dataset_id=self.id,
                **constraints),
            columns=['score'] + list(self.variableindex.fields)
        ).drop('dataset_id', axis=1)

    @staticmethod
    def _geo2str(geo):
        """Format geography dict as string for query"""
        return ' '.join(f'{k}:{v}' for k, v in geo.items())

    def __call__(self, fields, geo_for, *, geo_in=None,
                 groups=[]):
        """Special method to make dataset object invocable.

        Arguments:
          * fields: list of variables to return.
          * geo_* fields must be given as dictionaries, eg:
            `{'county': '*'}`
          * cache: cache in which to store results. Not cached by default.
          * groups: variable groups to retrieve
        """
        params = {
            'get': ','.join(fields + [f'group({group})'
                                      for group in groups]),
            'key': self.key,
            'for': self._geo2str(geo_for),
        }
        if geo_in:
            params['in'] = self._geo2str(geo_in)

        j = fetch(self.endpoint, self.session, params=params).json()
        ret = pd.DataFrame(data=j[1:], columns=j[0])
        for group in groups:
            if group in self.groups.index:
                fields += self.groups.loc[group, 'variables']
        for field in fields:
            basefield = re.sub(r'(?<=\d)[EM]A?$', 'E', field)
            if self.variables.loc[basefield, 'predicateType'] in (
                    'int', 'float'):
                ret[field] = pd.to_numeric(ret[field])
        return ret

    def _generateVariableRows(self):
        for k, v in self.variables_.items():
            yield VariableFields(
                dataset_id=self.id,
                variable=k,
                group=v.get('group', ''),
                label=v.get('label', ''),
                concept=v.get('concept', ''),
            )

    def __repr__(self):
        """Represent dataset endpoint by its title"""
        return self.title
