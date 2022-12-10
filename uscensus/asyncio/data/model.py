import asyncio
import logging
import re
from typing import Optional, Tuple

import pandas as pd
import httpx

# this is sync for now
from ...util.textindex import TextIndex, VariableFields
from ..util.webcache import afetch


_logger = logging.getLogger(__name__)


class AsyncCensusDataEndpoint:
    """A single census endpoint, with metadata about queryable variables
    and geography
    """
 
    concepts: set[str]
    dataset: Tuple
    description: str
    endpoint: str
    geographies: pd.DataFrame
    geographies_: dict
    groups: pd.DataFrame
    groups_: dict
    id: str
    key: str
    keywords: list[str]
    session: httpx.AsyncClient
    tags: list[str]
    title: str
    variableindex: TextIndex
    variables: pd.DataFrame
    variables_: dict
    vintage: Optional[str]

    @staticmethod
    async def create(key: str,
                     ds: dict,
                     session: httpx.AsyncClient,
                     variableindex: TextIndex):
        """Initialize a Census API endpoint wrapper.

        Arguments:
          * key: user's API key.
          * ds: census dataset descriptor metadata.
          * cache: cache in which to look up/store metadata.
          * session: httpx.Client to use for retrieving data.
          * variableindex: the Index in which to store variable data
        """
        self = AsyncCensusDataEndpoint()
        self.key = key                         # API key
        self.session = session                 # httpx.Client
        self.title = ds['title']               # title
        self.description = ds['description']   # long description
        self.__doc__ = self.description
        # dataset descriptors, (general to specific)
        self.dataset = tuple(ds['c_dataset'])
        # vintage, if dataset is year-specific
        self.vintage = str(ds['c_vintage']) if 'c_vintage' in ds else None
        # dataset endpoint URL
        for distribution in ds.get('distribution') or []:
            if distribution.get('format') == 'API':
                self.endpoint = distribution['accessURL']
        # short ID
        self.id = self.endpoint.replace(
            'http://api.census.gov/data/', ''
        ).replace(
            'https://api.census.gov/data/', ''
        )
        # list of valid geographies
        r = await afetch(ds['c_geographyLink'], self.session)
        self.geographies_ = r.json()
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
        r = await afetch(ds['c_variablesLink'], self.session)
        self.variables_ = r.json().get('variables', {})
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
        self.tags = []
        if 'c_tagsLink' in ds:
            # list of tags
            # Note: as of 2021-04-12, these are all broken
            try:
                r = await afetch(ds['c_tagsLink'], self.session)
                self.tags = r.json().get('tags', [])
            except httpx.HTTPStatusError as e:
                _logger.warning(f"Unable to fetch {ds['c_tagsLink']}: {e}")
        
        # list of groups
        self.groups_ = {}
        if 'c_groupsLink' in ds:
            # list of groups
            r = await afetch(ds['c_groupsLink'], self.session)
            data = r.json() or {}
            for row in data.get('groups', []):
                self.groups_[row['name']] = {
                    'descriptions': row['description']
                }
                if row['variables']:
                    r = await afetch(row['variables'], self.session)
                    self.groups_[row['name']]['variables'] = r.json().get(
                        'variables', {}).keys()
            self.groups = pd.DataFrame(self.groups_).T
        return self

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

    async def __call__(self, fields, geo_for, *, geo_in=None,
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

        r = await afetch(self.endpoint, self.session, params=params)
        response_json = r.json()
        ret = pd.DataFrame(data=response_json[1:], columns=response_json[0])
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
