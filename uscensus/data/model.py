from __future__ import print_function
from __future__ import unicode_literals

#from ..data.index import Index, VariableSchemaFields
from ..util.nopcache import NopCache
from ..util.webcache import fetchjson

from collections import namedtuple
import pandas as pd


class CensusDataEndpoint(object):
    """A single census endpoint, with metadata about queryable variables
    and geography
    """

    def __init__(self, key, ds, cache, session, variableindex):
        """Initialize a Census API endpoint wrapper.

        Arguments:
          * key: user's API key.
          * ds: census dataset descriptor metadata.
          * cache: cache in which to look up/store metadata.
          * session: requests.Session to use for retrieving data.
          * variableindex: the Index in which to store variable data
        """
        self.key = key                         # API key
        self.session = session                 # requests.Session
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
        self.id = self.endpoint.replace(
            'https://api.census.gov/data/',
            '')
        # list of valid geographies
        self.geographies_ = (
            fetchjson(ds['c_geographyLink'], cache,
                      self.session) or
            {}
        )
        self.geographies = None
        for scheme in self.geographies_:
            tmp = pd.DataFrame(
                self.geographies_[scheme],
                columns=[
                    'scheme',
                    'name',
                    'predicate_type',
                    'referenceDate',
                    'requires',
                    'optionalWithWCFor',
                    'wildcard'])
            tmp['scheme'] = scheme
            if self.geographies is not None:
                self.geographies.append(tmp)
            else:
                self.geographies = tmp

        # list of valid variables
        self.variables_ = (
            fetchjson(ds['c_variablesLink'], cache,
                      self.session)['variables'] or
            {}
        )
        self.variables = pd.DataFrame(self.variables_).T

        # index the variables
        self.variableindex = variableindex
        self.variableindex.add(self._generateVariableRows())

        # keep track of concepts for indexing
        self.concepts = list(sorted(set(self.variables['concept'].dropna())))

        # list of keywords
        self.keyword = ds.get('keyword', [])
        # list of tags
        if 'c_tagsLink' in ds:
            # list of tags
            self.tags = fetchjson(ds['c_tagsLink'], cache,
                                  self.session)['tags']
        else:
            self.tags = []

        # list of groups
        if 'c_groupsLink' in ds:
            # list of groups
            self.groups = {row['name']: row['description'] for row in
                           fetchjson(ds['c_groupsLink'], cache,
                                     self.session)['groups']}
        else:
            self.groups = []

    def searchVariables(self, query):
        """Return for variables matching a query string.

        Keywords are `variable` (ID), `label` (name) and `concept`
        (grouping of variables).

        """
        return pd.DataFrame(
            self.variableindex.query(
                query,
                api_id=self.id),
            columns=[
                'score', 'api_id', 'variable', 'group',
                'concept', 'label', 'predicate_type'
            ]
        ).drop('api_id', axis=1)

    @staticmethod
    def _geo2str(geo):
        """Format geography dict as string for query"""
        return ' '.join('{}:{}'.format(k, v) for k, v in geo.items())

    def __call__(self, fields, geo_for, geo_in=None, cache=NopCache()):
        """Special method to make API object invocable.

        Arguments:
          * fields: list of variables to return.
          * geo_* fields must be given as dictionaries, eg:
            `{'county': '*'}`
          * cache: cache in which to store results. Not cached by default.
        """
        params = {
            'get': ','.join(fields),
            'key': self.key,
            'for': self._geo2str(geo_for),
        }
        if geo_in:
            params['in'] = self._geo2str(geo_in)

        j = fetchjson(self.endpoint, cache, self.session, params=params)
        ret = pd.DataFrame(data=j[1:], columns=j[0])
        for field in fields:
            if self.variables_[field].get('predicateType') == 'int':
                ret[field] = pd.to_numeric(ret[field])
        return ret

    def _generateVariableRows(self):
        for k, v in self.variables_.items():
            yield dict(
                api_id=self.id,
                variable=k,
                group=v.get('group', ''),
                label=v.get('label', ''),
                concept=v.get('concept', ''),
                predicate_type=v.get('predicateType')
            )

    def __repr__(self):
        """Represent API endpoint by its title"""
        return self.title
