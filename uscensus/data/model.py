from __future__ import print_function
from __future__ import unicode_literals

from ..data.index import Index, VariableSchemaFields
from ..util.nopcache import NopCache
from ..util.webcache import fetchjson

from collections import namedtuple
import pandas as pd


class CensusDataEndpoint(object):
    """A single census endpoint, with metadata about queryable variables
    and geography
    """

    def __init__(self, key, ds, cache, session):
        """Initialize a Census API endpoint wrapper.

        Arguments:
          * key: user's API key.
          * ds: census dataset descriptor metadata.
          * cache: cache in which to look up/store metadata.
          * session: requests.Session to use for retrieving data.
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
        self.endpoint = ds['distribution'][0]['accessURL']
        # list of valid geographies
        self.geographies = (
            fetchjson(ds['c_geographyLink'], cache,
                      self.session) or
            {}
        )
        # list of valid variables
        self.variables = (
            fetchjson(ds['c_variablesLink'], cache,
                      self.session)['variables'] or
            {}
        )

        # index the variables
        self.variableindex = Index(VariableSchemaFields)
        self.variableindex.add(self._generateVariableRows())

        # keep track of concepts for indexing
        concepts = set()
        for var, desc in self.variables.items():
            concept = desc.get('concept')
            if concept:
                concepts.add(concept)
        self.concepts = list(concepts)

        # list of keywords
        self.keyword = ds.get('keyword', [])
        # list of tags
        if 'c_tagsLink' in ds:
            # list of tags
            self.tags = fetchjson(ds['c_tagsLink'], cache,
                                  self.session)['tags']
        else:
            self.tags = []

    def searchVariables(self, query):
        """Look for variables matching a query string.

        Keywords are `variable`, `label` and `concept`.
        """
        return [hit for hit in self.variableindex.query(query)]

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
            if self.variables[field].get('predicateType') == 'int':
                ret[field] = pd.to_numeric(ret[field])
        return ret

    _Row = namedtuple('_Row', VariableSchemaFields)

    def _generateVariableRows(self):
        for k, v in self.variables.items():
            yield CensusDataEndpoint._Row(
                variable=k,
                label=v.get('label', ''),
                concept=v.get('concept', ''),
            )

    def __repr__(self):
        """Represent API endpoint by its title"""
        return self.title
