from __future__ import print_function, unicode_literals

from uscensus.nopcache import NopCache
from uscensus.util import fetchjson

from collections import defaultdict
import pandas as pd


class CensusDataAPI(object):
    """A single census endpoint, with metadata about queryable variables
    and geography
    """

    def __init__(self, key, ds, cache, session):
        """Initialize a Census API endpoint wrapper.

        Arguments:
          * key: user's API key
          * ds: census dataset descriptor metadata
          * cache: cache in which to look up/store metadata
          * session: requests.Session to use for retrieving data
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
        self.geographies = fetchjson(ds['c_geographyLink'], cache,
                                     self.session)
        # list of valid variables
        self.variables = (
            fetchjson(ds['c_variablesLink'], cache,
                      self.session)['variables'])
        # concepts linking variables
        self.concepts = defaultdict(dict)
        for var, desc in self.variables.items():
            concept = desc.get('concept')
            if concept:
                self.concepts[concept][var] = desc
        # list of keywords
        self.keyword = ds.get('keyword', [])
        if 'c_tagsLink' in ds:
            # list of tags
            self.tags = fetchjson(ds['c_tagsLink'], cache,
                                  self.session)['tags']
        else:
            self.tags = []

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

    def __repr__(self):
        """Represent APi endpoint by its title"""

        return self.title
