from census.nopcache import NopCache
from census.util import fetchjson

from collections import defaultdict
import pandas as pd

class CensusDataAPI(object):
    """
    A single census endpoint, with metadata about queryable variables and geography
    """
    def __init__(self, key, ds, cache):
        """
        key: user's API key
        ds: census dataset descriptor metadata
        """
        self.key=key                        # API key
        self.title=ds['title']              # title
        self.description=ds['description']  # long description
        self.__doc__=self.description
        self.dataset=tuple(ds['c_dataset']) # dataset descriptors (general to specific)
        self.vintage=ds.get('c_vintage')    # vintage, if dataset is year-specific
        self.endpoint=ds['distribution'][0]['accessURL'] # API endpoint URL
        self.geographies=fetchjson(ds['c_geographyLink'], cache) # list of valid geographies
        self.variables=fetchjson(ds['c_variablesLink'], cache)['variables'] # list of valid variables
        self.concepts=defaultdict(dict) # concepts linking variables
        for var,desc in self.variables.items():
            concept=desc.get('concept')
            if concept:
                self.concepts[concept][var]=desc
        self.keyword=ds.get('keyword',[]) # list of keywords
        if 'c_tagsLink' in ds:
            self.tags=fetchjson(ds['c_tagsLink'], cache)['tags'] # list of tags
        else:
            self.tags=[]
    @staticmethod
    def _geo2str(geo):
        return ' '.join('{}:{}'.format(k,v) for k,v in geo.items())
    def __call__(self, fields, geo_for, geo_in=None, cache=NopCache()):
        """
        Special method to make API object invocable.
        geo_* fields must be given as dictionaries, eg:
        `{'county': '*'}`
        """
        params={
            'get': ','.join(fields),
            'key': self.key,
            'for': self._geo2str(geo_for),
        }
        if geo_in:
            params['in']=self._geo2str(geo_in)

        j=fetchjson(self.endpoint, cache, params=params)
        ret=pd.DataFrame(data=j[1:], columns=j[0])
        for field in fields:
            if self.variables[field].get('predicateType') == 'int':
                ret[field]=pd.to_numeric(ret[field])
        return ret
    def __repr__(self):
        return self.title
