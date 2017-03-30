from __future__ import print_function
import sqlite3

from census.errors import CensusError
from census.index import Index
from census.model import CensusDataAPI
from census.util import fetchjson

class CensusLoader(object):
    """
    Discover and bind census APIs
    """
    def __init__(self, key, cache):
        self.index = Index()
        self.apis={}
        resp = fetchjson('http://api.census.gov/data.json', cache)
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API discovery endpoint")
        for ds in datasets:
            try:
                api=CensusDataAPI(key, ds, cache)
                api_id=api.endpoint.replace('http://api.census.gov/data/','')
                # todo: add mode indexing; hier by dataset, by vintage, etc
                self.apis[api_id]=api
            except Exception as e:
                print("Error processing metadata; skipping API:", ds)
                print(e)
                print()
        self.index.add(
            (api_id,
             api.title,
             api.description,
             ' '.join(api.variables or []),
             ' '.join(api.geographies or []),
             ' '.join(api.concepts),
             ' '.join(api.keyword),
             ' '.join(api.tags),
            ) for api_id, api in self.apis.items())
    def search(self, query):
        return [self[nid] for nid in self.index.query(query)]
    
    def __getitem__(self, api_id):
        return self.apis.get(api_id)
    def __repr__(self):
        return repr(self.apis)
        
