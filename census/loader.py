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
        """Load and wrap census APIs.

        Prefers cached metadata if present and not stale, otherwise
        queries server.

        Arguments:
          * key: Census API key
          * cache: cache in which to fetch/store metadata
        """

        self.index = Index()
        self.apis = {}
        resp = fetchjson('http://api.census.gov/data.json', cache)
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API " +
                              " discovery endpoint")
        for ds in datasets:
            try:
                api = CensusDataAPI(key, ds, cache)
                api_id = api.endpoint.replace(
                    'http://api.census.gov/data/',
                    '')
                # todo: add mode indexing; hier by dataset, by vintage, etc
                self.apis[api_id] = api
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
        """Find a list of API objects matching the index query.
        Index queries default to searching API titles, but may also
        search

            * description: long description of an API
            * variables: variables to return from query
            * geographies: either variables to return from or to
              constrain a query
            * concepts: groupings of variables
            * keywords
            * tags

        by prefixing an individual term or groups of terms in parentheses
        by the field name and a colon.

        Elaborate queries can be constructed using parenthesized
        subqueries, ANDs, and ORs.
        """

        return [self[nid] for nid in self.index.query(query)]

    def __getitem__(self, api_id):
        """Return an identifier by API ID.

        Arguments:
          * api_id: the part of its endpoint name without the shared
            census API URL prefix.
        """

        return self.apis.get(api_id)

    def __repr__(self):
        """The readable string for an Loader is that of its `apis`
        dictionary.
        """

        return repr(self.apis)
