from __future__ import print_function
from __future__ import unicode_literals

from ..data.index import Index
from ..data.index import ApiSchemaFields
from ..data.model import CensusDataEndpoint
from ..util.errors import CensusError
from ..util.webcache import fetchjson
from ..util.ensuretext import ensuretext


class DiscoveryInterface(object):
    """Discover and bind census APIs.

    TODO: Move the functionality into DiscoveryInterface, and make the
    constructor into static method(s).

    """
    def __init__(self,
                 key,
                 cache,
                 session=None,
                 vintage=None,
                 index=True):
        """Load and wrap census APIs.

        Prefers cached metadata if present and not stale, otherwise
        queries server.

        Arguments:
          * key: Census API key
          * cache: cache in which to fetch/store metadata.
          * session: requests session to use for calling API.
          * vintage: discovery only data sets for this vintage, if present.
          * index: if true, index metadata for the `search` method.
        """

        self.apis = {}
        if vintage:
            url = 'http://api.census.gov/data/{}.json'.format(vintage)
        else:
            url = 'http://api.census.gov/data.json'
        resp = fetchjson(url, cache, session)
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API " +
                              " discovery endpoint")
        for ds in datasets:
            try:
                api = CensusDataEndpoint(key, ds, cache, session)
                api_id = api.endpoint.replace(
                    'http://api.census.gov/data/',
                    '')
                # todo: add more indexing; hier by dataset, by vintage, etc
                self.apis[api_id] = api
            except Exception as e:
                print("Error processing metadata; skipping API:", ds)
                print(type(e), e)
                print()
        if index:
            print("Indexing metadata")
            self.index = Index(ApiSchemaFields)
            self.index.add(
                (ensuretext(api_id),
                 ensuretext(api.title),
                 ensuretext(api.description),
                 ensuretext(api.variables),
                 ensuretext(
                     list(v.get('label', '')
                          for v in api.variables.values())),
                 ensuretext(api.geographies),
                 ensuretext(api.concepts),
                 ensuretext(api.keyword),
                 ensuretext(api.tags),
                 ensuretext(api.vintage),
                )
                for api_id, api in self.apis.items()
            )
            print("Done indexing metadata")
        else:
            self.index = None

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
        if not self.index:
            raise RuntimeError('Loader was created without an index;'
                               ' search is disabled')
        return [self[hit['api_id']] for hit in self.index.query(query)]

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
