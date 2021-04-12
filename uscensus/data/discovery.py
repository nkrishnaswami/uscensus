from ..data.model import CensusDataEndpoint
from ..util.errors import CensusError
from ..util.webcache import fetchjson
from ..util.textindex import SqliteFts5Index


class DiscoveryInterface(object):
    """Discover and bind census APIs.

    TODO: Move the functionality into DiscoveryInterface, and make the
    constructor into static method(s).

    """
    def __init__(self,
                 key,
                 cache,
                 session=None,
                 vintage=None):
        """Load and wrap census APIs.

        Prefers cached metadata if present and not stale, otherwise
        queries server.

        Arguments:
          * key: Census API key
          * cache: cache in which to fetch/store metadata.
          * session: requests session to use for calling API.
          * vintage: discovery only data sets for this vintage, if present.
        """

        self.apis = {}
        if vintage:
            url = 'https://api.census.gov/data/{}.json'.format(vintage)
        else:
            url = 'https://api.census.gov/data.json'
        resp = fetchjson(url, cache, session)
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API " +
                              " discovery endpoint")
        self.variableindex = SqliteFts5Index('Variable', 'variables', 'idx.db')
        with self.variableindex:
            for ds in datasets:
                try:
                    api = CensusDataEndpoint(
                        key, ds, cache, session,
                        self.variableindex)
                    # todo: add more indexing; hier by dataset, by vintage, etc
                    self.apis[api.id] = api
                except Exception as e:
                    print("Error processing metadata; skipping API:", ds)
                    print(type(e), e)
                    print()
        print("Indexing metadata")
        self.index = SqliteFts5Index('API', 'apis', 'idx.db')
        with self.index:
            self.index.add(
                {'api_id': api.id,
                 'title': api.title,
                 'description': api.description,
                 'geography': api.geographies['name'].values.tolist(),
                 'concept': api.concepts,
                 'keyword': api.keyword,
                 'tag': api.tags,
                 'vintage': api.vintage,
                 }
                for api in self.apis.values()
            )
            print("Done adding metadata")
        print("Done committing metadata")

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
        if query.find(':') < 0:
            query = 'title: ' + query
        return {self[hit['api_id']]: hit['description']
                for hit in self.index.query(query)}

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
