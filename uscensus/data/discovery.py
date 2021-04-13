import logging

from ..data.model import CensusDataEndpoint
from ..util.errors import CensusError
from ..util.webcache import fetchjson
from ..util.textindex import SqliteFts5Index


_logger = logging.getLogger(__name__)


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
            url = f'https://api.census.gov/data/{vintage}.json'
        else:
            url = 'https://api.census.gov/data.json'
        _logger.debug("Fetching root metadata")
        resp = fetchjson(url, cache, session)
        datasets = resp.get('dataset')
        if not datasets:
            raise CensusError("Unable to identify datasets from API " +
                              " discovery endpoint")

        _logger.debug("Fetching per-API metadata")
        self.variableindex = SqliteFts5Index('Variable', 'variables')
        with self.variableindex:
            for ds in datasets:
                try:
                    api = CensusDataEndpoint(
                        key, ds, cache, session,
                        self.variableindex)
                    # TODO: add more indexing; groups, hier by
                    #       dataset, geo schemes, by vintage, etc
                    self.apis[api.id] = api
                    _logger.debug('Finished processing metadata for API: ' +
                                  f'{api.id}')
                except Exception as e:
                    _logger.warn('Error processing metadata; skipping API ' +
                                 f'{ds["title"]}', exc_info=e)
        _logger.debug("Indexing API metadata")
        self.index = SqliteFts5Index('API', 'apis')
        with self.index:
            self.index.add(
                {
                    'api_id': api.id,
                    'title': api.title,
                    'description': api.description,
                    'geographies': ' '.join(api.geographies['name']),
                    'concepts': ' '.join(api.concepts),
                    'keywords': ' '.join(api.keywords),
                    'tags': ' '.join(api.tags),
                    'variables': ' '.join(api.variables['label']),
                    'vintage': api.vintage
                }
                for api in self.apis.values()
            )
            _logger.debug("Done adding metadata")
        _logger.debug("Done committing metadata")

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
