import logging
import time

import httpx
from httpx_caching import SyncCachingTransport

from ..util.errors import CensusError


_logger = logging.getLogger(__name__)


def fetch(
        url: str,
        session: httpx.Client,
        *,
        retries: int = 3,
        **kwargs
) -> httpx.Response:
    """Caching wrapper around httpx to get a URL, check for
    errors, and return the pickled reponse.

    The document will be retrieved and stored in the cached using the
    DataStore/Cache specified for the httpx CachingClient
    Session.

    Arguments:
      * url: URL from which to fetch JSON resonse.
      * session: caching httpx.Client for making API calls.
      * retries: number of times to retry failed GETs.
      * kwargs: additional arguments to `httpx.get`

    Exceptions:
      * httpx.HTTPError on HTTP failure.
      * ValueError on JSON parse failure.

    """

    if not isinstance(session._transport, SyncCachingTransport):
        raise CensusError('Caching not enabled in httpx client')

    req = httpx.Request('GET', url, **kwargs)

    # Requests fail transiently sometimes. We retry with backoff to
    # handle this.
    for retry in range(retries):
        _logger.debug(f'Trying: attempt {retry + 1}/{retries}: {req.url}')
        r = None
        try:
            r = session.send(req)
        except httpx.HTTPError as e:
            if retry < retries - 1:
                # Log and drop the exception if we will retry the
                # request.
                _logger.exception(e)
            else:
                # Otherwise let it percolate.
                raise
        if r and r.status_code < 400:
            break
        time.sleep(3**retry)

    # If we get here, r is not None.
    assert r is not None
    if r.extensions.get('from_cache'):
        _logger.debug('Cache hit')
    else:
        _logger.debug('Cache miss')

    r.raise_for_status()
    return r
