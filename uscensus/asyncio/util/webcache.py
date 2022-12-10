import asyncio
import logging

import httpx
from httpx_caching import AsyncCachingTransport

from ...util.errors import CensusError


_logger = logging.getLogger(__name__)
_counter = {'hit': 0, 'miss': 0}


async def afetch(
        url: str,
        session: httpx.AsyncClient,
        *,
        retries: int = 3,
        counter=_counter,
        **kwargs) -> httpx.Response:
    """Caching wrapper around httpx to get a URL, check for
    errors, and return the pickled reponse.

    The document will be retrieved and stored in the cached using the
    DataStore/Cache specified for the httpx CachingClient
    AsyncSession.

    Arguments:
      * url: URL from which to fetch JSON resonse.
      * session: caching httpx.AsyncClient for making API calls.
      * retries: number of times to retry failed GETs.
      * kwargs: additional arguments to `httpx.get`

    Exceptions:
      * httpx.HTTPError on HTTP failure.
      * ValueError on JSON parse failure.

    """

    if not isinstance(session._transport, AsyncCachingTransport):
        raise CensusError('Caching not enabled in httpx client')

    req = httpx.Request('GET', url, **kwargs)

    # Requests fail transiently sometimes. We retry with backoff to
    # handle this.
    for retry in range(retries):
        _logger.debug(f'Trying: attempt {retry + 1}/{retries}: {req.url}')
        r = None
        try:
            r = await session.send(req)
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
        await asyncio.sleep(3**retry)

    # If we get here, r is not None.
    assert r is not None
    if r.extensions.get('from_cache'):
        counter['hit'] += 1
    else:
        counter['miss'] += 1

    r.raise_for_status()
    return r
