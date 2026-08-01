import asyncio
import logging
import time
from collections.abc import Sequence
from typing import Any, TypedDict, cast

import httpx
from httpx_caching import AsyncCachingTransport, CachingClient, SyncCachingTransport
from httpx_caching._heuristics import BaseHeuristic, ExpiresAfterHeuristic

from uscensus.util.datastores.datastore import AsyncDataStore, SyncDataStore
from uscensus.util.errors import CensusError

_logger = logging.getLogger(__name__)


def make_client(*,
                cache,
                key=None,
                heuristic=ExpiresAfterHeuristic(days=30),
                max_connections=10,
                sync=False,
                transport: httpx.AsyncBaseTransport | httpx.BaseTransport | None = None,
                ) -> httpx.AsyncClient | httpx.Client:
    """Create a caching httpx AsyncClient with the caller-specified
    datastore and optionally caching heuristic.

    """
    params = None
    if key:
        params = {'key': key}
    class HttpxClientArgs(TypedDict):
        follow_redirects: bool
        params: dict[str, Any] | None
        limits: httpx.Limits

    class CachingClientArgs(TypedDict):
        cacheable_status_codes: Sequence[int]
        heuristic: BaseHeuristic
        cache: SyncDataStore | AsyncDataStore

    client_args: HttpxClientArgs = {
        'follow_redirects': True,
        'params': params,
        'limits': httpx.Limits(max_connections=max_connections),
    }
    caching_client_args: CachingClientArgs = {
        'cacheable_status_codes': (200, 203, 300, 301, 302, 308),
        'heuristic': heuristic,
        'cache': cache,
    }
    if sync:
        if not isinstance(cache, SyncDataStore):
            raise TypeError('cache is not not a SyncDataStore')
        if transport:
            if not isinstance(transport, httpx.BaseTransport):
                raise TypeError('transport is not not a BaseTransport')
        return CachingClient(
            httpx.Client(**client_args, transport=cast(httpx.BaseTransport, transport)),
            **caching_client_args)
    if not isinstance(cache, AsyncDataStore):
        raise TypeError('cache is not not an AsyncDataStore')
    if transport:
        if not isinstance(transport, httpx.AsyncBaseTransport):
            raise TypeError('transport is not not an AsyncBaseTransport')
    return CachingClient(
        httpx.AsyncClient(**client_args, transport=cast(httpx.AsyncBaseTransport, transport)),
        **caching_client_args)


async def afetch(
        url: str,
        session: httpx.AsyncClient,
        *,
        retries: int = 3,
        **kwargs) -> httpx.Response:
    """Caching wrapper around httpx to get a URL, check for
    errors, and return the pickled reponse.

    The document will be retrieved and stored in the cached using the
    DataStore/Cache specified for the httpx CachingClient
    AsyncSession.

    Arguments:
    ---------
      * url: URL from which to fetch JSON resonse.
      * session: caching httpx.AsyncClient for making API calls.
      * retries: number of times to retry failed GETs.
      * kwargs: additional arguments to `httpx.get`

    Exceptions:
      * httpx.HTTPError on HTTP failure.
      * ValueError on JSON parse failure.

    """
    _logger.debug(f'Fetching: {url}')
    if isinstance(session._transport, SyncCachingTransport):  # noqa: SLF001
        raise CensusError('Async fetch with sync httpx client')
    if not isinstance(session._transport, AsyncCachingTransport):  # noqa: SLF001
        raise CensusError('Caching not enabled in httpx client')

    req = httpx.Request('GET', url, **kwargs)
    r = None
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

    # If we get here, r should not be None.
    if r is None:
        raise ValueError('HTTP response is None')
    if r.extensions.get('from_cache'):
        _logger.debug(f'Cache hit for {url}')
    else:
        _logger.debug(f'Cache miss for {url}')

    r.raise_for_status()
    return r


def fetch(
        url: str,
        session: httpx.Client,
        *,
        retries: int = 3,
        **kwargs) -> httpx.Response:
    """See `afetch` for description of arguments and behavior."""
    _logger.debug(f'Fetching: {url}')
    if isinstance(session._transport, AsyncCachingTransport):  # noqa: SLF001
        raise CensusError('Sync fetch with async httpx client')
    if not isinstance(session._transport, SyncCachingTransport):  # noqa: SLF001
        raise CensusError('Caching not enabled in httpx client')

    req = httpx.Request('GET', url, **kwargs)
    r = None
    # Requests fail transiently sometimes. We retry with backoff to
    # handle this.
    for retry in range(retries):
        _logger.debug(f'Trying: attempt {retry + 1}/{retries}: {req.url}')
        r = None
        try:
            r = session.send(req)
            if not r.is_success:
                _logger.error('HTTP request failed with status code %d: %s', r.status_code, r.text)
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

    # If we get here, r should not be None.
    if r is None:
        raise ValueError('HTTP response is None')
    if r.extensions.get('from_cache'):
        _logger.debug(f'Cache hit for {url}')
    else:
        _logger.debug(f'Cache miss for {url}')

    r.raise_for_status()
    return r
