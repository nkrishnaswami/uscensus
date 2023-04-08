import json
import logging

import httpx
from httpx_caching import CachingClient
import pytest

from uscensus.data.discovery import AsyncDiscoveryInterface, DiscoveryInterface
from uscensus.util.webcache import make_client
from uscensus.util.datastores import AsyncNopDataStore

_logger = logging.getLogger(__name__)


@pytest.fixture
def cache():
    return AsyncNopDataStore()


@pytest.mark.asyncio()
async def test_AsyncDiscoveryInterface(catalog, cache, tags, httpx_transport):
    cl = await AsyncDiscoveryInterface.create(
        '', make_client(cache=cache, transport=httpx_transport))
    _logger.info(f'APIs are {cl.datasets}')
    assert len(cl.datasets) == 1
    k, v = next(iter(cl.datasets.items()))
    _logger.info(f'first key is {k}')
    ds = catalog['dataset'][0]
    assert k == '/'.join([str(ds['c_vintage'])] + ds['c_dataset'])
    assert v.tags == tags['tags']


def test_DiscoveryInterface(catalog, cache, tags, httpx_transport):
    cl = DiscoveryInterface('', make_client(cache=cache, transport=httpx_transport))
    _logger.info(f'APIs are {cl.datasets}')
    assert len(cl.datasets) == 1
    k, v = next(iter(cl.datasets.items()))
    _logger.info(f'first key is {k}')
    ds = catalog['dataset'][0]
    assert k == '/'.join([str(ds['c_vintage'])] + ds['c_dataset'])
    assert v.tags == tags['tags']
