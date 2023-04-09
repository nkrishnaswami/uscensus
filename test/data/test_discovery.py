import json
import logging

import httpx
import pytest

from uscensus.data.discovery import AsyncDiscoveryInterface, DiscoveryInterface

_logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_AsyncDiscoveryInterface(catalog, cache, tags, httpx_client_single):
    cl = await AsyncDiscoveryInterface.create('', httpx_client_single)
    _logger.info(f'APIs are {cl.datasets}')
    assert len(cl.datasets) == 1
    k, v = next(iter(cl.datasets.items()))
    _logger.info(f'first key is {k}')
    ds = catalog['dataset'][0]
    assert k == '/'.join([str(ds['c_vintage'])] + ds['c_dataset'])
    assert v.tags == tags['tags']


def test_DiscoveryInterface(catalog, cache, tags, httpx_client_single):
    cl = DiscoveryInterface('', httpx_client_single)
    _logger.info(f'APIs are {cl.datasets}')
    assert len(cl.datasets) == 1
    k, v = next(iter(cl.datasets.items()))
    _logger.info(f'first key is {k}')
    ds = catalog['dataset'][0]
    assert k == '/'.join([str(ds['c_vintage'])] + ds['c_dataset'])
    assert v.tags == tags['tags']
