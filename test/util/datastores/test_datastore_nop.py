import pytest

from uscensus.util.datastores.nop import AsyncNopDataStore


@pytest.mark.asyncio()
async def test_NopDatastore():
    nc = AsyncNopDataStore()
    doc, vary = await nc.aget('test')
    assert doc is None
    assert vary is None
