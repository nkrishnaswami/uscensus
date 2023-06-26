import pytest

from uscensus.util.datastores.nop import AsyncNopDataStore, SyncNopDataStore


@pytest.mark.asyncio()
async def test_AsyncNopDatastore():
    nc = AsyncNopDataStore()
    doc, vary = await nc.aget('test')
    assert doc is None
    assert vary is None


def test_SyncNopDatastore():
    nc = SyncNopDataStore()
    doc, vary = nc.get('test')
    assert doc is None
    assert vary is None
