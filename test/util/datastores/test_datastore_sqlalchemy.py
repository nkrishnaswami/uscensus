import pytest
import sqlalchemy
from httpx_caching._models import Headers, Response

from uscensus.util.datastores.sqlalchemy import (
    AsyncSqlAlchemyDataStore,
    SyncSqlAlchemyDataStore,
)


@pytest.mark.asyncio
async def test_AsyncSqlAlchemyCache():
    cache = await AsyncSqlAlchemyDataStore.create(
        'sqlite+aiosqlite://', table_name='test',
    )
    assert cache.table.name == 'test'

    async with cache.aengine.connect() as conn:
        row = (await conn.execute(sqlalchemy.text('SELECT COUNT(*) FROM test'))).fetchone()
        assert row[0] == 0

        assert (await cache.aget('empty')) == (None, None)

        await cache.aset('empty', Response(200, Headers(), False), {}, b'')
        resp, vary = await cache.aget('empty')
        assert vary == {}
        assert resp.extensions == {}
        assert resp.headers == Headers()
        assert resp.status_code == 200

        row = (await conn.execute(sqlalchemy.text('SELECT COUNT(*) FROM test'))).fetchone()
        assert row[0] == 1

        await cache.adelete('empty')
        assert await cache.aget('empty') == (None, None)

        row = (await conn.execute(sqlalchemy.text('SELECT COUNT(*) FROM test'))).fetchone()
        assert row[0] == 0


def test_SqlAlchemyCache_sync():
    cache = SyncSqlAlchemyDataStore(
        'sqlite://', table_name='test',
    )
    assert cache.table.name == 'test'

    with cache.engine.connect() as conn:
        row = conn.execute(sqlalchemy.text(
            'SELECT COUNT(*) FROM test')).fetchone()
        assert row[0] == 0

        assert cache.get('empty') == (None, None)

        cache.set('empty', Response(200, Headers(), False), {}, b'')
        resp, vary = cache.get('empty')
        assert vary == {}
        assert resp.extensions == {}
        assert resp.headers == Headers()
        assert resp.status_code == 200

        row = conn.execute(sqlalchemy.text(
            'SELECT COUNT(*) FROM test')).fetchone()
        assert row[0] == 1

        cache.delete('empty')
        assert cache.get('empty') == (None, None)

        row = conn.execute(sqlalchemy.text(
            'SELECT COUNT(*) FROM test')).fetchone()
        assert row[0] == 0
