from httpx_caching._models import Headers, Response

from uscensus.util.datastores.sqlalchemy import AsyncSqlAlchemyDataStore


async def test_AsyncSqlAlchemyCache():
    cache = AsyncSqlAlchemyDataStore(
        'sqlite://', table='test',
    )
    assert cache.table.name == 'test'

    row = await cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0

    assert cache.get('empty') == (None, None)

    cache.set('empty', Response(200, Headers(), False), {}, b'')
    resp, vary = await cache.aget('empty')
    assert vary == {}
    assert resp.extensions == {}
    assert resp.headers == Headers()
    assert resp.status_code == 200

    row = await cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 1

    await cache.adelete('empty')
    assert await cache.aget('empty') == (None, None)

    row = await cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0
