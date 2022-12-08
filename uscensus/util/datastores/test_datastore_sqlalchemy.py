from httpx_caching._models import Headers, Response

from ...util.datastores.sqlalchemy import SqlAlchemyDataStore


def test_SqlAlchemyCache():
    cache = SqlAlchemyDataStore(
        'sqlite://', table='test'
    )
    assert cache.table.name == 'test'

    row = cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0

    assert cache.get('empty') == (None, None)

    cache.set('empty', Response(200, Headers(), False), {}, b'')
    resp, vary = cache.get('empty')
    assert vary == {}
    assert resp.extensions == {}
    assert resp.headers == Headers()
    assert resp.status_code == 200

    row = cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 1

    cache.delete('empty')
    assert cache.get('empty') == (None, None)

    row = cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0
