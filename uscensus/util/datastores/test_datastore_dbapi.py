from httpx_caching._models import Headers, Response

import sqlite3

from ...util.datastores.dbapi import DBAPIDataStore


def test_DBAPICache():
    cache = DBAPIDataStore(
        sqlite3, table='test',
        database=':memory:',
    )
    assert cache.dbapi == sqlite3
    assert cache.table == 'test'
    row = cache.conn.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0

    assert cache.get('empty') == (None, None)

    cache.set('empty', Response(200, Headers(), False), {}, b'')
    resp, vary = cache.get('empty')
    assert vary == {}
    assert resp.extensions == {}
    assert resp.headers == Headers()
    assert resp.status_code == 200

    row = cache.conn.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 1

    cache.delete('empty')
    assert cache.get('empty') == (None, None)

    row = cache.conn.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0
