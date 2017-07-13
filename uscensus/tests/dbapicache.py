from uscensus.dbapicache import DBAPICache

import datetime as dt
import sqlite3


def DBAPICache_test():
    cache = DBAPICache(
        sqlite3, table='test',
        timeout=dt.timedelta(seconds=1),
        database=':memory:',
    )
    assert cache.dbapi == sqlite3
    assert cache.timeout == dt.timedelta(seconds=1)
    assert cache.table == 'test'
    row = cache.conn.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0
    time = dt.datetime.now()
    assert cache.put('empty', '{}', time) == {}
    assert cache.get('empty') == ({}, time)
