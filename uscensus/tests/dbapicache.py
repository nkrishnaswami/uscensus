from __future__ import print_function, unicode_literals

from ..util.dbapicache import DBAPICache

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
    t_plus_1 = time + dt.timedelta(1)
    cache.touch('empty', t_plus_1)
    assert cache.get('empty') == ({}, t_plus_1)
    cache.delete('empty')
    assert cache.get('empty') == (None, None)
