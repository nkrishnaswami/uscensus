from uscensus.dbapicache import DBAPICache

import datetime as dt
import sqlite3


def DBAPICache_test():
    cache = DBAPICache(
        sqlite3, table='test',
        timeout=dt.timedelta(seconds=1),
        database=':memory:',
        detect_types=sqlite3.PARSE_DECLTYPES,
    )
    assert cache.dbapi == sqlite3
    assert cache.timeout == dt.timedelta(seconds=1)
    assert cache.table == 'test'
    row = cache.conn.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0
    assert cache.put('empty', '{}') == {}
    assert cache.get('empty') == {}
