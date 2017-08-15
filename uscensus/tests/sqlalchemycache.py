from __future__ import print_function
from __future__ import unicode_literals

from ..util.sqlalchemycache import SqlAlchemyCache

import datetime as dt


def SqlAlchemyCache_test():
    cache = SqlAlchemyCache(
        'sqlite://', table='test',
        timeout=dt.timedelta(seconds=1)
    )
    assert cache.timeout == dt.timedelta(seconds=1)
    assert cache.table.name == 'test'
    row = cache.engine.execute('SELECT COUNT(*) FROM test').fetchone()
    assert row[0] == 0
    time = dt.datetime.now()
    assert cache.put('empty', '{}', time) == {}
    assert cache.get('empty') == ({}, time)
    t_plus_1 = time + dt.timedelta(1)
    cache.touch('empty', t_plus_1)
    assert cache.get('empty') == ({}, t_plus_1)
    cache.delete('empty')
    assert cache.get('empty') == (None, None)
