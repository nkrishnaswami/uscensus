from __future__ import print_function

from census.util import DBAPIQueryHelper

import datetime as dt
try:
    import ujson as json
except ImportError:
    import json


class DBAPICache(object):
    def __init__(self,
                 dbapi, table='urlcache',
                 timeout=dt.timedelta(days=7),
                 *dbargs, **dbkwargs):
        self.dbapi = dbapi
        self.timeout = timeout
        self.conn = self.dbapi.connect(*dbargs, **dbkwargs)
        self.query = DBAPIQueryHelper(self.dbapi, self.conn)
        self.table = table
        self.query(
            'CREATE TABLE IF NOT EXISTS ' + self.table +
            ' (url TEXT, data BLOB, date TIMESTAMP, ' +
            'PRIMARY KEY(url ASC))')

    def get(self, url):
        """Check if the URL is in the cache.
        If it is not, return None.
        If it is but has expired, delete the entry and return None.
        If it is and has not expired, parse the JSON and return it.
        """
        cur = self.query(
            'SELECT data,date FROM ' + self.table +
            ' WHERE url={url}',
            url=url)
        row = cur.fetchone()
        if row:
            if dt.datetime.now()-row[1] < self.timeout:
                return json.loads(row[0])
            else:
                cur = self.query(
                    'DELETE FROM ' + self.table +
                    ' WHERE url={url}',
                    url=url)

    def put(self, url, doc):
        """Insert the document for the URL into the cache.
        Parse the document as JSON and return it.
        """
        self.query(
            'INSERT INTO ' + self.table +
            ' VALUES ({url},{data},{date})',
            url=url,
            data=doc,
            date=dt.datetime.now(),
        )
        return json.loads(doc)
