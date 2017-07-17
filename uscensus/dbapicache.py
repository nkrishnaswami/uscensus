from __future__ import print_function, unicode_literals

from uscensus.util import DBAPIQueryHelper

import datetime as dt
import dateutil.parser
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
        self.table = table
        self.conn = None
        self.query = None
        self.open(*dbargs, **dbkwargs)

    def open(self, *dbargs, **dbkwargs):
        """Open the DB connection for caching."""
        if self.conn:
            raise RuntimeError('Re-opening open DB connection')
        self.conn = self.dbapi.connect(*dbargs, **dbkwargs)
        self.query = DBAPIQueryHelper(self.dbapi, self.conn)
        self.query(
            'CREATE TABLE IF NOT EXISTS ' + self.table +
            ' (url TEXT, data BLOB, date VARCHAR(28), ' +
            'PRIMARY KEY(url ASC))')

    def close(self):
        """Close the DB connection."""
        self.conn.close()
        self.conn = None
        self.query = None

    def get(self, url):
        """Check if the URL is in the cache.
        If it is not, return the pair (None, None).
        If it is, parse the JSON and return it with the date retrieved.
        """
        cur = self.query(
            'SELECT data,date FROM ' + self.table +
            ' WHERE url={url}',
            url=url)
        row = cur.fetchone()
        if row:
            # if dt.datetime.now()-row[1] < self.timeout:
            return json.loads(row[0]), dateutil.parser.parse(row[1])
        return None, None

    def delete(self, url):
        """Remove a URL from the cache."""
        cur = self.query(
            'DELETE FROM ' + self.table +
            ' WHERE url={url}',
            url=url)
        cur

    def touch(self, url, date):
        """Update the last update timestamp for a URL in the cache."""
        cur = self.query(
            'UPDATE ' + self.table +
            ' SET date={date}'
            ' WHERE url={url}',
            url=url, date=date)
        cur

    def put(self, url, doc, date):
        """Insert the document for the URL into the cache.
        Parse the document as JSON and return it.
        """
        self.query(
            'INSERT INTO ' + self.table +
            ' VALUES ({url},{data},{date})',
            url=url,
            data=doc,
            date=date.isoformat(),
        )
        return json.loads(doc)
