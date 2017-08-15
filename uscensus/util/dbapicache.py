from __future__ import print_function
from __future__ import unicode_literals

from ..util.dbapiqueryhelper import DBAPIQueryHelper

import datetime as dt
import dateutil.parser
try:
    import ujson as json
except ImportError:
    import json
import logging


class DBAPICache(object):
    """Cache HTTP requests/responses to a database via DBAPI."""

    def __init__(self,
                 dbapi, table='urlcache',
                 timeout=dt.timedelta(days=7),
                 *dbargs, **dbkwargs):
        """
        Cache for storing and retrieving web documents.

        Arguments:
          * dbapi: DBAPI module to use for caching.
          * table: name of table to use/create for cache.
          * timeout: period during which to avoid checking document staleness.
          * dbargs: additional args for `dbapi.connect`.
          * dbkwargs: additional kwargs for `dbapi.connect`.
        """
        self.dbapi = dbapi
        self.timeout = timeout
        self.table = table
        self.conn = None
        self.query = None
        self.open(*dbargs, **dbkwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            logging.warn("Error in cache context; rolling back changes")
            self.conn.rollback()
        else:
            logging.info("Committing cache updates to DB")
            self.conn.commit()
        self.close()

    def open(self, *dbargs, **dbkwargs):
        """Open the DB connection for caching."""
        logging.debug('Opening DB connection')
        if self.conn:
            raise RuntimeError('Re-opening open DB connection')
        self.conn = self.dbapi.connect(*dbargs, **dbkwargs)
        self.query = DBAPIQueryHelper(self.dbapi, self.conn)
        try:
            self.query('SELECT COUNT(*) FROM ' + self.table + ' LIMIT 1')
        except self.dbapi.Error:
            self.query(
                'CREATE TABLE IF NOT EXISTS ' + self.table +
                ' (url TEXT, data BLOB, date VARCHAR(28), ' +
                'PRIMARY KEY(url ASC))')

    def close(self):
        """Close the DB connection."""
        logging.debug('Closing DB connection')
        self.conn.close()
        self.conn = None
        self.query = None

    def get(self, url):
        """Check if the URL is in the cache.
        If it is not, return the pair (None, None).
        If it is, parse the JSON and return it with the date retrieved.

        Arguments:
          * url: document key.
        """
        logging.debug('Getting url={}'.format(url))
        cur = self.query(
            'SELECT data,date FROM ' + self.table +
            ' WHERE url={url}',
            url=url)
        row = cur.fetchone()
        if row:
            logging.debug('Hit: date={}'.format(row[1]))
            return json.loads(row[0]), dateutil.parser.parse(row[1])
        logging.debug('Miss')
        return None, None

    def delete(self, url):
        """Remove a URL from the cache."""
        logging.debug('Deleting: url={}'.format(url))
        cur = self.query(
            'DELETE FROM ' + self.table +
            ' WHERE url={url}',
            url=url)
        cur

    def touch(self, url, date):
        """Update the last update timestamp for a URL in the cache."""
        logging.debug('Updating timestamp: url={}'.format(url))
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
        logging.debug('Inserting: doc={} url={}'.format(not not doc, url))
        self.query(
            'INSERT INTO ' + self.table +
            ' VALUES ({url},{data},{date})',
            url=url,
            data=doc,
            date=date.isoformat(),
        )
        return json.loads(doc)
