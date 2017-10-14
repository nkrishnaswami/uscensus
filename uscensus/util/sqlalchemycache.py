from __future__ import print_function
from __future__ import unicode_literals
import datetime as dt
try:
    import ujson as json
except ImportError:
    import json
import logging
import sqlalchemy


class SqlAlchemyCache(object):
    """Cache HTTP requests/responses to a database via SQLAlchemy."""

    def __init__(self,
                 connstr, table='urlcache',
                 timeout=dt.timedelta(days=7)):
        """
        Cache for storing and retrieving web documents.

        Arguments:
          * connstr: sqlalchemy connection string.
          * table: name of table to use/create for cache.
          * timeout: period during which to avoid checking document staleness.
        """
        self.connstr = connstr
        self.timeout = timeout
        self.engine = sqlalchemy.create_engine(self.connstr)
        self.conn = self.engine.connect()
        self.txn = None
        with self.conn.begin():
            md = sqlalchemy.MetaData()
            self.table = sqlalchemy.Table(
                table,
                md,
                sqlalchemy.Column('url',
                                  sqlalchemy.String,
                                  primary_key=True),
                sqlalchemy.Column('data',
                                  sqlalchemy.String,
                                  nullable=False),
                sqlalchemy.Column('date',
                                  sqlalchemy.DateTime,
                                  nullable=False),
            )
            md.create_all(bind=self.conn)

    def __enter__(self):
        self.txn = self.conn.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        txn = self.txn
        if exc_type:
            logging.warn("Error in cache context; rolling back changes")
            if txn:
                txn.rollback()
        else:
            logging.info("Committing cache updates to DB")
            if txn:
                txn.commit()

    def get(self, url):
        """Check if the URL is in the cache.
        If it is not, return the pair (None, None).
        If it is, parse the JSON and return it with the date retrieved.

        Arguments:
          * url: document key.
        """
        logging.debug('Getting url={}'.format(url))
        cur = self.conn.execute(
            sqlalchemy.select(
                [self.table.c.data, self.table.c.date],
                from_obj=self.table,
            ).where(
                self.table.c.url == sqlalchemy.bindparam('url')
            ),
            url=url
        )
        row = cur.fetchone()
        if row:
            logging.debug('Hit: date={}'.format(row[self.table.c.date]))
            ret = (json.loads(row[self.table.c.data]),
                   row[self.table.c.date])
            cur.close()
            return ret
        logging.debug('Miss')
        return None, None

    def delete(self, url):
        """Remove a URL from the cache."""
        logging.debug('Deleting: url={}'.format(url))
        self.conn.execute(
            self.table.delete().where(
                self.table.c.url == sqlalchemy.bindparam('url')
            ),
            url=url,
        )

    def touch(self, url, date):
        """Update the last update timestamp for a URL in the cache."""
        logging.debug('Updating timestamp: url={}'.format(url))
        self.conn.execute(
            self.table.update().where(
                self.table.c.url == sqlalchemy.bindparam('tgturl'),
            ).values(
                date=sqlalchemy.bindparam('date')
            ),
            tgturl=url,
            date=date,
        )

    def put(self, url, doc, date):
        """Insert the document for the URL into the cache.
        Parse the document as JSON and return it.
        """
        logging.debug('Inserting: doc={} url={}'.format(not not doc, url))
        self.conn.execute(
            self.table.insert(),
            url=url,
            data=doc,
            date=date,
        )
        return json.loads(doc)
