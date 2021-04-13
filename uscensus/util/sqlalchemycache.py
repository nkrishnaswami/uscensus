import datetime as dt
import json
import logging

import sqlalchemy


_logger = logging.getLogger(__name__)


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
        self.engine = sqlalchemy.create_engine(
            self.connstr, logging_name=__name__)
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
        self.txn = None
        if exc_type:
            _logger.warn("Error in cache context; rolling back changes")
            if txn:
                txn.rollback()
        else:
            _logger.info("Committing cache updates to DB")
            if txn:
                txn.commit()

    def get(self, url):
        """Check if the URL is in the cache.
        If it is not, return the pair (None, None).
        If it is, parse the JSON and return it with the date retrieved.

        Arguments:
          * url: document key.
        """
        _logger.debug('Getting url={url}')
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
            _logger.debug(f'Hit: date={row[self.table.c.date]}')
            ret = (json.loads(row[self.table.c.data]),
                   row[self.table.c.date])
            cur.close()
            return ret
        _logger.debug('Miss')
        return None, None

    def delete(self, url):
        """Remove a URL from the cache."""
        _logger.debug(f'Deleting: url={url}')
        self.conn.execute(
            self.table.delete().where(
                self.table.c.url == sqlalchemy.bindparam('url')
            ),
            url=url,
        )

    def touch(self, url, date):
        """Update the last update timestamp for a URL in the cache."""
        _logger.debug('Updating timestamp: url={}'.format(url))
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
        _logger.debug(f'Inserting: doc={bool(doc)} url={url}')
        self.conn.execute(
            self.table.insert(),
            url=url,
            data=doc,
            date=date,
        )
        return json.loads(doc)
