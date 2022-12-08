import datetime
import logging
import zlib
from typing import Optional, Tuple

from httpx_caching._models import Response
from httpx_caching._serializer import Serializer as ResponseSerializer
import sqlalchemy

from ...util.datastores.datastore import DataStore


_logger = logging.getLogger(__name__)


class SqlAlchemyDataStore(DataStore):
    """Store HTTP requests/responses to a database via SQLAlchemy."""

    def __init__(self,
                 connstr, table='urlcache',
                 timeout=datetime.timedelta(days=7)):
        """
        Arguments:
          * connstr: sqlalchemy connection string.
          * table: name of table to use/create for response storage.
          * timeout: period during which to avoid checking document staleness.
        """
        self.connstr = connstr
        self.timeout = timeout
        self.engine = sqlalchemy.create_engine(
            self.connstr, logging_name=__name__)
        self.txn = None
        md = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table(
            table,
            md,
            sqlalchemy.Column('key',
                              sqlalchemy.String,
                              primary_key=True),
            sqlalchemy.Column('data',
                              sqlalchemy.BLOB,
                              nullable=False),
        )
        with self.engine.connect() as conn:
            md.create_all(bind=conn)

    def get(self, key: str) -> Tuple[Optional[Response], Optional[dict]]:
        """Check if the key is in the data store.
        If it is not, return the tuple (None, None).
        If it is, return its response and metadata.

        Arguments:
          * key: document key.
        """
        _logger.debug('get: key={key}')
        with self.engine.connect() as conn:
            cur = conn.execute(
                sqlalchemy.select(
                    [self.table.c.data],
                    from_obj=self.table,
                ).where(
                    self.table.c.key == sqlalchemy.bindparam('key')
                ),
                key=key
            )
            row = cur.fetchone()
            if row:
                _logger.debug('Hit')
                return ResponseSerializer().loads(zlib.decompress(
                    row[self.table.c.data]))
            _logger.debug('Miss')
        return None, None

    def delete(self, key: str) -> None:
        """Remove a key from the data store."""
        _logger.debug(f'delete: key={key}')
        with self.engine.connect() as conn:
            conn.execute(
                self.table.delete().where(
                    self.table.c.key == sqlalchemy.bindparam('key')
                ),
                key=key,
            )

    def set(self,
            key: str,
            response: Response,
            vary_header_dict: dict,
            response_body: bytes) -> None:
        """Insert the document for the key into the data store.
        Returns the input response.
        """
        _logger.debug(f'set: key={key}')
        with self.engine.begin() as conn:
            conn.execute(
                self.table.delete().where(self.table.c.key == key))
            conn.execute(
                self.table.insert().values({
                    self.table.c.key: key,
                    self.table.c.data: zlib.compress(
                        ResponseSerializer().dumps(
                            response,
                            vary_header_dict,
                            response_body)
                    )}))

    def close(self) -> None:
        pass
