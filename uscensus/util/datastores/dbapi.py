import logging
from typing import Optional, Tuple
import zlib

from httpx_caching._models import Response
from httpx_caching._serializer import Serializer as ResponseSerializer

from ...util.datastores.datastore import DataStore
from ...util.dbapiqueryhelper import DBAPIQueryHelper


_logger = logging.getLogger(__name__)


class DBAPIDataStore(DataStore):
    """Persist HTTP httpx/responses to a database via DBAPI."""

    ASYNC_CAPABLE = False

    def __init__(self,
                 dbapi, table='urlcache',
                 *dbargs, **dbkwargs):
        """
        Cache for storing and retrieving web documents.

        Arguments:
          * dbapi: DBAPI module to use for caching.
          * table: name of table to use/create for cache.
          * dbargs: additional args for `dbapi.connect`.
          * dbkwargs: additional kwargs for `dbapi.connect`.
        """
        self.dbapi = dbapi
        self.table = table
        self.conn = None
        self.query = None

        _logger.debug('Opening DB connection')
        self.conn = self.dbapi.connect(*dbargs, **dbkwargs)
        self.conn.set_trace_callback(_logger.debug)
        self.query = DBAPIQueryHelper(self.dbapi, self.conn)
        try:
            self.query(f'SELECT COUNT(*) FROM {self.table} LIMIT 1')
        except self.dbapi.Error:
            self.query(
                f'CREATE TABLE IF NOT EXISTS {self.table}' +
                ' (key TEXT, data BLOB, ' +
                'PRIMARY key(key ASC))')

    def get(self, key: str) -> Tuple[Optional[Response],
                                     Optional[dict]]:
        """Check if the key is in the cache.
        If it is not, return the pair (None, None).
        If it is, return its data.

        Arguments:
          * key: document key.
        """
        _logger.debug('Getting key={key}')
        cur = self.query(
            f'SELECT data FROM {self.table}' +
            ' WHERE key={key}',
            key=key)
        row = cur.fetchone()
        if row:
            _logger.debug('Hit: key={key}')
            return ResponseSerializer().loads(zlib.decompress(row[0]))
        _logger.debug('Miss')
        return None, None

    def set(self,
            key: str,
            response: Response,
            vary_header_dict: dict,
            response_body: bytes) -> None:
        _logger.debug(f'Inserting: response={bool(response)} key={key}')
        self.query(
            f'DELETE FROM {self.table}' +
            ' WHERE key={key}',
            key=key)
        self.query(
            f'INSERT INTO {self.table}' +
            ' VALUES ({key},{data})',
            key=key,
            data=zlib.compress(ResponseSerializer().dumps(
                response, vary_header_dict, response_body)))
        self.conn.commit()

    def delete(self, key: str) -> None:
        """Remove a key from the cache."""
        _logger.debug(f'Deleting: key={key}')
        self.query(
            f'DELETE FROM {self.table}' +
            ' WHERE key={key}',
            key=key)
        self.conn.commit()

    def close(self) -> None:
        pass
