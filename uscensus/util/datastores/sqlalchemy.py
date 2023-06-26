from __future__ import annotations

import logging
import zlib

import sqlalchemy
from httpx_caching._models import Response
from httpx_caching._serializer import Serializer as ResponseSerializer
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from uscensus.util.datastores.datastore import AsyncDataStore, SyncDataStore

_logger = logging.getLogger(__name__)


class AsyncSqlAlchemyDataStore(AsyncDataStore):
    """Async datastore for httpx_caching that backs to a database via
    SQLAlchemy.
    """

    connstr: str
    aengine: AsyncEngine
    md: sqlalchemy.MetaData
    table: sqlalchemy.Table
    _async_initialized: bool

    def __init__(self, connstr: str, table_name: str = 'urlcache') -> None:
        """Arguments:
        ---------
          * connstr: sqlalchemy connection string.
          * table: name of table to use/create for response storage.
        """
        _logger.debug(f'create: Instantiating async connection to {connstr}')
        self.connstr = connstr
        self.aengine = create_async_engine(self.connstr, logging_name=__name__)
        self.md = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table(
            table_name,
            self.md,
            sqlalchemy.Column('key',
                              sqlalchemy.String,
                              primary_key=True),
            sqlalchemy.Column('data',
                              sqlalchemy.BLOB,
                              nullable=False),
        )
        self._async_initialized = False

    async def _finish_async_init(self):
        if not self._async_initialized:
            async with self.aengine.connect() as conn:
                await conn.run_sync(self.md.create_all)
                self._async_initialized = True

    @classmethod
    async def create(cls, connstr: str, table_name: str = 'urlcache') -> AsyncSqlAlchemyDataStore:
        cache = cls(connstr, table_name)
        await cache._finish_async_init()
        return cache

    async def aget(self, key: str) -> tuple[Response | None,
                                            dict | None]:
        _logger.debug(f'aget: key={key}')
        await self._finish_async_init()
        async with self.aengine.connect() as conn:
            result = await conn.execute(
                sqlalchemy.select(self.table.c.data).where(
                    self.table.c.key == key))
            row = result.fetchone()
            if row:
                ret = ResponseSerializer().loads(
                    zlib.decompress(row.data))
                _logger.debug('Hit')
                result.close()
                return ret
            _logger.debug('Miss')
        return None, None

    async def adelete(self, key: str) -> None:
        _logger.debug(f'adelete: key={key}')
        await self._finish_async_init()
        async with self.aengine.connect() as conn:
            async with conn.begin():
                await conn.execute(
                    sqlalchemy.delete(self.table).where(
                        self.table.c.key == key))

    async def aset(self, key: str, response: Response,
                   vary_header_data: dict,
                   response_body: bytes) -> None:
        _logger.debug(f'aset: key={key}')
        await self._finish_async_init()
        async with self.aengine.connect() as conn:
            async with conn.begin():
                await conn.execute(
                    sqlalchemy.delete(self.table).where(
                        self.table.c.key == key))
                await conn.execute(
                    sqlalchemy.insert(self.table).values({
                        self.table.c.key: key,
                        self.table.c.data: zlib.compress(
                            ResponseSerializer().dumps(
                                response,
                                vary_header_data,
                                response_body)),
                    }))

    async def aclose(self):
        pass


class SyncSqlAlchemyDataStore(SyncDataStore):
    """Async datastore for httpx_caching that backs to a database via
    SQLAlchemy.
    """

    connstr: str
    engine: sqlalchemy.engine.Engine
    md: sqlalchemy.MetaData
    table: sqlalchemy.Table

    def __init__(self, connstr: str, table_name: str = 'urlcache') -> None:
        """Arguments:
        ---------
          * connstr: sqlalchemy connection string.
          * table: name of table to use/create for response storage.
        """
        _logger.debug(f'create: Instantiating async connection to {connstr}')
        self.connstr = connstr
        self.engine = sqlalchemy.create_engine(
            self.connstr, logging_name=__name__)
        self.md = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table(
            table_name,
            self.md,
            sqlalchemy.Column('key',
                              sqlalchemy.String,
                              primary_key=True),
            sqlalchemy.Column('data',
                              sqlalchemy.BLOB,
                              nullable=False),
        )
        self.md.create_all(self.engine)

    def get(self, key: str) -> tuple[Response | None,
                                     dict | None]:
        _logger.debug(f'aget: key={key}')
        with self.engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.select(self.table.c.data).where(
                    self.table.c.key == key))
            row = result.fetchone()
            if row:
                ret = ResponseSerializer().loads(
                    zlib.decompress(row.data))
                _logger.debug('Hit')
                result.close()
                return ret
            _logger.debug('Miss')
        return None, None

    def delete(self, key: str) -> None:
        _logger.debug(f'adelete: key={key}')
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    sqlalchemy.delete(self.table).where(
                        self.table.c.key == key))

    def set(self, key: str, response: Response,
            vary_header_data: dict,
            response_body: bytes) -> None:
        _logger.debug(f'aset: key={key}')
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    sqlalchemy.delete(self.table).where(
                        self.table.c.key == key))
                conn.execute(
                    sqlalchemy.insert(self.table).values(
                        {
                            self.table.c.key: key,
                            self.table.c.data: zlib.compress(
                                ResponseSerializer().dumps(
                                    response,
                                    vary_header_data,
                                    response_body)),
                        }))

    def close(self):
        pass
