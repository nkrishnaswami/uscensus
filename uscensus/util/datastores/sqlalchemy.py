from __future__ import annotations

import logging
import zlib

import sqlalchemy
from httpx_caching._models import Response
from httpx_caching._serializer import Serializer as ResponseSerializer
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from uscensus.util.datastores.datastore import AsyncDataStore

_logger = logging.getLogger(__name__)


class AsyncSqlAlchemyDataStore(AsyncDataStore):
    """Async datastore for httpx_caching that backs to a database via
    SQLAlchemy.
    """

    connstr: str
    table: sqlalchemy.Table
    engine: AsyncEngine

    def __init__(self, connstr: str, table_name: str = 'urlcache') -> None:
        """Arguments:
        ---------
          * connstr: sqlalchemy connection string.
          * table: name of table to use/create for response storage.
        """
        _logger.debug(f'create: Instantiating async connection to {connstr}')
        self.connstr = connstr
        self.engine = create_async_engine(self.connstr, logging_name=__name__)
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
        self._initialized = False

    async def _finish_init(self):
        if not self._initialized:
            async with self.engine.connect() as conn:
                await conn.run_sync(self.md.create_all)
                self._initialized = True

    @classmethod
    async def create(cls, connstr: str, table_name: str = 'urlcache') -> AsyncSqlAlchemyDataStore:
        cache = cls(connstr, table_name)
        await cache._finish_init()
        return cache

    async def aget(self, key: str) -> tuple[Response | None,
                                            dict | None]:
        _logger.debug(f'aget: key={key}')
        await self._finish_init()
        async with self.engine.connect() as conn:
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
        await self._finish_init()
        async with self.engine.connect() as conn:
            await conn.execute(
                sqlalchemy.delete(self.table).where(
                    self.table.c.key == key),
            )
            await conn.commit()

    async def aset(self, key: str, response: Response,
                   vary_header_data: dict,
                   response_body: bytes) -> None:
        _logger.debug(f'aset: key={key}')
        await self._finish_init()
        async with self.engine.begin() as conn:
            await conn.execute(
                sqlalchemy.delete(self.table).where(
                    self.table.c.key == key))
            await conn.execute(
                sqlalchemy.insert(self.table).values(
                    {
                        self.table.c.key: key,
                        self.table.c.data: zlib.compress(
                            ResponseSerializer().dumps(
                                response,
                                vary_header_data,
                                response_body)),
                    }))
            await conn.commit()

    async def aclose(self):
        pass
