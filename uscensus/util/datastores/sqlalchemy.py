import datetime
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

    def __init__(self, connstr: str, table: str = 'urlcache') -> None:
        """Arguments:
        ---------
          * connstr: sqlalchemy connection string.
          * table: name of table to use/create for response storage.
        """
        _logger.debug(f'create: Instantiating async connection to {connstr}')
        self.connstr = connstr
        self.table = table
        self.engine = create_async_engine(self.connstr, logging_name=__name__)
        self.initialized = False

    async def _finish_init(self):
        if not self.initialized:
            md = sqlalchemy.MetaData()
            self.table = sqlalchemy.Table(
                self.table,
                md,
                sqlalchemy.Column('key',
                                  sqlalchemy.String,
                                  primary_key=True),
                sqlalchemy.Column('data',
                                  sqlalchemy.BLOB,
                                  nullable=False),
            )
            async with self.engine.connect() as conn:
                await conn.run_sync(md.create_all)
            self.initialized = True

    async def aget(self, key: str) -> tuple[Response | None,
                                            dict | None]:
        _logger.debug(f'aget: key={key}')
        await self._finish_init()
        async with self.engine.connect() as conn:
            cur = await conn.execute(
                sqlalchemy.select(
                    [self.table.c.data],
                    from_obj=self.table,
                ).where(
                    self.table.c.key == key,
                ),
            )
            row = cur.fetchone()
            if row:
                ret = ResponseSerializer().loads(
                    zlib.decompress(row[self.table.c.data]))
                _logger.debug('Hit')
                cur.close()
                return ret
            _logger.debug('Miss')
        return None, None

    async def adelete(self, key: str) -> None:
        _logger.debug(f'adelete: key={key}')
        await self._finish_init()
        async with self.engine.connect() as conn:
            await conn.execute(
                self.table.delete().where(
                    self.table.c.key == key,
                ),
            )

    async def aset(self, key: str, response: Response,
                   vary_header_data: dict,
                   response_body: bytes) -> None:
        _logger.debug(f'aset: key={key}')
        await self._finish_init()
        async with self.engine.begin() as conn:
            await conn.execute(
                self.table.delete().where(
                    self.table.c.key == key),
            )
            await conn.execute(
                self.table.insert().values(
                    {
                        self.table.c.key: key,
                        self.table.c.data: zlib.compress(
                            ResponseSerializer().dumps(
                                response,
                                vary_header_data,
                                response_body)),
                    }))

    async def aclose(self):
        pass
