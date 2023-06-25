
from httpx_caching._models import Response

from uscensus.util.datastores.datastore import AsyncDataStore


class AsyncNopDataStore(AsyncDataStore):
    """Async data store implementation for webcache that does not store data."""

    async def aget(self, key: str) -> tuple[Response | None,
                                            dict | None]:
        return (None, None)

    async def adelete(self, key: str) -> None:
        pass

    async def aset(self,
                   key: str,
                   response: Response,
                   vary_header_dict: dict,
                   response_body: bytes) -> None:
        pass

    async def aclose(self):
        pass
