from abc import ABC, abstractmethod
from typing import Optional, Tuple

from httpx_caching._models import Response


class AsyncDataStore(ABC):
    """DataStore interface used for webcache functionality"""

    @abstractmethod
    async def aget(self, key: str) -> Tuple[Optional[Response],
                                            Optional[dict]]:
        """Retrieve the response and data for the specified key from
        the data store, if present."""
        pass

    @abstractmethod
    async def aset(self,
                   key: str,
                   response: Response,
                   vary_header_dict: dict,
                   response_body: bytes) -> None:
        """Insert the response and data into the data store for the
        specified key.
        """
        pass

    @abstractmethod
    async def adelete(self, key: str) -> None:
        """Remove the data for the specified key from the data store."""
        pass

    @abstractmethod
    async def aclose(self) -> None:
        pass
