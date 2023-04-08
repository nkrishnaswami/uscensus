from abc import ABC, abstractmethod

from httpx_caching._models import Response


class DataStore(ABC):
    """DataStore (cache) interface for httpx_caching."""

    @abstractmethod
    def get(self, key: str) -> tuple[Response | None , dict | None]:
        """Retrieve the response and info for the specified `key' from
        the data store, if present.
        """

    @abstractmethod
    def set(self,
            key: str,
            response: Response,
            vary_header_dict: dict,
            response_body: bytes) -> None:
        """Insert the response into the data store for the specified
        key.
        """

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove the data for the specified url from the data store."""

    @abstractmethod
    def close(self) -> None:
        pass


class AsyncDataStore(ABC):
    """DataStore interface used for webcache functionality."""

    @abstractmethod
    async def aget(self, key: str) -> tuple[Response | None,
                                            dict | None]:
        """Retrieve the response and data for the specified key from
        the data store, if present.
        """

    @abstractmethod
    async def aset(self,
                   key: str,
                   response: Response,
                   vary_header_dict: dict,
                   response_body: bytes) -> None:
        """Insert the response and data into the data store for the
        specified key.
        """

    @abstractmethod
    async def adelete(self, key: str) -> None:
        """Remove the data for the specified key from the data store."""

    @abstractmethod
    async def aclose(self) -> None:
        pass
