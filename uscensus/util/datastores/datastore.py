from abc import ABC, abstractmethod
from typing import Optional, Tuple

from httpx_caching._models import Response


class DataStore(ABC):
    """DataStore (cache) interface for httpx_caching."""

    @abstractmethod
    def get(self, key: str) -> Tuple[Optional[Response], Optional[dict]]:
        """Retrieve the response and info for the specified `key' from
        the data store, if present."""
        pass

    @abstractmethod
    def set(self,
            key: str,
            response: Response,
            vary_header_dict: dict,
            response_body: bytes) -> None:
        """Insert the response into the data store for the specified
        key.
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove the data for the specified url from the data store."""
        pass

    @abstractmethod
    def close(self) -> None:
        pass
