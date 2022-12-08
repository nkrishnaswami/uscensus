from typing import Optional, Tuple

from httpx_caching._models import Response

from ...util.datastores.datastore import DataStore


class NopDataStore(DataStore):
    """Data store implementation for webcache that does not store data"""

    ASYNC_CAPABLE = False

    def get(self, key: str) -> Tuple[Optional[Response], Optional[dict]]:
        return None, None

    def set(self,
            key: str,
            response: Response,
            vary_header_dict: dict,
            response_body: bytes) -> None:
        pass

    def delete(self, key: str) -> None:
        pass

    def close(self) -> None:
        pass
