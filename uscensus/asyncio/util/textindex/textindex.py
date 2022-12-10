from abc import ABC, abstractmethod
from typing import Iterable


class TextIndex(ABC):
    @abstractmethod
    async def __enter__(self):
        pass

    @abstractmethod
    async def __exit__(self, exc_type, exc_value, traceback):
        pass

    @abstractmethod
    async def add(self, iterator: Iterable, **kwargs):
        pass

    @abstractmethod
    async def query(self, querystring: str, **query):
        pass
