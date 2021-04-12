from abc import ABC, abstractmethod


class TextIndexBase(ABC):
    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @abstractmethod
    def add(self, iterator, **kwargs):
        pass

    @abstractmethod
    def query(self, querystring, **query):
        pass
