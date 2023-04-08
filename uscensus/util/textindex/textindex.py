from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Iterable
from enum import Enum


class FieldSet(Enum):
    """When creating a TextIndex, this indicates whether the add
    method should receive DatasetFields or VariableFields.
    """

    DATASET = 1
    VARIABLE = 2


DatasetFields = namedtuple('DatasetFields', ('dataset_id', 'title',
                                             'description', 'geographies',
                                             'concepts', 'keywords', 'tags',
                                             'variables', 'vintage'))

VariableFields = namedtuple('VariableFields', ('dataset_id', 'variable',
                                               'group', 'label',
                                               'concept'))


class TextIndex(ABC):
    """Full-text index for either the dataset field set or variables field set.

    Usage:

        class MyIndex(TextIndex):
            ...
        rows: List[DatasetFields] = getSomeRows()
        my_index = MyIndex(...)
        with my_index:
            my_index.add(rows)
    """

    @abstractmethod
    def __enter__(self):
        """Prepare the TextIndex for adding rows."""

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Commit or abandon the added rows."""

    @abstractmethod
    def add(self,
            iterable: Iterable[DatasetFields] | Iterable[VariableFields],
            **kwargs):
        """Add many rows to the index."""

    @abstractmethod
    def query(self, querystring: str, **query):
        """Search for matching rows."""


class AsyncTextIndex(ABC):
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
