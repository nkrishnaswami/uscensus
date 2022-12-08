from abc import ABC, abstractmethod
from collections import namedtuple
from enum import Enum
from typing import Iterable, Union


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
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Commit or abandon the added rows."""
        pass

    @abstractmethod
    def add(self,
            iterable: Union[Iterable[DatasetFields], Iterable[VariableFields]],
            **kwargs):
        """Add many rows to the index."""
        pass

    @abstractmethod
    def query(self, querystring: str, **query):
        """Search for matching rows."""
        pass
