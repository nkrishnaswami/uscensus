from _typeshed import Incomplete
from collections.abc import Generator
from whoosh.analysis.filters import Filter as Filter
from whoosh.compat import text_type as text_type, u as u, xrange as xrange

class CompoundWordFilter(Filter):
    wordset: Incomplete
    keep_compound: Incomplete
    def __init__(self, wordset, keep_compound: bool = True) -> None: ...
    def subwords(self, s, memo): ...
    def __call__(self, tokens) -> Generator[Incomplete]: ...

class BiWordFilter(Filter):
    sep: Incomplete
    def __init__(self, sep: str = '-') -> None: ...
    def __call__(self, tokens) -> Generator[Incomplete]: ...

class ShingleFilter(Filter):
    size: Incomplete
    sep: Incomplete
    def __init__(self, size: int = 2, sep: str = '-') -> None: ...
    def __call__(self, tokens) -> Generator[Incomplete, None, Incomplete]: ...

class IntraWordFilter(Filter):
    is_morph: bool
    __inittypes__: Incomplete
    delims: Incomplete
    between: Incomplete
    possessive: Incomplete
    boundary: Incomplete
    splitting: Incomplete
    mergewords: Incomplete
    mergenums: Incomplete
    def __init__(self, delims=..., splitwords: bool = True, splitnums: bool = True, mergewords: bool = False, mergenums: bool = False) -> None: ...
    def __eq__(self, other): ...
    def __call__(self, tokens) -> Generator[Incomplete]: ...
