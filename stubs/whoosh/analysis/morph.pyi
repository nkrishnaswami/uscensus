from _typeshed import Incomplete
from collections.abc import Generator
from whoosh.analysis.filters import Filter as Filter
from whoosh.compat import integer_types as integer_types
from whoosh.lang.dmetaphone import double_metaphone as double_metaphone
from whoosh.lang.porter import stem as stem
from whoosh.util.cache import lfu_cache as lfu_cache, unbound_cache as unbound_cache

class StemFilter(Filter):
    __inittypes__: Incomplete
    is_morph: bool
    stemfn: Incomplete
    lang: Incomplete
    ignore: Incomplete
    cachesize: Incomplete
    def __init__(self, stemfn=..., lang: Incomplete | None = None, ignore: Incomplete | None = None, cachesize: int = 50000) -> None: ...
    def clear(self) -> None: ...
    def cache_info(self): ...
    def __eq__(self, other): ...
    def __call__(self, tokens) -> Generator[Incomplete]: ...

class PyStemmerFilter(StemFilter):
    lang: Incomplete
    ignore: Incomplete
    cachesize: Incomplete
    def __init__(self, lang: str = 'english', ignore: Incomplete | None = None, cachesize: int = 10000) -> None: ...
    def algorithms(self): ...
    def cache_info(self) -> None: ...

class DoubleMetaphoneFilter(Filter):
    is_morph: bool
    primary_boost: Incomplete
    secondary_boost: Incomplete
    combine: Incomplete
    def __init__(self, primary_boost: float = 1.0, secondary_boost: float = 0.5, combine: bool = False) -> None: ...
    def __eq__(self, other): ...
    def __call__(self, tokens) -> Generator[Incomplete]: ...
