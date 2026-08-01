from _typeshed import Incomplete
from whoosh import highlight as highlight
from whoosh.compat import iteritems as iteritems, xrange as xrange

class Corrector:
    def suggest(self, text, limit: int = 5, maxdist: int = 2, prefix: int = 0): ...

class ReaderCorrector(Corrector):
    reader: Incomplete
    fieldname: Incomplete
    fieldobj: Incomplete
    def __init__(self, reader, fieldname, fieldobj) -> None: ...

class ListCorrector(Corrector):
    wordlist: Incomplete
    def __init__(self, wordlist) -> None: ...
    class Skipper:
        data: Incomplete
        i: int
        def __init__(self, data) -> None: ...
        def __call__(self, w): ...

class MultiCorrector(Corrector):
    correctors: Incomplete
    op: Incomplete
    def __init__(self, correctors, op) -> None: ...

class Correction:
    original_query: Incomplete
    query: Incomplete
    original_string: Incomplete
    tokens: Incomplete
    string: Incomplete
    def __init__(self, q, qstring, corr_q, tokens) -> None: ...
    def format_string(self, formatter): ...

class QueryCorrector:
    fieldname: Incomplete
    def __init__(self, fieldname) -> None: ...
    def correct_query(self, q, qstring) -> None: ...
    def field(self): ...

class SimpleQueryCorrector(QueryCorrector):
    correctors: Incomplete
    aliases: Incomplete
    termset: Incomplete
    prefix: Incomplete
    maxdist: Incomplete
    def __init__(self, correctors, terms, aliases: Incomplete | None = None, prefix: int = 0, maxdist: int = 2) -> None: ...
    def correct_query(self, q, qstring): ...
