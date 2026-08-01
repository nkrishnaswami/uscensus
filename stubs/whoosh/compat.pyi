import io
from _typeshed import Incomplete
from itertools import permutations as permutations
from operator import methodcaller as methodcaller
from pickle import dump as dump, dumps as dumps, load as load, loads as loads
from urllib.request import urlretrieve as urlretrieve

def htmlescape(s, quote: bool = True): ...

PY3: bool

def b(s): ...
BytesIO = io.BytesIO
callable: Incomplete
exec_: Incomplete
integer_types: Incomplete
iteritems: Incomplete
itervalues: Incomplete
iterkeys: Incomplete
izip = zip
long_type = int
next = next
StringIO = io.StringIO
string_type = str
text_type = str
bytes_type = bytes
unichr = chr

def byte(num): ...
def u(s): ...
def with_metaclass(meta, base=...): ...
xrange = range
zip_: Incomplete

def memoryview_(source, offset: Incomplete | None = None, length: Incomplete | None = None): ...
def array_tobytes(arry): ...
def array_frombytes(arry, bs): ...
