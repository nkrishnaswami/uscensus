from _typeshed import Incomplete
from multiprocessing import Process
from whoosh.codec import base as base
from whoosh.compat import iteritems as iteritems, pickle as pickle, queue as queue, xrange as xrange
from whoosh.externalsort import imerge as imerge
from whoosh.util import random_name as random_name
from whoosh.writing import PostingPool as PostingPool, SegmentWriter as SegmentWriter

def finish_subsegment(writer, k: int = 64): ...

class SubWriterTask(Process):
    storage: Incomplete
    indexname: Incomplete
    jobqueue: Incomplete
    resultqueue: Incomplete
    kwargs: Incomplete
    multisegment: Incomplete
    running: bool
    def __init__(self, storage, indexname, jobqueue, resultqueue, kwargs, multisegment) -> None: ...
    def run(self) -> None: ...
    def cancel(self) -> None: ...

class MpWriter(SegmentWriter):
    procs: Incomplete
    batchsize: Incomplete
    subargs: Incomplete
    multisegment: Incomplete
    tasks: Incomplete
    jobqueue: Incomplete
    resultqueue: Incomplete
    docbuffer: Incomplete
    def __init__(self, ix, procs: Incomplete | None = None, batchsize: int = 100, subargs: Incomplete | None = None, multisegment: bool = False, **kwargs) -> None: ...
    def cancel(self) -> None: ...
    def start_group(self) -> None: ...
    def end_group(self) -> None: ...
    def add_document(self, **fields) -> None: ...
    def commit(self, mergetype: Incomplete | None = None, optimize: Incomplete | None = None, merge: Incomplete | None = None) -> None: ...

class SerialMpWriter(MpWriter):
    procs: Incomplete
    batchsize: Incomplete
    subargs: Incomplete
    tasks: Incomplete
    pointer: int
    def __init__(self, ix, procs: Incomplete | None = None, batchsize: int = 100, subargs: Incomplete | None = None, **kwargs) -> None: ...
    def add_document(self, **fields) -> None: ...

class MultiSegmentWriter(MpWriter):
    multisegment: bool
    def __init__(self, *args, **kwargs) -> None: ...
