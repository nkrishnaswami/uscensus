from abc import abstractmethod
from typing import (Any, Generic, Iterator, Optional, Mapping,
                    Protocol, Sequence, Tuple, TypeVar, Union)


T = TypeVar('T')
PARAM = Union[Sequence, Mapping]
XID = TypeVar('XID', bound='Sequence')
CONN = TypeVar('CONN', bound='DBAPIConnection')
CONN_co = TypeVar('CONN_co', bound='DBAPIConnection', covariant=True)
CURSOR_contra = TypeVar('CURSOR_contra', bound='DBAPICursor', contravariant=True)


class DBAPIRow(Protocol):
    pass


class DBAPITypeObject(Generic[T], Protocol):
    type_code: int
    values: Sequence[T]

    @abstractmethod
    def __init__(self, *values: Sequence[T]):
        raise NotImplementedError

    @abstractmethod
    def __cmp__(self, other: T):
        raise NotImplementedError


class DBAPIErrorHandler(Generic[CONN_co, CURSOR_contra], Protocol):
    @abstractmethod
    def __call__(connection: CONN_co,
                 cursor: CURSOR_contra,
                 errorclass: type,
                 errorvalue: Exception):
        raise NotImplementedError
    

class DBAPICursor(Protocol[CONN]):
    description: Sequence[Sequence[DBAPITypeObject]]
    rowcount: int
    arraysize: int
    rownumber: Optional[int]
    connection: CONN
    messages: Optional[Sequence[Tuple[type, Exception]]]
    lastrowid: Optional[DBAPITypeObject]
    errorhandler: Optional[DBAPIErrorHandler]

    @abstractmethod
    def callproc(self, procname: str, *args, **kwargs) -> Sequence:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute(self, operation: str, parameters: PARAM) -> Any:
        raise NotImplementedError

    @abstractmethod
    def executemany(
            self, operation: str,
            seq_of_parameters: Sequence[PARAM]
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def fetchone(self) -> Optional[Sequence]:
        raise NotImplementedError

    @abstractmethod
    def fetchmany(self, size: int) -> Sequence[Sequence]:
        raise NotImplementedError

    @abstractmethod
    def fetchall(self) -> Sequence[Sequence]:
        raise NotImplementedError

    @abstractmethod
    def nextset(self) -> Optional[bool]:
        raise NotImplementedError

    @abstractmethod
    def setinputsizes(
            self,
            sizes: Sequence[Union[int, DBAPITypeObject, type[None]]]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def setoutputsize(
            self,
            size: int,
            column: Optional[int]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def next(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[DBAPIRow]:
        raise NotImplementedError


class DBAPIConnection(Protocol):
    messages: Optional[Sequence[Tuple[type, Exception]]]
    autocommit: Optional[bool]
    errorhandler: Optional[DBAPIErrorHandler]

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def cursor(self) -> DBAPICursor:
        raise NotImplementedError

    @abstractmethod
    def execute(self, operation: str, parameters: PARAM) -> DBAPICursor:
        raise NotImplementedError

    @abstractmethod
    def executemany(
            self, operation: str,
            seq_of_parameters: Sequence[PARAM]
    ) -> DBAPICursor:
        raise NotImplementedError


class DBAPIProtocol(Protocol):
    """A typing protocol for DBAPI 2.0.
    """

    apilevel: str
    threadsafety: int
    paramstyle: str
    STRING: DBAPITypeObject
    BINARY: DBAPITypeObject
    NUMBER: DBAPITypeObject
    DATETIME: DBAPITypeObject
    ROWID: DBAPITypeObject

    class Warning(Protocol):
        pass

    class Error(Protocol):
        pass

    class InterfaceError(Error, Protocol):
        pass

    class DatabaseError(Error, Protocol):
        pass

    class DataError(DatabaseError, Protocol):
        pass

    class OperationalError(DatabaseError, Protocol):
        pass

    class IntegrityError(DatabaseError, Protocol):
        pass

    class InternalError(DatabaseError, Protocol):
        pass

    class ProgrammingError(DatabaseError, Protocol):
        pass

    class NotSupportedError(DatabaseError, Protocol):
        pass

    @abstractmethod
    def connect(self, *args, **kwargs) -> DBAPIConnection:
        raise NotImplementedError

    @abstractmethod
    def Date(self, year: int, month: int, day: int) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def Time(self, hour: int, minute: int, second: int) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def Timestamp(
            self,
            year: int, month: int, day: int,
            hour: int, minute: int, second: int
    ) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def DateFromTicks(self, ticks: int) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def TimeFromTicks(self, ticks: int) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def TimestampFromTicks(self, ticks: int) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def Binary(self, string: str) -> DBAPITypeObject:
        raise NotImplementedError

    @abstractmethod
    def xid(self,
            format_id: str,
            global_transaction_id: str,
            branch_qualifier: str) -> XID:
        raise NotImplementedError

    @abstractmethod
    def tpc_begin(self, xid: XID):
        raise NotImplementedError

    @abstractmethod
    def tpc_prepare(self):
        raise NotImplementedError

    @abstractmethod
    def tpc_commit(self, xid: Optional[XID]):
        raise NotImplementedError

    @abstractmethod
    def tpc_rollback(self, xid: Optional[XID]):
        raise NotImplementedError

    @abstractmethod
    def tpc_recover(self) -> Sequence[XID]:
        raise NotImplementedError
