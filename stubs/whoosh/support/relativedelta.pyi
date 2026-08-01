from _typeshed import Incomplete

__all__ = ['relativedelta', 'MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU']

class weekday:
    weekday: Incomplete
    n: Incomplete
    def __init__(self, weekday, n: Incomplete | None = None) -> None: ...
    def __call__(self, n): ...
    def __eq__(self, other): ...

MO: Incomplete
TU: Incomplete
WE: Incomplete
TH: Incomplete
FR: Incomplete
SA: Incomplete
SU: Incomplete

class relativedelta:
    years: int
    months: int
    days: int
    leapdays: int
    hours: int
    minutes: int
    seconds: int
    microseconds: int
    year: Incomplete
    month: Incomplete
    day: Incomplete
    weekday: Incomplete
    hour: Incomplete
    minute: Incomplete
    second: Incomplete
    microsecond: Incomplete
    def __init__(self, dt1: Incomplete | None = None, dt2: Incomplete | None = None, years: int = 0, months: int = 0, days: int = 0, leapdays: int = 0, weeks: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, microseconds: int = 0, year: Incomplete | None = None, month: Incomplete | None = None, day: Incomplete | None = None, weekday: Incomplete | None = None, yearday: Incomplete | None = None, nlyearday: Incomplete | None = None, hour: Incomplete | None = None, minute: Incomplete | None = None, second: Incomplete | None = None, microsecond: Incomplete | None = None) -> None: ...
    def __radd__(self, other): ...
    def __rsub__(self, other): ...
    def __add__(self, other): ...
    def __sub__(self, other): ...
    def __neg__(self): ...
    def __nonzero__(self): ...
    __bool__ = __nonzero__
    def __mul__(self, other): ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __div__(self, other): ...
