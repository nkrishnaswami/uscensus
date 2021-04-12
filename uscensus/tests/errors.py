from ..util.errors import CensusError, DBError


def CensusError_test():
    e = CensusError("test")
    assert isinstance(e, Exception)


def DBError_test():
    e = DBError("test")
    assert isinstance(e, CensusError)
    assert isinstance(e, Exception)
