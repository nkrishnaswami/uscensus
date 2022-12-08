from ..util.errors import CensusError, DBError


def test_CensusError():
    e = CensusError("test")
    assert isinstance(e, Exception)


def test_DBError():
    e = DBError("test")
    assert isinstance(e, CensusError)
    assert isinstance(e, Exception)
