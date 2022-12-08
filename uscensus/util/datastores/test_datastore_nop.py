from ...util.datastores.nop import NopDataStore


def test_NopDatastore():
    nc = NopDataStore()
    doc, vary = nc.get('test')
    assert doc is None
    assert vary is None
