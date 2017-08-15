from __future__ import print_function
from __future__ import unicode_literals

from ..util.nopcache import NopCache


def NopCache_test():
    nc = NopCache()
    doc, date = nc.get('test')
    assert doc is None
    assert date is None
