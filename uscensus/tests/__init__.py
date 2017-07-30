from __future__ import print_function, unicode_literals

from uscensus.tests.dbapicache import DBAPICache_test
from uscensus.tests.errors import CensusError_test, DBError_test
from uscensus.tests.index import Index_test
from uscensus.tests.discovery import DiscoveryInterface_test
from uscensus.tests.model import CensusDataEndpoint_test
from uscensus.tests.nopcache import NopCache_test
from uscensus.tests.webcache import fetchjson_test
from uscensus.tests.webcache import condget_test
from uscensus.tests.dbapiqueryhelper import DBAPIQueryHelper_test

"""Test cases for each class/function in the census package"""

__all__ = [
    'DBAPICache_test',
    'CensusError_test',
    'DBError_test',
    'Index_test',
    'DiscoveryInterface_test',
    'CensusDataEndpoint_test',
    'NopCache_test',
    'fetchjson_test',
    'condget_test',
    'DBAPIQueryHelper_test',
    'setup',
    'teardown',
]


def setup():
    pass


def teardown():
    pass
