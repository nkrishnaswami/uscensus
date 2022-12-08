from .data import DiscoveryInterface
from .data import get_county_boundaries
from .data import get_county_codes
from .data import get_state_boundaries
from .data import get_state_codes
from .geocode.bulk import CensusBulkGeocoder
from .util import CensusError
from .util import DBError
from .util import DBAPIDataStore
from .util import NopDataStore
from .util import SqlAlchemyDataStore

"""This module reads the Census's API discovery interface at
http://api.census.gov/data.json, and provides callable wrappers for
each API it finds.  It indexes each of their metadata fields to make
the APIs and variables related to them easier to find.

The fields in the dataset discovery interface are described at
https://project-open-data.cio.gov/v1.1/schema/ .

Using this module requires a Census API key, which you can request at
https://www.census.gov/developers/ .

Exceptions:
 * CensusError(Exception): base class for module exceptions
 * DBError(CensusError): errors accessing databases

Classes:
 * SqlAlchemyDataStore: caches APIa metadata in any SqlAlchemy-compatible DBMS
 * DiscoveryInterface: retrieves and caches census API metadata. This
   indexes metadata and has a dict of wrapper objects for each API.
 * model.CensusDataEndpoint: wraps a Census API endpoint given its
   metadata.  These are constructed by the DiscoveryInterface.
 * NopDataStore: dummy implementation of the datastore interface

Functions:
 * get_state_codes: retrieve state codes/names/abbreviations

Usage: Instantiate a DiscoveryInterface using a DBAPICache and your
Census API key.  Call census APIs and receive the results as a pandas
DataFrame.

"""


__all__ = [
    "CensusBulkGeocoder",
    "DiscoveryInterface",
    "DBAPIDataStore",
    "SqlAlchemyDataStore",
    "NopDataStore",
    "CensusError",
    "DBError",
    "get_county_boundaries",
    "get_county_codes",
    "get_state_boundaries",
    "get_state_codes",
]
