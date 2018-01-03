from uscensus.data.discovery import DiscoveryInterface
from uscensus.data.states import get_state_codes
from uscensus.geocode.bulk import CensusBulkGeocoder
from uscensus.util.errors import CensusError
from uscensus.util.errors import DBError
from uscensus.util.nopcache import NopCache
from uscensus.util.sqlalchemycache import SqlAlchemyCache

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
 * SqlAlchemyCache: caches APIa metadata in any SqlAlchemy compatible DBMS
 * DBAPICache: caches API metadata in any DBAPI compatible DBMS
 * DiscoveryInterface: retrieves and caches census API metadata. This
   indexes metadata and has a dict of wrapper objects for each API.
 * model.CensusDataEndpoint: wraps a Census API endpoint given its
   metadata.  These are constructed by the DiscoveryInterface.
 * NopCache: dummy implementation of the cache interface

Functions:
 * get_state_codes: retrieve state codes/names/abbreviations

Usage: Instantiate a DiscoveryInterface using a DBAPICache and your
Census API key.  Call census APIs and receive the results as a pandas
DataFrame.

"""


__all__ = [
    "CensusBulkGeocoder",
    "DiscoveryInterface",
    "DBAPICache",
    "SqlAlchemyCache",
    "NopCache",
    "CensusError",
    "DBError",
    "get_state_codes",
]
