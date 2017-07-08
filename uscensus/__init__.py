from uscensus.errors import *
from uscensus.dbapicache import DBAPICache
from uscensus.nopcache import NopCache
from uscensus.loader import *

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
 * DBAPICache: caches API metadata in any DBAPI compatible DBMS
 * CensusLoader: retrieves and caches census API metadata. This
   indexes metadata and has a dict of wrapper objects for each API.
 * model.CensusAPI: wraps a Census API endpoint given its metadata.
   These are constructed by the CensusLoader
 * NopCache: dummy implementation of the cache interface

Usage: Instantiate a CensusLoader using a DBAPICache and your Census
API key.  Call census APIs and receive the results as a pandas
DataFrame.

"""
