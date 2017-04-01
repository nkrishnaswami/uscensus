from census.errors import *
from census.mongocache import MongoCache
from census.dbapicache import DBAPICache
from census.nopcache import NopCache
from census.loader import *

"""This module reads the Census's API discovery interface at
http://api.census.gov/data.json, and provides callable wrappers for
each API it finds.  It indexes each of their metadata fields to make
the APIs and variables related to them easier to find.
"""
