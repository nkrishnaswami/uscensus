from __future__ import print_function
from __future__ import unicode_literals


class CensusError(Exception):
    """Superclass for errors in the census API wrapper library.
    """
    pass


class DBError(CensusError):
    """Class for reporting database errors in the census API wrapper
    library.
    """
    pass
