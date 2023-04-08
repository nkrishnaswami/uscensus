class CensusError(Exception):
    """Superclass for errors in the census API wrapper library."""


class DBError(CensusError):
    """Class for reporting database errors in the census API wrapper
    library.
    """
