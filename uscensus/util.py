from __future__ import print_function, unicode_literals

from uscensus.errors import DBError
import datetime
try:
    from email.utils import format_datetime, parsedate_to_datetime
except ImportError:
    # these were introduced in 3.3; quick hack:
    from email.utils import formatdate, parsedate
    import time

    def format_datetime(dt):
        return formatdate(time.mktime(dt.timetuple()))

    def parsedate_to_datetime(date):
        return datetime.datetime(*parsedate(date)[:6])

    
import requests


class UTC(datetime.tzinfo):
    """UTC"""
    ZERO = datetime.timedelta(0)

    def utcoffset(self, dt):
        return UTC.ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return UTC.ZERO


utc = UTC()


def fetchjson(url, cache, session, **kwargs):
    """Very thin wrapper around requests.py, to get a URL, check for
    errors, and return the parsed JSON reponse.

    This will cache queried data and try to avoid the call on hit.

    Arguments:
      * url: URL from which to fetch JSON resonse
      * cache: Cache in which to store response
      * session: optional requests.Session for making API call
      * kwargs: additional arguments to `requests.get`

    Exceptions:
      * requests.exceptions.HTTPError on HTTP failure
      * ValueError on JSON parse failure
    """
    doc, date = cache.get(url)
    stale = False
    if date:
        stale = (datetime.datetime.now(tz=utc) - date) > cache.timeout
    if stale or not doc:
        headers = {}
        if date:
            headers['If-Modified-Since'] = format_datetime(date)
        r = (session or requests).get(url, headers=headers, **kwargs)
        r.raise_for_status()
        if r.status_code == 304:
            cache.touch(url, r.headers['Date'])
        elif r.text:
            if stale:
                # already in db; remove it
                cache.delete(url)
            return cache.put(
                url,
                r.text,
                parsedate_to_datetime(r.headers['Date']))
    return doc


class DBAPIQueryHelper(object):
    """Helper to simplify binding DBAPI parameters"""

    __paramstyle_positional = {
        'qmark': True,
        'numeric': True,
        'named': False,
        'format': True,
        'pyformat': False,
    }

    __paramstyle_format_args = {
        'qmark': lambda names: ['?']*len(names),
        'numeric': lambda names: [':{}'.format(idx+1)
                                  for idx in range(len(names))],
        'named': lambda names: [':{}'.format(name) for name in names],
        'format': lambda names: ['%s']*len(names),
        'pyformat': lambda names: ['%({})'.format(name) for name in names],
    }

    def __init__(self, dbapi, conn):
        """Construct a DBAPIQuery helper

        Arguments:
          * dbapi: DBAPI module corresponding to `conn`
          * conn: a DBAPI connection

        Exceptions:
          * DBEror: if dbapi.paramstyle is not expected
        """

        self.dbapi = dbapi
        self.conn = conn
        self.positional = self.__paramstyle_positional.get(
            self.dbapi.paramstyle)
        self.fmt_args = self.__paramstyle_format_args.get(
            self.dbapi.paramstyle)
        if self.positional is None or self.fmt_args is None:
            raise DBError("Invalid paramstyle: " + self.dbapi.paramstyle)

    def __call__(self, template, **kwargs):
        """Query a DBAPI db, agnostic of paramstyle.

        Arguments:
          * template: string to be formatted into a SQL template.
            Bindable-parameters are specified as named format params.
          * kwargs: named arguments to be bound into the formatted
            template.

        Exceptions:
          * self.dbapi.Error: any errors executing the SQL. See PEP
            249 or DBAPI module documentation for details.

        """

        names = sorted([name for name in kwargs],
                       key=lambda x: template.find('{'+x+'}'))
        querystr = template.format(**dict(zip(names,
                                              self.fmt_args(names))))
        if self.positional:
            vals = [val for key, val in sorted(
                kwargs.items(),
                key=lambda kv: template.find('{'+kv[0]+'}'))]
            return self.conn.execute(querystr, vals)
        else:
            return self.conn.execute(querystr, kwargs)
