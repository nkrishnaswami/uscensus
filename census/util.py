from census.errors import DBError

import requests


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
    doc = cache.get(url)
    if not doc:
        if session:
            r = session.get(url, **kwargs)
        else:
            r = requests.get(url, **kwargs)
        r.raise_for_status()
        return cache.put(url, r.text)
    return doc


class DBAPIQueryHelper(object):
    """Helper to simplify binding DBAPI parameters"""

    __paramstyle_positional = {
        'qmark': True,
        'numeric': True,
        'named': False,
        'format': True,
        'format': False,
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
