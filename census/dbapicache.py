from __future__ import print_function
from census.errors import DBError

import datetime as dt
try:
    import ujson as json
except ImportError:
    import json

__paramstyle_positional={
    "qmark": True,
    "numeric": True,
    "named": False,
    "format": True,
    "format": False,
}
__paramstyle_format_args={
    "qmark": lambda names: ["?"]*len(names),
    "numeric": lambda names: [":{}".format(idx+1) for idx in range(len(names))],
    "named": lambda names: [":{}".format(name) for name in names],
    "format": lambda names: ["%s"]*len(names),
    "pyformat": lambda names: ["%({})".format(name) for name in names],
}

def _query(dbapi, conn, template, **kwargs):
    """Query a DBAPI db, agnostic of paramstyle
    """
    positional = __paramstyle_positional.get(dbapi.paramstyle)
    fmt_args = __paramstyle_format_args.get(dbapi.paramstyle)
    if positional is None or fmt_args is None:
        raise DBError("Invalid paramstyle: " + dbapi.paramstyle)

    names = sorted([name for name in kwargs],
               key=lambda x: template.find('{'+x+'}'))
    querystr = template.format(**dict(zip(names, fmt_args(names))))

    if positional:
        vals = [val for key,val in sorted(
            kwargs.items(),
            key=lambda kv: template.find('{'+kv[0]+'}'))]

        
        return conn.execute(querystr, vals)
    else:
        return conn.execute(querystr, kwargs)

class DBAPICache(object):
    def __init__(self,
                 dbapi, table='urlcache',
                 timeout=dt.timedelta(days=7),
                 *dbargs, **dbkwargs):
        self.dbapi = dbapi
        self.timeout = timeout
        self.conn = self.dbapi.connect(*dbargs, **dbkwargs)
        self.table = table
        _query(
            self.dbapi, self.conn,
            'CREATE TABLE IF NOT EXISTS ' + self.table +
            ' (url TEXT, data BLOB, date TIMESTAMP, '+
            'PRIMARY KEY(url ASC))')
    def get(self, url):
        """Check if the URL is in the cache.
        If it is not, return None.
        If it is but has expired, delete the entry and return None.
        If it is and has not expired, parse the JSON and return it.
        """
        cur=_query(
            self.dbapi, self.conn,
            'SELECT data,date FROM ' + self.table +
            ' WHERE url={url}',
            url=url)
        row=cur.fetchone()
        if row:
            if dt.datetime.now()-row[1] < self.timeout:
                return json.loads(row[0])
            else:
                cur=_query(
                    self.dbapi, self.conn,
                    'DELETE FROM ' + self.table +
                    ' WHERE url={url}',
                    url=url)
    def put(self, url, doc):
        """Insert the document for the URL into the cache.
        Parse the document as JSON and return it.
        """
        _query(
            self.dbapi, self.conn,
            'INSERT INTO ' + self.table +
            ' VALUES ({url},{data},{date})',
            url=url,
            data=doc,
            date=dt.datetime.now(),
        )
        return json.loads(doc)
