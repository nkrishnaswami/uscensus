import logging

from supersqlite import sqlite3

from .textindexbase import TextIndexBase


_logger = logging.getLogger(__name__)

ApiSchemaFields = ('api_id', 'title', 'description', 'geographies',
                   'concepts', 'keywords', 'tags', 'variables', 'vintage')

VariableSchemaFields = ('api_id', 'variable', 'group', 'label',
                        'concept')


class Connections:
    def __init__(self):
        self.connections = {}

    def get_connection(self, dbname):
        conn = self.connections.get(dbname)
        if conn is None:
            conn = sqlite3.connect(dbname)
            conn.row_factory = sqlite3.Row
            conn.set_trace_callback(_logger.debug)
            self.connections[dbname] = conn
        return conn


class SqliteFts5Index(TextIndexBase):
    CONNECTIONS = Connections()

    def __init__(self, fieldset, table, dbname=':memory:'):
        if fieldset == 'API':
            self.fields = ApiSchemaFields
        elif fieldset == 'Variable':
            self.fields = VariableSchemaFields
        else:
            raise KeyError(f'"{fieldset}" is not one of "API" or "Variable"')
        self.quoted_fields = [f'"{field}"' for field in self.fields]
        self.table = table
        self.conn = self.CONNECTIONS.get_connection(dbname)
        self._execute(
            f'DROP TABLE IF EXISTS {self.table};')
        self._execute(
            f'CREATE VIRTUAL TABLE {self.table} USING ' +
            f'fts5({", ".join(self.quoted_fields)});')
        self._execute(
            f"INSERT INTO {self.table}({self.table}, rank) " +
            "VALUES('automerge', 16);")

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.conn.cursor().execute(
                f"INSERT INTO {self.table}({self.table}) VALUES('optimize');")
        self.conn.__exit__(exc_type, exc_value, traceback)

    def add(self, iterator, **kwargs):
        self._execute_many(
            f"""INSERT INTO {self.table}({", ".join(self.quoted_fields)})
            VALUES (:{", :".join(self.fields)});""",
            (dict(row) for row in iterator))

    def query(self, querystring, **constraints):
        if querystring:
            if not querystring.startswith('"'):
                querystring = f'"{querystring}"'
            if constraints:
                querystring += ' AND '
        querystring += ' AND '.join(
            (f'{field}: "{subquery}"'
             for field, subquery in constraints.items()))
        return self._execute(
            f"""SELECT rank as score, {", ".join(self.quoted_fields)}
            FROM {self.table}
            WHERE {self.table} MATCH '{querystring}'
            ORDER BY rank;""")

    def _execute(self, sql, *args, **kwargs):
        _logger.debug(f'Executing: {sql}')
        return self.conn.cursor().execute(sql, *args, **kwargs)

    def _execute_many(self, sql, iterator, *args, **kwargs):
        _logger.debug(f'Executing many: {sql}')
        self.conn.cursor().executemany(sql, iterator, *args, **kwargs)
