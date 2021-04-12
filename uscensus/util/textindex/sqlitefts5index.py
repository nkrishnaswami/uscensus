from supersqlite import sqlite3

from .textindexbase import TextIndexBase

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
        self.quoted_fields = [f"'{field}'" for field in self.fields]
        self.table = table
        self.conn = self.CONNECTIONS.get_connection(dbname)
        self.cursor = self.conn.cursor()
        sql = (f'CREATE VIRTUAL TABLE {self.table} USING ' +
               f'fts5({", ".join(self.quoted_fields)});')
        print(sql)
        self.cursor.execute(sql)
        sql = (f"INSERT INTO {self.table}({self.table}, rank) " +
               "VALUES('automerge', 16);")
        self.cursor.execute(sql)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self.cursor.execute(
                f"INSERT INTO {self.table}({self.table}) VALUES('optimize');")

    def add(self, iterator, **kwargs):
        sql = f"""INSERT INTO {self.table}({", ".join(self.quoted_fields)})
                VALUES (:{", :".join(self.fields)});"""
        print(sql)
        self.cursor.executemany(
            sql, ({key: row.get(key)
                   for key in self.fields}
                  for row in iterator))

    def query(self, querystring, **constraints):
        if querystring and constraints:
            querystring += ' AND '
        querystring += ' AND '.join(
            (f'{field}: {subquery}'
             for field, subquery in constraints.items()))
        for row in self.cursor.execute(
                f"""SELECT rank as score, {", ".join(self.quoted_fields)}
                FROM {self.table}
                WHERE MATCH '{querystring}'
                ORDER BY rank;"""):
            yield row
