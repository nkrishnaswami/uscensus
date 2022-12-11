from collections.abc import Mapping
import logging
import sqlite3
from typing import Iterable, Tuple, Union

from .textindex import TextIndex, FieldSet, DatasetFields, VariableFields


_logger = logging.getLogger(__name__)


class SqliteFts5Index(TextIndex):
    """Full-text index backing to a sqlite DB with FTS5."""
    fields: Tuple

    def __init__(self,
                 fieldset: Union[DatasetFields, VariableFields],
                 table: str,
                 dbname: str = ':memory:'):
        if fieldset == FieldSet.DATASET:
            self.fields = DatasetFields._fields
        elif fieldset == FieldSet.VARIABLE:
            self.fields = VariableFields._fields
        else:
            raise KeyError(f'"{fieldset}" is not one of DATASET or VARIABLE')
        self.quoted_fields = [f'"{field}"' for field in self.fields]
        self.table = table
        self.conn = sqlite3.connect(dbname)
        self.conn.row_factory = sqlite3.Row
        self.conn.set_trace_callback(_logger.debug)

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

    def add(self,
            documents: Union[Iterable[DatasetFields],
                             Iterable[VariableFields]],
            **kwargs) -> None:
        self._execute_many(
            f"""INSERT INTO {self.table}({", ".join(self.quoted_fields)})
            VALUES (:{", :".join(self.fields)});""",
            [doc._asdict() for doc in documents])

    def query(self,
              querystring: str,
              **constraints: Mapping[str, str]):
        if querystring:
            if not (querystring.startswith('"') or ':' in querystring):
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
