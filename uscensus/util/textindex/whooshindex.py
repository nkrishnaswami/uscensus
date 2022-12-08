from typing import Iterable, OrderedDict, Union

from whoosh.analysis.filters import StopFilter
from whoosh.analysis import (KeywordAnalyzer, StandardAnalyzer)
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.fields import Schema, FieldType, KEYWORD, ID, TEXT
from whoosh.qparser import QueryParser
from whoosh.writing import AsyncWriter

from ...util.errors import CensusError
from .textindex import TextIndex, FieldSet, DatasetFields, VariableFields


KWAnalyzer = KeywordAnalyzer(lowercase=True) | StopFilter()
Analyzer = StandardAnalyzer()

DatasetSchemaFields = OrderedDict((
    ('dataset_id', ID(unique=True, stored=True)),
    ('title', KEYWORD(analyzer=KWAnalyzer)),
    ('description', TEXT(analyzer=Analyzer)),
    ('geographies', KEYWORD(analyzer=KWAnalyzer)),
    ('concepts', KEYWORD(analyzer=KWAnalyzer)),
    ('keywords', KEYWORD(analyzer=KWAnalyzer)),
    ('tags', KEYWORD(analyzer=KWAnalyzer)),
    ('variables', KEYWORD(analyzer=KWAnalyzer)),
    ('vintage', ID),
))

VariableSchemaFields = OrderedDict((
    ('dataset_id', ID(stored=True)),
    ('variable', ID(stored=True)),
    ('group', ID(stored=True)),
    ('label', TEXT(analyzer=Analyzer)),
    ('concept', KEYWORD(analyzer=Analyzer)),
))


class WhooshIndex(TextIndex):
    """Census API metadata indexer."""

    schema_fields: OrderedDict[str, FieldType]

    def __init__(self,
                 name: str,
                 fieldset: FieldSet,
                 dflt_query_field: str,
                 path: str = None):
        """Initialize Whoosh index specified fields.

          Arguments:
            * fieldset: the enum FieldSet.DATASET or VARIABLE, to select a
              schema.
            * path: if specified, the path in which to create a
              persistent index. If not specified, index to RAM.

        """
        if fieldset == FieldSet.DATASET:
            self.schema_fields = DatasetSchemaFields
        elif fieldset == FieldSet.VARIABLE:
            self.schema_fields = VariableSchemaFields
        else:
            raise KeyError(f'"{fieldset}" is not one of DATASET or VARIABLE')
        # Initialize index
        fs = FileStorage(path).create() if path else RamStorage()
        if fs.index_exists():
            self.index = fs.open_index(name)
            schema = self.index.schema()
        else:
            schema = Schema(**self.schema_fields)
            self.index = fs.create_index(schema, name)
        self.qparser = QueryParser(dflt_query_field,
                                   schema=schema)
        self.writer = None

    def __enter__(self):
        self.writer = AsyncWriter(
            self.index, writerargs=dict(limitmb=1000))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.writer.cancel()
        else:
            self.writer.commit()
        self.writer = None

    def add(self, iterable: Iterable[Union[DatasetFields, VariableFields]],
            **kwargs):
        """Add entries to the index

        Arguments:

          * iterable: iterable (one for each endpoint) of tuples containing
            data for each schema field, viz.  dataset_id, title, description,
            variables, geographies, concepts, keywords, tags, and
            vintage.

        """
        if not self.writer:
            raise CensusError('Text indexer called outside of context manager')
        for vals in iterable:
            self.writer.add_document(
                **dict(zip(self.schema_fields, vals)))

    def query(self, querystring: str, **query_ignored):
        """Find dataset IDs matching querystring"""
        query = self.qparser.parse(querystring)
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=None)
            ret = []
            for hit in results:
                val = dict(hit.items())
                val['score'] = hit.score
                ret.append(val)
            return ret
