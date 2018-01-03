from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict
from whoosh.analysis.filters import StopFilter
from whoosh.analysis import (KeywordAnalyzer, StandardAnalyzer)
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.fields import Schema, KEYWORD, ID, TEXT
from whoosh.qparser import QueryParser
from whoosh.writing import AsyncWriter


KWAnalyzer = KeywordAnalyzer(lowercase=True) | StopFilter()
Analyzer = StandardAnalyzer()
ApiSchemaFields = OrderedDict((
    ('api_id', ID(unique=True, stored=True)),
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
    ('api_id', ID(stored=True)),
    ('variable', ID(stored=True)),
    ('group', ID(stored=True)),
    ('label', TEXT(analyzer=Analyzer)),
    ('concept', KEYWORD(analyzer=Analyzer)),
))


class Index(object):
    """Census API metadata indexer."""
    def __init__(self, name, schema_fields, dflt_query_field, path=None):
        """Initialize Whoosh index specified fields.

          Arguments:
            * schema_fields: an OrderedDict of column names to whoosh
              field types.
            * path: if specified, the path in which to create a
              persistent index. If not specified, index to RAM.
        """
        self.schema_fields = schema_fields
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

    def add(self, iterator, **kwargs):
        """Add entries to the index

        Arguments:
          * iterator: iterator over tuples of field metadata, viz.
            api_id, title, description, variables, geographies, concepts,
            keywords, tags, and vintage.
        """
        for vals in iterator:
            self.writer.add_document(
                **dict(zip(self.schema_fields, vals)))

    def query(self, querystring):
        """Find API IDs matching querystring"""
        query = self.qparser.parse(querystring)
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=None)
            ret = []
            for hit in results:
                val = dict(hit.items())
                val['score'] = hit.score
                ret.append(val)
            return ret
