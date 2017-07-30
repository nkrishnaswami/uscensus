from __future__ import print_function, unicode_literals

from collections import OrderedDict
from whoosh.analysis import (KeywordAnalyzer, StandardAnalyzer)
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.fields import Schema, KEYWORD, ID, TEXT
from whoosh.qparser import QueryParser


class Index(object):
    """Census API metadata indexer."""
    _KWAnalyzer = KeywordAnalyzer(lowercase=True)
    _Analyzer = StandardAnalyzer()
    _SchemaFields = OrderedDict((
        ('api_id', ID(stored=True)),
        ('title', KEYWORD(stored=True, analyzer=_KWAnalyzer)),
        ('description', TEXT(analyzer=_Analyzer)),
        ('variables', KEYWORD(analyzer=_KWAnalyzer)),
        ('geographies', KEYWORD(analyzer=_KWAnalyzer)),
        ('concepts', KEYWORD(stored=True, analyzer=_KWAnalyzer)),
        ('keywords', KEYWORD(stored=True, analyzer=_KWAnalyzer)),
        ('tags', KEYWORD(stored=True, analyzer=_KWAnalyzer)),
        ('vintage', ID),
    ))
    _CensusMetadataSchema = Schema(**_SchemaFields)

    def __init__(self, path=None):
        """Initialize Whoosh index for Census API metadata fields"""
        # Initialize index
        fs = FileStorage(path).create() if path else RamStorage()
        if fs.index_exists():
            self.index = fs.open_index()
        else:
            self.index = fs.create_index(self._CensusMetadataSchema)
        self.qparser = QueryParser("title", schema=self._CensusMetadataSchema)

    def add(self, iterator, **kwargs):
        """Add entries to the index

        Arguments:
          * iterator: iterator over tuples of field metadata, viz.
            api_id, title, description, variables, geographies, concepts,
            keywords, tags, and vintage.
        """
        with self.index.writer() as writer:
            for vals in iterator:
                writer.update_document(
                    **dict(zip(self._SchemaFields, vals)))

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
