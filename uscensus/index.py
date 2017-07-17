from __future__ import print_function, unicode_literals

from collections import OrderedDict
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.fields import Schema, ID, KEYWORD, TEXT
from whoosh.qparser import QueryParser

class Index(object):
    """Census API metadata indexer."""

    _SchemaFields = OrderedDict((
        ('api_id', ID(stored=True)),
        ('title', TEXT(stored=True)),
        ('description', TEXT),
        ('variables', KEYWORD),
        ('geographies', KEYWORD),
        ('concepts', KEYWORD),
        ('keywords', KEYWORD),
        ('tags', KEYWORD),
        ('vintage', KEYWORD),
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

    def add(self, iterator):
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
