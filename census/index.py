from whoosh.filedb.filestore import RamStorage
from whoosh.fields import Schema, ID, KEYWORD, TEXT
from whoosh.index import FileIndex
from whoosh.qparser import QueryParser


class Index(object):
    """Census API metadata indexer."""

    def __init__(self):
        """Initialize Whoosh index for Census API metadata fields"""
        self.schema = Schema(
            api_id=ID(stored=True),
            title=TEXT(stored=True),
            description=TEXT,
            variables=KEYWORD,
            geographies=KEYWORD,
            concepts=KEYWORD,
            keywords=KEYWORD,
            tags=KEYWORD,
            vintage=KEYWORD,
            )
        # Initialize index
        self.index = RamStorage().create_index(self.schema)
        self.qparser = QueryParser("title", schema=self.schema)

    def add(self, iterator):
        """Add entries to the index

        Arguments:
          * iterator: iterator over tuples of field metadata, viz.
            api_id, title, description, variables, geographies, concepts,
            keywords, tags, and vintage.
        """

        with self.index.writer() as writer:
            for (api_id, title, description, variables,
                 geographies, concepts, keywords, tags, vintage
            ) in iterator:
                writer.update_document(
                    api_id=api_id,
                    title=title,
                    description=description,
                    variables=variables,
                    geographies=geographies,
                    concepts=concepts,
                    keywords=keywords,
                    tags=tags,
                    vintage=vintage,
                )

    def query(self, querystring):
        """Find API IDs matching querystring"""

        query = self.qparser.parse(querystring)
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=None)
            return [result['api_id'] for result in results]
