from whoosh.filedb.filestore import RamStorage
from whoosh.fields import Schema, ID, KEYWORD, TEXT
from whoosh.index import FileIndex
from whoosh.qparser import QueryParser

class Index(object):
    def __init__(self):
        self.schema = Schema(
            api_id=ID(stored=True),
            title=TEXT(stored=True),
            description=TEXT,
            variables=KEYWORD,
            geographies=KEYWORD,
            concepts=KEYWORD,
            keywords=KEYWORD,
            tags=KEYWORD,
            )
        # Initialize index
        self.index = RamStorage().create_index(self.schema)
        self.qparser = QueryParser("title", schema=self.schema)
    def add(self, iterator):
        with self.index.writer() as writer:
            for (api_id, title, description, variables,
                 geographies, concepts, keywords, tags) in iterator:
                writer.update_document(
                    api_id=api_id,
                    title=title,
                    description=description,
                    variables=variables,
                    geographies=geographies,
                    concepts=concepts,
                    keywords=keywords,
                    tags=tags,
                )
    def query(self, querystring):
        query = self.qparser.parse(querystring)
        with self.index.searcher() as searcher:
            results = searcher.search(query, limit=None)
            return [result['api_id'] for result in results]
