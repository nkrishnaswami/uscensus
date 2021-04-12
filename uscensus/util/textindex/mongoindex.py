from pymongo import MongoClient

from .textindexbase import TextIndexBase


class MongoIndex(TextIndexBase):
    """Census API metadata indexer based on MongoDb."""
    def __init__(self, name,
                 client=MongoClient('mongodb://localhost:27017'),
                 db='census', dflt_query_field='api_id'):
        """Initialize index specified fields.

          Arguments:
            * name: Collection to index to.
            * dflt_query_field: the default field to query.
        """
        client[db].drop_collection(name)
        self.coll = client[db][name]

    def __enter__(self):
        self.coll.drop_index('text_index')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.coll.create_index(
            [('$**', 'text')],
            name='text_index',
            default_language='en')

    def add(self, iterator, **kwargs):
        """Add entries to the index

        Arguments:
          * iterator: iterator over tuples of field metadata, viz.
            api_id, title, description, variables, geographies, concepts,
            keywords, tags, and vintage.
        """
        docs = [doc for doc in iterator]
        if docs:
            self.coll.insert_many(docs)

    def query(self, querystring, **query):
        """Find API IDs matching querystring"""
        ret = []
        query.update({'$text': {'$search': querystring}})
        for doc in self.coll.find(
                query,
                {'_id': False,
                 'score': {'$meta': "textScore"}}
        ).sort([('score', {'$meta': "textScore"})]):
            ret.append(doc)
        return ret


if __name__ == '__main__':
    idx = MongoIndex('test')
    with idx:
        idx.add([
            {'a': ['a cage', 'b']},
            {'b': ['a cage', 'c']},
        ])
    print(idx.query('cage'))
