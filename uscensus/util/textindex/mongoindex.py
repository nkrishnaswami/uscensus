from typing import Iterable, Union

from pymongo import MongoClient

from .textindex import TextIndex, DatasetFields, VariableFields


class MongoIndex(TextIndex):
    """Census API metadata indexer based on MongoDb."""
    def __init__(self, name,
                 client: MongoClient = MongoClient(
                     'mongodb://localhost:27017'),
                 db: str = 'census',
                 dflt_query_field: str = 'dataset_id'):
        """Initialize index specified fields.

          Arguments:
            * name: name of the Collection for the metadata.
            * client: Client connected to MongoDB
            * db: name of the MongoDB DB to use.
            * dflt_query_field: the default field to query.
        """
        client[db].drop_collection(name)
        self.coll = client[db][name]

    def __enter__(self):
        """Context manager that drops the index pre-insertion."""
        self.coll.drop_index('text_index')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager that indexes the collection
        post-insertion."""
        self.coll.create_index(
            [('$**', 'text')],
            name='text_index',
            default_language='en')

    def add(self,
            iterable: Union[Iterable[DatasetFields], Iterable[VariableFields]],
            **kwargs):
        """Add entries to the index

        Arguments:
          * iteratble: iterator over tuples of field metadata, viz.
            dataset_id, title, description, variables, geographies, concepts,
            keywords, tags, and vintage.
        """
        docs = [doc._asdict() for doc in iterable]
        if docs:
            self.coll.insert_many(docs)

    def query(self, querystring: str, **query):
        """Find dataset IDs matching querystring"""
        ret = []
        query.update({'$text': {'$search': querystring}})
        for doc in self.coll.find(
                query,
                {'_id': False,
                 'score': {'$meta': "textScore"}}
        ).sort([('score', {'$meta': "textScore"})]):
            ret.append(doc)
        return ret
