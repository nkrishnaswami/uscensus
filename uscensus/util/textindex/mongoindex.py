import logging
from typing import Iterable, Union

from pymongo import MongoClient

from .textindex import TextIndex, FieldSet, DatasetFields, VariableFields


_logger = logging.getLogger('pymongo')


class MongoIndex(TextIndex):
    """Census API metadata indexer based on MongoDb."""
    def __init__(self,
                 fieldset: FieldSet,
                 name: str,
                 client: MongoClient = MongoClient(
                     'mongodb://localhost:27017'),
                 db: str = 'varindex',
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
            documents: Union[Iterable[DatasetFields],
                             Iterable[VariableFields]],
            **kwargs):
        """Add entries to the index

        Arguments:
          * iteratble: iterator over tuples of field metadata, viz.
            dataset_id, title, description, variables, geographies, concepts,
            keywords, tags, and vintage.
        """
        if documents:
            self.coll.insert_many([doc._asdict() for doc in documents])

    def query(self, querystring: str, **colqueries):
        """Find dataset IDs matching querystring"""
        ret = []
        query = {}
        proj = {'_id': False}
        if querystring:
            query.update({'$text': {'$search': querystring}})
            proj['score'] = {'$meta': "textScore"}
        for col, colquery in colqueries.items():
            query[col] = colquery
        _logger.debug(f'Final query is {query}')
        find_op = self.coll.find(query, proj)
        if 'score' in proj:
            find_op = find_op.sort([('score', {'$meta': "textScore"})])
        for doc in find_op:
            ret.append(doc)
        return ret
