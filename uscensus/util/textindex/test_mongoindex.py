from pymongo import MongoClient

from ...util.textindex import FieldSet, DatasetFields
from ...util.textindex.mongoindex import MongoIndex


def test_Index():
    index = MongoIndex(FieldSet.DATASET, 'varindex')
    data = [
        DatasetFields(dataset_id='id1',
                      title='title one',
                      description='description of dataset one',
                      geographies='',
                      concepts='',
                      keywords='key key1',
                      tags='tag tag1',
                      variables='var1',
                      vintage='2015'),
        DatasetFields(dataset_id='id2',
                      title='title two',
                      description='description of dataset two',
                      geographies='',
                      concepts='',
                      keywords='key key2',
                      tags='tag tag2',
                      variables='var2',
                      vintage='2015'),
    ]
    with index:
        index.add(data)

    def dataset_ids(results):
        def gen_dataset_ids(results):
            for hit in results:
                yield hit['dataset_id']
        return list(gen_dataset_ids(results))
    assert dataset_ids(index.query('one')) == ['id1']
    assert dataset_ids(index.query('two')) == ['id2']
    assert sorted(dataset_ids(index.query('title'))) == ['id1', 'id2']
    assert dataset_ids(index.query('', title={'$regex': 'one'})) == ['id1']
    assert dataset_ids(index.query('', title={'$regex': 'two'})) == ['id2']

    assert dataset_ids(index.query('', description={'$regex': 'one'})) == ['id1']
    assert dataset_ids(index.query('', description={'$regex': 'two'})) == ['id2']

    assert dataset_ids(index.query('', keywords={'$regex': 'key1'})) == ['id1']
    assert dataset_ids(index.query('', keywords={'$regex': 'key2'})) == ['id2']
    assert sorted(dataset_ids(index.query('', keywords={'$regex': 'key'}))) == \
        ['id1', 'id2']
    assert dataset_ids(index.query('', tags={'$regex': 'tag1'})) == ['id1']
    assert dataset_ids(index.query('', tags={'$regex': 'tag2'})) == ['id2']
    assert sorted(dataset_ids(index.query('', tags={'$regex': 'tag'}))) == \
        ['id1', 'id2']
