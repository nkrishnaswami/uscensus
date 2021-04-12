from collections import namedtuple

from ..util.textindex.whooshindex import ApiSchemaFields, WhooshIndex


def Index_test():
    index = WhooshIndex('index', 'API', 'title')
    Row = namedtuple('Row', ApiSchemaFields)
    data = [
        Row(api_id='id1',
            title='title one',
            description='description of api one',
            geographies='',
            concepts='',
            keywords='key key1',
            tags='tag tag1',
            variables='var1',
            vintage='2015'),
        Row(api_id='id2',
            title='title two',
            description='description of api two',
            geographies='',
            concepts='',
            keywords='key key2',
            tags='tag tag2',
            variables='var2',
            vintage='2015'),
    ]
    with index:
        index.add(data)

    def api_ids(results):
        def gen_api_ids(results):
            for hit in results:
                yield hit['api_id']
        return list(gen_api_ids(results))
    assert api_ids(index.query('one')) == ['id1']
    assert api_ids(index.query('two')) == ['id2']
    assert sorted(api_ids(index.query('title'))) == ['id1', 'id2']
    assert api_ids(index.query('title:one')) == ['id1']
    assert api_ids(index.query('title:two')) == ['id2']

    assert api_ids(index.query('description:one')) == ['id1']
    assert api_ids(index.query('description:two')) == ['id2']

    assert api_ids(index.query('keywords:key1')) == ['id1']
    assert api_ids(index.query('keywords:key2')) == ['id2']
    assert sorted(api_ids(index.query('keywords:key'))) == \
        ['id1', 'id2']
    assert api_ids(index.query('tags:tag1')) == ['id1']
    assert api_ids(index.query('tags:tag2')) == ['id2']
    assert sorted(api_ids(index.query('tags:tag'))) == \
        ['id1', 'id2']
