from __future__ import print_function
from __future__ import unicode_literals

from ..data.index import Index, ApiSchemaFields

from collections import namedtuple


def Index_test():
    index = Index(ApiSchemaFields)
    Row = namedtuple('Row', ApiSchemaFields)
    data = [
        Row(api_id='id1',
            title='title one',
            description='description of api one',
            variable_name='var1_1 var1_2',
            variable_desc='Variable One One Variable One Two',
            geographies='',
            concepts='',
            keywords='key key1',
            tags='tag tag1',
            vintage='2015'),
        Row(api_id='id2',
            title='title two',
            description='description of api two',
            variable_name='var2_1 var2_2',
            variable_desc='Variable Two One Variable Two Two',
            geographies='',
            concepts='',
            keywords='key key2',
            tags='tag tag2',
            vintage='2015'),
    ]
    index.add(data)

    def api_ids(results):
        def gen_api_ids(results):
            for hit in results:
                yield hit['title']
        return list(gen_api_ids(results))
    assert api_ids(index.query('one')) == ['title one']
    assert api_ids(index.query('two')) == ['title two']
    assert sorted(api_ids(index.query('title'))) == ['title one', 'title two']
    assert api_ids(index.query('title:one')) == ['title one']
    assert api_ids(index.query('title:two')) == ['title two']

    assert api_ids(index.query('description:one')) == ['title one']
    assert api_ids(index.query('description:two')) == ['title two']

    assert api_ids(index.query('keywords:key1')) == ['title one']
    assert api_ids(index.query('keywords:key2')) == ['title two']
    assert sorted(api_ids(index.query('keywords:key'))) == \
        ['title one', 'title two']
    assert api_ids(index.query('tags:tag1')) == ['title one']
    assert api_ids(index.query('tags:tag2')) == ['title two']
    assert sorted(api_ids(index.query('tags:tag'))) == \
        ['title one', 'title two']
