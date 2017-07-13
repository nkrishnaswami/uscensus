from uscensus.index import Index


def Index_test():
    index = Index()
    data = [
        ('id1', 'title one', 'description of api one', ['var1_1', 'var1_2'],
         [], [], ['key', 'key1'], ['tag', 'tag1'], '2015'),
        ('id2', 'title two', 'description of api two', ['var2_1', 'var2_2'],
         [], [], ['key', 'key2'], ['tag', 'tag2'], '2015'),
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
