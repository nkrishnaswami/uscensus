import logging

from uscensus.incremental import wrappers


def test_catalog(catalog, groups, one_group, variables, httpx_client_single):
    cat = wrappers.Catalog.get_catalog(httpx_client_single)
    assert len(cat.dataset) == 1
    ds = cat.dataset[0]
    raw = catalog['dataset'][0]
    assert ds.c_dataset == raw['c_dataset']
    assert ds.title == raw['title']
    assert ds.description == raw['description']
    assert len(ds.variables) == len(variables['variables'])
    for (key1, var), (key2, rawvar) in zip(ds.variables.items(),
                                           variables['variables'].items()):
        assert key1 == key2
        assert var.label == rawvar['label']
    assert len(ds.groups) == len(groups['groups'])
    logging.info(ds.groups)
    raw_group = groups['groups'][0]
    group = ds.groups[raw_group['name']]
    assert group.name == raw_group['name']
    assert group.description == raw_group['description']
    assert len(group.variables) == len(one_group['variables'])
