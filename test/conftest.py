import json
import logging
from importlib.resources import files

import httpx
import pytest

from uscensus.util.datastores import AsyncNopDataStore, SyncNopDataStore
from uscensus.util.webcache import make_client

_logger = logging.getLogger(__name__)
resource_files = files('test.sample_data')


@pytest.fixture
def catalog():
    return json.loads(resource_files.joinpath('data.json').read_text())


@pytest.fixture
def examples():
    return json.loads(resource_files.joinpath('examples.json').read_text())


@pytest.fixture
def geography():
    return json.loads(resource_files.joinpath('geography.json').read_text())


@pytest.fixture
def groups():
    return json.loads(resource_files.joinpath('groups.json').read_text())


@pytest.fixture
def one_group():
    return json.loads(resource_files.joinpath('group_B17015.json').read_text())


@pytest.fixture
def tags():
    return json.loads(resource_files.joinpath('tags.json').read_text())


@pytest.fixture
def variables():
    return json.loads(resource_files.joinpath('variables.json').read_text())


@pytest.fixture
def query_results():
    return json.loads(resource_files.joinpath('query_results.json').read_text())


def make_response(data):
    return httpx.Response(200,
                          headers={'content-type': 'application/json'},
                          content=json.dumps(data))


class FakeHttpxTransport:
    def __init__(self, catalog, examples, geography, one_group, groups,
                 tags, variables, query_results) -> None:
        self.catalog = catalog
        self.examples = examples
        self.geography = geography
        self.one_group = one_group
        self.groups = groups
        self.tags = tags
        self.variables = variables
        self.query_results = query_results

    def handle_request(self, req):
        if req.url.path.endswith('data.json'):
            return make_response(self.catalog)
        if req.url.path.endswith('examples.json'):
            return make_response(self.examples)
        if req.url.path.endswith('geography.json'):
            return make_response(self.geography)
        if req.url.path.endswith('groups.json'):
            return make_response(self.groups)
        if req.url.path.find('/groups/') >= 0:
            return make_response(self.one_group)
        if req.url.path.endswith('tags.json'):
            return make_response(self.tags)
        if req.url.path.endswith('variables.json'):
            return make_response(self.variables)
        if 'for' in req.url.params or 'tabulate' in req.urls.params:
            return make_response(self.query_results)
        _logger.warning('Unexpected url: %s', req.url)
        return httpx.Response(404)

    async def handle_async_request(self, req):
        return self.handle_request(req)


class FakeHttpxTransportSync(FakeHttpxTransport, httpx.BaseTransport):
    pass


class FakeHttpxTransportAsync(FakeHttpxTransport, httpx.AsyncBaseTransport):
    pass


@pytest.fixture
def httpx_transport_single_sync(catalog, examples, geography,
                                one_group, groups, tags, variables, query_results):
    catalog['dataset'] = [catalog['dataset'][0]]
    return FakeHttpxTransportSync(catalog, examples, geography,
                                  one_group, groups, tags, variables,
                                  query_results)


@pytest.fixture
def httpx_transport_full_sync(catalog, examples, geography,
                              one_group, groups, tags, variables, query_results):
    return FakeHttpxTransportSync(catalog, examples, geography,
                                  one_group, groups, tags, variables,
                                  query_results)


@pytest.fixture
def httpx_transport_single_async(catalog, examples, geography,
                                 one_group, groups, tags, variables, query_results):
    catalog['dataset'] = [catalog['dataset'][0]]
    return FakeHttpxTransportAsync(catalog, examples, geography,
                                   one_group, groups, tags, variables,
                                   query_results)


@pytest.fixture
def httpx_transport_full_async(catalog, examples, geography,
                               one_group, groups, tags, variables, query_results):
    return FakeHttpxTransportAsync(catalog, examples, geography,
                                   one_group, groups, tags, variables,
                                   query_results)


@pytest.fixture
def async_cache():
    return AsyncNopDataStore()


@pytest.fixture
def sync_cache():
    return SyncNopDataStore()


@pytest.fixture
def httpx_client_single_sync(sync_cache, httpx_transport_single_sync):
    return make_client(cache=sync_cache, transport=httpx_transport_single_sync, sync=True)


@pytest.fixture
def httpx_client_full_sync(sync_cache, httpx_transport_full_sync):
    return make_client(cache=sync_cache, transport=httpx_transport_full_sync, sync=True)


@pytest.fixture
def httpx_client_single_async(async_cache, httpx_transport_single_async):
    return make_client(cache=async_cache, transport=httpx_transport_single_async, sync=False)


@pytest.fixture
def httpx_client_full_async(async_cache, httpx_transport_full_async):
    return make_client(cache=async_cache, transport=httpx_transport_full_async, sync=False)
