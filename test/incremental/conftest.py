import logging
from importlib.resources import files

import httpx
import pytest

from uscensus.util.webcache import make_client

_logger = logging.getLogger(__name__)
resource_files = files('test.sample_data.microdata')


def make_response(data):
    return httpx.Response(200,
                          headers={'content-type': 'application/json'},
                          content=data)


def read_file(basename):
    return resource_files.joinpath(f'{basename}.json').read_text()


class FakeHttpxTransport:
    def __init__(self, catalog, examples, geography, groups,
                 variables, query_results) -> None:
        self.catalog = catalog
        self.examples = examples
        self.geography = geography
        self.groups = groups
        self.variables = variables
        self.query_results = query_results

    def handle_request(self, req):
        if req.url.path.endswith('/apr.json'):
            return make_response(self.catalog)
        if req.url.path.endswith('/apr/examples.json'):
            return make_response(self.examples)
        if req.url.path.endswith('/apr/geography.json'):
            return make_response(self.geography)
        if req.url.path.endswith('/apr/groups.json'):
            return make_response(self.groups)
        if req.url.path.endswith('/apr/variables.json'):
            return make_response(self.variables)
        if req.url.path.endswith('/apr'):
            return make_response(self.query_results)
        _logger.warning('Unexpected url: %s', req.url)
        return httpx.Response(404)

    async def handle_async_request(self, req):
        return self.handle_request(req)


class FakeHttpxTransportSync(FakeHttpxTransport, httpx.BaseTransport):
    pass


class FakeHttpxTransportAsync(FakeHttpxTransport, httpx.AsyncBaseTransport):
    pass


@pytest.fixture()
def udata_httpx_client_sync(request, sync_cache):
    transport = FakeHttpxTransportSync(read_file('apr'),
                                       read_file('examples'),
                                       read_file('geography'),
                                       read_file('groups'),
                                       read_file('variables'),
                                       read_file(request.param))
    return make_client(cache=sync_cache, transport=transport, sync=True)


@pytest.fixture()
def udata_httpx_client_async(request, async_cache):
    transport = FakeHttpxTransportAsync(read_file('apr'),
                                        read_file('examples'),
                                        read_file('geography'),
                                        read_file('groups'),
                                        read_file('variables'),
                                        read_file(request.param))
    return make_client(cache=async_cache, transport=transport, sync=True)
