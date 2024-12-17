import json

import httpx
import pytest
from httpx_caching import CachingClient

from uscensus.util.webcache import afetch, fetch


class MockAsyncTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, req):
        message = {'text': 'Hello, world!'}
        content = json.dumps(message).encode('utf-8')
        stream = httpx.ByteStream(content)
        headers = [(b'content-type', b'application/json')]
        return httpx.Response(200, headers=headers, stream=stream, request=req)


class MockSyncTransport(httpx.BaseTransport):
    def handle_request(self, req):
        message = {'text': 'Hello, world!'}
        content = json.dumps(message).encode('utf-8')
        stream = httpx.ByteStream(content)
        headers = [(b'content-type', b'application/json')]
        return httpx.Response(200, headers=headers, stream=stream, request=req)


def test_fetch():
    session = CachingClient(httpx.Client(transport=MockSyncTransport()))
    r = fetch('https://fake.invalid', session)
    r.raise_for_status()
    assert r.status_code == 200
    assert r.headers['content-type'] == 'application/json'
    assert r.json()['text'] == 'Hello, world!'
    assert r.request.url == 'https://fake.invalid'


@pytest.mark.asyncio
async def test_afetch():
    session = CachingClient(httpx.AsyncClient(transport=MockAsyncTransport()))
    r = await afetch('https://fake.invalid', session)
    r.raise_for_status()
    assert r.status_code == 200
    assert r.headers['content-type'] == 'application/json'
    assert r.json()['text'] == 'Hello, world!'
    assert r.request.url == 'https://fake.invalid'
