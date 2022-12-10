import json
import pytest

import httpx
from httpx_caching import CachingClient

from .webcache import afetch


class AsyncMockTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, req):
        message = {"text": "Hello, world!"}
        content = json.dumps(message).encode("utf-8")
        stream = httpx.ByteStream(content)
        headers = [(b"content-type", b"application/json")]
        return httpx.Response(200, headers=headers, stream=stream, request=req)


@pytest.mark.asyncio
async def test_afetch():
    session = CachingClient(httpx.AsyncClient(
        transport=AsyncMockTransport()))
    r = await afetch('https://fake.invalid', session)
    r.raise_for_status()
    assert r.status_code == 200
    assert r.headers['content-type'] == 'application/json'
    assert r.json()['text'] == "Hello, world!"
    assert r.request.url == "https://fake.invalid"
