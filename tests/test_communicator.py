import types
from decimal import Decimal
import json
import asyncio

import pytest
import requests
import httpx

from datastore_sdk.communicator import Communicator
from datastore_sdk.exceptions import AuthError, APICompatibilityError, APIConcurrencyLimitError


# ------------------------
# Helpers
# ------------------------

def make_requests_response(status: int, payload):
    resp = requests.Response()
    resp.status_code = status
    body = json.dumps(payload).encode("utf-8")
    resp._content = body
    resp.headers["Content-Type"] = "application/json"
    return resp


def make_httpx_response(status: int, payload):
    # Provide a dummy request so that response.raise_for_status() is callable
    req = httpx.Request("GET", "https://dummy")
    return httpx.Response(status_code=status, json=payload, request=req)


def fake_get_factory(response, expected_url=None, expected_params=None, expected_headers=None):
    """Factory for a requests.get stub with optional assertions."""
    def _fake_get(u, params=None, headers=None, **kwargs):
        if expected_url is not None:
            assert u == expected_url
        if expected_params is not None:
            assert params == expected_params
        if expected_headers is not None:
            assert headers == expected_headers
        return response
    return _fake_get


def fake_post_factory(response, expected_url=None, expected_params=None, expected_headers=None, expected_json=None):
    """Factory for a requests.post stub with optional assertions."""
    def _fake_post(u, params=None, headers=None, json=None, **kwargs):
        if expected_url is not None:
            assert u == expected_url
        if expected_params is not None:
            assert params == expected_params
        if expected_headers is not None:
            assert headers == expected_headers
        if expected_json is not None:
            assert json == expected_json
        return response
    return _fake_post


class DummyAsyncClient:
    """Reusable dummy for httpx.AsyncClient with injectable callbacks."""
    def __init__(self, get_cb=None, post_cb=None, *args, **kwargs):
        self._get_cb = get_cb
        self._post_cb = post_cb
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False
    async def get(self, u, params=None, headers=None):
        if self._get_cb is None:
            raise RuntimeError("No get_cb provided for DummyAsyncClient")
        return await self._maybe_await(self._get_cb(u=u, params=params, headers=headers))
    async def post(self, u, params=None, headers=None, json=None):
        if self._post_cb is None:
            raise RuntimeError("No post_cb provided for DummyAsyncClient")
        return await self._maybe_await(self._post_cb(u=u, params=params, headers=headers, json=json))
    @staticmethod
    async def _maybe_await(val):
        if isinstance(val, types.CoroutineType) or hasattr(val, "__await__"):
            return await val
        return val


class DummyRaw:
    """Mimics the .raw attribute of a requests Response for streaming."""
    def __init__(self):
        self.decode_content = False


class DummyCtx:
    """Synchronous context manager that yields a given value."""
    def __init__(self, yield_value):
        self._yield_value = yield_value
    def __enter__(self):
        return self._yield_value
    def __exit__(self, exc_type, exc, tb):
        return False


class DummyStream:
    """Simple object with a .content attribute for aiohttp streaming tests."""
    def __init__(self, content=None):
        self.content = content if content is not None else object()


class DummySession:
    """Lightweight replacement for aiohttp.ClientSession used in tests.

    Provides a .get(...) method returning an async context manager that yields a DummyStream.
    """
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False
    def get(self, *args, **kwargs):
        stream = DummyStream()
        class _Ctx:
            async def __aenter__(self_inner):
                return stream
            async def __aexit__(self_inner, exc_type, exc, tb):
                return False
        return _Ctx()


# ------------------------
# Sync GET
# ------------------------

def test_send_get_request_success(monkeypatch):
    url = "https://example.com/items"
    token = "abc"
    payload = {"ok": True, "items": [1, 2, 3]}

    monkeypatch.setattr(
        requests,
        "get",
        fake_get_factory(
            response=make_requests_response(200, payload),
            expected_url=url,
            expected_params={"q": "x"},
            expected_headers={"Authorization": f"Bearer {token}"},
        ),
    )

    out = Communicator.send_get_request(url, token, q="x")
    assert out == payload


def test_send_get_request_auth_error(monkeypatch):
    def fake_get(u, params=None, headers=None):
        return make_requests_response(401, {"detail": "unauthorized"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(AuthError):
        Communicator.send_get_request("https://e", "tok")


def test_send_get_request_not_found(monkeypatch):
    def fake_get(u, params=None, headers=None):
        return make_requests_response(404, {"detail": "missing"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(APICompatibilityError):
        Communicator.send_get_request("https://e", "tok")


def test_send_get_request_rate_limited(monkeypatch):
    def fake_get(u, params=None, headers=None):
        return make_requests_response(429, {"detail": "rate limited"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(APIConcurrencyLimitError):
        Communicator.send_get_request("https://e", "tok")


def test_send_get_request_other_error(monkeypatch):
    def fake_get(u, params=None, headers=None):
        # 500 -> should propagate Response.raise_for_status() as HTTPError
        return make_requests_response(500, {"detail": "oops"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        Communicator.send_get_request("https://e", "tok")


# ------------------------
# Sync POST
# ------------------------

def test_send_post_request_success(monkeypatch):
    url = "https://example.com/create"
    token = "abc"
    payload = {"id": 1}

    monkeypatch.setattr(
        requests,
        "post",
        fake_post_factory(
            response=make_requests_response(200, payload),
            expected_url=url,
            expected_headers={"Authorization": f"Bearer {token}"},
            expected_json={"name": "Bob"},
        ),
    )

    out = Communicator.send_post_request(url, token, {"name": "Bob"})
    assert out == payload


def test_send_post_request_error_mapping(monkeypatch):
    cases = [
        (401, AuthError),
        (403, AuthError),
        (404, APICompatibilityError),
        (429, APIConcurrencyLimitError),
    ]

    for status, exc in cases:
        def fake_post(u, params=None, headers=None, json=None, _status=status):
            return make_requests_response(_status, {})
        monkeypatch.setattr(requests, "post", fake_post, raising=True)
        with pytest.raises(exc):
            Communicator.send_post_request("https://e", "tok", {})

    # other error
    def fake_post_500(u, params=None, headers=None, json=None):
        return make_requests_response(500, {})

    monkeypatch.setattr(requests, "post", fake_post_500)
    with pytest.raises(requests.HTTPError):
        Communicator.send_post_request("https://e", "tok", {})


# ------------------------
# Async GET/POST
# ------------------------
@pytest.mark.asyncio
async def test_send_get_request_async_success(monkeypatch):
    url = "https://api/x"
    token = "t"
    payload = {"x": 1}

    async def get_cb(u, params=None, headers=None):
        assert u == url
        assert headers == {"Authorization": f"Bearer {token}"}
        return make_httpx_response(200, payload)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(get_cb=get_cb))

    out = await Communicator.send_get_request_async(url, token)
    assert out == payload


@pytest.mark.asyncio
async def test_send_get_request_async_error_mapping(monkeypatch):
    async def run_status(status, expected_exc):
        async def get_cb(u, params=None, headers=None):
            return make_httpx_response(status, {})
        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(get_cb=get_cb))
        if expected_exc is None:
            with pytest.raises(httpx.HTTPStatusError):
                await Communicator.send_get_request_async("https://e", "t")
        else:
            with pytest.raises(expected_exc):
                await Communicator.send_get_request_async("https://e", "t")

    for status, exc in [(401, AuthError), (403, AuthError), (404, APICompatibilityError), (429, APIConcurrencyLimitError), (500, None)]:
        await run_status(status, exc)


@pytest.mark.asyncio
async def test_send_post_request_async_success(monkeypatch):
    payload = {"ok": True}

    async def post_cb(u, params=None, headers=None, json=None):
        assert json == {"n": 1}
        return make_httpx_response(200, payload)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(post_cb=post_cb))

    out = await Communicator.send_post_request_async("https://e", "t", {"n": 1})
    assert out == payload


@pytest.mark.asyncio
async def test_send_post_request_async_error_mapping(monkeypatch):
    async def run_status(status, expected_exc):
        async def post_cb(u, params=None, headers=None, json=None):
            return make_httpx_response(status, {})
        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(post_cb=post_cb))
        if expected_exc is None:
            with pytest.raises(httpx.HTTPStatusError):
                await Communicator.send_post_request_async("https://e", "t", {})
        else:
            with pytest.raises(expected_exc):
                await Communicator.send_post_request_async("https://e", "t", {})

    for status, exc in [(401, AuthError), (403, AuthError), (404, APICompatibilityError), (429, APIConcurrencyLimitError), (500, None)]:
        await run_status(status, exc)


# ------------------------
# Streaming (sync)
# ------------------------

def test_steaming_get_generator_yields_converted_items(monkeypatch):
    # Prepare a fake streaming Response that passes isinstance(Response)
    stream_resp = make_requests_response(200, [])

    # patch raw so that ijson.items can be called; but we will patch ijson.items to ignore input
    stream_resp.raw = DummyRaw()

    # Patch requests.get to return a context manager yielding our response
    def fake_get(u, params=None, headers=None, stream=None, timeout=None):
        assert stream is True
        return DummyCtx(stream_resp)

    monkeypatch.setattr(requests, "get", fake_get)

    # Patch ijson.items to yield two objects
    import ijson as _ijson

    def fake_items(source, prefix):
        for obj in [
            {"a": Decimal("1.5"), "b": "ok"},
            {"a": 2, "b": "Infinity"},
        ]:
            yield obj

    monkeypatch.setattr(_ijson, "items", fake_items)

    out = list(Communicator.steaming_get_generator("https://e", "t"))
    assert out == [
        {"a": 1.5, "b": "ok"},
        {"a": 2, "b": None},
    ]


def test_steaming_get_generator_error_mapping(monkeypatch):
    # Use various statuses to ensure mapping
    def run_status(status, expected_exc):
        stream_resp = make_requests_response(status, [])
        def fake_get(u, params=None, headers=None, stream=None, timeout=None):
            return DummyCtx(stream_resp)
        monkeypatch.setattr(requests, "get", fake_get)
        if expected_exc is None:
            with pytest.raises(requests.HTTPError):
                list(Communicator.steaming_get_generator("https://e", "t"))
        else:
            with pytest.raises(expected_exc):
                list(Communicator.steaming_get_generator("https://e", "t"))

    for status, exc in [(401, AuthError), (403, AuthError), (404, APICompatibilityError), (429, APIConcurrencyLimitError), (500, None)]:
        run_status(status, exc)


# ------------------------
# Streaming (async)
# ------------------------
@pytest.mark.asyncio
async def test_steaming_get_generator_async_yields_converted_items(monkeypatch):
    # We'll bypass strict type checking by patching Communicator._check_response_status_code to no-op
    monkeypatch.setattr(Communicator, "_check_response_status_code", lambda _r: None)

    # Fake session.get async context manager returning an object with .content
    import aiohttp as _aiohttp
    monkeypatch.setattr(_aiohttp, "ClientSession", lambda timeout=None: DummySession())

    # Patch ijson.items to return an async iterator
    import ijson as _ijson

    async def async_gen():
        for obj in [
            {"a": Decimal("2.5")},
            {"a": "Infinity"},
        ]:
            await asyncio.sleep(0)
            yield obj

    monkeypatch.setattr(_ijson, "items", lambda source, prefix: async_gen())

    items = []
    async for obj in Communicator.steaming_get_generator_async("https://e", "t"):
        items.append(obj)

    assert items == [{"a": 2.5}, {"a": None}]


@pytest.mark.asyncio
async def test_steaming_get_generator_async_error_mapping(monkeypatch):
    # Here we simulate that _check_response_status_code raises specific exceptions
    def make_check(exc):
        def _check(_):
            if isinstance(exc, type) and issubclass(exc, Exception):
                raise exc()
            # Otherwise raise HTTPStatus-like error by mimicking raise_for_status behavior
        return _check

    for exc in [AuthError, APICompatibilityError, APIConcurrencyLimitError]:
        monkeypatch.setattr(Communicator, "_check_response_status_code", make_check(exc))

        import aiohttp as _aiohttp
        monkeypatch.setattr(_aiohttp, "ClientSession", lambda timeout=None: DummySession())

        with pytest.raises(exc):
            async for _ in Communicator.steaming_get_generator_async("https://e", "t"):
                pass
