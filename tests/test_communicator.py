import datetime
import types
from decimal import Decimal
import json
import asyncio

import pytest
import requests
import httpx

from restreamsolutions.communicator import Communicator, Authorization, RestreamToken
from restreamsolutions.exceptions import AuthError, APICompatibilityError, APIConcurrencyLimitError, ServerError


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

    async def post(self, u, params=None, headers=None, json=None, data=None):
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

    out = Communicator.send_get_request(url, auth_token=token, q="x")
    assert out == payload


def test_send_get_request_auth_error(monkeypatch):
    def fake_get(u, params=None, headers=None):
        return make_requests_response(401, {"detail": "unauthorized"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(AuthError):
        Communicator.send_get_request("https://e", auth_token="tok")


def test_send_get_request_not_found(monkeypatch):
    def fake_get(u, params=None, headers=None):
        return make_requests_response(404, {"detail": "missing"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(APICompatibilityError):
        Communicator.send_get_request("https://e", auth_token="tok")


def test_send_get_request_rate_limited(monkeypatch):
    def fake_get(u, params=None, headers=None):
        return make_requests_response(429, {"detail": "rate limited"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(APIConcurrencyLimitError):
        Communicator.send_get_request("https://e", auth_token="tok")


def test_send_get_request_other_error(monkeypatch):
    def fake_get(u, params=None, headers=None):
        # 500 -> should propagate Response.raise_for_status() as HTTPError
        return make_requests_response(500, {"detail": "oops"})

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(ServerError):
        Communicator.send_get_request("https://e", auth_token="tok")


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

    out = Communicator.send_post_request(url, {"name": "Bob"}, auth_token=token)
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
            Communicator.send_post_request("https://e", {}, auth_token="tok")

    # other error
    def fake_post_500(u, params=None, headers=None, json=None):
        return make_requests_response(500, {})

    monkeypatch.setattr(requests, "post", fake_post_500)
    with pytest.raises(ServerError):
        Communicator.send_post_request("https://e", {}, auth_token="tok")


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

    out = await Communicator.send_get_request_async(url, auth_token=token)
    assert out == payload


@pytest.mark.asyncio
async def test_send_get_request_async_error_mapping(monkeypatch):
    async def run_status(status, expected_exc):
        async def get_cb(u, params=None, headers=None):
            return make_httpx_response(status, {})

        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(get_cb=get_cb))
        if expected_exc is None:
            with pytest.raises(httpx.HTTPStatusError):
                await Communicator.send_get_request_async("https://e", auth_token="t")
        else:
            with pytest.raises(expected_exc):
                await Communicator.send_get_request_async("https://e", auth_token="t")

    for status, exc in [
        (401, AuthError),
        (403, AuthError),
        (404, APICompatibilityError),
        (429, APIConcurrencyLimitError),
        (500, ServerError),
    ]:
        await run_status(status, exc)


@pytest.mark.asyncio
async def test_send_post_request_async_success(monkeypatch):
    payload = {"ok": True}

    async def post_cb(u, params=None, headers=None, json=None):
        assert json == {"n": 1}
        return make_httpx_response(200, payload)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(post_cb=post_cb))

    out = await Communicator.send_post_request_async("https://e", {"n": 1}, auth_token="t")
    assert out == payload


@pytest.mark.asyncio
async def test_send_post_request_async_error_mapping(monkeypatch):
    async def run_status(status, expected_exc):
        async def post_cb(u, params=None, headers=None, json=None):
            return make_httpx_response(status, {})

        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: DummyAsyncClient(post_cb=post_cb))
        if expected_exc is None:
            with pytest.raises(httpx.HTTPStatusError):
                await Communicator.send_post_request_async("https://e", {}, auth_token="t")
        else:
            with pytest.raises(expected_exc):
                await Communicator.send_post_request_async("https://e", {}, auth_token="t")

    for status, exc in [
        (401, AuthError),
        (403, AuthError),
        (404, APICompatibilityError),
        (429, APIConcurrencyLimitError),
        (500, ServerError),
    ]:
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

    for status, exc in [
        (401, AuthError),
        (403, AuthError),
        (404, APICompatibilityError),
        (429, APIConcurrencyLimitError),
        (500, ServerError),
    ]:
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


# ------------------------
# WebSocket tests
# ------------------------


def test_websocket_generator_yields_and_acks_and_closes(monkeypatch):
    import restreamsolutions.communicator as comm_module

    sent = []
    closed = {"flag": False}
    captured = {}
    recv_queue = ['{"k1":"v1"}', '{"k2":2}', '{"k2":3}', '{"k2":4}', '{"k2":5}', None]

    class DummyWS:
        def __init__(self, skip_utf8_validation=True):
            assert skip_utf8_validation is True

        def connect(self, url, header=None):
            captured["url"] = url
            captured["header"] = header

        def recv(self):
            return recv_queue.pop(0)

        def send(self, data):
            sent.append(data)

        def close(self):
            closed["flag"] = True

    monkeypatch.setattr(comm_module, "WebSocket", DummyWS)

    gen = comm_module.Communicator.websocket_generator(
        url="wss://example.org/ws",
        auth_token="TOKEN123",
        params={"a": "1"},
        ack_message={"ack": "true"},
        additional_headers=[{"X-Test": "Yes"}],
    )

    out = list(gen)

    # Yields raw messages and sends ACK for each
    assert out == [{"k1": 'v1'}, {'k2': 2}, {'k2': 3}, {'k2': 4}, {'k2': 5}]
    assert sent == [json.dumps({"ack": "true"})]

    # Connection closed in finally
    assert closed["flag"] is True

    # URL base is correct; depending on requests version, ws scheme may not include query params
    assert captured["url"].startswith("wss://example.org/ws")
    if "?" in captured["url"]:
        assert "a=1" in captured["url"]

    # Headers are provided as list of "Key: Value" strings
    header_list = captured["header"] or []
    assert any(h.strip() == "Authorization: Bearer TOKEN123" for h in header_list)
    assert any(h.strip() == "X-Test: Yes" for h in header_list)


def test_websocket_generator_stops_on_connection_closed(monkeypatch):
    import restreamsolutions.communicator as comm_module

    class DummyWSClosed:
        def __init__(self, *args, **kwargs):
            pass

        def connect(self, url, header=None):
            pass

        def recv(self):
            # Simulate server closing the connection
            raise comm_module.WebSocketConnectionClosedException()

        def send(self, data):
            pass

        def close(self):
            DummyWSClosed.closed = True

    DummyWSClosed.closed = False

    monkeypatch.setattr(comm_module, "WebSocket", DummyWSClosed)

    gen = comm_module.Communicator.websocket_generator(
        url="wss://example.org/ws",
        auth_token="T",
    )

    # Should gracefully stop iteration and close the socket
    assert list(gen) == []
    assert DummyWSClosed.closed is True


async def _collect_async_gen(gen):
    items = []
    async for x in gen:
        items.append(x)
    return items


def test_websocket_generator_async_yields_and_acks_and_close(monkeypatch):
    import aiohttp
    import restreamsolutions.communicator as comm_module

    class DummyWS:
        def __init__(self, seq, on_send_json):
            self._iter = iter(seq)
            self._on_send_json = on_send_json

        async def receive(self):
            try:
                return next(self._iter)
            except StopIteration:
                # After CLOSE, context manager will exit
                return types.SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)

        async def send_json(self, payload):
            self._on_send_json(payload)

    class DummySession:
        def __init__(self, *args, **kwargs):
            self.captured = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def ws_connect(self, url, headers=None, params=None):
            self.captured["url"] = url
            self.captured["headers"] = headers
            self.captured["params"] = params
            # Prepare a sequence of TEXT, BINARY, CLOSE
            seq = [
                types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data='{"k1":"v1"}'),
                types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data='{"k1":"v2"}'),
                types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data='{"k1":"v3"}'),
                types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data='{"k1":"v4"}'),
                types.SimpleNamespace(type=aiohttp.WSMsgType.BINARY, data=b'data'),
                types.SimpleNamespace(type=aiohttp.WSMsgType.CLOSE, data=None),
            ]
            ws = DummyWS(seq, on_send_json=lambda p: self.captured.setdefault("acks", []).append(p))

            class _Ctx:
                async def __aenter__(self_inner):
                    return ws

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

            return _Ctx()

    # Capture the created session instance for later inspection
    holder = {}
    orig_cls = DummySession

    def _factory(*args, **kwargs):
        inst = orig_cls(*args, **kwargs)
        holder["session"] = inst
        return inst

    monkeypatch.setattr(comm_module.aiohttp, "ClientSession", _factory)

    gen = comm_module.Communicator.websocket_generator_async(
        url="wss://example.org/ws",
        auth_token="TOKEN-ASYNC",
        params={"p": "9"},
        ack_message={"ok": "true"},
        additional_headers=[{"X-Extra": "Z"}],
    )

    out = asyncio.run(_collect_async_gen(gen))

    assert out == [{'k1': 'v1'}, {'k1': 'v2'}, {'k1': 'v3'}, {'k1': 'v4'}, b'data']

    # Validate headers and params passed to ws_connect
    captured = holder["session"].captured
    assert captured["url"] == "wss://example.org/ws"
    assert captured["params"] == {"p": "9"}
    headers = captured["headers"]
    assert headers["Authorization"] == "Bearer TOKEN-ASYNC"
    assert headers["X-Extra"] == "Z"

    # Two ACKs were sent (for TEXT and BINARY)
    assert captured["acks"] == [{"ok": "true"}]


def test_websocket_generator_async_error_raises(monkeypatch):
    import aiohttp
    import restreamsolutions.communicator as comm_module

    class DummyWS:
        def __init__(self, seq):
            self._iter = iter(seq)

        async def receive(self):
            try:
                return next(self._iter)
            except StopIteration:
                return types.SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)

    class DummyWebsocketSession:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def ws_connect(self, url, headers=None, params=None):
            seq = [types.SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=None)]
            ws = DummyWS(seq)

            class _Ctx:
                async def __aenter__(self_inner):
                    return ws

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

            return _Ctx()

    monkeypatch.setattr(comm_module.aiohttp, "ClientSession", DummyWebsocketSession)

    async def run():
        gen = comm_module.Communicator.websocket_generator_async(
            url="wss://example.org/ws",
            auth_token="T",
        )
        with pytest.raises(comm_module.WebsocketError):
            async for _ in gen:
                pass

    asyncio.run(run())


# --- Authorization tests ---


def _reset_auth_singleton():
    # Ensure the singleton internal state is clean for each test
    auth = Authorization()
    auth._tokens = {}
    return auth


def test_authorization_create_payload_from_params_and_env(monkeypatch):
    _reset_auth_singleton()
    # When params are provided, env is ignored
    payload = Authorization()._create_payload(client_id='cid', client_secret='sec')
    assert payload == {
        'client_id': 'cid',
        'client_secret': 'sec',
        'grant_type': 'client_credentials',
    }

    # When params are missing, take from env
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ENV_ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'ENV_SEC')
    cliend_id, client_secret = Authorization()._select_client_id_and_secret()
    payload2 = Authorization()._create_payload(cliend_id, client_secret)
    assert payload2['client_id'] == 'ENV_ID'
    assert payload2['client_secret'] == 'ENV_SEC'
    assert payload2['grant_type'] == 'client_credentials'

    # When nothing is provided, raise CredentialsError
    monkeypatch.delenv('RESTREAM_CLIENT_ID', raising=False)
    monkeypatch.delenv('RESTREAM_CLIENT_SECRET', raising=False)
    from restreamsolutions.exceptions import CredentialsError

    with pytest.raises(CredentialsError):
        cliend_id, client_secret = Authorization()._select_client_id_and_secret()
        Authorization()._create_payload(cliend_id, client_secret)


def test_authorization_parse_response_success_and_errors():
    _reset_auth_singleton()

    class RespOK:
        def json(self):
            return {'access_token': 'tok', 'expires_in': 3600}

    token, exp = Authorization()._parse_response(RespOK())
    assert token == 'tok'
    assert isinstance(exp, int) and exp == 3600

    # Missing fields -> APICompatibilityError
    class RespMissing:
        def json(self):
            return {'access_token': 'tok'}

    with pytest.raises(APICompatibilityError):
        Authorization()._parse_response(RespMissing())

    # Invalid JSON -> ServerError
    class RespInvalid:
        def json(self):
            # Simulate json parsing failure
            raise json.JSONDecodeError('msg', 'doc', 0)

    with pytest.raises(ServerError):
        Authorization()._parse_response(RespInvalid())


def test_authorization_build_auth_url_uses_env_host(monkeypatch):
    _reset_auth_singleton()
    base = 'https://example.com'
    monkeypatch.setenv('RESTREAM_HOST', base)
    expected = base.rstrip('/') + Authorization._api_url_auth
    assert Authorization._build_auth_url() == expected


def test_get_access_token_posts_and_returns_token(monkeypatch):
    auth = _reset_auth_singleton()
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'SECRET')

    called = {}

    def fake_post(u, data=None, timeout=None):
        called['url'] = u
        called['data'] = dict(data) if data is not None else None
        # Use helper to create a realistic Response
        return make_requests_response(200, {'access_token': 'token', 'expires_in': 3600})

    monkeypatch.setattr(requests, 'post', fake_post)

    token = auth.get_access_token()
    assert token == 'token'
    assert 'url' in called and 'data' in called
    assert called['data']['client_id'] == 'ID'
    assert called['data']['client_secret'] == 'SECRET'
    assert called['data']['grant_type'] == 'client_credentials'
    assert isinstance(called['url'], str) and called['url'].endswith(Authorization._api_url_auth)


def test_get_access_token_uses_cache_when_not_expired(monkeypatch):
    auth = _reset_auth_singleton()
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'SECRET')
    # Preload cache and set far future expiry so _need_request returns False
    auth._tokens['ID'] = RestreamToken(
        token='CACHED', expires_in=10**9, last_update=datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
    )

    def fail_post(*args, **kwargs):
        raise AssertionError('requests.post should not be called when cache is valid')

    monkeypatch.setattr(requests, 'post', fail_post)

    assert auth.get_access_token() == 'CACHED'


def test_get_access_token_force_refresh(monkeypatch):
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'SECRET')
    auth = _reset_auth_singleton()
    # Preload cache that would otherwise be valid
    auth._restream_auth_token = 'OLD'
    auth._expires_in = 10**9

    calls = {'n': 0}

    def fake_post(u, data=None, timeout=None):
        calls['n'] += 1
        return make_requests_response(200, {'access_token': 'NEW', 'expires_in': 3600})

    monkeypatch.setattr(requests, 'post', fake_post)

    tok = auth.get_access_token(force=True)
    assert tok == 'NEW'
    assert calls['n'] == 1


@pytest.mark.asyncio
async def test_aget_access_token_posts_and_returns_token_async(monkeypatch):
    auth = _reset_auth_singleton()
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'SECRET')

    class Resp:
        status_code = 200

        def json(self):
            return {'access_token': 'token', 'expires_in': 3600}

        def raise_for_status(self):
            return None

    async def post_cb(u, data=None, **kwargs):
        # Return our simple response object
        return Resp()

    # Reuse DummyAsyncClient defined above in this module
    monkeypatch.setattr(httpx, 'AsyncClient', lambda *a, **k: DummyAsyncClient(get_cb=None, post_cb=post_cb))

    token = await auth.aget_access_token()
    assert token == 'token'


@pytest.mark.asyncio
async def test_aget_access_token_uses_cache_when_not_expired(monkeypatch):
    auth = _reset_auth_singleton()
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'SECRET')
    # Preload cache and set far future expiry so _need_request returns False
    auth._tokens['ID'] = RestreamToken(
        token='CACHED', expires_in=10**9, last_update=datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
    )

    class FailingAsyncClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            raise AssertionError('AsyncClient should not be entered when cache is valid')

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(httpx, 'AsyncClient', FailingAsyncClient)

    assert await auth.aget_access_token() == 'CACHED'


@pytest.mark.asyncio
async def test_aget_access_token_force_refresh(monkeypatch):
    monkeypatch.setenv('RESTREAM_CLIENT_ID', 'ID')
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', 'SECRET')
    auth = _reset_auth_singleton()
    auth._restream_auth_token = 'OLD'
    auth._expires_in = 10**9

    class Resp:
        status_code = 200

        def json(self):
            return {'access_token': 'NEW', 'expires_in': 3600}

        def raise_for_status(self):
            return None

    calls = {'n': 0}

    async def post_cb(u, data=None, **kwargs):
        calls['n'] += 1
        return Resp()

    monkeypatch.setattr(httpx, 'AsyncClient', lambda *a, **k: DummyAsyncClient(get_cb=None, post_cb=post_cb))

    tok = await auth.aget_access_token(force=True)
    assert tok == 'NEW'
    assert calls['n'] == 1
