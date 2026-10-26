import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import format_datetime
from enum import Enum

import pytest

from restreamsolutions.communicator import Authorization, Communicator
from restreamsolutions.exceptions import (
    APICompatibilityError,
    APIConcurrencyLimitError,
    AuthError,
    BadRequestError,
    ForbiddenError,
    NotFoundError,
    ServerError,
)
from tests.fake_api.server import API_PREFIX, TEST_STREAM_PATH, WS_PREFIX

SITE_ID = 1112
SITE_WS_PATH = f'{WS_PREFIX}/sites/{SITE_ID}/'
SITE_DATA_WS_PATH = f'{WS_PREFIX}/sites/{SITE_ID}/data/'


class _Color(Enum):
    RED = 'red'


# ------------------------
# Pure helpers
# ------------------------


def test_build_query_params_normalizes_values():
    """Query params skip None, expand lists and convert booleans and enums; URLs keep their existing query."""
    params = {'none': None, 'ids': [1, None, 2], 'flag': True, 'off': False, 'enum': _Color.RED, 'text': 'x'}

    query = Communicator._build_query_params(params)

    assert query == [('ids', '1'), ('ids', '2'), ('flag', 'true'), ('off', 'false'), ('enum', 'red'), ('text', 'x')]
    assert Communicator._add_query_params('wss://h/ws?x=1', {'k': 'v', 'skip': None}) == 'wss://h/ws?x=1&k=v'
    assert Communicator._add_query_params('wss://h/ws', None) == 'wss://h/ws'


@pytest.mark.parametrize(
    'status, exception',
    [
        (401, AuthError),
        (403, ForbiddenError),
        (404, NotFoundError),
        (429, APIConcurrencyLimitError),
        (400, BadRequestError),
        (418, BadRequestError),
        (500, ServerError),
        (502, ServerError),
        (504, ServerError),
    ],
)
def test_status_code_mapping(status, exception):
    """Error statuses are mapped to SDK exceptions, most with the URL and the server detail; success codes pass."""
    with pytest.raises(exception) as excinfo:
        Communicator._check_response_status_code(status, {}, '{"detail": "boom"}', 'http://h/x/')
    if status not in (401, 429):
        assert 'http://h/x/' in str(excinfo.value)
        assert 'boom' in str(excinfo.value)
    if hasattr(excinfo.value, 'status_code'):
        assert excinfo.value.status_code == status

    # Success codes pass silently
    Communicator._check_response_status_code(200, {}, '', 'http://h/x/')
    Communicator._check_response_status_code(204, None, None, 'http://h/x/')


def test_auth_error_carries_the_rejected_token():
    """HTTP 401 keeps the rejected token on the exception without exposing it in the message."""
    with pytest.raises(AuthError) as excinfo:
        Communicator._check_response_status_code(401, {}, '', 'http://h/x/', 'secret-token')
    assert excinfo.value.rejected_token == 'secret-token'
    assert 'secret-token' not in str(excinfo.value)
    assert 'secret-token' not in repr(excinfo.value)


def test_retry_after_parsing():
    """Retry-After is parsed from seconds or an HTTP date in any header case and is attached to the 429 error."""
    assert Communicator._parse_retry_after({'Retry-After': '12'}) == 12.0
    assert Communicator._parse_retry_after({'Retry-After': '-3'}) == 0.0
    assert Communicator._parse_retry_after({}) is None
    assert Communicator._parse_retry_after(None) is None
    assert Communicator._parse_retry_after({'Retry-After': 'garbage'}) is None
    # Non-finite numbers would break the sleep, so they are treated as a missing header
    assert Communicator._parse_retry_after({'Retry-After': 'inf'}) is None
    assert Communicator._parse_retry_after({'Retry-After': 'nan'}) is None
    # websocket-client passes handshake headers as a plain dict with lower-case names
    assert Communicator._parse_retry_after({'retry-after': '7'}) == 7.0
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    http_date = format_datetime(retry_at, usegmt=True)
    assert 25 <= Communicator._parse_retry_after({'Retry-After': http_date}) <= 30
    # The -0000 zone is parsed as a naive datetime and must be treated as UTC, not as local time
    naive_utc_date = format_datetime(retry_at.replace(tzinfo=None))
    assert naive_utc_date.endswith('-0000')
    assert 25 <= Communicator._parse_retry_after({'Retry-After': naive_utc_date}) <= 30

    with pytest.raises(APIConcurrencyLimitError) as excinfo:
        Communicator._check_response_status_code(429, {'Retry-After': '5'}, '', 'u')
    assert excinfo.value.retry_after == 5.0


def test_json_body_parsing_and_value_conversion():
    """Empty bodies parse to None, non-JSON bodies raise APICompatibilityError and special values are converted."""
    assert Communicator._parse_json_body('', 'u') is None
    assert Communicator._parse_json_body(None, 'u') is None
    assert Communicator._parse_json_body('{"a": 1}', 'u') == {'a': 1}
    with pytest.raises(APICompatibilityError):
        Communicator._parse_json_body('<html>', 'u')

    converted = Communicator._convert_values({'a': Decimal('1.5'), 'b': 'Infinity', 'c': 'ok', 'd': None})
    assert converted == {'a': 1.5, 'b': None, 'c': 'ok', 'd': None}


def test_websocket_message_parsing():
    """WebSocket messages are parsed into lists of items, optionally unwrapped from a nested key."""
    parse = Communicator._parse_websocket_message
    assert parse('', None) == []
    assert parse('null', None) == []
    assert parse('{"a": 1}', None) == [{'a': 1}]
    assert parse('[{"a": 1}, {"a": 2}]', None) == [{'a': 1}, {'a': 2}]
    assert parse('{"session_key": "k", "message": {"a": 1}}', 'message') == [{'a': 1}]
    assert parse('{"session_key": "k", "message": [{"a": 1}, {"a": 2}]}', 'message') == [{'a': 1}, {'a': 2}]
    assert parse(b'raw', 'message') == [b'raw']
    with pytest.raises(ValueError):
        parse('not json', None)
    with pytest.raises(KeyError):
        parse('{"other": 1}', 'message')


# ------------------------
# HTTP requests against the fake API
# ------------------------


def test_requests_carry_bearer_token_and_normalized_query(api):
    """Sync and async GET requests send the cached bearer token and the normalized query."""
    url = f'{api.base_url}{API_PREFIX}/sites/{SITE_ID}/aggregations/'

    payload = Communicator.send_get_request(url, histories=[1, 2], complete=True, skipped=None)
    async_payload = asyncio.run(Communicator.send_get_request_async(url, histories=[1, 2], complete=True, skipped=None))

    assert payload == async_payload
    assert api.count('GET', f'{API_PREFIX}/sites/{SITE_ID}/aggregations/') == 2
    for request in api.requests[-2:]:
        assert request.bearer_token == Authorization().get_access_token()
        assert request.query == {'histories': ['1', '2'], 'complete': ['true']}
    assert api.token_requests == 1


def test_write_requests_send_json_payload_and_delete_returns_none(api):
    """POST, PATCH and PUT requests send JSON bodies and return the response; DELETE returns None."""
    url = f'{api.base_url}{API_PREFIX}/pads/697/parameters/'
    body = [{'name': 'p', 'config': {'field': 'f', 'aggregation': 'average'}}]

    created = Communicator.send_post_request(url, body)
    assert created[0]['name'] == 'p'
    assert api.last('POST', f'{API_PREFIX}/pads/697/parameters/').body == body

    one_url = f'{url}{created[0]["id"]}/'
    patched = Communicator.send_patch_request(one_url, {'description': 'd'})
    assert patched['description'] == 'd'
    assert api.last('PATCH', f'{API_PREFIX}/pads/697/parameters/{created[0]["id"]}/').body == {'description': 'd'}

    replaced = asyncio.run(Communicator.send_put_request_async(one_url, {'config': {'field': 'g'}}))
    assert replaced['config'] == {'field': 'g'}
    assert replaced['description'] == ''

    assert Communicator.send_delete_request(one_url) is None
    with pytest.raises(NotFoundError):
        asyncio.run(Communicator.send_delete_request_async(one_url))


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_streaming_generators_convert_values(api, is_async):
    """Streaming generators send the streaming header and the query and yield items with converted values."""
    url = f'{api.base_url}{TEST_STREAM_PATH}'

    if is_async:

        async def _collect():
            """Read the async streaming generator to the end.

            Returns:
                The list of streamed items.
            """
            return [item async for item in Communicator.streaming_get_generator_async(url, extra='1')]

        items = asyncio.run(_collect())
    else:
        items = list(Communicator.streaming_get_generator(url, extra='1'))

    assert items == [{'a': 1.5, 'b': None}, {'a': 2, 'b': 'ok'}]
    assert isinstance(items[0]['a'], float)
    request = api.last('GET', TEST_STREAM_PATH)
    assert request.headers['Prefer'] == 'streaming'
    assert request.query == {'extra': ['1']}


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
@pytest.mark.parametrize(
    'status, exception',
    [
        (401, AuthError),
        (403, ForbiddenError),
        (404, NotFoundError),
        (429, APIConcurrencyLimitError),
        (500, ServerError),
    ],
)
def test_streaming_generators_map_error_statuses(api, is_async, status, exception):
    """Error statuses of a streaming request are mapped to the same SDK exceptions as regular requests."""
    url = f'{api.base_url}{TEST_STREAM_PATH}'
    api.plan('GET', TEST_STREAM_PATH, status)

    with pytest.raises(exception):
        if is_async:

            async def _collect():
                """Read the async streaming generator to the end.

                Returns:
                    The list of streamed items.
                """
                return [item async for item in Communicator.streaming_get_generator_async(url)]

            asyncio.run(_collect())
        else:
            list(Communicator.streaming_get_generator(url))


# ------------------------
# WebSockets against the fake API
# ------------------------


def _ws_url(api, path: str) -> str:
    """Build a WebSocket URL to the fake API server.

    Args:
        api: The running fake API server.
        path: WebSocket path without the host.

    Returns:
        The ws URL of the path.
    """
    return f'{api.base_url}{path}'.replace('http://', 'ws://')


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_websocket_generators_yield_decoded_messages(api, is_async):
    """WebSocket generators send the auth and extra headers and yield decoded messages, skipping empty ones."""
    api.ws_scripts[SITE_WS_PATH] = ['{"k": 1}', '', '[{"k": 2}, {"k": 3}]', 'null', {'k': 4}]
    api.ws_scripts[SITE_DATA_WS_PATH] = [{'v': 1}, [{'v': 2}, {'v': 3}]]
    headers = [{'X-Test': 'yes'}]

    if is_async:

        async def _collect(url, **kwargs):
            """Read the async WebSocket generator to the end.

            Args:
                url: WebSocket URL to connect to.
                **kwargs: Arguments forwarded to websocket_generator_async.

            Returns:
                The list of received messages.
            """
            return [item async for item in Communicator.websocket_generator_async(url, **kwargs)]

        updates = asyncio.run(_collect(_ws_url(api, SITE_WS_PATH), additional_headers=headers))
        data = asyncio.run(
            _collect(_ws_url(api, SITE_DATA_WS_PATH), params={'session_key': 'abc'}, get_nested_key='message')
        )
    else:
        updates = list(Communicator.websocket_generator(_ws_url(api, SITE_WS_PATH), additional_headers=headers))
        data = list(
            Communicator.websocket_generator(
                _ws_url(api, SITE_DATA_WS_PATH), params={'session_key': 'abc'}, get_nested_key='message'
            )
        )

    assert updates == [{'k': 1}, {'k': 2}, {'k': 3}, {'k': 4}]
    assert data == [{'v': 1}, {'v': 2}, {'v': 3}]
    updates_ws, data_ws = api.websockets
    assert updates_ws.headers['Authorization'] == f'Bearer {Authorization().get_access_token()}'
    assert updates_ws.headers['X-Test'] == 'yes'
    assert data_ws.param('session_key') == 'abc'


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_websocket_handshake_errors_are_mapped(api, is_async):
    """A rejected WebSocket handshake is mapped to SDK exceptions; HTTP 403 is reported as AuthError."""

    def connect(url):
        """Open the WebSocket through the sync or the async generator and read it to the end.

        Args:
            url: WebSocket URL to connect to.

        Returns:
            The list of received messages.
        """
        if is_async:

            async def _collect():
                """Read the async WebSocket generator to the end.

                Returns:
                    The list of received messages.
                """
                return [item async for item in Communicator.websocket_generator_async(url)]

            return asyncio.run(_collect())
        return list(Communicator.websocket_generator(url))

    token = Authorization().get_access_token()

    # The server answers 403 both for a missing access and for an invalid token, so both are AuthError
    api.forbidden_ids.add(SITE_ID)
    with pytest.raises(AuthError) as excinfo:
        connect(_ws_url(api, SITE_WS_PATH))
    assert not isinstance(excinfo.value, ForbiddenError)
    assert 'HTTP 403' in str(excinfo.value)
    assert excinfo.value.rejected_token == token

    api.forbidden_ids.clear()
    api.revoke_tokens()
    with pytest.raises(AuthError) as excinfo:
        connect(_ws_url(api, SITE_WS_PATH))
    assert excinfo.value.rejected_token == token

    # Other statuses keep their regular mapping
    Authorization().get_access_token(force=True)
    api.missing_ids.add(SITE_ID)
    with pytest.raises(NotFoundError):
        connect(_ws_url(api, SITE_WS_PATH))
