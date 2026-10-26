import asyncio
import errno
import socket
import ssl
import threading

import aiohttp
import pytest
import requests
from urllib3.exceptions import ProtocolError
from websocket import WebSocketAddressException

from restreamsolutions import Pad, Site
from restreamsolutions.communicator import Authorization, Communicator
from restreamsolutions.constants import (
    MAX_RETRY_ATTEMPTS,
    RETRY_AFTER_MAX_SECONDS,
    RETRY_BACKOFF_FACTOR,
    RETRY_MAX_DELAY_SECONDS,
)
from restreamsolutions.exceptions import (
    APIConcurrencyLimitError,
    AuthError,
    BadRequestError,
    ForbiddenError,
    NotFoundError,
    ServerError,
)
from restreamsolutions.retry import RetryPolicy, resolve_max_attempts
from tests.fake_api.server import API_PREFIX, WS_PREFIX

SITE_ID = 1112
PAD_ID = 697
SITE_PATH = f'{API_PREFIX}/sites/{SITE_ID}/'
PAD_AGGREGATIONS_PATH = f'{API_PREFIX}/pads/{PAD_ID}/parameters/'
SITE_DATA_PATH = f'{API_PREFIX}/sites/{SITE_ID}/data/'
SITE_DATA_WS_PATH = f'{WS_PREFIX}/sites/{SITE_ID}/data/'
NEW_AGGREGATION = [{'name': 'avg_rate', 'config': {'field': 'slurry_flow', 'aggregation': 'average'}}]


def get_site(is_async: bool, **kwargs):
    """Fetch a site through the sync or the async API.

    Args:
        is_async: If True, the async API is used.
        **kwargs: Arguments forwarded to get_model or aget_model.

    Returns:
        The fetched Site object.
    """
    if is_async:
        return asyncio.run(Site.aget_model(id=SITE_ID, **kwargs))
    return Site.get_model(id=SITE_ID, **kwargs)


def expected_delay_range(retry_index: int) -> tuple[float, float]:
    """Return the jitter range of the delay before the given retry.

    Args:
        retry_index: Zero-based index of the retry.

    Returns:
        The lowest and the highest expected delay in seconds.
    """
    base = min(RETRY_MAX_DELAY_SECONDS, RETRY_BACKOFF_FACTOR**retry_index)
    return base * 0.5, min(RETRY_MAX_DELAY_SECONDS, base * 1.5)


def collect_stream(data) -> list:
    """Read a Data or DataAsync stream to the end.

    Args:
        data: The Data or DataAsync object to consume.

    Returns:
        The list of streamed items.
    """
    if hasattr(data, 'asave'):

        async def _collect():
            """Read the async stream to the end.

            Returns:
                The list of streamed items.
            """
            return [item async for item in data.data_fetcher]

        return asyncio.run(_collect())
    return list(data.data_fetcher)


def open_measurements_stream(site: Site, is_async: bool, **kwargs):
    """Open a real-time measurements stream through the sync or the async API.

    Args:
        site: The site to stream measurements for.
        is_async: If True, the async API is used.
        **kwargs: Arguments forwarded to get_realtime_measurements_data or its async counterpart.

    Returns:
        The Data or DataAsync object of the stream.
    """
    if is_async:
        data, _ = asyncio.run(site.aget_realtime_measurements_data(**kwargs))
    else:
        data, _ = site.get_realtime_measurements_data(**kwargs)
    return data


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_transient_server_errors_are_retried_with_backoff(api, recorded_delays, is_async):
    """HTTP 5xx responses are retried with a jittered exponential backoff until the request succeeds."""
    api.plan('GET', SITE_PATH, 500, 502, 503)

    site = get_site(is_async)

    assert site.id == SITE_ID
    assert api.count('GET', SITE_PATH) == 4
    assert len(recorded_delays) == 3
    for retry_index, delay in enumerate(recorded_delays):
        low, high = expected_delay_range(retry_index)
        assert low <= delay <= high


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_rate_limit_waits_at_least_retry_after(api, recorded_delays, is_async):
    """An HTTP 429 response is retried after waiting at least the Retry-After delay, up to RETRY_AFTER_MAX_SECONDS."""
    api.plan('GET', SITE_PATH, {'status': 429, 'headers': {'Retry-After': '7'}})

    get_site(is_async)

    assert api.count('GET', SITE_PATH) == 2
    assert recorded_delays[0] >= 7.0

    # A huge Retry-After is clamped so that the server cannot block the caller for an arbitrary time
    recorded_delays.clear()
    api.plan('GET', SITE_PATH, {'status': 429, 'headers': {'Retry-After': '100000'}})
    get_site(is_async)
    assert recorded_delays == [RETRY_AFTER_MAX_SECONDS]


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_attempts_are_exhausted(api, recorded_delays, monkeypatch, is_async):
    """A server error is raised after MAX_RETRY_ATTEMPTS attempts, or fewer if RESTREAM_MAX_RETRY_ATTEMPTS is lower."""
    api.plan('GET', SITE_PATH, *([500] * MAX_RETRY_ATTEMPTS))
    with pytest.raises(ServerError) as excinfo:
        get_site(is_async)
    assert excinfo.value.status_code == 500
    assert api.count('GET', SITE_PATH) == MAX_RETRY_ATTEMPTS
    assert len(recorded_delays) == MAX_RETRY_ATTEMPTS - 1

    # The environment variable lowers the number of attempts at call time
    monkeypatch.setenv('RESTREAM_MAX_RETRY_ATTEMPTS', '2')
    api.plan('GET', SITE_PATH, 500, 500, 200)
    with pytest.raises(ServerError):
        get_site(is_async)
    assert api.count('GET', SITE_PATH) == MAX_RETRY_ATTEMPTS + 2


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
@pytest.mark.parametrize(
    'status, exception',
    [(400, BadRequestError), (403, ForbiddenError), (404, NotFoundError), (409, BadRequestError)],
)
def test_client_errors_are_not_retried(api, recorded_delays, is_async, status, exception):
    """Client errors other than HTTP 401 and 429 are raised at once without retries."""
    api.plan('GET', SITE_PATH, status)

    with pytest.raises(exception) as excinfo:
        get_site(is_async)

    assert api.count('GET', SITE_PATH) == 1
    assert recorded_delays == []
    assert str(status) in str(excinfo.value) or status in (403, 404)


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_unauthorized_triggers_a_single_token_refresh(api, recorded_delays, is_async):
    """An HTTP 401 response refreshes the token once and repeats the request; a second 401 is raised."""
    Authorization().get_access_token()
    api.revoke_tokens()

    site = get_site(is_async)

    assert site.id == SITE_ID
    assert api.token_requests == 2
    assert api.count('GET', SITE_PATH) == 2
    assert api.last('GET', SITE_PATH).bearer_token == Authorization().get_access_token()
    assert recorded_delays == []

    # A second 401 right after the refresh means the credentials are wrong: no endless refresh loop
    api.plan('GET', SITE_PATH, 401, 401)
    with pytest.raises(AuthError):
        get_site(is_async)
    assert api.token_requests == 3
    assert api.count('GET', SITE_PATH) == 4


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_concurrent_unauthorized_requests_share_a_single_token_refresh(api, recorded_delays, is_async):
    """Requests rejected with the same token request only one replacement token between them."""
    Authorization().get_access_token()
    api.revoke_tokens()
    # A slow endpoint makes the requests overlap and queue in the concurrency limiter with the old token
    api.delays[SITE_PATH] = 0.1
    callers = 8

    if is_async:

        async def fetch_all():
            """Fetch the site concurrently from several tasks.

            Returns:
                The list of fetched sites.
            """
            return await asyncio.gather(*(Site.aget_model(id=SITE_ID) for _ in range(callers)))

        sites = asyncio.run(fetch_all())
    else:
        sites = []
        threads = [threading.Thread(target=lambda: sites.append(Site.get_model(id=SITE_ID))) for _ in range(callers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(sites) == callers and all(site.id == SITE_ID for site in sites)
    assert api.token_requests == 2


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_explicit_token_is_never_refreshed(api, is_async):
    """A rejected user-provided token raises AuthError without requesting an SDK token."""
    with pytest.raises(AuthError):
        get_site(is_async, auth_token='user-provided-token')

    assert api.token_requests == 0
    assert api.count('GET', SITE_PATH) == 1


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_streams_with_a_rejected_explicit_token_raise_auth_error(api, recorded_delays, monkeypatch, is_async):
    """A rejected user-provided token ends a stream with AuthError and never needs the SDK credentials."""
    monkeypatch.delenv('RESTREAM_CLIENT_ID')
    monkeypatch.delenv('RESTREAM_CLIENT_SECRET')
    site = Site(id=SITE_ID, auth_token='user-provided-token')

    with pytest.raises(AuthError):
        collect_stream(asyncio.run(site.aget_data()) if is_async else site.get_data())
    assert api.count('GET', SITE_DATA_PATH) == 1

    with pytest.raises(AuthError):
        collect_stream(open_measurements_stream(site, is_async))
    assert len(api.websockets) == 0
    assert api.token_requests == 0
    assert recorded_delays == []


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_websocket_streams_refresh_a_rejected_token_once(api, recorded_delays, is_async):
    """A WebSocket handshake rejected with HTTP 403 makes the SDK request a new token once and reconnect."""
    site = Site(id=SITE_ID)
    api.ws_scripts[SITE_DATA_WS_PATH] = [{'timestamp': 1, 'a': 1}]
    Authorization().get_access_token()
    api.revoke_tokens()

    items = collect_stream(open_measurements_stream(site, is_async, restart_on_close=False))
    assert items == [{'timestamp': 1, 'a': 1}]
    assert api.token_requests == 2
    assert recorded_delays == []

    # A rejection of the new token as well means a missing access: the stream ends after one more refresh
    api.forbidden_ids.add(SITE_ID)
    with pytest.raises(AuthError):
        collect_stream(open_measurements_stream(site, is_async, restart_on_close=False))
    assert api.token_requests == 3


def test_non_idempotent_create_is_not_retried_after_the_server_may_have_processed_it(api, recorded_delays):
    """A create is retried only after HTTP 429 or 401, while an idempotent POST is retried after HTTP 5xx too."""
    pad = Pad(id=PAD_ID)

    api.plan('POST', PAD_AGGREGATIONS_PATH, 500)
    with pytest.raises(ServerError):
        pad.create_stages_aggregations(NEW_AGGREGATION)
    assert api.count('POST', PAD_AGGREGATIONS_PATH) == 1

    # 429 and 401 are returned before the server processes the request, so repeating is safe
    api.plan('POST', PAD_AGGREGATIONS_PATH, 429)
    created = pad.create_stages_aggregations(NEW_AGGREGATION)
    assert created[0]['name'] == 'avg_rate'
    assert api.count('POST', PAD_AGGREGATIONS_PATH) == 3

    api.plan('POST', PAD_AGGREGATIONS_PATH, 401)
    assert asyncio.run(pad.acreate_stages_aggregations(NEW_AGGREGATION))[0]['name'] == 'avg_rate'
    assert api.count('POST', PAD_AGGREGATIONS_PATH) == 5
    assert api.token_requests == 2

    # Acknowledging data changes is idempotent and follows the regular policy.
    # A change event is acknowledged through the endpoint of the site it belongs to.
    changes, _ = pad.get_data_changes()
    acknowledge_path = f'{API_PREFIX}/sites/{changes[0].site}/data_changes/'
    api.plan('POST', acknowledge_path, 500, 503)
    assert changes[0].confirm_data_received() is True
    assert api.count('POST', acknowledge_path) == 3


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_connection_errors_are_retried_until_attempts_are_exhausted(api, recorded_delays, monkeypatch, is_async):
    """Connection errors are retried for reads and creates until the configured attempts are exhausted."""
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        closed_port = probe.getsockname()[1]
    monkeypatch.setenv('RESTREAM_HOST', f'http://127.0.0.1:{closed_port}')
    # Windows reports a refused loopback connection only after about two seconds, so the attempts are
    # lowered through the environment variable, which also exercises the runtime override itself
    attempts = 2
    monkeypatch.setenv('RESTREAM_MAX_RETRY_ATTEMPTS', str(attempts))
    Authorization()._tokens.clear()
    # The token endpoint is unreachable as well, so use an explicit token to exercise the data request
    expected = aiohttp.ClientConnectorError if is_async else requests.exceptions.ConnectionError

    with pytest.raises(expected):
        get_site(is_async, auth_token='token')
    assert len(recorded_delays) == attempts - 1

    # Connection errors happen before the server sees the request, so even creates are retried
    recorded_delays.clear()
    pad = Pad(id=PAD_ID, auth_token='token')
    with pytest.raises(expected):
        if is_async:
            asyncio.run(pad.acreate_stages_aggregations(NEW_AGGREGATION))
        else:
            pad.create_stages_aggregations(NEW_AGGREGATION)
    assert len(recorded_delays) == attempts - 1


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_streaming_restarts_only_before_the_first_item(api, recorded_delays, is_async):
    """A stream is restarted after a failure before the first item, but a drop in the middle is propagated."""
    site = Site(id=SITE_ID)

    def collect():
        """Read the site data stream to the end through the sync or the async API.

        Returns:
            The list of streamed items.
        """
        if is_async:

            async def _collect():
                """Fetch the site data asynchronously and read the stream to the end.

                Returns:
                    The list of streamed items.
                """
                data = await site.aget_data()
                return [item async for item in data.data_fetcher]

            return asyncio.run(_collect())
        return list(site.get_data().data_fetcher)

    # A failure before any item is delivered is retried transparently
    api.plan('GET', SITE_DATA_PATH, 503, 429)
    items = collect()
    assert len(items) == 3600
    assert api.count('GET', SITE_DATA_PATH) == 3
    assert len(recorded_delays) == 2

    # A connection drop in the middle of the stream is propagated: a restart would duplicate the delivered items.
    # Items are made large so that the buffered sync reader delivers every item before it hits the drop.
    api.stream_items[SITE_DATA_PATH] = [{'timestamp': index, 'blob': 'x' * 70_000} for index in range(8)]
    api.stream_drop_after[SITE_DATA_PATH] = 5
    received = []

    def collect_partial():
        """Read the site data stream through the sync or the async API, appending each item to received."""
        if is_async:

            async def _collect():
                """Fetch the site data asynchronously and append each streamed item to received."""
                data = await site.aget_data()
                async for item in data.data_fetcher:
                    received.append(item)

            asyncio.run(_collect())
        else:
            for item in site.get_data().data_fetcher:
                received.append(item)

    with pytest.raises(Exception) as excinfo:
        collect_partial()
    assert not isinstance(excinfo.value, (BadRequestError, AuthError))
    # How many of the sent items reach the consumer before the drop depends on client-side buffering
    assert 1 <= len(received) <= 5
    assert api.count('GET', SITE_DATA_PATH) == 4


def test_environment_override_validation(monkeypatch):
    """A valid RESTREAM_MAX_RETRY_ATTEMPTS lowers the attempts, an invalid one is ignored and zero is rejected."""
    monkeypatch.setenv('RESTREAM_MAX_RETRY_ATTEMPTS', '3')
    assert resolve_max_attempts(7) == 3
    assert RetryPolicy().attempts == 3
    # An explicit value caps the environment override
    assert RetryPolicy(attempts=2).attempts == 2

    for invalid in ('0', '-1', 'abc', ''):
        monkeypatch.setenv('RESTREAM_MAX_RETRY_ATTEMPTS', invalid)
        assert resolve_max_attempts(7) == 7

    monkeypatch.delenv('RESTREAM_MAX_RETRY_ATTEMPTS')
    assert RetryPolicy().attempts == MAX_RETRY_ATTEMPTS
    with pytest.raises(ValueError):
        RetryPolicy(attempts=0)


def test_policy_classification():
    """Fatal errors are never retried, any other error is; non-idempotent calls only before reaching the server."""
    policy = RetryPolicy()
    non_idempotent = RetryPolicy(idempotent=False)
    assert policy.is_retryable(ServerError())
    assert policy.is_retryable(APIConcurrencyLimitError())
    assert policy.is_retryable(requests.exceptions.ConnectionError())
    assert policy.is_retryable(TimeoutError())
    # Errors that are not known to be fatal are treated as transient
    unknown_errors = (
        OSError(errno.ENETUNREACH, 'Network is unreachable'),
        ssl.SSLEOFError(8, 'EOF'),
        WebSocketAddressException('Name or service not known'),
        ProtocolError('Connection broken'),
        KeyError('unexpected'),
    )
    for exc in unknown_errors:
        assert policy.is_retryable(exc)
        assert not non_idempotent.is_retryable(exc)
    for exc in (BadRequestError(), ForbiddenError(), NotFoundError(), AuthError(), ValueError(), TypeError()):
        assert not policy.is_retryable(exc)
    assert policy.can_refresh_token(AuthError(), auth_token_provided=False)
    assert not policy.can_refresh_token(AuthError(), auth_token_provided=True)
    assert not policy.can_refresh_token(ForbiddenError(), auth_token_provided=False)

    assert not non_idempotent.is_retryable(ServerError())
    assert non_idempotent.is_retryable(APIConcurrencyLimitError())
    assert not non_idempotent.is_retryable(requests.exceptions.ConnectionError())
    assert non_idempotent.is_retryable(requests.exceptions.ConnectTimeout())

    no_jitter = RetryPolicy(jitter=False, initial_delay=2, factor=2, max_delay=5, max_retry_after=20)
    assert [no_jitter.delay_for(i) for i in range(4)] == [2, 4, 5, 5]
    assert no_jitter.delay_for(0, retry_after=9) == 9
    assert no_jitter.delay_for(0, retry_after=100) == 20
    # The exponent stops growing once the delay reaches max_delay, so long failure streaks do not overflow
    assert no_jitter.delay_for(100_000) == 5
    assert RetryPolicy(jitter=False, initial_delay=2, factor=1).delay_for(100_000) == 2
    with_jitter = RetryPolicy(initial_delay=1, factor=3, max_delay=30)
    assert all(with_jitter.delay_for(index) <= 30 for index in range(1000))
    with pytest.raises(ValueError):
        RetryPolicy(factor=0.5)


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_token_refresh_does_not_consume_an_attempt(api, recorded_delays, monkeypatch, is_async):
    """With a single attempt a rejected SDK token is still refreshed once, while a server error is not retried."""
    monkeypatch.setenv('RESTREAM_MAX_RETRY_ATTEMPTS', '1')
    Authorization().get_access_token()
    api.revoke_tokens()

    site = get_site(is_async)

    assert site.id == SITE_ID
    assert api.token_requests == 2
    assert api.count('GET', SITE_PATH) == 2
    assert recorded_delays == []

    api.plan('GET', SITE_PATH, 500)
    with pytest.raises(ServerError):
        get_site(is_async)
    assert api.count('GET', SITE_PATH) == 3


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_empty_token_means_the_sdk_managed_token(api, is_async):
    """An empty auth_token is treated as no token: the SDK token is used and refreshed when it is rejected."""
    Authorization().get_access_token()
    api.revoke_tokens()

    site = get_site(is_async, auth_token='')

    assert site.id == SITE_ID
    assert site._auth_token is None
    assert api.token_requests == 2
    assert api.count('GET', SITE_PATH) == 2


def test_post_requests_are_not_repeated_unless_marked_idempotent(api, recorded_delays):
    """A POST is retried after HTTP 5xx only when the caller marks it as idempotent."""
    url = f'{api.base_url}{PAD_AGGREGATIONS_PATH}'

    api.plan('POST', PAD_AGGREGATIONS_PATH, 500)
    with pytest.raises(ServerError):
        Communicator.send_post_request(url, NEW_AGGREGATION)
    assert api.count('POST', PAD_AGGREGATIONS_PATH) == 1

    api.plan('POST', PAD_AGGREGATIONS_PATH, 500)
    created = asyncio.run(Communicator.send_post_request_async(url, NEW_AGGREGATION, idempotent=True))
    assert created[0]['name'] == 'avg_rate'
    assert api.count('POST', PAD_AGGREGATIONS_PATH) == 3
