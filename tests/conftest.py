import pytest

from restreamsolutions.communicator import AsyncConcurrencyLimiter, Authorization, ConcurrencyLimiter
from restreamsolutions.retry import RetryPolicy
from tests.fake_api.server import FakeRestreamServer


@pytest.fixture(scope='session')
def api_server():
    """Start the fake ReStream API once per test session."""
    server = FakeRestreamServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture
def recorded_delays(monkeypatch):
    """Replace the retry sleeps with a recorder so that tests never wait for real backoff delays.

    Returns:
        The list that receives every delay the SDK would have slept for.
    """
    delays = []

    def sleep(self, seconds):
        """Record a synchronous retry delay instead of sleeping.

        Args:
            seconds: The delay the SDK requested.
        """
        delays.append(seconds)

    async def asleep(self, seconds):
        """Record an asynchronous retry delay instead of sleeping.

        Args:
            seconds: The delay the SDK requested.
        """
        delays.append(seconds)

    monkeypatch.setattr(RetryPolicy, 'sleep', sleep)
    monkeypatch.setattr(RetryPolicy, 'asleep', asleep)
    return delays


@pytest.fixture
def api(api_server, monkeypatch, recorded_delays):
    """Provide the fake API in a clean state and point the SDK at it through environment variables."""
    api_server.reset()
    monkeypatch.setenv('RESTREAM_HOST', api_server.base_url)
    monkeypatch.setenv('RESTREAM_CLIENT_ID', api_server.DEFAULT_CLIENT_ID)
    monkeypatch.setenv('RESTREAM_CLIENT_SECRET', api_server.DEFAULT_CLIENT_SECRET)
    monkeypatch.delenv('RESTREAM_MAX_RETRY_ATTEMPTS', raising=False)
    Authorization()._tokens.clear()
    for limiter in (ConcurrencyLimiter, AsyncConcurrencyLimiter):
        limiter._semaphores.clear()
        limiter._limits.clear()
        limiter._users.clear()
    return api_server
