import asyncio
import threading

import pytest

from restreamsolutions.communicator import Authorization
from restreamsolutions.constants import AUTH_MAX_RETRY_ATTEMPTS
from restreamsolutions.exceptions import APICompatibilityError, CredentialsError, ServerError
from tests.fake_api.server import TOKEN_PATH


def test_token_is_requested_once_and_cached(api):
    """The token is requested once with the client credentials grant and then served from the cache."""
    auth = Authorization()

    token = auth.get_access_token()

    assert token == auth.get_access_token()
    assert api.token_requests == 1
    request = api.last('POST', TOKEN_PATH)
    assert request.body == {
        'client_id': api.DEFAULT_CLIENT_ID,
        'client_secret': api.DEFAULT_CLIENT_SECRET,
        'grant_type': 'client_credentials',
    }


async def test_async_token_shares_the_cache_with_sync(api):
    """A token requested through the async API is cached for both the sync and the async API."""
    auth = Authorization()

    token = await auth.aget_access_token()

    assert token == auth.get_access_token()
    assert await auth.aget_access_token() == token
    assert api.token_requests == 1


def test_cached_token_is_not_returned_without_matching_secret(api, monkeypatch):
    """A cached token is returned only for the matching secret; missing credentials are rejected without a request."""
    auth = Authorization()
    cached = auth.get_access_token()
    assert api.token_requests == 1

    # A wrong secret must bypass the cache; the server then rejects the credentials
    with pytest.raises(CredentialsError):
        auth.get_access_token(client_secret='wrong-secret')
    assert api.count('POST', TOKEN_PATH) == 2

    # Missing credentials are rejected before any request is made, even though a token is cached
    monkeypatch.delenv('RESTREAM_CLIENT_SECRET')
    with pytest.raises(CredentialsError):
        auth.get_access_token()
    with pytest.raises(CredentialsError):
        auth.get_access_token(client_id=api.DEFAULT_CLIENT_ID)
    assert api.count('POST', TOKEN_PATH) == 2

    # The right secret still hits the cache
    assert auth.get_access_token(client_secret=api.DEFAULT_CLIENT_SECRET) == cached
    assert api.count('POST', TOKEN_PATH) == 2


async def test_async_cached_token_is_not_returned_without_matching_secret(api, monkeypatch):
    """The async API returns a cached token only for the matching secret and rejects missing credentials."""
    auth = Authorization()
    cached = await auth.aget_access_token()

    with pytest.raises(CredentialsError):
        await auth.aget_access_token(client_secret='wrong-secret')
    assert api.count('POST', TOKEN_PATH) == 2

    monkeypatch.delenv('RESTREAM_CLIENT_SECRET')
    with pytest.raises(CredentialsError):
        await auth.aget_access_token()
    assert api.count('POST', TOKEN_PATH) == 2

    assert await auth.aget_access_token(client_secret=api.DEFAULT_CLIENT_SECRET) == cached


def test_explicit_credentials_get_their_own_cache_entry(api):
    """Tokens for explicitly passed credentials are cached separately from the default credentials."""
    api.credentials['other-client'] = 'other-secret'
    auth = Authorization()

    default_token = auth.get_access_token()
    other_token = auth.get_access_token(client_id='other-client', client_secret='other-secret')

    assert default_token != other_token
    assert auth.get_access_token(client_id='other-client', client_secret='other-secret') == other_token
    assert auth.get_access_token() == default_token
    assert api.token_requests == 2


def test_expired_or_forced_token_is_refreshed(api):
    """An expired token or a forced refresh requests a new token."""
    auth = Authorization()
    first = auth.get_access_token()

    auth._tokens[api.DEFAULT_CLIENT_ID].last_update -= 10**6
    second = auth.get_access_token()
    assert second != first
    assert api.token_requests == 2

    third = auth.get_access_token(force=True)
    assert third != second
    assert api.token_requests == 3


def test_concurrent_callers_share_a_single_token_request(api):
    """Concurrent threads asking for a token share a single token request."""
    api.delays[TOKEN_PATH] = 0.2
    auth = Authorization()
    tokens = []
    threads = [threading.Thread(target=lambda: tokens.append(auth.get_access_token())) for _ in range(8)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(tokens) == 8
    assert len(set(tokens)) == 1
    assert api.token_requests == 1


async def test_concurrent_async_callers_share_a_single_token_request(api):
    """Concurrent async tasks asking for a token share a single token request."""
    api.delays[TOKEN_PATH] = 0.2

    tokens = await asyncio.gather(*(Authorization().aget_access_token() for _ in range(8)))

    assert len(set(tokens)) == 1
    assert api.token_requests == 1


def test_token_endpoint_errors(api, recorded_delays):
    """Token endpoint 404 is not retried, 5xx is retried up to AUTH_MAX_RETRY_ATTEMPTS and a bad body raises."""
    auth = Authorization()

    # A wrong host or path is a configuration problem and is not retried
    api.plan('POST', TOKEN_PATH, 404)
    with pytest.raises(APICompatibilityError):
        auth.get_access_token()
    assert api.count('POST', TOKEN_PATH) == 1
    assert recorded_delays == []

    # Transient server errors are retried
    api.plan('POST', TOKEN_PATH, 500, 503)
    assert auth.get_access_token()
    assert api.count('POST', TOKEN_PATH) == 4
    assert len(recorded_delays) == 2

    # But only up to the limit for the token endpoint
    api.plan('POST', TOKEN_PATH, *([502] * AUTH_MAX_RETRY_ATTEMPTS))
    with pytest.raises(ServerError):
        auth.get_access_token(force=True)
    assert api.count('POST', TOKEN_PATH) == 4 + AUTH_MAX_RETRY_ATTEMPTS

    # A response without the expected fields is an API compatibility problem
    api.plan('POST', TOKEN_PATH, {'status': 200, 'body': {'token_type': 'Bearer'}})
    with pytest.raises(APICompatibilityError):
        auth.get_access_token(force=True)


def test_rejected_token_is_replaced_only_once(api):
    """A refresh for a token that was already replaced reuses the replacement instead of requesting another one."""
    auth = Authorization()
    first = auth.get_access_token()

    second = auth.get_access_token(force=True, rejected_token=first)
    assert second != first
    assert api.token_requests == 2

    # Another caller rejected with the same old token gets the replacement from the cache
    assert auth.get_access_token(force=True, rejected_token=first) == second
    assert api.token_requests == 2

    # Without a rejected token force always requests a new token
    assert auth.get_access_token(force=True) != second
    assert api.token_requests == 3


async def test_async_rejected_token_is_replaced_only_once(api):
    """The async token request follows the same rule for rejected tokens as the sync one."""
    auth = Authorization()
    first = await auth.aget_access_token()

    second = await auth.aget_access_token(force=True, rejected_token=first)
    assert second != first
    assert await auth.aget_access_token(force=True, rejected_token=first) == second
    assert api.token_requests == 2
