"""HTTP, streaming and WebSocket communication with the ReStream API, plus OAuth2 token management."""

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, AsyncGenerator, Dict, Generator, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp
import ijson
import requests
from websocket import WebSocket, WebSocketBadStatusException, WebSocketConnectionClosedException

from .constants import (
    AUTH_REQUEST_TIMEOUT_SECONDS,
    ENDPOINTS,
    MAX_CONCURRENT_CALLS_TO_ENDPOINT,
    MAX_CONCURRENT_CALLS_TO_WEBSOCKET,
    REQUEST_TIMEOUT_SECONDS,
    RESTREAM_HOST,
    STREAMING_CONNECT_TIMEOUT_SECONDS,
    TOKEN_EXPIRY_MARGIN_SECONDS,
)
from .exceptions import (
    APICompatibilityError,
    APIConcurrencyLimitError,
    AuthError,
    BadRequestError,
    CredentialsError,
    ForbiddenError,
    NotFoundError,
    ServerError,
    WebsocketError,
)
from .retry import RetryPolicy, auth_retry_policy, retry
from .utils.singleton import Singleton

logger = logging.getLogger(__name__)


def _refresh_access_token(rejected_token: Optional[str] = None) -> str:
    """Obtain a new SDK-managed access token (sync). Used by retry policies after HTTP 401.

    Args:
        rejected_token: The token rejected by the server. If a concurrent call has already replaced it,
            the cached token is reused without a token request. None always requests a new token.

    Returns:
        The bearer token to use for the next attempt.
    """
    return Authorization().get_access_token(force=rejected_token is None, rejected_token=rejected_token)


async def _arefresh_access_token(rejected_token: Optional[str] = None) -> str:
    """Obtain a new SDK-managed access token (async). Used by retry policies after HTTP 401.

    Args:
        rejected_token: The token rejected by the server. If a concurrent call has already replaced it,
            the cached token is reused without a token request. None always requests a new token.

    Returns:
        The bearer token to use for the next attempt.
    """
    return await Authorization().aget_access_token(force=rejected_token is None, rejected_token=rejected_token)


def _create_sdk_retry_policy(**kwargs) -> RetryPolicy:
    """Create a RetryPolicy wired to the SDK token refresh.

    Args:
        **kwargs: Keyword arguments forwarded to RetryPolicy; refresh callbacks are set unless provided.

    Returns:
        A new RetryPolicy instance.
    """
    kwargs.setdefault('refresh_token', _refresh_access_token)
    kwargs.setdefault('arefresh_token', _arefresh_access_token)
    return RetryPolicy(**kwargs)


# Policy for requests that can be safely repeated (GET, PUT, PATCH, DELETE, idempotent POST)
_IDEMPOTENT_POLICY = _create_sdk_retry_policy()

# Policy for requests that must not be repeated once the server may have processed them (creating objects)
_NON_IDEMPOTENT_POLICY = _create_sdk_retry_policy(idempotent=False)


class BaseConcurrencyLimiter:
    """Base logic for concurrency limiters."""

    _semaphores = {}
    _limits = {}
    # Number of instances that hold or wait for the semaphore of each key
    _users = {}

    def __init__(self, url_key: str, client_key: str, limit: int, with_id_in_url: bool = False):
        """Initialize the limiter base.

        Args:
            url_key: The URL or endpoint string used to derive the semaphore key.
            client_key: Identifier of the client such as client_id or token.
            limit: Maximum number of concurrent operations associated with the url_key that
                are allowed for a client linked to the client_key.
            with_id_in_url: If False (default), all digit sequences in url_key are replaced with '$' so that
                requests to the same endpoint but with different numeric IDs share the same semaphore. If True,
                the url_key is used as-is, creating separate semaphores for distinct numeric IDs.
        """
        _url_key = url_key if with_id_in_url else self._replace_numbers(url_key)
        self._key: str = f'{_url_key}:{client_key}'
        self._limit: int = limit
        self._semaphore: threading.BoundedSemaphore | asyncio.BoundedSemaphore = None

    @staticmethod
    def _replace_numbers(key: str) -> str:
        """Normalize a key by replacing all digit sequences with a dollar sign.

        This groups URLs that differ only by numeric identifiers (e.g., IDs)
        under the same semaphore key so they share the same concurrency limit.

        Args:
            key: The URL or endpoint string.

        Returns:
            The key with digit sequences replaced, e.g. 'https://api/items/123/details' -> 'https://api/items/$/details'.
        """
        return re.sub(r'\d+', '$', key)

    def _get_or_create_semaphore(self, semaphore_type: object) -> threading.BoundedSemaphore | asyncio.BoundedSemaphore:
        """Get an existing semaphore for the key or create a new one.

        Args:
            semaphore_type: The semaphore class to instantiate (threading.BoundedSemaphore or asyncio.BoundedSemaphore).

        Returns:
            The semaphore instance associated with the composed key.

        Raises:
            ValueError: If a semaphore for the same key already exists with a different limit.
        """
        if self._key not in self._semaphores:
            self._semaphores[self._key] = semaphore_type(self._limit)
            self._limits[self._key] = self._limit
        elif self._limits.get(self._key) != self._limit:
            raise ValueError(
                f"Key '{self._key}' already initialized with limit={self._limits[self._key]}, got {self._limit}"
            )
        return self._semaphores[self._key]

    def _register_user(self, semaphore_type: object) -> None:
        """Bind this instance to the semaphore of its key and count it as a user of the key.

        Must be called before waiting for the semaphore, so that waiters are counted too.

        Args:
            semaphore_type: The semaphore class to instantiate if the key has no semaphore yet.
        """
        self._semaphore = self._get_or_create_semaphore(semaphore_type)
        self._users[self._key] = self._users.get(self._key, 0) + 1

    def _unregister_user(self) -> None:
        """Stop counting this instance as a user of its key and drop the registry entries once nobody uses it.

        Waiters are users too, so a semaphore is never replaced by a new one while someone still waits for it.
        """
        remaining = self._users.get(self._key, 1) - 1
        if remaining > 0:
            self._users[self._key] = remaining
            return
        self._users.pop(self._key, None)
        self._semaphores.pop(self._key, None)
        self._limits.pop(self._key, None)


class ConcurrencyLimiter(BaseConcurrencyLimiter):
    """Thread-safe context manager to limit concurrent sync calls per key.

    Purpose:
        Prevents sending too many simultaneous requests to the Restream backend,
        helping avoid APIConcurrencyLimitError (HTTP 429 / concurrency limit).

    How it works:
        - Uses a shared BoundedSemaphore per unique key (e.g., endpoint URL).
        - The first initialization for a given key creates the semaphore with the provided limit.
        - Subsequent initializations for the same key must pass the same limit, otherwise a ValueError is raised.
        - Entering the context acquires a slot; exiting releases it.

    Usage:
        with ConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = requests.get(url, ...)
    """

    _semaphores: dict[str, threading.BoundedSemaphore] = {}
    _limits: dict[str, int] = {}
    _users: dict[str, int] = {}
    _lock = threading.Lock()

    def __enter__(self):
        """Enter the context by acquiring a slot in the semaphore.

        Returns:
            self: The limiter instance so it can be used in a with-statement.
        """
        with self._lock:
            self._register_user(threading.BoundedSemaphore)
        try:
            self._semaphore.acquire()
        except BaseException:
            # The slot was not acquired, so only the registration is undone
            with self._lock:
                self._unregister_user()
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context by releasing the semaphore slot.

        This also removes internal references to the semaphore if nobody else holds
        or waits for it, so future usages can recreate it cleanly.

        Args:
            exc_type, exc_val, exc_tb: Exception information of the with-block, if any; it is not suppressed.
        """
        with self._lock:
            self._unregister_user()
        self._semaphore.release()


class AsyncConcurrencyLimiter(BaseConcurrencyLimiter):
    """Async context manager to limit concurrent async calls per key.

    Purpose:
        Prevents sending too many simultaneous async requests to the Restream backend,
        helping avoid APIConcurrencyLimitError (HTTP 429 / concurrency limit).

    How it works:
        - Maintains a shared asyncio.BoundedSemaphore per unique key (e.g., endpoint URL).
        - The first initialization for a given key creates the semaphore with the provided limit.
        - Subsequent initializations for the same key must pass the same limit, otherwise a ValueError is raised.
        - Entering the async context awaits a slot; exiting releases it.

    Usage:
        async with AsyncConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = await session.get(url, ...)
    """

    _semaphores: dict[str, asyncio.BoundedSemaphore] = {}
    _limits: dict[str, int] = {}
    _users: dict[str, int] = {}

    # The registry is updated without a lock on purpose: the updates contain no await, so a single
    # event loop cannot interleave them, and an asyncio.Lock would be bound to the first loop that used it.

    async def __aenter__(self):
        """Enter the async context by awaiting a slot in the semaphore.

        Returns:
            self: The limiter instance so it can be used in an async with-statement.
        """
        self._register_user(asyncio.BoundedSemaphore)
        try:
            await self._semaphore.acquire()
        except BaseException:
            # The slot was not acquired (e.g. the task was cancelled), so only the registration is undone
            self._unregister_user()
            raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context by releasing the semaphore slot.

        Also clears internal references to the semaphore if nobody else holds
        or waits for it, so a future usage can recreate it.

        Args:
            exc_type, exc_val, exc_tb: Exception information of the with-block, if any; it is not suppressed.
        """
        self._unregister_user()
        self._semaphore.release()


class Communicator:
    """Utility class that encapsulates HTTP communication (sync and async) with the REST API.

    It provides helpers to:
    - Build authorization headers and query parameters.
    - Validate HTTP response status codes and map errors to SDK exceptions.
    - Send GET/POST/PUT/PATCH/DELETE requests (sync via requests, async via aiohttp) with retries.
    - Stream large JSON arrays incrementally (sync and async) with on-the-fly value normalization.
    - Read WebSocket streams (sync via websocket-client, async via aiohttp).
    """

    @staticmethod
    def create_retry_policy(**kwargs) -> RetryPolicy:
        """Create a RetryPolicy wired to the SDK token refresh.

        Args:
            **kwargs: Keyword arguments forwarded to RetryPolicy; refresh callbacks are set unless provided.

        Returns:
            A new RetryPolicy instance.
        """
        return _create_sdk_retry_policy(**kwargs)

    @staticmethod
    def _create_headers(
        auth_token: str,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        as_list_of_strings: bool = False,
    ) -> Optional[Dict[str, str] | List[str]]:
        """Create request headers with optional Authorization and merge additional headers.

        Args:
            auth_token: The raw access token string. If empty or falsy, Authorization is omitted.
            additional_headers: Optional iterable of dicts with extra headers to merge. Later dicts override earlier ones.
            as_list_of_strings: If True, return headers as a list of "Key: Value" strings (websocket-client format).

        Returns:
            A headers dictionary (or list of strings) or None if neither auth_token nor additional_headers provided.
        """
        headers: Dict[str, str] | List[str] = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        if additional_headers:
            for hdr in additional_headers:
                if hdr:
                    headers.update(hdr)
        if as_list_of_strings:
            headers = [f"{k}: {v}" for k, v in headers.items()]
        return headers or None

    @staticmethod
    def _build_query_params(params: Mapping[str, Any]) -> List[Tuple[str, str]]:
        """Convert keyword filters into a list of (key, value) pairs accepted by both requests and aiohttp.

        None values are skipped, lists/tuples/sets are expanded into repeated keys, booleans become
        'true'/'false' and enums are replaced with their values.

        Args:
            params: Mapping of query parameter names to values.

        Returns:
            A list of (name, value) string pairs.
        """
        query: List[Tuple[str, str]] = []
        for key, value in params.items():
            values = value if isinstance(value, (list, tuple, set)) else [value]
            for item in values:
                if item is None:
                    continue
                if isinstance(item, Enum):
                    item = item.value
                if isinstance(item, bool):
                    item = str(item).lower()
                query.append((key, str(item)))
        return query

    @staticmethod
    def _add_query_params(url: str, params: Optional[Mapping[str, Any]]) -> str:
        """Append query parameters to a URL, keeping the ones already present.

        Args:
            url: The base URL, possibly with an existing query string.
            params: Mapping of query parameter names to values (see _build_query_params).

        Returns:
            The URL with the merged query string.
        """
        if not params:
            return url
        parts = list(urlparse(url))  # [scheme, netloc, path, params, query, fragment]
        existing = parse_qsl(parts[4], keep_blank_values=True)
        parts[4] = urlencode(existing + Communicator._build_query_params(params))
        return str(urlunparse(parts))

    @staticmethod
    def _parse_retry_after(headers: Optional[Mapping[str, str]]) -> Optional[float]:
        """Extract the delay suggested by the Retry-After header.

        Args:
            headers: Response headers or None. Header names are matched case-insensitively.

        Returns:
            The delay in seconds, or None if the header is absent, malformed or not finite.
        """
        value = None
        if headers:
            # websocket-client returns handshake headers as a plain dict with lower-case names
            value = next((v for k, v in headers.items() if k.lower() == 'retry-after'), None)
        if not value:
            return None
        try:
            seconds = float(value)
        except ValueError:
            seconds = None
        if seconds is not None:
            return max(0.0, seconds) if math.isfinite(seconds) else None
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            # parsedate_to_datetime returns a naive datetime for the -0000 zone, which stands for UTC
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())

    @staticmethod
    def _check_response_status_code(
        status_code: int,
        headers: Optional[Mapping[str, str]],
        body: Optional[str],
        url: str,
        auth_token: Optional[str] = None,
    ) -> None:
        """Validate an HTTP status code and raise the matching SDK exception.

        Args:
            status_code: HTTP status code of the response.
            headers: Response headers, used to read Retry-After for HTTP 429.
            body: Response body text, included in error messages for diagnostics.
            url: Requested URL, included in error messages.
            auth_token: Token sent with the request. It is attached to AuthError, so that a token refresh
                can tell whether a concurrent call has already replaced it.

        Raises:
            AuthError: HTTP 401.
            ForbiddenError: HTTP 403.
            NotFoundError: HTTP 404.
            APIConcurrencyLimitError: HTTP 429.
            ServerError: HTTP 5xx.
            BadRequestError: Any other HTTP 4xx.
        """
        if status_code < 400:
            return
        detail = body.strip()[:500] if body else ''
        suffix = f": {detail}" if detail else ''
        if status_code == 401:
            raise AuthError(rejected_token=auth_token)
        if status_code == 403:
            raise ForbiddenError(f"Forbidden: no access to {url}{suffix}")
        if status_code == 404:
            raise NotFoundError(f"Not found: {url}{suffix}")
        if status_code == 429:
            raise APIConcurrencyLimitError(retry_after=Communicator._parse_retry_after(headers))
        if status_code >= 500:
            raise ServerError(f"Server error {status_code} for {url}{suffix}", status_code=status_code)
        raise BadRequestError(f"HTTP {status_code} for {url}{suffix}", status_code=status_code)

    @staticmethod
    def _check_websocket_handshake_status(
        status_code: int,
        headers: Optional[Mapping[str, str]],
        body: Optional[str],
        url: str,
        auth_token: Optional[str],
    ) -> None:
        """Validate the HTTP status of a rejected WebSocket handshake and raise the matching SDK exception.

        The server rejects a WebSocket handshake with HTTP 403 both for an invalid or expired token and for
        a missing access to the object, so 403 is reported as AuthError: the SDK refreshes its own token once
        and gives up if the new token is rejected as well.

        Args:
            status_code: HTTP status code of the handshake response.
            headers: Handshake response headers.
            body: Handshake response body text, included in error messages for diagnostics.
            url: WebSocket URL, included in error messages.
            auth_token: Token sent with the handshake.

        Raises:
            AuthError: HTTP 401 or 403.
            NotFoundError, APIConcurrencyLimitError, ServerError, BadRequestError: As in _check_response_status_code.
        """
        if status_code == 403:
            raise AuthError(
                f"WebSocket handshake rejected with HTTP 403 for {url}: "
                "the token is invalid or has no access to the object",
                rejected_token=auth_token,
            )
        Communicator._check_response_status_code(status_code, headers, body, url, auth_token)

    @staticmethod
    def _parse_json_body(body: Optional[str], url: str) -> dict | list | None:
        """Parse a JSON response body.

        Args:
            body: Response body text; empty bodies (e.g. HTTP 204) are allowed.
            url: Requested URL, included in the error message.

        Returns:
            The parsed JSON value, or None for an empty body.

        Raises:
            APICompatibilityError: If the body is not valid JSON.
        """
        if not body or not body.strip():
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise APICompatibilityError(f"Invalid JSON response from {url}: {exc}") from exc

    @staticmethod
    def _convert_values(obj: dict[str, Any]) -> dict[str, Any]:
        """Normalize values in a dictionary to be JSON/HTTP friendly.

        - Decimal -> float
        - 'Infinity' string -> None
        - Other types are left as-is

        Args:
            obj: A dictionary to normalize.

        Returns:
            A new dictionary with values converted as described above.
        """

        def convert_value(value: Any) -> Any:
            """Convert a single value as described above.

            Args:
                value: The value to convert.

            Returns:
                The converted value.
            """
            if isinstance(value, Decimal):
                return float(value)
            elif value == 'Infinity':
                return None
            else:
                return value

        return {k: convert_value(v) for k, v in obj.items()}

    @staticmethod
    def _request(
        method: str, url: str, *, auth_token: str = None, payload: dict | list | None = None, **params
    ) -> dict | list | None:
        """Send a single synchronous HTTP request without retries.

        Args:
            method: HTTP method name.
            url: Target endpoint.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            payload: JSON-serializable request body or None.
            **params: Query parameters.

        Returns:
            Parsed JSON content (dict or list), or None for an empty body.
        """
        auth_token = auth_token or Authorization().get_access_token()
        headers = Communicator._create_headers(auth_token)
        query = Communicator._build_query_params(params)
        with ConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = requests.request(
                method, url, params=query, headers=headers, json=payload, timeout=REQUEST_TIMEOUT_SECONDS
            )
        Communicator._check_response_status_code(response.status_code, response.headers, response.text, url, auth_token)
        return Communicator._parse_json_body(response.text, url)

    @staticmethod
    async def _request_async(
        method: str, url: str, *, auth_token: str = None, payload: dict | list | None = None, **params
    ) -> dict | list | None:
        """Send a single asynchronous HTTP request without retries.

        Args:
            method: HTTP method name.
            url: Target endpoint.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            payload: JSON-serializable request body or None.
            **params: Query parameters.

        Returns:
            Parsed JSON content (dict or list), or None for an empty body.
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        headers = Communicator._create_headers(auth_token)
        query = Communicator._build_query_params(params)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with AsyncConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
                async with session.request(method, url, params=query, headers=headers, json=payload) as response:
                    status_code, response_headers = response.status, response.headers
                    body = await response.text()
        Communicator._check_response_status_code(status_code, response_headers, body, url, auth_token)
        return Communicator._parse_json_body(body, url)

    @staticmethod
    def send_get_request(url: str, *, auth_token: str = None, **params) -> dict | list | None:
        """Send a synchronous HTTP GET request with retries.

        Args:
            url: Target endpoint.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, ForbiddenError, NotFoundError, BadRequestError, APIConcurrencyLimitError, ServerError,
            APICompatibilityError.
        """
        return _IDEMPOTENT_POLICY.execute(Communicator._request, 'GET', url, auth_token=auth_token, **params)

    @staticmethod
    async def send_get_request_async(url: str, *, auth_token: str = None, **params) -> dict | list | None:
        """Send an asynchronous HTTP GET request with retries.

        Args:
            url: Target endpoint.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, ForbiddenError, NotFoundError, BadRequestError, APIConcurrencyLimitError, ServerError,
            APICompatibilityError.
        """
        return await _IDEMPOTENT_POLICY.aexecute(
            Communicator._request_async, 'GET', url, auth_token=auth_token, **params
        )

    @staticmethod
    def send_post_request(
        url: str, payload: dict | list, *, auth_token: str = None, idempotent: bool = False, **params
    ) -> dict | list | None:
        """Send a synchronous HTTP POST request with a JSON payload and retries.

        Args:
            url: Target endpoint.
            payload: JSON-serializable body.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            idempotent: True for requests that can be safely repeated. By default a POST is assumed to create
                objects and is retried only when the server could not have processed it (connection errors,
                HTTP 401, HTTP 429).
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, ForbiddenError, NotFoundError, BadRequestError, APIConcurrencyLimitError, ServerError,
            APICompatibilityError.
        """
        policy = _IDEMPOTENT_POLICY if idempotent else _NON_IDEMPOTENT_POLICY
        return policy.execute(Communicator._request, 'POST', url, auth_token=auth_token, payload=payload, **params)

    @staticmethod
    async def send_post_request_async(
        url: str, payload: dict | list, *, auth_token: str = None, idempotent: bool = False, **params
    ) -> dict | list | None:
        """Send an asynchronous HTTP POST request with a JSON payload and retries.

        Args:
            url: Target endpoint.
            payload: JSON-serializable body.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            idempotent: True for requests that can be safely repeated. By default a POST is assumed to create
                objects and is retried only when the server could not have processed it (connection errors,
                HTTP 401, HTTP 429).
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, ForbiddenError, NotFoundError, BadRequestError, APIConcurrencyLimitError, ServerError,
            APICompatibilityError.
        """
        policy = _IDEMPOTENT_POLICY if idempotent else _NON_IDEMPOTENT_POLICY
        return await policy.aexecute(
            Communicator._request_async, 'POST', url, auth_token=auth_token, payload=payload, **params
        )

    @staticmethod
    def send_put_request(url: str, payload: dict, *, auth_token: str = None, **params) -> dict | list | None:
        """Send a synchronous HTTP PUT request with a JSON payload and retries.

        Args:
            url: Target endpoint.
            payload: JSON-serializable body.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).
        """
        return _IDEMPOTENT_POLICY.execute(
            Communicator._request, 'PUT', url, auth_token=auth_token, payload=payload, **params
        )

    @staticmethod
    async def send_put_request_async(
        url: str, payload: dict, *, auth_token: str = None, **params
    ) -> dict | list | None:
        """Send an asynchronous HTTP PUT request with a JSON payload and retries.

        Args:
            url: Target endpoint.
            payload: JSON-serializable body.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).
        """
        return await _IDEMPOTENT_POLICY.aexecute(
            Communicator._request_async, 'PUT', url, auth_token=auth_token, payload=payload, **params
        )

    @staticmethod
    def send_patch_request(url: str, payload: dict, *, auth_token: str = None, **params) -> dict | list | None:
        """Send a synchronous HTTP PATCH request with a JSON payload and retries.

        Args:
            url: Target endpoint.
            payload: JSON-serializable body with the fields to update.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).
        """
        return _IDEMPOTENT_POLICY.execute(
            Communicator._request, 'PATCH', url, auth_token=auth_token, payload=payload, **params
        )

    @staticmethod
    async def send_patch_request_async(
        url: str, payload: dict, *, auth_token: str = None, **params
    ) -> dict | list | None:
        """Send an asynchronous HTTP PATCH request with a JSON payload and retries.

        Args:
            url: Target endpoint.
            payload: JSON-serializable body with the fields to update.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).
        """
        return await _IDEMPOTENT_POLICY.aexecute(
            Communicator._request_async, 'PATCH', url, auth_token=auth_token, payload=payload, **params
        )

    @staticmethod
    def send_delete_request(url: str, *, auth_token: str = None, **params) -> dict | list | None:
        """Send a synchronous HTTP DELETE request with retries.

        Args:
            url: Target endpoint.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content, or None when the server responds without a body (HTTP 204).
        """
        return _IDEMPOTENT_POLICY.execute(Communicator._request, 'DELETE', url, auth_token=auth_token, **params)

    @staticmethod
    async def send_delete_request_async(url: str, *, auth_token: str = None, **params) -> dict | list | None:
        """Send an asynchronous HTTP DELETE request with retries.

        Args:
            url: Target endpoint.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content, or None when the server responds without a body (HTTP 204).
        """
        return await _IDEMPOTENT_POLICY.aexecute(
            Communicator._request_async, 'DELETE', url, auth_token=auth_token, **params
        )

    # Reconnection logic for the streaming generators lives in Data and DataAsync.
    @staticmethod
    def streaming_get_generator(url: str, auth_token: str = None, **params) -> Generator[dict, None, None]:
        """Stream a JSON array from a GET endpoint synchronously.

        Items are yielded one-by-one without loading the whole response into memory
        and each item is passed through _convert_values.

        Args:
            url: Target endpoint returning a JSON array.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Yields:
            Normalized dict objects representing each item in the streamed array.

        Raises:
            AuthError, ForbiddenError, NotFoundError, BadRequestError, APIConcurrencyLimitError, ServerError.
        """
        auth_token = auth_token or Authorization().get_access_token()
        headers = Communicator._create_headers(auth_token, additional_headers=[{'Prefer': 'streaming'}])
        query = Communicator._build_query_params(params)
        with ConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            with requests.get(
                url,
                params=query,
                headers=headers,
                stream=True,
                timeout=(STREAMING_CONNECT_TIMEOUT_SECONDS, None),
            ) as stream:
                # The body is read only for error responses: reading it would consume the stream otherwise
                body = stream.text if stream.status_code >= 400 else ''
                Communicator._check_response_status_code(stream.status_code, stream.headers, body, url, auth_token)
                stream.raw.decode_content = True
                for obj in ijson.items(stream.raw, 'item'):
                    yield Communicator._convert_values(obj)

    @staticmethod
    async def streaming_get_generator_async(
        url: str, auth_token: str = None, **params
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Stream a JSON array from a GET endpoint asynchronously.

        Items are yielded one-by-one without loading the whole response into memory
        and each item is passed through _convert_values.

        Args:
            url: Target endpoint returning a JSON array.
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            **params: Query parameters to include in the request.

        Yields:
            Normalized dict objects representing each item in the streamed array.

        Raises:
            AuthError, ForbiddenError, NotFoundError, BadRequestError, APIConcurrencyLimitError, ServerError.
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        headers = Communicator._create_headers(auth_token, additional_headers=[{'Prefer': 'streaming'}])
        query = Communicator._build_query_params(params)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=STREAMING_CONNECT_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with AsyncConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
                async with session.get(url, params=query, headers=headers) as stream:
                    body = await stream.text() if stream.status >= 400 else ''
                    Communicator._check_response_status_code(stream.status, stream.headers, body, url, auth_token)
                    async for obj in ijson.items(stream.content, 'item'):
                        yield Communicator._convert_values(obj)

    @staticmethod
    def _parse_websocket_message(data: str | bytes, get_nested_key: Optional[str]) -> List[Any]:
        """Decode a WebSocket frame into the list of items to yield.

        Args:
            data: Raw frame payload. Text frames are parsed as JSON, binary frames are returned as-is.
            get_nested_key: If set, only this key of the decoded JSON object is used.

        Returns:
            A list of items: the decoded value itself, or its elements when the value is a JSON array.
            Empty strings and JSON null produce an empty list.
        """
        if isinstance(data, (bytes, bytearray)):
            return [data]
        # Sometimes the server sends empty messages; they carry no data
        if data == '':
            return []
        decoded = json.loads(data)
        if decoded is None:
            return []
        if get_nested_key is not None:
            decoded = decoded[get_nested_key]
        return decoded if isinstance(decoded, list) else [decoded]

    @staticmethod
    def websocket_generator(
        url: str,
        auth_token: str = None,
        params: Optional[dict] = None,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        get_nested_key: str = None,
    ) -> Generator[Any, None, None]:
        """Connect to a WebSocket and yield incoming messages synchronously using websocket-client.

        Args:
            url: WebSocket endpoint URL (ws:// or wss://).
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            params: Optional query parameters appended to the URL.
            additional_headers: Optional list of dicts to merge into the request headers.
            get_nested_key: Receive only this key from each message.

        Yields:
            Decoded message payloads (dicts for JSON text frames, bytes for binary frames).

        Raises:
            AuthError (HTTP 401 or 403), NotFoundError, APIConcurrencyLimitError, ServerError on a failed
            handshake; APICompatibilityError if a message cannot be parsed.
        """
        auth_token = auth_token or Authorization().get_access_token()
        full_url = Communicator._add_query_params(url, params)
        header_list = Communicator._create_headers(auth_token, additional_headers, as_list_of_strings=True)
        ws = WebSocket(skip_utf8_validation=True)
        # Blocking reads without a timeout: users can wrap the generator in their own timeout logic if needed
        try:
            # with_id_in_url=True gives URLs with different pad/site IDs independent limits
            with ConcurrencyLimiter(url, auth_token, MAX_CONCURRENT_CALLS_TO_WEBSOCKET, with_id_in_url=True):
                ws.connect(full_url, header=header_list)
                while True:
                    data = ws.recv()
                    for item in Communicator._parse_websocket_message(data, get_nested_key):
                        yield item
        except WebSocketConnectionClosedException:
            # The server closed the connection: stop iteration gracefully
            return
        except WebSocketBadStatusException as e:
            body = getattr(e, 'resp_body', None)
            if isinstance(body, bytes):
                body = body.decode('utf-8', errors='replace')
            Communicator._check_websocket_handshake_status(
                e.status_code, getattr(e, 'resp_headers', None), body, url, auth_token
            )
            raise
        except (ValueError, KeyError, TypeError) as e:
            raise APICompatibilityError(f'Cannot parse WebSocket message: {repr(e)}') from e
        finally:
            try:
                ws.close()
            except Exception:
                pass

    @staticmethod
    async def websocket_generator_async(
        url: str,
        auth_token: str = None,
        params: Optional[dict] = None,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        get_nested_key: str = None,
    ) -> AsyncGenerator[Any, None]:
        """Connect to a WebSocket and yield incoming messages asynchronously using aiohttp.

        Args:
            url: WebSocket endpoint URL (ws:// or wss://).
            auth_token: Access token for the Authorization header. Obtained from Authorization if None.
            params: Optional query parameters appended to the URL.
            additional_headers: Optional list of dicts to merge into the request headers.
            get_nested_key: Receive only this key from each message.

        Yields:
            Decoded message payloads (dicts for JSON text frames, bytes for binary frames).

        Raises:
            AuthError (HTTP 401 or 403), NotFoundError, APIConcurrencyLimitError, ServerError on a failed
            handshake; WebsocketError on a protocol error; APICompatibilityError if a message cannot be parsed.
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        headers = Communicator._create_headers(auth_token, additional_headers)
        query = Communicator._build_query_params(params or {})
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            ws = None
            try:
                # with_id_in_url=True gives URLs with different pad/site IDs independent limits
                async with AsyncConcurrencyLimiter(
                    url, auth_token, MAX_CONCURRENT_CALLS_TO_WEBSOCKET, with_id_in_url=True
                ):
                    async with session.ws_connect(url, headers=headers, params=query) as ws:
                        while True:
                            msg = await ws.receive()
                            if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                for item in Communicator._parse_websocket_message(msg.data, get_nested_key):
                                    yield item
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                break
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise WebsocketError(f'WebSocket error: {ws.exception()!r}')
                            # Other control frames are ignored
            except aiohttp.WSServerHandshakeError as e:
                Communicator._check_websocket_handshake_status(e.status, e.headers, e.message, url, auth_token)
                raise
            except (KeyError, ValueError, TypeError) as e:
                raise APICompatibilityError(f'Cannot parse WebSocket message: {repr(e)}') from e
            finally:
                try:
                    if ws is not None and not ws.closed:
                        await ws.close(code=aiohttp.WSCloseCode.OK)
                except Exception:
                    pass


@dataclass
class RestreamToken:
    """A cached OAuth2 access token together with the data needed to validate its reuse."""

    token: str
    expires_in: int
    last_update: int
    # Hash of the credentials that produced the token; the token is reused only for the same credentials
    secret_hash: str

    def is_expired(self) -> bool:
        """Return True if the token has expired or is about to expire within TOKEN_EXPIRY_MARGIN_SECONDS."""
        return datetime.now().timestamp() >= self.expires_in + self.last_update - TOKEN_EXPIRY_MARGIN_SECONDS

    def matches_secret(self, secret_hash: str) -> bool:
        """Return True if the token was issued for the credentials with the given hash.

        Args:
            secret_hash: Hash of the client_id and client_secret pair to compare with.

        Returns:
            True on a constant-time match, False otherwise.
        """
        return hmac.compare_digest(self.secret_hash, secret_hash)


class Authorization(metaclass=Singleton):
    """Handles ReStream OAuth2 client-credentials authentication and token caching.

    Tokens are cached per client_id and are reused only when the caller presents the same
    client_secret the token was obtained with. The class is a process-wide singleton.
    """

    _api_url_auth: str = ENDPOINTS.auth_access_token.value

    def __init__(self) -> None:
        """Initialize the token cache. The instance is created only once because the class is a singleton."""
        self._tokens: dict[str, RestreamToken] = {}

    @classmethod
    def _build_auth_url(cls) -> str:
        """Build the absolute auth endpoint URL using the RESTREAM_HOST environment variable or the default host.

        Returns:
            The absolute token endpoint URL.
        """
        base_url = os.environ.get('RESTREAM_HOST', RESTREAM_HOST).rstrip('/')
        return f"{base_url}{cls._api_url_auth}"

    @staticmethod
    def _select_client_id_and_secret(client_id: str = None, client_secret: str = None) -> Tuple[str, str]:
        """Pick client_id and client_secret from arguments or environment.

        Args:
            client_id: Optional explicit client ID. If not provided, reads RESTREAM_CLIENT_ID from env.
            client_secret: Optional explicit client secret. If not provided, reads RESTREAM_CLIENT_SECRET from env.

        Returns:
            A tuple (client_id, client_secret) which may include None values if not set.
        """
        return (
            client_id or os.environ.get("RESTREAM_CLIENT_ID"),
            client_secret or os.environ.get("RESTREAM_CLIENT_SECRET"),
        )

    @staticmethod
    def _create_payload(client_id: str, client_secret: str) -> dict:
        """Build the form payload for the client-credentials token request.

        Args:
            client_id: ReStream OAuth2 client ID.
            client_secret: ReStream OAuth2 client secret.

        Returns:
            Dict suitable for an x-www-form-urlencoded POST body.

        Raises:
            CredentialsError: If either value is missing.
        """
        if not (client_id and client_secret):
            raise CredentialsError(
                "Must provide client_id and client_secret via method parameters or RESTREAM_CLIENT_ID,"
                " RESTREAM_CLIENT_SECRET environment variables"
            )
        return {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
        }

    @staticmethod
    def _hash_secret(client_id: str, client_secret: str) -> str:
        """Hash the credentials pair so that the plain secret is never stored next to the token.

        Args:
            client_id: ReStream OAuth2 client ID.
            client_secret: ReStream OAuth2 client secret.

        Returns:
            Hex digest of the credentials pair.
        """
        return hashlib.sha256(f"{client_id}:{client_secret}".encode('utf-8')).hexdigest()

    @staticmethod
    def _parse_response(json_response: Any) -> Tuple[str, int]:
        """Extract the token and its lifetime from the parsed token endpoint response.

        Args:
            json_response: Parsed JSON body of the token endpoint response.

        Returns:
            Tuple of (access_token, expires_in_seconds).

        Raises:
            APICompatibilityError: If required fields are missing.
        """
        if (
            not isinstance(json_response, dict)
            or "access_token" not in json_response
            or 'expires_in' not in json_response
        ):
            raise APICompatibilityError("Can't get access token from the response")
        return json_response['access_token'], int(json_response['expires_in'])

    def _get_cached_token(self, client_id: str, secret_hash: str) -> Optional[str]:
        """Return a cached token that is valid for the given credentials.

        Args:
            client_id: The client identifier used as the cache key.
            secret_hash: Hash of the credentials pair; a cached token issued for other credentials is ignored.

        Returns:
            The bearer token, or None if there is no usable token in the cache.
        """
        cached = self._tokens.get(client_id)
        if cached is None or cached.is_expired() or not cached.matches_secret(secret_hash):
            return None
        return cached.token

    def _get_reusable_token(
        self, client_id: str, secret_hash: str, force: bool, rejected_token: Optional[str]
    ) -> Optional[str]:
        """Return the cached token if it can be used instead of requesting a new one.

        Args:
            client_id: The client identifier used as the cache key.
            secret_hash: Hash of the credentials pair the token must be issued for.
            force: If True and rejected_token is None, the cache is never used.
            rejected_token: Token the server has just rejected. The cache is used only if it holds another
                token, i.e. a concurrent call has already replaced the rejected one.

        Returns:
            The cached bearer token, or None if a new token must be requested.
        """
        if force and rejected_token is None:
            return None
        cached = self._get_cached_token(client_id, secret_hash)
        if cached is None or cached == rejected_token:
            return None
        return cached

    def _save_token(self, json_response: Any, client_id: str, secret_hash: str) -> None:
        """Parse and store the token for the given credentials in the in-memory cache.

        Args:
            json_response: Parsed JSON body of the token endpoint response.
            client_id: The client identifier used as the cache key.
            secret_hash: Hash of the credentials pair the token was issued for.
        """
        restream_auth_token, expires_in = self._parse_response(json_response)
        self._tokens[client_id] = RestreamToken(
            token=restream_auth_token,
            expires_in=expires_in,
            last_update=int(datetime.now().timestamp()),
            secret_hash=secret_hash,
        )

    @staticmethod
    def _check_response_status_code(
        status_code: int, headers: Optional[Mapping[str, str]], body: Optional[str], url: str
    ) -> None:
        """Validate the status code of a token endpoint response.

        Args:
            status_code: HTTP status code of the response.
            headers: Response headers.
            body: Response body text.
            url: Token endpoint URL.

        Raises:
            CredentialsError: On 401 or 403, i.e. the credentials were rejected.
            APICompatibilityError: On 404, i.e. the host or the path is wrong.
            APIConcurrencyLimitError, ServerError, BadRequestError: As for regular requests.
        """
        try:
            Communicator._check_response_status_code(status_code, headers, body, url)
        except AuthError as exc:
            raise CredentialsError("Unauthorized: invalid client_id and/or client_secret") from exc
        except NotFoundError as exc:
            raise APICompatibilityError(f"Wrong authorization url: {url}") from exc

    @retry(auth_retry_policy())
    def get_access_token(
        self,
        client_id: str = None,
        client_secret: str = None,
        force: bool = False,
        rejected_token: Optional[str] = None,
    ) -> str:
        """Synchronously obtain a valid ReStream access token with caching.

        Args:
            client_id: Optional override for client ID; falls back to RESTREAM_CLIENT_ID environment variable.
            client_secret: Optional override for client secret; falls back to RESTREAM_CLIENT_SECRET environment variable.
            force: If True, bypass the cache and request a new token (unless rejected_token is given).
            rejected_token: Token the server has just rejected. A new token is requested only if the cache
                still holds it, so concurrent authorization failures lead to a single token request.

        Returns:
            Bearer token string.

        Raises:
            CredentialsError: If the credentials are missing or rejected by the server.
        """
        client_id, client_secret = Authorization._select_client_id_and_secret(client_id, client_secret)
        # Credentials are validated before touching the cache: a cached token must never be
        # returned for a request that does not present the matching secret
        payload = self._create_payload(client_id, client_secret)
        secret_hash = self._hash_secret(client_id, client_secret)

        if (cached := self._get_reusable_token(client_id, secret_hash, force, rejected_token)) is not None:
            return cached

        url = self._build_auth_url()
        # Works as an independent lock for each client_id
        with ConcurrencyLimiter(url, client_id, 1):
            # Another thread may have acquired the token while we were waiting for the lock
            if (cached := self._get_reusable_token(client_id, secret_hash, force, rejected_token)) is not None:
                return cached

            response = requests.post(url, data=payload, timeout=AUTH_REQUEST_TIMEOUT_SECONDS)
            Authorization._check_response_status_code(response.status_code, response.headers, response.text, url)
            self._save_token(Communicator._parse_json_body(response.text, url), client_id, secret_hash)
        return self._tokens[client_id].token

    @retry(auth_retry_policy())
    async def aget_access_token(
        self,
        client_id: str = None,
        client_secret: str = None,
        force: bool = False,
        rejected_token: Optional[str] = None,
    ) -> str:
        """Asynchronously obtain a valid ReStream access token with caching.

        Args:
            client_id: Optional override for client ID; falls back to RESTREAM_CLIENT_ID environment variable.
            client_secret: Optional override for client secret; falls back to RESTREAM_CLIENT_SECRET environment variable.
            force: If True, bypass the cache and request a new token (unless rejected_token is given).
            rejected_token: Token the server has just rejected. A new token is requested only if the cache
                still holds it, so concurrent authorization failures lead to a single token request.

        Returns:
            Bearer token string.

        Raises:
            CredentialsError: If the credentials are missing or rejected by the server.
        """
        client_id, client_secret = Authorization._select_client_id_and_secret(client_id, client_secret)
        # Credentials are validated before touching the cache: a cached token must never be
        # returned for a request that does not present the matching secret
        payload = self._create_payload(client_id, client_secret)
        secret_hash = self._hash_secret(client_id, client_secret)

        if (cached := self._get_reusable_token(client_id, secret_hash, force, rejected_token)) is not None:
            return cached

        url = self._build_auth_url()
        # Works as an independent lock for each client_id
        async with AsyncConcurrencyLimiter(url, client_id, 1):
            # Another task may have acquired the token while we were waiting for the lock
            if (cached := self._get_reusable_token(client_id, secret_hash, force, rejected_token)) is not None:
                return cached

            timeout = aiohttp.ClientTimeout(total=AUTH_REQUEST_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, data=payload) as response:
                    status_code, headers = response.status, response.headers
                    body = await response.text()
            Authorization._check_response_status_code(status_code, headers, body, url)
            self._save_token(Communicator._parse_json_body(body, url), client_id, secret_hash)
        return self._tokens[client_id].token
