import asyncio
import os
import re
import threading
import time
import functools
import sys
from datetime import datetime
from decimal import Decimal
from json import JSONDecodeError
from typing import Generator, Any, AsyncGenerator, Optional, Iterable, Dict, List, Tuple
import warnings
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import aiohttp
import requests
import httpx
import ijson
import json

from aiohttp import WSServerHandshakeError
from websocket import WebSocket, WebSocketConnectionClosedException, WebSocketBadStatusException

from .constants import ENDPOINTS, RESTREAM_HOST, MAX_CONCURRENT_CALLS_TO_ENDPOINT, MAX_CONCURRENT_CALLS_TO_WEBSOCKET
from .exceptions import (
    AuthError,
    APICompatibilityError,
    APIConcurrencyLimitError,
    WebsocketError,
    ServerError,
    CredentialsError,
)
from .utils.singleton import Singleton


def exponential_backoff(_func=None, *, attempts: int = 4, initial_delay: float = 1, factor: float = 4.0):
    """A decorator that retries a function with exponential backoff on exceptions.

    Supports both synchronous and asynchronous functions. Will not retry on
    AuthError, APICompatibilityError, or APIConcurrencyLimitError, propagating
    them immediately.

    Can be used with or without parameters:
        @exponential_backoff
        @exponential_backoff(attempts=5, initial_delay=1.0)

    Parameters:
        attempts: Total number of attempts to try (including the first call).
        initial_delay: Delay before the first retry in seconds.
        factor: Multiplier applied to the delay after each failed attempt.
    """

    def decorator(func):
        # During pytest runs, force attempts=1; otherwise use provided/default attempts
        effective_attempts = 1 if 'pytest' in sys.modules else attempts
        if effective_attempts < 1:
            raise ValueError("attempts must be >= 1")

        is_coro = asyncio.iscoroutinefunction(func)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(effective_attempts):
                try:
                    return await func(*args, **kwargs)
                except (CredentialsError, APICompatibilityError, APIConcurrencyLimitError):
                    # Do not retry on these errors
                    raise
                except (AuthError, Exception) as e:
                    if i == effective_attempts - 1:
                        raise
                    if isinstance(e, AuthError):
                        warnings.warn(
                            f'Authorization failed for {func.__name__}. '
                            f'Requesting a new access token and retrying after {delay} seconds.',
                            RuntimeWarning,
                        )
                        # Recreates auth token in the Authorization singleton class
                        # Never raise the AuthError within the Authorization class to avoid recursion!
                        await Authorization().aget_access_token(force=True)
                    else:
                        warnings.warn(
                            f"Unexpected exception raised by {func.__name__}: {e}, retry after {delay} seconds.",
                            RuntimeWarning,
                        )
                    await asyncio.sleep(delay)
                    delay *= factor

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(effective_attempts):
                try:
                    return func(*args, **kwargs)
                except (CredentialsError, APICompatibilityError, APIConcurrencyLimitError):
                    # Do not retry on these errors
                    raise
                except (AuthError, Exception) as e:
                    if i == effective_attempts - 1:
                        raise
                    if isinstance(e, AuthError):
                        warnings.warn(
                            f'Authorization failed for {func.__name__}. '
                            f'Requesting a new access token and retrying after {delay} seconds.',
                            RuntimeWarning,
                        )
                        # Recreates auth token in the Authorization singleton class
                        # Never raise the AuthError within the Authorization class to avoid recursion!
                        Authorization().get_access_token(force=True)
                    else:
                        warnings.warn(
                            f"Unexpected exception raised by {func.__name__}: {e}, retry after {delay} seconds.",
                            RuntimeWarning,
                        )
                    time.sleep(delay)
                    delay *= factor

        return async_wrapper if is_coro else sync_wrapper

    # If used without parentheses: @exponential_backoff
    if _func is None:
        return decorator
    else:
        return decorator(_func)


class BaseConcurrencyLimiter:
    def __init__(self, key: str, limit: int, with_id_in_url: bool = False):
        self._key = key if with_id_in_url else self._replace_numbers(key)
        self._limit = limit

    @staticmethod
    def _replace_numbers(key: str) -> str:
        return re.sub(r'\d+', '$', key)


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
        with ConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = requests.get(url, ...)
    """

    _semaphores: dict[str, threading.BoundedSemaphore] = {}
    _limits: dict[str, int] = {}
    _lock = threading.Lock()

    def __init__(self, key: str, limit: int, with_id_in_url: bool = False):
        """Initialize the concurrency limiter for a specific key.

        Parameters:
            key: A stable identifier that groups calls sharing the same limit (e.g., full request URL or endpoint).
            limit: Maximum number of concurrent sync calls allowed for the given key.
            with_id_in_url: If False (default), any sequences of digits in the key are replaced with '$' so that
                requests to the same endpoint but with different numeric IDs share one semaphore. If True, the key
                is used as-is which creates separate semaphores per distinct numeric ID in the URL.
        """
        super().__init__(key, limit, with_id_in_url)
        with self._lock:
            if self._key not in self._semaphores:
                self._semaphores[self._key] = threading.BoundedSemaphore(limit)
                self._limits[self._key] = limit
            elif self._limits.get(self._key) != limit:
                raise ValueError(
                    f"Key '{self._key}' already initialized with limit={self._limits[self._key]}, got {limit}"
                )
        self._semaphore = self._semaphores[self._key]

    def __enter__(self):
        self._semaphore.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
        async with AsyncConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = await client.get(url, ...)
    """

    _semaphores: dict[str, asyncio.BoundedSemaphore] = {}
    _limits: dict[str, int] = {}

    def __init__(self, key: str, limit: int, with_id_in_url: bool = False):
        """Initialize the async concurrency limiter for a specific key.

        Parameters:
            key: A stable identifier that groups calls sharing the same limit (e.g., full request URL or endpoint).
            limit: Maximum number of concurrent async calls allowed for the given key.
            with_id_in_url: If False (default), any sequences of digits in the key are replaced with '$' so that
                requests to the same endpoint but with different numeric IDs share one semaphore. If True, the key
                is used as-is which creates separate semaphores per distinct numeric ID in the URL.
        """
        super().__init__(key, limit, with_id_in_url)
        if self._key not in self._semaphores:
            self._semaphores[self._key] = asyncio.BoundedSemaphore(limit)
            self._limits[self._key] = limit
        elif self._limits.get(self._key) != limit:
            raise ValueError(f"Key '{self._key}' already initialized with limit={self._limits[self._key]}, got {limit}")
        self._semaphore = self._semaphores[self._key]

    async def __aenter__(self):
        await self._semaphore.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()


class Communicator:
    """Utility class that encapsulates HTTP communication (sync and async) with a REST API.

    It provides helpers to:
    - Build authorization headers.
    - Validate HTTP response status codes and map common errors to SDK exceptions.
    - Send GET/POST requests (sync and async).
    - Stream large JSON arrays incrementally (sync and async) with on-the-fly value normalization.
    """

    @staticmethod
    def _create_headers(
        auth_token: str,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        as_list_of_strings: bool = False,
    ) -> Optional[Dict[str, str] | List[str]]:
        """Create request headers with optional Authorization and merge additional headers.

        Parameters:
            auth_token: The raw access token string. If empty or falsy, Authorization is omitted.
            additional_headers: Optional iterable of dicts with extra headers to merge. Later dicts override earlier ones.

        Returns:
            A headers dictionary or None if neither auth_token nor additional_headers provided.
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
    def _add_query_params(url: str, params: dict) -> str:
        """Add query params to a URL."""
        if not params:
            return url
        parts = list(urlparse(url))  # [scheme, netloc, path, params, query, fragment]
        if len(parts) < 5:
            warnings.warn(f"URL is too short: {url}", RuntimeWarning)
        existing = parse_qsl(parts[4], keep_blank_values=True)

        extra = []
        for k, v in params.items():
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                extra.extend((k, str(x)) for x in v if x is not None)
            else:
                extra.append((k, str(v)))

        parts[4] = urlencode(existing + extra, doseq=True)
        return str(urlunparse(parts))

    @staticmethod
    def _check_response_status_code(
        response: (
            httpx.Response
            | requests.Response
            | aiohttp.ClientResponse
            | WSServerHandshakeError
            | WebSocketBadStatusException
        ),
    ):
        """Validate HTTP response status and raise SDK-specific exceptions.

        Parameters:
            response: An httpx.Response, requests.Response or aiohttp.ClientResponse instance.

        Raises:
            AuthError: When the server returns 401 or 403.
            APICompatibilityError: When the server returns 404 (endpoint not found).
            APIConcurrencyLimitError: When the server returns 429 (rate limit / concurrency limit).
            HTTPError: Propagated from the underlying client for other non-2xx codes.
            RuntimeError: If the response type is not supported.
        """
        if isinstance(response, (httpx.Response, requests.Response, WebSocketBadStatusException)):
            status_code = response.status_code
        elif isinstance(response, (aiohttp.ClientResponse, WSServerHandshakeError)):
            status_code = response.status
        else:
            raise RuntimeError('Unknown response type')
        if status_code in [401, 403]:
            raise AuthError()
        if status_code == 404:
            raise APICompatibilityError("The endpoint does not exist")
        if status_code == 429:
            raise APIConcurrencyLimitError()
        if status_code == 500:
            raise ServerError()
        if isinstance(response, (WSServerHandshakeError, WebSocketBadStatusException)):
            raise response
        response.raise_for_status()

    @staticmethod
    def _convert_values(obj: dict[str, Any]) -> dict[str, Any]:
        """Normalize values in a dictionary to be JSON/HTTP friendly.

        - Decimal -> float
        - 'Infinity' string -> None
        - Other types are left as-is

        Parameters:
            obj: A dictionary to normalize.

        Returns:
            A new dictionary with values converted as described above.
        """

        def convert_value(value: Any) -> Any:
            if isinstance(value, Decimal):
                return float(value)
            elif value == 'Infinity':
                return None
            else:
                return value

        return {k: convert_value(v) for k, v in obj.items()}

    @staticmethod
    @exponential_backoff
    def send_get_request(url: str, auth_token: str = None, **params) -> dict | list:
        """Send a synchronous HTTP GET request.

        Parameters:
            url: Target endpoint.
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        auth_token = auth_token or Authorization().get_access_token()
        headers = Communicator._create_headers(auth_token)
        with ConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = requests.get(url, params=params, headers=headers)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    @exponential_backoff
    async def send_get_request_async(url: str, auth_token: str = None, **params) -> dict | list:
        """Send an asynchronous HTTP GET request.

        Parameters:
            url: Target endpoint.
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        headers = Communicator._create_headers(auth_token)
        # In contrast to requests.get(), it doesn’t clean up the final URL from parameters whose values are None
        params_cleaned = {k: v for k, v in params.items() if v is not None}
        async with httpx.AsyncClient(timeout=60) as client:
            async with AsyncConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
                response = await client.get(url, params=params_cleaned, headers=headers)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    @exponential_backoff
    def send_post_request(url: str, payload: dict, auth_token: str = None, **params) -> dict | list:
        """Send a synchronous HTTP POST request with a JSON payload.

        Parameters:
            url: Target endpoint.
            payload: JSON-serializable dictionary to send in the request body.
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        auth_token = auth_token or Authorization().get_access_token()
        headers = Communicator._create_headers(auth_token)
        with ConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            response = requests.post(url, params=params, headers=headers, json=payload)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    @exponential_backoff
    async def send_post_request_async(url: str, payload: dict, auth_token: str = None, **params) -> dict | list:
        """Send an asynchronous HTTP POST request using httpx.AsyncClient.

        Parameters:
            url: Target endpoint.
            payload: JSON-serializable dictionary to send in the request body.
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
            as documented in _check_response_status_code.
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        headers = Communicator._create_headers(auth_token)
        async with httpx.AsyncClient(timeout=60) as client:
            async with AsyncConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
                response = await client.post(url, params=params, headers=headers, json=payload)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    def steaming_get_generator(url: str, auth_token: str = None, **params) -> Generator[dict, dict, None]:
        """Stream a JSON array from a GET endpoint synchronously.

        This yields items one-by-one without loading the whole response into memory.
        Each yielded item is passed through _convert_values.

        Parameters:
            url: Target endpoint returning a JSON array (e.g., NDJSON-like or standard array).
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            **params: Query parameters to include in the request.

        Yields:
            Normalized dict objects representing each item in the streamed array.

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        steaming_header = {'Prefer': 'streaming'}
        auth_token = auth_token or Authorization().get_access_token()
        headers = Communicator._create_headers(auth_token, additional_headers=[steaming_header])
        with ConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
            with requests.get(url, params=params, headers=headers, stream=True, timeout=(5, None)) as stream:
                Communicator._check_response_status_code(stream)
                stream.raw.decode_content = True
                for obj in ijson.items(stream.raw, 'item'):
                    yield Communicator._convert_values(obj)

    @staticmethod
    async def steaming_get_generator_async(
        url: str, auth_token: str = None, **params
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Stream a JSON array from a GET endpoint asynchronously.

        This yields items one-by-one without loading the whole response into memory.
        Each yielded item is passed through _convert_values.

        Parameters:
            url: Target endpoint returning a JSON array.
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            **params: Query parameters to include in the request.

        Yields:
            Normalized dict objects representing each item in the streamed array.

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        # TODO: Fix the issue with receiving stream in the async mode
        # steaming_header = {'Prefer': 'streaming'}
        # headers = Communicator._create_headers(auth_token, additional_headers=[steaming_header])
        headers = Communicator._create_headers(auth_token)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with AsyncConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_ENDPOINT):
                async with session.get(url, params=params, headers=headers) as stream:
                    Communicator._check_response_status_code(stream)
                    async for obj in ijson.items(stream.content, 'item'):
                        yield Communicator._convert_values(obj)

    @staticmethod
    def websocket_generator(
        url: str,
        auth_token: str = None,
        params: Optional[dict] = None,
        ack_message: Optional[dict] = None,
        ack_after: int = 5,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        get_nested_key: str = None,
        **kwargs,
    ) -> Generator[Any, None, None]:
        """Connect to a WebSocket and yield incoming messages synchronously using websocket-client.

        Parameters:
            url: WebSocket endpoint URL (ws:// or wss://).
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            params: Optional query parameters passed to the connection (added to URL using requests' prepared request).
            ack_message: Optional dict to send as JSON after each received message as an ACK.
            ack_after: Send ack message after this number of received messages.
            additional_headers: Optional list of dicts to merge into the request headers.
            get_nested_key: Receive only this key from the message.

        Yields:
            Raw message payloads as provided by the server (str for TEXT, bytes for BINARY).
        """
        auth_token = auth_token or Authorization().get_access_token()
        full_url = Communicator._add_query_params(url, params)
        # Build headers and convert to list of "Key: Value" strings as expected by websocket-client
        header_list = Communicator._create_headers(auth_token, additional_headers, as_list_of_strings=True)
        ws = WebSocket(skip_utf8_validation=True)
        # Use unlimited timeout (blocking). Users can wrap this in their own timeout logic if needed.
        try:
            # We use with_id_in_url = True, so URLs with different pad/site IDs will have independent limits.
            with ConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_WEBSOCKET, with_id_in_url=True):
                ws.connect(full_url, header=header_list)
                i = 0
                while True:
                    data = ws.recv()
                    i += 1
                    if data is None:
                        break
                    if isinstance(data, str):
                        # Sometimes we receive messages that contain an empty string. Let's just skip them.
                        if data == "":
                            continue
                        data = json.loads(data)
                    if data is None:
                        continue
                    if get_nested_key is not None:
                        data = data[get_nested_key]
                    if isinstance(data, list):
                        for item in data:
                            yield item
                    else:
                        yield data
                    if ack_message and (i % ack_after) == 0:
                        ws.send(json.dumps(ack_message))

        except WebSocketConnectionClosedException:
            # Gracefully stop iteration when the server closes the connection
            return

        except WebSocketBadStatusException as e:
            Communicator._check_response_status_code(e)

        except (ValueError, KeyError) as e:
            raise APICompatibilityError(f'Cannot parse WebSocket message: {repr(e)}')

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
        ack_message: Optional[dict] = None,
        ack_after: int = 5,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        get_nested_key: str = None,
        **kwargs,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Connect to a WebSocket and yield incoming messages asynchronously.

        Parameters:
            url: WebSocket endpoint URL (ws:// or wss://).
            auth_token (Optional): Access token for Authorization header. Will be created if None is provided.
            params: Optional query parameters to append to the URL.
            ack_message: Optional dict to send as JSON after each received message as an ACK.
            ack_after: Send ack message after this number of received messages.
            additional_headers: Optional list of dicts to merge into the request headers.
            get_nested_key: Receive only this key from the message.

        Yields:
            Raw message payloads as provided by the server (str for TEXT, bytes for BINARY).
        """
        auth_token = auth_token or await Authorization().aget_access_token()
        headers = Communicator._create_headers(auth_token, additional_headers)

        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            ws = None
            try:
                # We use with_id_in_url = True, so URLs with different pad/site IDs will have independent limits.
                async with AsyncConcurrencyLimiter(url, MAX_CONCURRENT_CALLS_TO_WEBSOCKET, with_id_in_url=True):
                    async with session.ws_connect(url, headers=headers, params=params) as ws:
                        i = 0
                        while True:
                            msg = await ws.receive()
                            i += 1
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = msg.data
                                if isinstance(data, str):
                                    # Sometimes we receive messages that contain an empty string. Let's just skip them.
                                    if data == "":
                                        continue
                                    data = json.loads(data)
                                if data is None:
                                    continue
                                if get_nested_key is not None:
                                    # Only attempt to parse JSON and extract when a nested key is requested
                                    data = data[get_nested_key]
                                if isinstance(data, list):
                                    for item in data:
                                        yield item
                                else:
                                    yield data
                                if ack_message and (i % ack_after) == 0:
                                    try:
                                        await ws.send_json(ack_message)
                                    except Exception:
                                        # If sending ACK fails (e.g., during shutdown), we just stop gracefully
                                        break
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                yield msg.data
                                if ack_message and (i % ack_after) == 0:
                                    try:
                                        await ws.send_json(ack_message)
                                    except Exception:
                                        break
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                break
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise WebsocketError()
                            # Ignore other control frames implicitly

            except asyncio.CancelledError:
                # Propagate cancellation after letting context managers attempt a clean shutdown
                raise
            except WSServerHandshakeError as e:
                Communicator._check_response_status_code(e)

            except (KeyError, ValueError) as e:
                raise APICompatibilityError(f'Cannot parse WebSocket message: {repr(e)}')
            finally:
                # Ensure the websocket is closed if it was created
                try:
                    if ws is not None and not ws.closed:
                        await ws.close(code=1000)
                except Exception:
                    pass


class Authorization(metaclass=Singleton):
    """Handles ReStream OAuth2 client-credentials authentication and token caching.

    Provides synchronous and asynchronous helpers to obtain and reuse
    access tokens for Restream APIs with optional retries and thread/async
    safety.
    """

    _api_url_auth: str = ENDPOINTS.auth_access_token.value

    def __init__(self) -> None:
        """Initializes the token cache and concurrency primitives.
        This class is a singleton — the instance is created only once.
        """
        self._restream_auth_token: Optional[str] = None
        self._expires_in: int = 0
        self._last_update: int = 0
        self._sync_lock = threading.RLock()
        self._async_lock = asyncio.Lock()

    @classmethod
    def _build_auth_url(cls) -> str:
        """Build the absolute auth endpoint URL using RESTREAM_HOST."""
        base_url = os.environ.get('RESTREAM_HOST', RESTREAM_HOST).rstrip('/')
        path = cls._api_url_auth
        return f"{base_url}{path}"

    def _create_payload(self, client_id: str = None, client_secret: str = None) -> dict:
        """Build form payload for the client-credentials token request.

        Parameters:
            client_id (str | None): ReStream OAuth2 client ID (falls back to RESTREAM_CLIENT_ID env var).
            client_secret (str | None): ReStream OAuth2 client secret (falls back to RESTREAM_CLIENT_SECRET env var).

        Returns:
            Dict suitable for x-www-form-urlencoded POST body.

        Raises:
            ValueError: If neither parameters nor environment variables provide credentials.
        """
        client_id = client_id or os.environ.get("RESTREAM_CLIENT_ID")
        client_secret = client_secret or os.environ.get("RESTREAM_CLIENT_SECRET")
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

    def _parse_response(self, response: Any) -> Tuple[str, int]:
        """Extract token and expiration from HTTP response JSON.

        Parameters:
            response: Response-like object with a .json() method (requests/httpx/aiohttp).

        Returns:
            Tuple of (access_token, expires_in_seconds).

        Raises:
            ServerError: If the response body is not valid JSON.
            APICompatibilityError: If required fields are missing.
        """
        try:
            json_response = response.json()
        except JSONDecodeError:
            raise ServerError('Invalid response from server')
        if "access_token" not in json_response or 'expires_in' not in json_response:
            raise APICompatibilityError("Can't get access token from the response")
        return json_response['access_token'], int(json_response['expires_in'])

    def _need_request(self, force: bool) -> bool:
        """Return True if a new token should be requested.

        A new request is needed when force=True, when there is no cached
        token, or when the cached token is expired.
        """
        if force or self._restream_auth_token is None:
            return True
        current_timestamp = datetime.now().timestamp()
        return current_timestamp >= self._expires_in + self._last_update

    @exponential_backoff
    def get_access_token(self, client_id: str = None, client_secret: str = None, force: bool = False) -> str:
        """Synchronously obtain a valid ReStream access token with caching.

        Parameters:
            client_id: Optional override for client ID; falls back to RESTREAM_CLIENT_ID environment variable.
            client_secret: Optional override for client secret; falls back to RESTREAM_CLIENT_SECRET environment variable.
            force: If True, bypass cache and request a new token.

        Returns:
            Bearer token string.
        """
        if not self._need_request(force):
            return self._restream_auth_token

        payload = self._create_payload(client_id, client_secret)
        with self._sync_lock:
            # If a token was acquired during the lock
            if not self._need_request(force):
                return self._restream_auth_token
            url = self._build_auth_url()
            response = requests.post(url, data=payload, timeout=10)
            response.raise_for_status()
            self._restream_auth_token, self._expires_in = self._parse_response(response)
            self._last_update = datetime.now().timestamp()
        return self._restream_auth_token

    @exponential_backoff
    async def aget_access_token(self, client_id: str = None, client_secret: str = None, force: bool = False) -> str:
        """Asynchronously obtain a valid ReStream access token with caching.

        Parameters:
            client_id: Optional override for client ID; falls back to RESTREAM_CLIENT_ID environment variable.
            client_secret: Optional override for client secret; falls back to RESTREAM_CLIENT_SECRET environment variable.
            force: If True, bypass cache and request a new token.

        Returns:
            Bearer token string.
        """
        if not self._need_request(force):
            return self._restream_auth_token

        payload = self._create_payload(client_id, client_secret)
        async with self._async_lock:
            # If a token was acquired during the lock
            if not self._need_request(force):
                return self._restream_auth_token
            url = self._build_auth_url()
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, data=payload)
            response.raise_for_status()
            self._restream_auth_token, self._expires_in = self._parse_response(response)
            self._last_update = datetime.now().timestamp()
        return self._restream_auth_token
