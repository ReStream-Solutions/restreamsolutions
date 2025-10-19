import asyncio
import time
import functools
from decimal import Decimal
from typing import Generator, Any, AsyncGenerator, Optional, Iterable, Dict, List
import warnings

import aiohttp
import requests
import httpx
import ijson
import json

from aiohttp import WSServerHandshakeError
from websocket import WebSocket, WebSocketConnectionClosedException, WebSocketBadStatusException

from .exceptions import AuthError, APICompatibilityError, APIConcurrencyLimitError, WebsocketError


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
        if attempts < 1:
            raise ValueError("attempts must be >= 1")

        is_coro = asyncio.iscoroutinefunction(func)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except (AuthError, APICompatibilityError, APIConcurrencyLimitError):
                    # Do not retry on these errors
                    raise
                except Exception as e:
                    if i == attempts - 1:
                        raise
                    warnings.warn(
                        f"Unexpected exception raised by {func.__name__}: {e}, retry after {delay} seconds.",
                        RuntimeWarning
                    )
                    await asyncio.sleep(delay)
                    delay *= factor

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(attempts):
                try:
                    return func(*args, **kwargs)
                except (AuthError, APICompatibilityError, APIConcurrencyLimitError):
                    # Do not retry on these errors
                    raise
                except Exception as e:
                    if i == attempts - 1:
                        raise
                    warnings.warn(
                        f"Unexpected exception raised by {func.__name__}: {e}, retry after {delay} seconds.",
                        RuntimeWarning
                    )
                    time.sleep(delay)
                    delay *= factor

        return async_wrapper if is_coro else sync_wrapper

    # If used without parentheses: @exponential_backoff
    if _func is None:
        return decorator
    else:
        return decorator(_func)


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
    def _check_response_status_code(
            response: httpx.Response |
                      requests.Response |
                      aiohttp.ClientResponse |
                      WSServerHandshakeError |
                      WebSocketBadStatusException
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
    def send_get_request(url: str, auth_token: str, **params) -> dict | list:
        """Send a synchronous HTTP GET request.

        Parameters:
            url: Target endpoint.
            auth_token: Access token for Authorization header.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        headers = Communicator._create_headers(auth_token)
        response = requests.get(url, params=params, headers=headers)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    @exponential_backoff
    async def send_get_request_async(url: str, auth_token: str, **params) -> dict | list:
        """Send an asynchronous HTTP GET request.

        Parameters:
            url: Target endpoint.
            auth_token: Access token for Authorization header.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        headers = Communicator._create_headers(auth_token)
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, params=params, headers=headers)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    @exponential_backoff
    def send_post_request(url: str, auth_token: str, payload: dict,  **params) -> dict | list:
        """Send a synchronous HTTP POST request with a JSON payload.

        Parameters:
            url: Target endpoint.
            auth_token: Access token for Authorization header.
            payload: JSON-serializable dictionary to send in the request body.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        headers = Communicator._create_headers(auth_token)
        response = requests.post(url, params=params, headers=headers, json=payload)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    @exponential_backoff
    async def send_post_request_async(url: str, auth_token: str, payload: dict, **params) -> dict | list:
        """Send an asynchronous HTTP POST request using httpx.AsyncClient.

        Parameters:
            url: Target endpoint.
            auth_token: Access token for Authorization header.
            payload: JSON-serializable dictionary to send in the request body.
            **params: Query parameters to include in the request.

        Returns:
            Parsed JSON content (dict or list).

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
            as documented in _check_response_status_code.
        """
        headers = Communicator._create_headers(auth_token)
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(url, params=params, headers=headers, json=payload)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    def steaming_get_generator(url: str, auth_token: str, **params) -> Generator[dict, dict, None]:
        """Stream a JSON array from a GET endpoint synchronously.

        This yields items one-by-one without loading the whole response into memory.
        Each yielded item is passed through _convert_values.

        Parameters:
            url: Target endpoint returning a JSON array (e.g., NDJSON-like or standard array).
            auth_token: Access token for Authorization header.
            **params: Query parameters to include in the request.

        Yields:
            Normalized dict objects representing each item in the streamed array.

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        headers = Communicator._create_headers(auth_token)
        with requests.get(url, params=params, headers=headers, stream=True, timeout=(5, None)) as stream:
            Communicator._check_response_status_code(stream)
            stream.raw.decode_content = True
            for obj in ijson.items(stream.raw, 'item'):
                yield Communicator._convert_values(obj)

    @staticmethod
    async def steaming_get_generator_async(url: str, auth_token: str, **params) -> AsyncGenerator[dict[str, Any], None]:
        """Stream a JSON array from a GET endpoint asynchronously.

        This yields items one-by-one without loading the whole response into memory.
        Each yielded item is passed through _convert_values.

        Parameters:
            url: Target endpoint returning a JSON array.
            auth_token: Access token for Authorization header.
            **params: Query parameters to include in the request.

        Yields:
            Normalized dict objects representing each item in the streamed array.

        Raises:
            AuthError, APICompatibilityError, APIConcurrencyLimitError, HTTPError
        """
        headers = Communicator._create_headers(auth_token)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params, headers=headers) as stream:
                Communicator._check_response_status_code(stream)
                async for obj in ijson.items(stream.content, 'item'):
                    yield Communicator._convert_values(obj)

    @staticmethod
    def websocket_generator(
        url: str,
        auth_token: str,
        params: Optional[dict] = None,
        ack_message: Optional[dict] = None,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        get_nested_key: str = None,
        **kwargs,
    ) -> Generator[Any, None, None]:
        """Connect to a WebSocket and yield incoming messages synchronously using websocket-client.

        Parameters:
            url: WebSocket endpoint URL (ws:// or wss://).
            auth_token: Access token for Authorization header.
            params: Optional query parameters passed to the connection (added to URL using requests' prepared request).
            ack_message: Optional dict to send as JSON after each received message as an ACK.
            additional_headers: Optional list of dicts to merge into the request headers.

        Yields:
            Raw message payloads as provided by the server (str for TEXT, bytes for BINARY).
        """

        # Build URL with params using requests' PreparedRequest (no custom URL builder)
        if params:
            req = requests.Request("GET", url, params=params).prepare()
            full_url = req.url
        else:
            full_url = url

        # Build headers and convert to list of "Key: Value" strings as expected by websocket-client
        header_list = Communicator._create_headers(auth_token, additional_headers, as_list_of_strings=True)
        ws = WebSocket(skip_utf8_validation=True)
        # Use unlimited timeout (blocking). Users can wrap this in their own timeout logic if needed.
        try:
            ws.connect(full_url, header=header_list)
            while True:
                data = ws.recv()
                if data is None:
                    break
                if isinstance(data, str):
                    data = json.loads(data)
                if get_nested_key is not None:
                    data = data[get_nested_key]
                yield data
                if ack_message:
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
        auth_token: str,
        params: Optional[dict] = None,
        ack_message: Optional[dict] = None,
        additional_headers: Optional[Iterable[Dict[str, str]]] = None,
        get_nested_key: str = None,
        **kwargs,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Connect to a WebSocket and yield incoming messages asynchronously.

        Parameters:
            url: WebSocket endpoint URL (ws:// or wss://).
            auth_token: Access token for Authorization header.
            params: Optional query parameters to append to the URL.
            ack_message: Optional dict to send as JSON after each received message as an ACK.
            additional_headers: Optional list of dicts to merge into the request headers.

        Yields:
            Raw message payloads as provided by the server (str for TEXT, bytes for BINARY).
        """
        headers = Communicator._create_headers(auth_token, additional_headers)

        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.ws_connect(url, headers=headers, params=params) as ws:
                    while True:
                        msg = await ws.receive()
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.data
                            if isinstance(data, str):
                                data = json.loads(data)
                            if get_nested_key is not None:
                                # Only attempt to parse JSON and extract when a nested key is requested
                                data = data[get_nested_key]
                            yield data
                            if ack_message:
                                await ws.send_json(ack_message)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            yield msg.data
                            if ack_message:
                                await ws.send_json(ack_message)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise WebsocketError()
                        # Ignore other control frames implicitly

            except WSServerHandshakeError as e:
                Communicator._check_response_status_code(e)

            except (KeyError, ValueError) as e:
                raise APICompatibilityError(f'Cannot parse WebSocket message: {repr(e)}')
