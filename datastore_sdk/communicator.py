from decimal import Decimal
from typing import Generator, Any, AsyncGenerator

import aiohttp
import requests
import httpx
import ijson

from .exceptions import AuthError, APICompatibilityError, APIConcurrencyLimitError


class Communicator:
    """Utility class that encapsulates HTTP communication (sync and async) with a REST API.

    It provides helpers to:
    - Build authorization headers.
    - Validate HTTP response status codes and map common errors to SDK exceptions.
    - Send GET/POST requests (sync and async).
    - Stream large JSON arrays incrementally (sync and async) with on-the-fly value normalization.
    """

    @staticmethod
    def _create_headers(auth_token: str) -> str | None:
        """Create Authorization headers for Bearer-token based APIs.

        Parameters:
            auth_token: The raw access token string. If empty or falsy, no header is produced.

        Returns:
            A headers dictionary with the Authorization field set, or None if no token provided.
        """
        bearer_token = f'Bearer {auth_token}'
        return {"Authorization": bearer_token} if auth_token else None

    @staticmethod
    def _check_response_status_code(response: httpx.Response | requests.Response | aiohttp.ClientResponse):
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
        if isinstance(response, (httpx.Response, requests.Response)):
            status_code = response.status_code
        elif isinstance(response, aiohttp.ClientResponse):
            status_code = response.status
        else:
            raise RuntimeError('Unknown response type')
        if status_code in [401, 403]:
            raise AuthError()
        if status_code == 404:
            raise APICompatibilityError("The endpoint does not exist")
        if status_code == 429:
            raise APIConcurrencyLimitError()
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
