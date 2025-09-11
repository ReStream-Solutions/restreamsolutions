from decimal import Decimal
from math import inf
from typing import Generator, Any, AsyncGenerator

import aiohttp
import requests
import httpx
import ijson

from .exceptions import AuthError, APICompatibilityError


class Communicator:

    @staticmethod
    def _create_headers(auth_token: str) -> str | None:
        bearer_token = f'Bearer {auth_token}'
        return {"Authorization": bearer_token} if auth_token else None

    @staticmethod
    def _check_response_status_code(response: httpx.Response | requests.Response | aiohttp.ClientResponse):
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
        response.raise_for_status()

    @staticmethod
    def _convert_values(obj: dict[str, Any]) -> dict[str, Any]:

        def convert_value(value: Any) -> Any:
            if isinstance(value, Decimal):
                return float(value)
            elif value == 'Infinity':
                return inf
            else:
                return value

        return {k: convert_value(v) for k, v in obj.items()}


    @staticmethod
    def send_get_request(url: str, auth_token: str, **params) -> dict | list:
        headers = Communicator._create_headers(auth_token)
        response = requests.get(url, params=params, headers=headers)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    async def send_get_request_async(url: str, auth_token: str, **params) -> dict | list:
        headers = Communicator._create_headers(auth_token)
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, params=params, headers=headers)
        Communicator._check_response_status_code(response)
        return response.json()

    @staticmethod
    def steaming_get_generator(url: str, auth_token: str, **params) -> Generator[dict, dict, None]:
        headers = Communicator._create_headers(auth_token)
        with requests.get(url, params=params, headers=headers, stream=True, timeout=(5, None)) as stream:
            print(stream.url)
            Communicator._check_response_status_code(stream)
            stream.raw.decode_content = True
            for obj in ijson.items(stream.raw, 'item'):
                yield Communicator._convert_values(obj)

    @staticmethod
    async def steaming_get_generator_async(url: str, auth_token: str, **params) -> AsyncGenerator[dict[str, Any], None]:
        headers = Communicator._create_headers(auth_token)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params, headers=headers) as stream:
                Communicator._check_response_status_code(stream)
                async for obj in ijson.items(stream.content, 'item'):
                    yield Communicator._convert_values(obj)
