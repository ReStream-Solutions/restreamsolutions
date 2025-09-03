import requests
import httpx

from .exceptions import AuthError, APICompatibilityError


class Communicator:

    @staticmethod
    def send_get_request(url: str, auth_token: str, **params) -> dict | list:
        bearer_token = f'Bearer {auth_token}'
        headers = {"Authorization": bearer_token} if auth_token else None
        response = requests.get(url, params=params, headers=headers)
        if response.status_code in [401, 403]:
            raise AuthError()
        if response.status_code == 404:
            raise APICompatibilityError("The endpoint does not exist")
        response.raise_for_status()
        return response.json()

    @staticmethod
    async def send_get_request_async(url: str, auth_token: str, **params) -> dict | list:
        bearer_token = f'Bearer {auth_token}'
        headers = {"Authorization": bearer_token} if auth_token else None
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, params=params, headers=headers)
        if response.status_code == 401:
            raise AuthError()
        response.raise_for_status()
        return response.json()
