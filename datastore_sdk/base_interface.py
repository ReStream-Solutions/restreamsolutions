import os

import requests


class Communicator:

    @staticmethod
    def send_get_request(url: str, auth_token: str, **params) -> dict | list:
        bearer_token = f'Bearer {auth_token}'
        response = requests.get(url, params=params, auth=bearer_token)
        response.raise_for_status()
        return response.json()

class BaseInterface:
    _auth_token: str = None
    _api_url_single_object: str = None
    _api_url_multiple_objects: str = None
    id: int = None

    def __init__(self, id: int, auth_token: str = None, **kwargs):
        self.id = id
        self._auth_token = auth_token
        for key, value in kwargs.items():
            setattr(self, key, value)


    @classmethod
    def get_model(cls, id: int = None, auth_token: str = None, **kwargs):
        current_auth_token = auth_token or os.environ.get("TALLY_AUTH_TOKEN")
        url = cls._api_url_single_object.format(id=id)
        json_response = Communicator.send_get_request(url, current_auth_token, **kwargs)
        if not json_response is dict:
            raise TypeError
        return cls(id=id, auth_token=auth_token, **json_response)

    @classmethod
    def get_models(cls, auth_token: str = None, **kwargs):
        current_auth_token = auth_token or os.environ.get("TALLY_AUTH_TOKEN")
        json_response = Communicator.send_get_request(cls._api_url_multiple_objects, current_auth_token, **kwargs)
        if not json_response is list or not all([o is dict for o in json_response]):
            raise TypeError
        return [cls(auth_token=auth_token, **json_object) for json_object in json_response]