import os
from datetime import datetime
from typing import get_type_hints

from dateutil import parser

from .exceptions import APICompatibilityError
from .constants import RESTREAM_HOST
from .communicator import Communicator


class BaseInterface:
    _auth_token: str = None
    _api_url_single_object: str = None
    _api_url_multiple_objects: str = None
    id: int = None

    def __init__(self, id: int, auth_token: str = None, **kwargs):
        self.id = id
        self._auth_token = auth_token
        self._hints = get_type_hints(self.__class__)
        for key, value in kwargs.items():
            setattr(self, key, self._try_convert_value(key, value))

    @classmethod
    def _format_url(cls, url, **params) -> str:
        base_url = os.environ.get('RESTREAM_HOST', RESTREAM_HOST)
        return f'{base_url}{url}'.format(**params)

    @classmethod
    def _build_single_from_response(cls, json_response, id: int, auth_token: str, as_dict: bool):
        if as_dict:
            return json_response
        if not isinstance(json_response, dict):
            raise APICompatibilityError(f"Expected a JSON object for a single model, but received: {json_response}")
        return cls(auth_token=auth_token, **{'id': id, **json_response})

    @classmethod
    def _select_token(cls, auth_token: str) -> str | None:
        return auth_token or os.environ.get("TALLY_AUTH_TOKEN")

    @classmethod
    def _build_multiple_from_response(cls, json_response, auth_token: str, as_dict: bool):
        if as_dict:
            return json_response
        if not isinstance(json_response, list) or not all(isinstance(o, dict) for o in json_response):
            raise APICompatibilityError(f"Expected a JSON array, but received: {json_response}")
        return [cls(auth_token=auth_token, **json_object) for json_object in json_response]

    def _try_convert_value(self, key, value):
        # TODO: Reimplement using pydantic
        if value is None:
            return None
        if key in self._hints and not isinstance(value, self._hints[key]):
            try:
                if self._hints[key] == datetime:
                    return parser.parse(str(value), ignoretz=False)
                if issubclass(self._hints[key], BaseInterface) and isinstance(value, dict):
                    return self._hints[key](**value)
                return self._hints[key](value)
            except TypeError:
                raise APICompatibilityError(f"Can not convert {key}={value}: to {self._hints[key]}")
        return value

    @classmethod
    def get_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_single_object, id=id)
        json_response = Communicator.send_get_request(url, current_auth_token, **filters)
        return cls._build_single_from_response(json_response, id=id, auth_token=auth_token, as_dict=as_dict)

    @classmethod
    def get_models(cls, auth_token: str = None, as_dict=False, **filters):
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_multiple_objects)
        json_response = Communicator.send_get_request(url, current_auth_token, **filters)
        return cls._build_multiple_from_response(json_response, auth_token=auth_token, as_dict=as_dict)

    @classmethod
    async def aget_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_single_object, id=id)
        json_response = await Communicator.send_get_request_async(url, current_auth_token, **filters)
        return cls._build_single_from_response(json_response, id=id, auth_token=auth_token, as_dict=as_dict)

    @classmethod
    async def aget_models(cls, auth_token: str = None, as_dict=False, **filters):
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_multiple_objects)
        json_response = await Communicator.send_get_request_async(url, current_auth_token, **filters)
        return cls._build_multiple_from_response(json_response, auth_token=auth_token, as_dict=as_dict)

    def update(self):
        updated_dict = self.get_model(id=self.id, as_dict=True)
        for key, value in updated_dict.items():
            setattr(self, key, self._try_convert_value(key, value))

    async def aupdate(self):
        updated_dict = await self.aget_model(id=self.id, as_dict=True)
        for key, value in updated_dict.items():
            setattr(self, key, self._try_convert_value(key, value))

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id})"
