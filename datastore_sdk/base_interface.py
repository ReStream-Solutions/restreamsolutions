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
        """Initialize the model instance.

        Args:
            id: Unique identifier of the model instance.
            auth_token: Optional auth token used for API requests; if not provided, environment variable TALLY_AUTH_TOKEN
            will be used instead.
            **kwargs: Additional fields which will be set as attributes of the instance with possible type conversions
            according to the type hints of the child class.
        """
        self.id = id
        self._auth_token = auth_token
        self._hints = get_type_hints(self.__class__)
        for key, value in kwargs.items():
            setattr(self, key, self._try_convert_value(key, value))

    @classmethod
    def _format_url(cls, url, is_websocket: bool = False, **params) -> str:
        """Compose a full API URL using the base host and path template.

        Args:
            url: API path template (e.g., "/api/v1/resource/{id}").
            is_websocket: If True, convert the resulting URL to a WebSocket scheme (http->ws, https->wss).
            **params: Parameters to format into the URL template.

        Returns:
            The fully formatted absolute URL as a string.
        """
        base_url = os.environ.get('RESTREAM_HOST', RESTREAM_HOST)
        api_url =  f'{base_url}{url}'.format(**params)

        if not is_websocket:
            return api_url

        if api_url.startswith('https://'):
            api_url = api_url.replace('https://', 'wss://')
        elif api_url.startswith('http://'):
            api_url = api_url.replace('http://', 'ws://')
        return api_url

    @classmethod
    def _build_single_from_response(cls, json_response, id: int, auth_token: str, as_dict: bool):
        """Create a single model instance (or dict) from a JSON response.

        Args:
            json_response: Parsed JSON body expected to be a dict for object details.
            id: Identifier of the object being fetched.
            auth_token: Token to be passed to the constructed instance.
            as_dict: If True, return the raw JSON response as python dict instead of a model instance.

        Returns:
            An instance of the current class or the raw JSON dict if as_dict is True.

        Raises:
            APICompatibilityError: If the response is not a dict when a model instance is expected.
        """
        if as_dict:
            return json_response
        if not isinstance(json_response, dict):
            raise APICompatibilityError(f"Expected a JSON object for a single model, but received: {json_response}")
        return cls(auth_token=auth_token, **{'id': id, **json_response})

    @classmethod
    def _select_token(cls, auth_token: str) -> str | None:
        """Choose the authentication token to use for requests.

        Prefers the explicitly provided token; falls back to the TALLY_AUTH_TOKEN
        environment variable if not provided.

        Args:
            auth_token: Optional token passed directly to the call.

        Returns:
            The selected token string or None if nothing is available.
        """
        return auth_token or os.environ.get("TALLY_AUTH_TOKEN")

    @classmethod
    def _build_multiple_from_response(cls, json_response, auth_token: str, as_dict: bool):
        """Create multiple model instances (or list of dicts) from a JSON response.

        Args:
            json_response: Parsed JSON body expected to be a list of dicts.
            auth_token: Token to be passed to each constructed instance.
            as_dict: If True, return the raw JSON response as list instead of model instances.

        Returns:
            A list of class instances or the raw JSON list if as_dict is True.

        Raises:
            APICompatibilityError: If the response is not a list of dicts when instances are expected.
        """
        if as_dict:
            return json_response
        if not isinstance(json_response, list) or not all(isinstance(o, dict) for o in json_response):
            raise APICompatibilityError(f"Expected a JSON array, but received: {json_response}")
        return [cls(auth_token=auth_token, **json_object) for json_object in json_response]

    def _try_convert_value(self, key, value):
        """Attempt to convert a raw value to the annotated type in a child class for the given attribute.

        - Converts ISO-like strings to datetime when the type hint is datetime.
        - Instantiates nested BaseInterface subclasses from dicts when appropriate.
        - Falls back to direct casting using the target type.

        Args:
            key: The attribute name.
            value: The raw value to convert.

        Returns:
            The converted value or the original value if no conversion is needed.

        Raises:
            APICompatibilityError: If the value cannot be converted to the hinted type.
        """
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
    def get_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters) -> "BaseInterface | dict":
        """Fetch a single model from the API.

        Args:
            id: Identifier of the object to fetch.
            auth_token: Optional auth token; falls back to environment variable TALLY_AUTH_TOKEN if not provided.
            as_dict: If True, return raw JSON dict instead of a model instance.
            **filters: Extra query parameters passed to the HTTP request.

        Returns:
            An instance of the current class or a raw dict if as_dict is True.
        """
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_single_object, id=id)
        json_response = Communicator.send_get_request(url, current_auth_token, **filters)
        return cls._build_single_from_response(json_response, id=id, auth_token=auth_token, as_dict=as_dict)

    @classmethod
    def get_models(cls, auth_token: str = None, as_dict=False, **filters) -> "BaseInterface | list[dict]":
        """Fetch a collection of models from the API.

        Args:
            auth_token: Optional auth token; falls back to environment variable TALLY_AUTH_TOKEN if not provided.
            as_dict: If True, return raw JSON list instead of model instances.
            **filters: Extra query parameters passed to the HTTP request.

        Returns:
            A list of class instances or a raw list of dicts if as_dict is True.
        """
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_multiple_objects)
        json_response = Communicator.send_get_request(url, current_auth_token, **filters)
        return cls._build_multiple_from_response(json_response, auth_token=auth_token, as_dict=as_dict)

    @classmethod
    async def aget_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        """Asynchronously fetch a single model from the API.

        Args:
            id: Identifier of the object to fetch.
            auth_token: Optional auth token; falls back to environment variable TALLY_AUTH_TOKEN if not provided.
            as_dict: If True, return raw JSON dict instead of a model instance.
            **filters: Extra query parameters passed to the HTTP request.

        Returns:
            An instance of the current class or a raw dict if as_dict is True.
        """
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_single_object, id=id)
        json_response = await Communicator.send_get_request_async(url, current_auth_token, **filters)
        return cls._build_single_from_response(json_response, id=id, auth_token=auth_token, as_dict=as_dict)

    @classmethod
    async def aget_models(cls, auth_token: str = None, as_dict=False, **filters):
        """Asynchronously fetch a collection of models from the API.

        Args:
            auth_token: Optional auth token; falls back to environment variable TALLY_AUTH_TOKEN if not provided.
            as_dict: If True, return raw JSON list instead of model instances.
            **filters: Extra query parameters passed to the HTTP request.

        Returns:
            A list of class instances or a raw list of dicts if as_dict is True.
        """
        current_auth_token = cls._select_token(auth_token)
        url = cls._format_url(cls._api_url_multiple_objects)
        json_response = await Communicator.send_get_request_async(url, current_auth_token, **filters)
        return cls._build_multiple_from_response(json_response, auth_token=auth_token, as_dict=as_dict)

    def update(self):
        """Refresh this instance with the latest data from the API using its id.

        Returns:
            None. The instance is updated in-place.
        """
        updated_dict = self.get_model(id=self.id, as_dict=True, auth_token=self._auth_token)
        for key, value in updated_dict.items():
            setattr(self, key, self._try_convert_value(key, value))

    async def aupdate(self):
        """Asynchronously refresh this instance with the latest data from the API using its id.

        Returns:
            None. The instance is updated in-place.
        """
        updated_dict = await self.aget_model(id=self.id, as_dict=True, auth_token=self._auth_token)
        for key, value in updated_dict.items():
            setattr(self, key, self._try_convert_value(key, value))

    def __repr__(self):
        """Return a concise string representation of the instance."""
        return f"{self.__class__.__name__}(id={self.id})"
