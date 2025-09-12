import warnings
from datetime import datetime
from typing import Any

from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.communicator import Communicator
from datastore_sdk.constants import ENDPOINTS
from datastore_sdk.exceptions import APICompatibilityError


class DataChanges(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.data_changes_site_one.value
    _api_url_multiple_objects: str = ENDPOINTS.data_changes_site_many.value

    created_at: datetime
    modification_type: str
    modification_subtype: str
    start_date: datetime
    end_date: datetime
    site: int
    update_received: bool

    def __init__(self, *args, **kwargs) -> None:
        self.update_received = False
        super().__init__(*args, **kwargs)

    @classmethod
    def get_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    @classmethod
    def get_models(cls, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    @classmethod
    async def aget_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    @classmethod
    async def aget_models(cls, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    def _select_token_and_url(self) -> tuple[str, str]:
        current_auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_multiple_objects, parent_id=self.site)
        return current_auth_token, url

    def _update_self_state(self, response_data: dict[str, Any]) -> None:
        if (not isinstance(response_data, dict) or 'change_log' not in response_data
            or not isinstance(response_data['change_log'], list)
        ):
            raise APICompatibilityError(
                f'change_log must be a list of dicts, but received {response_data["change_log"]}'
            )

        new_instance_dict = {}
        for change_log in response_data['change_log']:
            if change_log['id'] == self.id:
                new_instance_dict = change_log
        if not new_instance_dict:
            self.update_received = True
            return

        for key, value in new_instance_dict.items():
            setattr(self, key, self._try_convert_value(key, value))
        self.update_received = False

    def update(self):
        current_auth_token, url = self._select_token_and_url()
        response_data = Communicator.send_get_request(url, current_auth_token)
        self._update_self_state(response_data)

    async def aupdate(self):
        current_auth_token, url = self._select_token_and_url()
        response_data = await Communicator.send_get_request_async(url, current_auth_token)
        self._update_self_state(response_data)

    def get_data(self) -> 'Data':
        from .site import Site
        site = Site(self.site)
        return site.get_data(start_datetime=self.start_date, end_datetime=self.end_date)

    async def aget_data(self) -> 'DataAsync':
        from .site import Site
        site = Site(self.site)
        return await site.aget_data(start_datetime=self.start_date, end_datetime=self.end_date)

    def _create_confirm_data_received_payload(self):
        return {
            "change_log": [
                {
                    "id": self.id,
                    "update_received": True
                }
            ]
        }

    def confirm_data_received(self) -> bool:
        if self.update_received:
            warnings.warn(f"{str(self)} - confirmation has already been received.")
            return True
        payload = self._create_confirm_data_received_payload()
        current_auth_token, url = self._select_token_and_url()
        response_data = Communicator.send_post_request(url, current_auth_token, payload)
        self._update_self_state(response_data)
        return self.update_received

    async def aconfirm_data_received(self) -> bool:
        if self.update_received:
            warnings.warn(f"{str(self)} - confirmation has already been received.")
            return True
        payload = self._create_confirm_data_received_payload()
        current_auth_token, url = self._select_token_and_url()
        response_data = await Communicator.send_post_request_async(url, current_auth_token, payload)
        self._update_self_state(response_data)
        return self.update_received