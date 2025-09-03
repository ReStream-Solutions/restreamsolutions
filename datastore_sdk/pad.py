from datetime import datetime
from typing import Any

from datastore_sdk import StageNameFilter
from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.communicator import Communicator
from datastore_sdk.constants import ENDPOINTS


class Pad(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.pads_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.pads_get_many.value

    id: str
    name: str
    lease_name: str
    customer_name: str
    simops_config: dict
    completion_date: datetime
    wireline_enabled: bool

    def get_sites(self, as_dict=False, **filters) -> list['Site']:
        from .site import Site
        auth_token = self._auth_token if self._auth_token else None
        final_filters = {**filters, 'pad__id': self.id}
        return Site.get_models(auth_token=auth_token, as_dict=as_dict, **final_filters)

    async def aget_sites(self, as_dict=False, **filters) -> list['Site']:
        from .site import Site
        auth_token = self._auth_token if self._auth_token else None
        final_filters = {**filters, 'pad__id': self.id}
        return await Site.aget_models(auth_token=auth_token, as_dict=as_dict, **final_filters)

    def get_states(self, stage_name_filter: StageNameFilter = None, as_dict=False, **filters) -> list['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        final_filters = {**filters, 'pad__id': self.id}
        return State.get_models(stage_name_filter=stage_name_filter, auth_token=auth_token, as_dict=as_dict, **final_filters)

    async def aget_states(self, stage_name_filter: StageNameFilter = None, as_dict=False, **filters) -> list['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        final_filters = {**filters, 'pad__id': self.id}
        return await State.aget_models(stage_name_filter=stage_name_filter, auth_token=auth_token, as_dict=as_dict, **final_filters)

    def get_stages_metadata(
            self,
            start: datetime = None,
            end: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilter = None,
            **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.stages_for_pad.value, id=self.id)
        filters = self._mix_stage_metadata_filters(start, end, stage_number, stage_name_filter, **filters)
        return Communicator.send_get_request(url, auth_token, **filters)

    async def aget_stages_metadata(
            self,
            start: datetime = None,
            end: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilter = None,
            **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.stages_for_pad.value, id=self.id)
        filters = self._mix_stage_metadata_filters(start, end, stage_number, stage_name_filter, **filters)
        return await Communicator.send_get_request_async(url, auth_token, **filters)

    def get_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.fields_for_pad.value, id=self.id)
        return Communicator.send_get_request(url, auth_token, **filters)

    async def aget_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.fields_for_pad.value, id=self.id)
        return await Communicator.send_get_request_async(url, auth_token, **filters)