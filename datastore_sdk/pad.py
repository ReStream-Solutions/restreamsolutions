from datetime import datetime
from typing import Any

from datastore_sdk import StageNameFilter
from datastore_sdk.base_pad_site import BasePadSite
from datastore_sdk.communicator import Communicator
from datastore_sdk.constants import ENDPOINTS
from datastore_sdk.exceptions import APICompatibilityError


class Pad(BasePadSite):
    _api_url_single_object: str = ENDPOINTS.pads_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.pads_get_many.value
    _api_url_fields_metadata: str = ENDPOINTS.fields_for_pad.value
    _api_url_stages_metadata: str = ENDPOINTS.stages_for_pad.value
    _api_url_aggregations_metadata: str = ENDPOINTS.aggregations_for_pad.value

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

    def _get_measurement_sources(self) -> dict:
        if getattr(self, 'simops_config') is None:
            return {}
        try:
            return self.simops_config['measurement_sources']
        except KeyError:
            raise APICompatibilityError("measurement_sources section not found.")

    def get_measurement_sources_metadata(self) -> dict:
        if not hasattr(self, 'simops_config'):
            self.update()
        return self._get_measurement_sources()

    async def aget_measurement_sources_metadata(self) -> dict:
        if not hasattr(self, 'simops_config'):
            await self.aupdate()
        return self._get_measurement_sources()
