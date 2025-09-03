from datetime import datetime
from typing import Optional, Any

from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.communicator import Communicator
from datastore_sdk.constants import ENDPOINTS, StageNameFilter
from datastore_sdk.exceptions import APICompatibilityError


class Site(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.sites_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.sites_get_many.value

    name: str
    date_created: datetime
    latitude: int
    longitude: int
    lease_name: str
    operator_name: str
    crew: str
    division: str
    metadata: dict
    well_api: str
    pad_id: int
    is_demo_site: bool
    stage_total: int
    timezone: str

    def get_state(self, as_dict: bool = False) -> Optional['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        states = State.get_models(auth_token=auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    async def aget_state(self, as_dict: bool = False) -> Optional['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        states = await State.aget_models(auth_token=auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    def get_stages_metadata(
            self,
            start: datetime = None,
            end: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilter = None,
            **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.stages_for_site.value, id=self.id)
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
        url = self._format_url(ENDPOINTS.stages_for_site.value, id=self.id)
        filters = self._mix_stage_metadata_filters(start, end, stage_number, stage_name_filter, **filters)
        return await Communicator.send_get_request_async(url, auth_token, **filters)

    def get_pad(self, as_dict: bool = False) -> Optional['Pad']:
        from .pad import Pad
        if not hasattr(self, 'pad_id'):
            self.update()
        if getattr(self, 'pad_id') is None:
            return None
        auth_token = self._auth_token if self._auth_token else None
        return Pad.get_model(id=self.pad_id, auth_token=auth_token, as_dict=as_dict)

    async def aget_pad(self, as_dict: bool = False) -> Optional['Pad']:
        from .pad import Pad
        if not hasattr(self, 'pad_id'):
            await self.aupdate()
        if getattr(self, 'pad_id') is None:
            return None
        auth_token = self._auth_token if self._auth_token else None
        return await Pad.aget_model(id=self.pad_id, auth_token=auth_token, as_dict=as_dict)

    def get_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.fields_for_site.value, id=self.id)
        return Communicator.send_get_request(url, auth_token, **filters)

    async def aget_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(ENDPOINTS.fields_for_site.value, id=self.id)
        return await Communicator.send_get_request_async(url, auth_token, **filters)

    def _extract_site_measurement_sources(self, pad_measurement_sources: dict) -> dict:
        if not pad_measurement_sources:
            return {}
        site_measurement_sources = {}
        try:
            for name, sources_list in pad_measurement_sources.items():
                site_measurement_sources[name] = []
                for source in sources_list:
                    if self.id in source['attached_sites']:
                        source.pop('attached_sites')
                        site_measurement_sources[name].append(source)
            return site_measurement_sources
        except KeyError as e:
            raise APICompatibilityError(f"API compatibility error: {e}")

    def get_measurement_sources_metadata(self) -> dict:
        pad = self.get_pad()
        pad_measurement_sources = pad.get_measurement_sources_metadata()
        return self._extract_site_measurement_sources(pad_measurement_sources)

    async def aget_measurement_sources_metadata(self) -> dict:
        pad = await self.aget_pad()
        pad_measurement_sources = await pad.aget_measurement_sources_metadata()
        return self._extract_site_measurement_sources(pad_measurement_sources)
