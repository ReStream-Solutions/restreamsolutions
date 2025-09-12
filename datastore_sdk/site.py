from datetime import datetime
from typing import Optional

from datastore_sdk.base_pad_site import BasePadSite
from datastore_sdk.constants import ENDPOINTS, StageNameFilters, DataResolutions, DataAggregations
from datastore_sdk.data_object import Data, DataAsync
from datastore_sdk.exceptions import APICompatibilityError


class Site(BasePadSite):
    _api_url_single_object: str = ENDPOINTS.sites_one.value
    _api_url_multiple_objects: str = ENDPOINTS.sites_many.value
    _api_url_fields_metadata: str = ENDPOINTS.fields_site.value
    _api_url_stages_metadata: str = ENDPOINTS.stages_site.value
    _api_url_aggregations_metadata: str = ENDPOINTS.aggregations_site.value
    _api_url_data: str = ENDPOINTS.data_site.value
    _api_url_data_changes_single: str = ENDPOINTS.data_changes_site_one.value
    _api_url_data_changes_multiple: str = ENDPOINTS.data_changes_site_many.value

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

    def get_data(
            self,
            start_datetime: datetime = None,
            end_datetime: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilters = None,
            resolution: DataResolutions = DataResolutions.SECOND,
            aggregation: DataAggregations = None,
            si_units: bool = False,
            measurement_sources_names: str | list[str] = None,
    ) -> Data:
        return super().get_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
        )

    async def aget_data(
            self,
            start_datetime: datetime = None,
            end_datetime: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilters = None,
            resolution: DataResolutions = DataResolutions.SECOND,
            aggregation: DataAggregations = None,
            si_units: bool = False,
            measurement_sources_names: str | list[str] = None,
    ) -> DataAsync:
        return await super().aget_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
        )
