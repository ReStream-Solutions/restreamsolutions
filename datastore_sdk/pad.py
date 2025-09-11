from datetime import datetime

from datastore_sdk import StageNameFilters
from datastore_sdk.base_pad_site import BasePadSite
from datastore_sdk.constants import ENDPOINTS, DataResolutions, DataAggregations
from datastore_sdk.data_object import Data, DataAsync
from datastore_sdk.exceptions import APICompatibilityError


class Pad(BasePadSite):
    _api_url_single_object: str = ENDPOINTS.pads_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.pads_get_many.value
    _api_url_fields_metadata: str = ENDPOINTS.fields_for_pad.value
    _api_url_stages_metadata: str = ENDPOINTS.stages_for_pad.value
    _api_url_aggregations_metadata: str = ENDPOINTS.aggregations_for_pad.value
    _api_url_data: str = ENDPOINTS.data_for_pad.value

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

    def get_states(self, stage_name_filter: StageNameFilters = None, as_dict=False, **filters) -> list['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        final_filters = {**filters, 'pad__id': self.id}
        return State.get_models(stage_name_filter=stage_name_filter, auth_token=auth_token, as_dict=as_dict, **final_filters)

    async def aget_states(self, stage_name_filter: StageNameFilters = None, as_dict=False, **filters) -> list['State']:
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
            is_routed: bool = False,
    ) ->Data:

        if not is_routed and stage_number is not None:
            raise ValueError('Please select is_routed = True to query data by stage_number.')

        return super().get_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
            is_routed=is_routed,
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
            is_routed: bool = False,
    ) -> DataAsync:

        if not is_routed and stage_number is not None:
            raise ValueError('Please select is_routed = True to query data by stage_number.')

        return await super().aget_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
            is_routed=is_routed,
        )