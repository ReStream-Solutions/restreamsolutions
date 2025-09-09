import warnings
from datetime import datetime, timezone
from typing import Any

from datastore_sdk import StageNameFilter
from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.communicator import Communicator


class BasePadSite(BaseInterface):
    _api_url_fields_metadata: str = None
    _api_url_stages_metadata: str = None
    _api_url_aggregations_metadata: str = None

    def _mix_stage_metadata_filters(self,
            start: datetime = None,
            end: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilter = None,
            **filters) -> dict[str, int | str]:

        filters = filters.copy()
        if start:
            start_utc = start.astimezone(timezone.utc)
            filters['start'] = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        if end:
            end_utc = end.astimezone(timezone.utc)
            filters['end'] = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        if stage_number:
            filters['stage_number'] = stage_number
        if stage_name_filter:
            filters['state'] = stage_name_filter.value
        return filters

    def get_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_fields_metadata, id=self.id)
        return Communicator.send_get_request(url, auth_token, **filters)

    async def aget_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_fields_metadata, id=self.id)
        return await Communicator.send_get_request_async(url, auth_token, **filters)

    def get_stages_metadata(
            self,
            start: datetime = None,
            end: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilter = None,
            add_aggregations: bool = False,
            **filters) -> list[dict[str, Any]]:
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_stages_metadata, id=self.id)
        filters = self._mix_stage_metadata_filters(start, end, stage_number, stage_name_filter, **filters)
        stages_metadata = Communicator.send_get_request(url, auth_token, **filters)

        if add_aggregations:
            stages_metadata = self._add_aggregations(stages_metadata, auth_token)

        return stages_metadata

    async def aget_stages_metadata(
            self,
            start: datetime = None,
            end: datetime = None,
            stage_number: int = None,
            stage_name_filter: StageNameFilter = None,
            add_aggregations: bool = False,
            **filters) -> list[dict[str, Any]]:

        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_stages_metadata, id=self.id)
        filters = self._mix_stage_metadata_filters(start, end, stage_number, stage_name_filter, **filters)
        stages_metadata = await Communicator.send_get_request_async(url, auth_token, **filters)

        if add_aggregations:
            stages_metadata = await self._add_aggregations_async(stages_metadata, auth_token)

        return stages_metadata

    @staticmethod
    def _merge_aggregations_with_stages(
            stages_metadata: list[dict[str, Any]],
            aggregations_metadata: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:

        aggregations_by_stage_id = {}
        for site_id, aggregations in aggregations_metadata.items():
            if isinstance(aggregations, str):
                # We got error from the endpoint
                warnings.warn(f'{site_id}: {aggregations}. Skipping this site.')
                continue
            for stage_aggregation in aggregations:
                if 'id' in stage_aggregation:
                    history_id = stage_aggregation.pop('id')
                    aggregations_by_stage_id[history_id] = stage_aggregation
        for stage_metadata in stages_metadata:
            if stage_metadata.get('id') in aggregations_by_stage_id:
                stage_metadata['aggregations'] = aggregations_by_stage_id[stage_metadata['id']]
            else:
                stage_metadata['aggregations'] = None
        return stages_metadata

    def _add_aggregations(self, stages_metadata: list[dict[str, Any]], auth_token: str) -> list[dict[str, Any]]:
        if not stages_metadata:
            return stages_metadata
        stages_ids = [stage.get('id') for stage in stages_metadata if stage.get('id') is not None]
        url = self._format_url(self._api_url_aggregations_metadata, id=self.id)
        aggregations = Communicator.send_get_request(url, auth_token, histories=stages_ids)
        stages_metadata = self._merge_aggregations_with_stages(stages_metadata, aggregations)
        return stages_metadata

    async def _add_aggregations_async(self, stages_metadata: list[dict[str, Any]], auth_token: str) -> list[dict[str, Any]]:
        if not stages_metadata:
            return stages_metadata
        stages_ids = [stage.get('id') for stage in stages_metadata if stage.get('id') is not None]
        url = self._format_url(self._api_url_aggregations_metadata, id=self.id)
        aggregations = await Communicator.send_get_request_async(url, auth_token, histories=stages_ids)
        stages_metadata = self._merge_aggregations_with_stages(stages_metadata, aggregations)
        return stages_metadata


    def get_measurement_sources_metadata(self) -> dict:
        raise NotImplementedError()

    async def aget_measurement_sources_metadata(self) -> dict:
        raise NotImplementedError()