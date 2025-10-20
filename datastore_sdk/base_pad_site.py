import secrets
import warnings
from datetime import datetime, timezone
from typing import Any, Tuple

from datastore_sdk import StageNameFilters
from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.communicator import Communicator
from datastore_sdk.constants import DataResolutions, DataAggregations, DataFillMethods
from datastore_sdk.data_changes import DataChanges
from datastore_sdk.data_object import Data, DataAsync


class BasePadSite(BaseInterface):
    """Base class for Pad and Site model objects that exposes metadata and data access methods.

    This class provides common functionality for interacting with Restream customer endpoints related to
    fields metadata, stages (histories) metadata (with optional aggregations), measurement sources, raw/streaming
    data retrieval, data changes, etc. Both synchronous and asynchronous methods are provided
    where applicable. Concrete subclasses are expected to define API URL templates via the
    class attributes below and may override abstract/NotImplemented methods.

    Expected class attributes (URL templates with placeholders):
    - _api_url_fields_metadata
    - _api_url_stages_metadata
    - _api_url_aggregations_metadata
    - _api_url_data
    - _api_url_data_changes_single
    - _api_url_data_changes_multiple
    """

    _api_url_fields_metadata: str = None
    _api_url_stages_metadata: str = None
    _api_url_aggregations_metadata: str = None
    _api_url_data: str = None
    _api_url_data_changes_single: str = None
    _api_url_data_changes_multiple: str = None
    _api_url_data_websocket: str = None
    _api_url_instance_updates_websocket: str = None
    _api_url_changelog_updates_websocket: str = None

    def _mix_stage_metadata_filters(
        self,
        start: datetime = None,
        end: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        **filters,
    ) -> dict[str, int | str]:
        """Compose query params for stages metadata endpoints.

        Parameters:
            start: Optional start datetime (tz aware); will be converted to UTC and formatted as ISO string.
            end: Optional end datetime (tz aware); will be converted to UTC and formatted as ISO string.
            stage_number: Optional stage number to filter by.
            stage_name_filter: Optional StageNameFilters enum to filter by stage name (frac, wireline, etc.).
            **filters: Additional filters to be merged.

        Returns:
            A new dict containing the merged and normalized filters.
        """
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
        """Fetch all available data fields and their metadata related to the current entity (pad/site).

        Parameters:
            **filters: Optional query parameters supported by the endpoint.

        Returns:
            A list of metadata dictionaries for available fields.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_fields_metadata, id=self.id)
        return Communicator.send_get_request(url, auth_token, **filters)

    async def aget_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        """Asynchronously fetch all available data fields names and their metadata related
        to the current entity (Pad or Site).

        Parameters:
            **filters: Optional query parameters supported by the endpoint.

        Returns:
            A list of metadata dictionaries for available fields.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_fields_metadata, id=self.id)
        return await Communicator.send_get_request_async(url, auth_token, **filters)

    def get_stages_metadata(
        self,
        start: datetime = None,
        end: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        add_aggregations: bool = False,
        **filters,
    ) -> list[dict[str, Any]]:
        """Fetch historic stages metadata for this entity (pad/site). Optionally adds aggregated metrics to the metadata.

        Parameters:
            start: Optional start datetime (timezone-aware) to filter stages.
            end: Optional end datetime (timezone-aware) to filter stages.
            stage_number: Optional stage number to filter by also requires stage_name_filter, if provided.
            stage_name_filter: Optional StageNameFilters enum value to filter stage name.
            add_aggregations: If True, enrich each stage with its data aggregations.
            **filters: Additional endpoint filters, supported by the endpoint.

        Returns:
            A list of dictionaries describing stages. If add_aggregations=True, each item may
            include an "aggregations" key with aggregation details (or None if unavailable).
        """
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
        stage_name_filter: StageNameFilters = None,
        add_aggregations: bool = False,
        **filters,
    ) -> list[dict[str, Any]]:
        """Asynchronously fetch historic stages metadata for this entity (pad/site).
        Optionally adds aggregated metrics to the metadata.

        Parameters:
            start: Optional start datetime (timezone-aware) to filter stages.
            end: Optional end datetime (timezone-aware) to filter stages.
            stage_number: Optional stage number to filter by also requires stage_name_filter, if provided.
            stage_name_filter: Optional StageNameFilters enum value to filter stage name.
            add_aggregations: If True, enrich each stage with its data aggregations.
            **filters: Additional endpoint filters, supported by the endpoint.

        Returns:
            A list of dictionaries describing stages. If add_aggregations=True, each item may
            include an "aggregations" key with aggregation details (or None if unavailable).
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_stages_metadata, id=self.id)
        filters = self._mix_stage_metadata_filters(start, end, stage_number, stage_name_filter, **filters)
        stages_metadata = await Communicator.send_get_request_async(url, auth_token, **filters)

        if add_aggregations:
            stages_metadata = await self._add_aggregations_async(stages_metadata, auth_token)

        return stages_metadata

    @staticmethod
    def _merge_aggregations_with_stages(
        stages_metadata: list[dict[str, Any]], aggregations_metadata: dict[str, list[dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """Attach aggregation metadata to corresponding stage items by its id.

        Parameters:
            stages_metadata: List of stage metadata dicts, each should contain an 'id'.
            aggregations_metadata: Mapping from site id to a list of aggregation dicts; if a value
                is a string, it's treated as an error message for that site and is skipped.

        Returns:
            The input stages_metadata list with an added 'aggregations' key for each item
            (either a dict with aggregation fields or None if not available).
        """
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
        """Fetch and attach aggregations to the provided historic stages list.

        Parameters:
            stages_metadata: List of historic stage dicts obtained from get_stages_metadata.
            auth_token: Authorization token.

        Returns:
            The same list enriched with an 'aggregations' key for each stage.
        """
        if not stages_metadata:
            return stages_metadata
        stages_ids = [stage.get('id') for stage in stages_metadata if stage.get('id') is not None]
        url = self._format_url(self._api_url_aggregations_metadata, id=self.id)
        aggregations = Communicator.send_get_request(url, auth_token, histories=stages_ids)
        stages_metadata = self._merge_aggregations_with_stages(stages_metadata, aggregations)
        return stages_metadata

    async def _add_aggregations_async(
        self, stages_metadata: list[dict[str, Any]], auth_token: str
    ) -> list[dict[str, Any]]:
        """Asynchronously fetch and attach aggregations to the provided historic stages list.

        Parameters:
            stages_metadata: List of historic stage dicts obtained from get_stages_metadata.
            auth_token: Authorization token.

        Returns:
            The same list enriched with an 'aggregations' key for each stage.
        """
        if not stages_metadata:
            return stages_metadata
        stages_ids = [stage.get('id') for stage in stages_metadata if stage.get('id') is not None]
        url = self._format_url(self._api_url_aggregations_metadata, id=self.id)
        aggregations = await Communicator.send_get_request_async(url, auth_token, histories=stages_ids)
        stages_metadata = self._merge_aggregations_with_stages(stages_metadata, aggregations)
        return stages_metadata

    def get_measurement_sources_metadata(self) -> dict:
        """Fetch metadata about measurement sources for this entity.

        Note:
            This is expected to be implemented by subclasses with the proper endpoint.
        """
        raise NotImplementedError()

    async def aget_measurement_sources_metadata(self) -> dict:
        """Asynchronously fetch metadata about measurement sources for this entity.

        Note:
            This is expected to be implemented by subclasses with the proper endpoint.
        """
        raise NotImplementedError()

    def _build_get_data_params(self, **filters: dict) -> dict:
        """Normalize and prepare query parameters for data retrieval endpoints.

        Supported filter keys in **filters:
            - start_datetime (datetime | None): timezone-aware; converted to UTC with '%Y-%m-%d %H:%M:%S'.
            - end_datetime (datetime | None): timezone-aware; converted to UTC with '%Y-%m-%d %H:%M:%S'.
            - fields (str | list[str]): which fields to return; defaults to 'exposed_to_customer'.
            - si_units (bool): return values in SI units if True; default False.
            - resolution (DataResolutions): time resolution; defaults to SECOND.
            - stage_number (int | None): stage number; requires stage_name_filter alongside.
            - stage_name_filter (StageNameFilters | None): stage state name filter.
            - aggregation (DataAggregations | None): optional aggregation to apply.
            - measurement_sources_names (str | list[str] | None): names to filter by measurement source.
            - is_routed (bool | None): For pads entities only. If true - returns the data separately for each site,
            otherwise each item will contain data for the entire pad for a given timestamp.
            - fill_data_method (DataFillMethods | None): Optional gap-filling method. Use DataFillMethods.FORWARD_FILL
                to fill missing values forward after the last known value, or DataFillMethods.BACKWARD_FILL
                to fill missing values backward before the next known value. If provided, resolution must be
                DataResolutions.SECOND and aggregation must be None.
            - fill_data_limit (int | None): Required when fill_data_method is provided. Maximum number of seconds to fill
                per gap: for FORWARD_FILL this applies after a known value; for BACKWARD_FILL this applies before a
                known value. Has no effect if fill_data_method is not provided. Must be a positive integer.
            - inside_area_only (bool): Optional. Default True. Controls whether filling is restricted to internal gaps
                between original values (True) or may extend across edges (False). Has effect only when
                fill_data_method is provided.

        Returns:
            A dict of parameters ready to be passed into the Communicator requests.

        Raises:
            ValueError: if provided datetimes are not timezone-aware, or if stage_number is
            provided without stage_name_filter.
        """
        start_datetime: datetime | None = filters.get('start_datetime')
        end_datetime: datetime | None = filters.get('end_datetime')
        fields: str | list[str] = filters.get('fields', 'exposed_to_customer')
        si_units: bool = filters.get('si_units', False)
        resolution: DataResolutions = filters.get('resolution', DataResolutions.SECOND)
        stage_number: int | None = filters.get('stage_number')
        stage_name_filter: StageNameFilters | None = filters.get('stage_name_filter')
        aggregation: DataAggregations | None = filters.get('aggregation')
        measurement_sources_names: str | list[str] | None = filters.get('measurement_sources_names')
        is_routed: bool | None = filters.get('is_routed')
        fill_data_method: DataFillMethods | None = filters.get('fill_data_method')
        fill_data_limit: int | None = filters.get('fill_data_limit')
        inside_area_only: bool = filters.get('inside_area_only', True)

        if start_datetime is not None and start_datetime.tzinfo is None:
            raise ValueError("start_datetime must have a timezone")

        if end_datetime is not None and end_datetime.tzinfo is None:
            raise ValueError("end_datetime must have a timezone")

        if stage_number is not None and stage_name_filter is None:
            raise ValueError("Please provide stage_name_filter together with the stage_number.")

        # Validate fill_data_method and fill_data_limit
        if fill_data_method is not None:
            if not isinstance(fill_data_method, DataFillMethods):
                raise ValueError('fill_data_method must be of type DataFillMethods')
            if fill_data_limit is None:
                raise ValueError('Please provide "fill_data_limit" together with "fill_data_method".')
            if resolution != DataResolutions.SECOND:
                raise ValueError('fill_data_method option can only be used with DataResolutions.SECOND resolution.')
            if aggregation is not None:
                raise ValueError('fill_data_method option cannot be used together with aggregation.')
        # Validate limit if provided (even without method, it just has no effect)
        if fill_data_limit is not None:
            if not isinstance(fill_data_limit, int) or fill_data_limit <= 0:
                raise ValueError('"fill_data_limit" must be a positive integer.')

        dt_format = '%Y-%m-%d %H:%M:%S'

        params = {
            'si_units': str(si_units).lower(),
            'resolution': resolution.value,
        }

        if start_datetime is not None:
            params['start_datetime'] = start_datetime.astimezone(timezone.utc).strftime(dt_format)

        if end_datetime is not None:
            params['end_datetime'] = end_datetime.astimezone(timezone.utc).strftime(dt_format)

        if fields is not None:
            params['fields'] = ','.join(fields)

        if stage_number is not None:
            params['stage_number'] = stage_number

        if stage_name_filter is not None:
            params['state_imatch'] = stage_name_filter.value

        if aggregation is not None:
            params['agg'] = aggregation.value

        if measurement_sources_names is not None:
            if isinstance(measurement_sources_names, str):
                params['measurement_source'] = measurement_sources_names
            elif isinstance(measurement_sources_names, list):
                params['measurement_source'] = ','.join(measurement_sources_names)
            else:
                raise ValueError('measurement_sources_names must be a string or list of strings')

        if is_routed is not None:
            params['routed'] = str(is_routed).lower()

        if fill_data_method is not None and fill_data_limit is not None:
            params['fill_data_method'] = fill_data_method.value
            params['fill_data_limit'] = str(fill_data_limit)
            params['inside_area_only'] = str(inside_area_only).lower()

        return params

    def get_data(self, **filters: dict) -> Data:
        """Return a Data object that streams data records for this entity.

        Parameters:
            **filters: See _build_get_data_params for the complete list of supported filters.

        Returns:
            A Data object that lazily iterates over streamed records from the API.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_data, id=self.id)
        params = self._build_get_data_params(**filters)
        data_generator_factory = lambda: Communicator.steaming_get_generator(url, auth_token, **params)
        return Data(data_generator_factory)

    async def aget_data(self, **filters: dict) -> DataAsync:
        """Return a DataAsync object that streams data records asynchronously.

        Parameters:
            **filters: See _build_get_data_params for the complete list of supported filters.

        Returns:
            A DataAsync object that lazily iterates over streamed records from the API.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_data, id=self.id)
        params = self._build_get_data_params(**filters)
        data_generator_factory = lambda: Communicator.steaming_get_generator_async(url, auth_token, **params)
        return DataAsync(data_generator_factory)

    def get_data_changes(self, as_dict: bool = False, **filters: dict) -> tuple[list[dict | DataChanges], Data]:
        """Fetch all data change events for the current object (Site or Pad) and a Data object
        that allows to stream the affected data or save it to a file.

        Parameters:
            as_dict: If True, each change is returned as a dict; otherwise as DataChanges objects.
            **filters: Optional filters accepted by the change log endpoint.

        Returns:
            A tuple of (changes_list, combined_data) where:
              - changes_list is a list of dicts or DataChanges instances depending on as_dict.
              - combined_data is a Data object representing a concatenation of change intervals.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_data_changes_multiple, parent_id=self.id)
        response = Communicator.send_get_request(url, auth_token, **filters)
        raw_changes: list[dict[str, Any]] = response.get('change_log', [])

        changes_list = DataChanges._build_multiple_from_response(
            json_response=raw_changes,
            auth_token=auth_token,
            as_dict=as_dict,
        )

        combined_data = DataChanges._build_combined_data_object(raw_changes, auth_token)
        return changes_list, combined_data

    async def aget_data_changes(
        self, as_dict: bool = False, **filters: dict
    ) -> tuple[list[dict | DataChanges], DataAsync]:
        """Asynchronously fetch all data change events for the current object (Site or Pad), along with
        a DataAsync object that allows you to stream the affected data or save it to a file.

        Parameters:
            as_dict: If True, each change is returned as a dict; otherwise as DataChanges objects.
            **filters: Optional filters accepted by the change log endpoint.

        Returns:
            A tuple of (changes_list, combined_data) where:
              - changes_list is a list of dicts or DataChanges instances depending on as_dict.
              - combined_data is a DataAsync object representing a concatenation of change intervals.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_data_changes_multiple, parent_id=self.id)
        response = await Communicator.send_get_request_async(url, auth_token, **filters)
        raw_changes: list[dict[str, Any]] = response.get('change_log', [])

        changes_list = DataChanges._build_multiple_from_response(
            json_response=raw_changes,
            auth_token=auth_token,
            as_dict=as_dict,
        )

        combined_data_async = DataChanges._build_combined_data_async_object(raw_changes, auth_token)
        return changes_list, combined_data_async

    def _prepare_measurements_websocket_params(self, endpoint_url: str, session_key: str) -> dict[str, Any]:
        """Prepare shared parameters for measurement WebSocket connections.

        Parameters:
            endpoint_url: URL template for the WebSocket endpoint.
            session_key: Optional client-provided session key; a random one is generated if not provided.

        Returns:
            A dict with auth token, formatted URL, headers, ack message config, and the resolved session key.
        """
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(endpoint_url, is_websocket=True, id=self.id)

        if not session_key:
            session_key = secrets.token_hex(16)
        params = {'session_key': session_key}

        # All measurement WebSockets expect to receive an acknowledgement message after each successful delivery.
        ack_message = {"ack": "true"}
        # Each message from the WebSocket follows this schema:
        # {"session_key": "key", "message": {"k1": "v1", "k2": "v2"...}}
        get_nested_key = 'message'

        return {
            'auth_token': auth_token,
            'url': url,
            'params': params,
            'ack_message': ack_message,
            'get_nested_key': get_nested_key,
            'session_key': session_key,
        }

    def get_realtime_measurements_data(
        self,
        session_key: str = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> Tuple[Data, str]:
        """Open a WebSocket stream of real-time measurements (sync).

        Parameters:
            session_key: Optional session identifier to resume/associate a stream.
            restart_on_error: If True, the returned Data instance will automatically attempt to reconnect
            to the server if an error occurs.
            restart_on_close: If True, the Data instance will also recreate the underlying generator when
            the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        Returns:
            A tuple of (Data, session_key) where Data lazily yields messages and session_key is the key in use.
        """
        params = self._prepare_measurements_websocket_params(self._api_url_data_websocket, session_key)
        data_generator_factory = lambda: Communicator.websocket_generator(**params)
        return (
            Data(
                data_generator_factory,
                restart_on_error=restart_on_error,
                restart_on_close=restart_on_close,
            ),
            params['session_key'],
        )

    async def aget_realtime_measurements_data(
        self,
        session_key: str = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> Tuple[DataAsync, str]:
        """Open a WebSocket stream of real-time measurements (async).

        Parameters:
            session_key: Optional session identifier to resume/associate a stream.
            restart_on_error: If True, the returned DataAsync instance will automatically attempt to reconnect
            to the server if an error occurs.
            restart_on_close: If True, the DataAsync instance will also recreate the underlying async generator when
            the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        Returns:
            A tuple of (DataAsync, session_key) where DataAsync lazily yields messages on async iteration.
        """
        params = self._prepare_measurements_websocket_params(self._api_url_data_websocket, session_key)
        data_generator_factory = lambda: Communicator.websocket_generator_async(**params)
        return (
            DataAsync(
                data_generator_factory,
                restart_on_error=restart_on_error,
                restart_on_close=restart_on_close,
            ),
            params['session_key'],
        )

    def _get_real_time_updates_object(
        self,
        endpoint_url: str,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> Data:
        """Helper to build a Data object for a generic real-time WebSocket endpoint (sync)."""
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(endpoint_url, is_websocket=True, id=self.id)
        data_generator_factory = lambda: Communicator.websocket_generator(url, auth_token=auth_token)
        return Data(data_generator_factory, restart_on_error=restart_on_error, restart_on_close=restart_on_close)

    async def _aget_real_time_updates_object(
        self,
        endpoint_url: str,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> DataAsync:
        """Helper to build a DataAsync for a generic real-time WebSocket endpoint (async)."""
        auth_token = self._select_token(self._auth_token)
        url = self._format_url(endpoint_url, is_websocket=True, id=self.id)
        data_generator_factory = lambda: Communicator.websocket_generator_async(url, auth_token=auth_token)
        return DataAsync(data_generator_factory, restart_on_error=restart_on_error, restart_on_close=restart_on_close)

    def get_realtime_instance_updates(self, restart_on_error: bool = True, restart_on_close: bool = True) -> Data:
        """Get a Data stream of real-time instance (Pad/Site) updates over WebSocket (sync).
        See the documentation in the overridden methods for more information."""
        return self._get_real_time_updates_object(
            self._api_url_instance_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
        )

    async def aget_realtime_instance_updates(
        self, restart_on_error: bool = True, restart_on_close: bool = True
    ) -> DataAsync:
        """Get a DataAsync stream of real-time instance (Pad/Site) updates over WebSocket (async).
        See the documentation in the overridden methods for more information."""
        return await self._aget_real_time_updates_object(
            self._api_url_instance_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
        )

    def get_realtime_data_changes_updates(self, restart_on_error: bool = True, restart_on_close: bool = True) -> Data:
        """Create a Data object that provides a lazy WebSocket stream of real-time data-change
        events for this Pad/Site.

        Parameters:
            restart_on_error: If True, the returned Data instance will automatically attempt to reconnect
            to the server if an error occurs.
            restart_on_close: If True, the Data instance will also recreate the underlying generator when
            the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        data.data_fetcher is a lazy synchronous generator. Each iteration blocks until the next
        update message is received. You can also persist the stream to a file via
        data.save(path: str, overwrite: bool = False).

        Each message is a JSON object describing a single change event in the data for a specific site.
        A typical payload contains the following fields (example shown below):
          - id (int): Unique identifier of the change event.
          - created_at (str, ISO 8601): Timestamp when the change was recorded on the server.
          - modification_type (str): One of "translation_layer" or "transaction".
          - modification_subtype (str): Specific change action, e.g. "move", "move_delete", "move_copy",
            "delete", "augment", "augment_update", "augment_insert", "annotate", "override", "add",
            "change", "reverse".
          - start_date (str, ISO 8601): Start of the affected time interval.
          - end_date (str, ISO 8601): End of the affected time interval.
          - site (int): Identifier of the parent site.

        Example message:
            {
                "id": 61285,
                "created_at": "2025-10-17T11:39:43.688700Z",
                "modification_type": "translation_layer",
                "modification_subtype": "add",
                "start_date": "2022-06-29T19:25:52Z",
                "end_date": "2025-10-17T11:39:44.687991Z",
                "site": 1173
            }

        Returns:
            Data: A Data object whose data_fetcher yields update messages one by one.
        """
        return self._get_real_time_updates_object(
            self._api_url_changelog_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
        )

    async def aget_realtime_data_changes_updates(
        self,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> DataAsync:
        """Create a DataAsync object that provides a lazy WebSocket stream of real-time
        data-change events for this Pad/Site.

        Parameters:
            restart_on_error: If True, the returned DataAsync instance will automatically attempt to reconnect
            to the server if an error occurs.
            restart_on_close: If True, the DataAsync wrapper will also recreate the underlying async generator when
            the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        data.data_fetcher is a lazy asynchronous generator. Each async iteration awaits the next
        update message. You can also persist the stream to a file via
        data.asave(path: str, overwrite: bool = False).

        Each message is a JSON object describing a single change event in the data for a specific site.
        A typical payload contains the following fields (example shown below):
          - id (int): Unique identifier of the change event.
          - created_at (str, ISO 8601): Timestamp when the change was recorded on the server.
          - modification_type (str): One of "translation_layer" or "transaction".
          - modification_subtype (str): Specific change action, e.g. "move", "move_delete", "move_copy",
            "delete", "augment", "augment_update", "augment_insert", "annotate", "override", "add",
            "change", "reverse".
          - start_date (str, ISO 8601): Start of the affected time interval.
          - end_date (str, ISO 8601): End of the affected time interval.
          - site (int): Identifier of the parent site.

        Example message:
            {
                "id": 61285,
                "created_at": "2025-10-17T11:39:43.688700Z",
                "modification_type": "translation_layer",
                "modification_subtype": "add",
                "start_date": "2022-06-29T19:25:52Z",
                "end_date": "2025-10-17T11:39:44.687991Z",
                "site": 1173
            }

        Returns:
            DataAsync: A DataAsync object whose data_fetcher yields update messages one by one when
            asynchronously iterated over.
        """
        return await self._aget_real_time_updates_object(
            self._api_url_changelog_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
        )
