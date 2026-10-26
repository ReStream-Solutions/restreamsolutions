import logging
import secrets
from datetime import datetime, timezone
from typing import Any, Tuple

from restreamsolutions.base_interface import BaseInterface
from restreamsolutions.communicator import Communicator
from restreamsolutions.constants import DataAggregations, DataFillMethods, DataResolutions, StageNameFilters
from restreamsolutions.data_changes import DataChanges
from restreamsolutions.data_object import Data, DataAsync
from restreamsolutions.exceptions import APICompatibilityError

logger = logging.getLogger(__name__)


class BasePadSite(BaseInterface):
    """Base class for Pad and Site model objects that exposes metadata and data access methods.

    This class provides common functionality for interacting with Restream customer endpoints related to
    fields metadata, stages (histories) metadata (with optional aggregations), measurement sources, raw/streaming
    data retrieval, data changes, stage aggregations, etc. Both synchronous and asynchronous methods are provided
    where applicable. Concrete subclasses are expected to define API URL templates via the
    class attributes below and may override abstract/NotImplemented methods.

    Expected class attributes (URL templates with placeholders):
    - _api_url_fields_metadata
    - _api_url_stages_metadata
    - _api_url_aggregations_metadata
    - _api_url_data
    - _api_url_data_changes_single
    - _api_url_data_changes_multiple
    - _api_url_data_websocket
    - _api_url_instance_updates_websocket
    - _api_url_changelog_updates_websocket
    - _api_url_stages_aggregations
    - _api_url_stages_aggregations_one
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
    _api_url_stages_aggregations: str = None
    _api_url_stages_aggregations_one: str = None

    def _mix_stage_metadata_filters(
        self,
        start: datetime = None,
        end: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        stage_numbers: list[int] | None = None,
        **filters,
    ) -> dict[str, int | str]:
        """Compose query params for stages metadata endpoints.

        Args:
            start: Optional start datetime (tz aware); will be converted to UTC and formatted as ISO string.
            end: Optional end datetime (tz aware); will be converted to UTC and formatted as ISO string.
            stage_number: Optional stage number to filter by.
            stage_name_filter: Optional StageNameFilters enum to filter by stage name (frac, wireline, etc.).
            stage_numbers: Optional list of stage numbers; stages matching any of them are returned.
            **filters: Additional filters to be merged.

        Returns:
            A new dict containing the merged and normalized filters.

        Raises:
            ValueError: If stage_numbers is not a list of integers.
        """
        if stage_numbers is not None and (
            not isinstance(stage_numbers, (list, tuple))
            or not all(isinstance(number, int) and not isinstance(number, bool) for number in stage_numbers)
        ):
            raise ValueError('stage_numbers must be a list of integers')
        filters = filters.copy()
        if start:
            start_utc = start.astimezone(timezone.utc)
            filters['start'] = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        if end:
            end_utc = end.astimezone(timezone.utc)
            filters['end'] = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        if stage_number is not None:
            filters['stage_number'] = stage_number
        if stage_numbers:
            filters['stage_numbers'] = ','.join(str(number) for number in stage_numbers)
        if stage_name_filter:
            filters['state'] = stage_name_filter.value
        return filters

    def get_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        """Fetch all available data fields and their metadata related to the current entity (pad/site).

        Args:
            **filters: Optional query parameters supported by the endpoint.

        Returns:
            A list of metadata dictionaries for available fields.
        """
        url = self._format_url(self._api_url_fields_metadata, id=self.id)
        return Communicator.send_get_request(url, auth_token=self._auth_token, **filters)

    async def aget_fields_metadata(self, **filters) -> list[dict[str, Any]]:
        """Asynchronously fetch all available data fields names and their metadata related
        to the current entity (Pad or Site).

        Args:
            **filters: Optional query parameters supported by the endpoint.

        Returns:
            A list of metadata dictionaries for available fields.
        """
        url = self._format_url(self._api_url_fields_metadata, id=self.id)
        return await Communicator.send_get_request_async(url, auth_token=self._auth_token, **filters)

    def get_stages_metadata(
        self,
        start: datetime = None,
        end: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        add_aggregations: bool = False,
        stage_numbers: list[int] | None = None,
        **filters,
    ) -> list[dict[str, Any]]:
        """Fetch historic stages metadata for this entity (pad/site). Optionally adds aggregated metrics to the metadata.

        Args:
            start: Optional start datetime (timezone-aware) to filter stages.
            end: Optional end datetime (timezone-aware) to filter stages.
            stage_number: Optional stage number to filter by; usually used together with stage_name_filter.
            stage_name_filter: Optional StageNameFilters enum value to filter stage name.
            add_aggregations: If True, enrich each stage with its data aggregations.
            stage_numbers: Optional list of stage numbers; stages matching any of them are returned.
            **filters: Additional endpoint filters supported by the endpoint, e.g. aggregations_filter
                (comma-separated list of aggregation names) used together with add_aggregations.

        Returns:
            A list of dictionaries describing stages. If add_aggregations=True, each item may
            include an "aggregations" key with aggregation details (or None if unavailable).
        """
        url = self._format_url(self._api_url_stages_metadata, id=self.id)
        filters = self._mix_stage_metadata_filters(
            start, end, stage_number, stage_name_filter, stage_numbers=stage_numbers, **filters
        )
        stages_metadata = Communicator.send_get_request(url, auth_token=self._auth_token, **filters)

        if add_aggregations:
            stages_metadata = self._add_aggregations(stages_metadata, self._auth_token, **filters)

        return stages_metadata

    async def aget_stages_metadata(
        self,
        start: datetime = None,
        end: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        add_aggregations: bool = False,
        stage_numbers: list[int] | None = None,
        **filters,
    ) -> list[dict[str, Any]]:
        """Asynchronously fetch historic stages metadata for this entity (pad/site).
        Optionally adds aggregated metrics to the metadata.

        Args:
            start: Optional start datetime (timezone-aware) to filter stages.
            end: Optional end datetime (timezone-aware) to filter stages.
            stage_number: Optional stage number to filter by; usually used together with stage_name_filter.
            stage_name_filter: Optional StageNameFilters enum value to filter stage name.
            add_aggregations: If True, enrich each stage with its data aggregations.
            stage_numbers: Optional list of stage numbers; stages matching any of them are returned.
            **filters: Additional endpoint filters supported by the endpoint, e.g. aggregations_filter
                (comma-separated list of aggregation names) used together with add_aggregations.

        Returns:
            A list of dictionaries describing stages. If add_aggregations=True, each item may
            include an "aggregations" key with aggregation details (or None if unavailable).
        """
        url = self._format_url(self._api_url_stages_metadata, id=self.id)
        filters = self._mix_stage_metadata_filters(
            start, end, stage_number, stage_name_filter, stage_numbers=stage_numbers, **filters
        )
        stages_metadata = await Communicator.send_get_request_async(url, auth_token=self._auth_token, **filters)

        if add_aggregations:
            stages_metadata = await self._add_aggregations_async(stages_metadata, self._auth_token, **filters)

        return stages_metadata

    @staticmethod
    def _merge_aggregations_with_stages(
        stages_metadata: list[dict[str, Any]], aggregations_metadata: dict[str, list[dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """Attach aggregation metadata to corresponding stage items by its id.

        Args:
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
                # The endpoint reports an error for this site instead of a list of aggregations
                logger.warning('%s: %s. Skipping this site.', site_id, aggregations)
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

    def _add_aggregations(
        self, stages_metadata: list[dict[str, Any]], auth_token: str = None, **filters
    ) -> list[dict[str, Any]]:
        """Fetch and attach aggregations to the provided historic stages list.

        Args:
            stages_metadata: List of historic stage dicts obtained from get_stages_metadata.
            auth_token: Authorization token (Optional).
            **filters: Additional endpoint filters, supported by the endpoint.

        Returns:
            The same list enriched with an 'aggregations' key for each stage.
        """
        stages_ids = [stage.get('id') for stage in stages_metadata or [] if stage.get('id') is not None]
        if not stages_ids:
            return stages_metadata
        url = self._format_url(self._api_url_aggregations_metadata, id=self.id)
        aggregations = Communicator.send_get_request(url, auth_token=auth_token, histories=stages_ids, **filters)
        stages_metadata = self._merge_aggregations_with_stages(stages_metadata, aggregations)
        return stages_metadata

    async def _add_aggregations_async(
        self, stages_metadata: list[dict[str, Any]], auth_token: str = None, **filters
    ) -> list[dict[str, Any]]:
        """Asynchronously fetch and attach aggregations to the provided historic stages list.

        Args:
            stages_metadata: List of historic stage dicts obtained from get_stages_metadata.
            auth_token: Authorization token (Optional).
            **filters: Additional endpoint filters, supported by the endpoint.

        Returns:
            The same list enriched with an 'aggregations' key for each stage.
        """
        stages_ids = [stage.get('id') for stage in stages_metadata or [] if stage.get('id') is not None]
        if not stages_ids:
            return stages_metadata
        url = self._format_url(self._api_url_aggregations_metadata, id=self.id)
        aggregations = await Communicator.send_get_request_async(
            url, auth_token=auth_token, histories=stages_ids, **filters
        )
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
            - fields (str | list[str] | None): which fields to return; None omits the parameter and the server
                falls back to exposed_to_customer.
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
            A dict of parameters, ready to be passed into the Communicator requests.

        Raises:
            ValueError: if provided datetime objects are not timezone-aware, or if stage_number is
            provided without stage_name_filter.
        """
        start_datetime: datetime | None = filters.get('start_datetime')
        end_datetime: datetime | None = filters.get('end_datetime')
        fields: str | list[str] | None = filters.get('fields')
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

        if fill_data_method is not None:
            if not isinstance(fill_data_method, DataFillMethods):
                raise ValueError('fill_data_method must be of type DataFillMethods')
            if fill_data_limit is None:
                raise ValueError('Please provide "fill_data_limit" together with "fill_data_method".')
            if resolution != DataResolutions.SECOND:
                raise ValueError('fill_data_method option can only be used with DataResolutions.SECOND resolution.')
            if aggregation is not None:
                raise ValueError('fill_data_method option cannot be used together with aggregation.')
        # The limit is validated even without a method, in which case it just has no effect
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
            params['fields'] = fields if isinstance(fields, str) else ','.join(fields)

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

        Args:
            **filters: See _build_get_data_params for the complete list of supported filters.

        Returns:
            A Data object that lazily iterates over streamed records from the API.
        """
        url = self._format_url(self._api_url_data, id=self.id)
        params = self._build_get_data_params(**filters)
        data_generator_factory = lambda: Communicator.streaming_get_generator(url, self._auth_token, **params)
        policy = Communicator.create_retry_policy()
        # A finite stream must not be restarted after it started delivering items: the restart would
        # re-send records that were already consumed
        return Data(
            data_generator_factory,
            auth_token=self._auth_token,
            restart_on_error=True,
            attempts=policy.attempts,
            retry_policy=policy,
            restart_after_partial=False,
        )

    async def aget_data(self, **filters: dict) -> DataAsync:
        """Return a DataAsync object that streams data records asynchronously.

        Args:
            **filters: See _build_get_data_params for the complete list of supported filters.

        Returns:
            A DataAsync object that lazily iterates over streamed records from the API.
        """
        url = self._format_url(self._api_url_data, id=self.id)
        params = self._build_get_data_params(**filters)
        data_generator_factory = lambda: Communicator.streaming_get_generator_async(url, self._auth_token, **params)
        policy = Communicator.create_retry_policy()
        # A finite stream must not be restarted after it started delivering items: the restart would
        # re-send records that were already consumed
        return DataAsync(
            data_generator_factory,
            auth_token=self._auth_token,
            restart_on_error=True,
            attempts=policy.attempts,
            retry_policy=policy,
            restart_after_partial=False,
        )

    @staticmethod
    def _extract_change_log(response: dict | list | None, url: str) -> list[dict[str, Any]]:
        """Extract the change log entries from a change log endpoint response.

        Args:
            response: Parsed JSON body returned by Communicator, or None for an empty body.
            url: Requested URL, included in the error message.

        Returns:
            The list of raw change log entries.

        Raises:
            APICompatibilityError: If the response is not a JSON object.
        """
        if not isinstance(response, dict):
            raise APICompatibilityError(f'Unexpected change log response from {url}: {response!r}')
        return response.get('change_log', [])

    def get_data_changes(
        self, as_dict: bool = False, get_acknowledged: bool | None = None, **filters: dict
    ) -> tuple[list[dict | DataChanges], Data]:
        """Fetch all data change events for the current object (Site or Pad) and a Data object
        that allows to stream the affected data or save it to a file.

        Args:
            as_dict: If True, each change is returned as a dict; otherwise as DataChanges objects.
            get_acknowledged: If True, already acknowledged change events are included as well (for auditing).
            **filters: Optional filters accepted by the change log endpoint.

        Returns:
            A tuple of (changes_list, combined_data) where:
              - changes_list is a list of dicts or DataChanges instances depending on as_dict.
              - combined_data is a Data object representing a concatenation of change intervals.
        """
        url = self._format_url(self._api_url_data_changes_multiple, parent_id=self.id)
        response = Communicator.send_get_request(
            url, auth_token=self._auth_token, get_acknowledged=get_acknowledged, **filters
        )
        raw_changes = self._extract_change_log(response, url)

        changes_list = DataChanges._build_multiple_from_response(
            json_response=raw_changes,
            auth_token=self._auth_token,
            as_dict=as_dict,
        )

        combined_data = DataChanges._build_combined_data_object(raw_changes, self._auth_token)
        return changes_list, combined_data

    async def aget_data_changes(
        self, as_dict: bool = False, get_acknowledged: bool | None = None, **filters: dict
    ) -> tuple[list[dict | DataChanges], DataAsync]:
        """Asynchronously fetch all data change events for the current object (Site or Pad), along with
        a DataAsync object that allows you to stream the affected data or save it to a file.

        Args:
            as_dict: If True, each change is returned as a dict; otherwise as DataChanges objects.
            get_acknowledged: If True, already acknowledged change events are included as well (for auditing).
            **filters: Optional filters accepted by the change log endpoint.

        Returns:
            A tuple of (changes_list, combined_data) where:
              - changes_list is a list of dicts or DataChanges instances depending on as_dict.
              - combined_data is a DataAsync object representing a concatenation of change intervals.
        """
        url = self._format_url(self._api_url_data_changes_multiple, parent_id=self.id)
        response = await Communicator.send_get_request_async(
            url, auth_token=self._auth_token, get_acknowledged=get_acknowledged, **filters
        )
        raw_changes = self._extract_change_log(response, url)

        changes_list = DataChanges._build_multiple_from_response(
            json_response=raw_changes,
            auth_token=self._auth_token,
            as_dict=as_dict,
        )

        combined_data_async = DataChanges._build_combined_data_async_object(raw_changes, self._auth_token)
        return changes_list, combined_data_async

    def _prepare_measurements_websocket_params(
        self, endpoint_url: str, session_key: str | None, query_params: dict[str, Any]
    ) -> Tuple[dict[str, Any], str]:
        """Prepare the keyword arguments for a measurements WebSocket connection.

        Args:
            endpoint_url: URL template for the WebSocket endpoint.
            session_key: Optional client-provided session key; a random one is generated if not provided.
            query_params: Data filters already normalized by _build_get_data_params.

        Returns:
            A tuple (websocket_kwargs, session_key) where websocket_kwargs are the arguments for the
            Communicator websocket generators and session_key is the key in use.
        """
        url = self._format_url(endpoint_url, is_websocket=True, id=self.id)
        if not session_key:
            session_key = secrets.token_hex(16)
        websocket_kwargs = {
            'url': url,
            'auth_token': self._auth_token,
            'params': {'session_key': session_key, **query_params},
            # Each message from the measurement WebSockets follows this schema:
            # {"session_key": "key", "message": {"k1": "v1", "k2": "v2"...}}
            'get_nested_key': 'message',
        }
        return websocket_kwargs, session_key

    def get_realtime_measurements_data(
        self,
        session_key: str = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
        **filters,
    ) -> Tuple[Data, str]:
        """Open a WebSocket stream of real-time measurements (sync).

        You can use the same named parameters for filters as those used in the get_data method of the current instance
        (Site or Pad). In this case, the WebSocket will first return historical data according to the specified
        parameters and then start sending new real-time data.

        Args:
            session_key: Optional session identifier to resume/associate a stream.
            restart_on_error: If True, the returned Data instance will automatically attempt to reconnect
                to the server if an error occurs.
            restart_on_close: If True, the Data instance will also recreate the underlying generator when
                the stream completes normally (e.g., clean WebSocket close) and continue streaming.
            **filters: Named parameters available for the get_data method.

        Returns:
            A tuple of (Data, session_key) where Data lazily yields messages and session_key is the key in use.
        """
        query_params = self._build_get_data_params(**filters) if filters else {}
        websocket_kwargs, session_key = self._prepare_measurements_websocket_params(
            self._api_url_data_websocket, session_key, query_params
        )
        data_generator_factory = lambda: Communicator.websocket_generator(**websocket_kwargs)
        return (
            Data(
                data_generator_factory,
                auth_token=self._auth_token,
                restart_on_error=restart_on_error,
                restart_on_close=restart_on_close,
                retry_policy=Communicator.create_retry_policy(),
                attempts=None,
            ),
            session_key,
        )

    async def aget_realtime_measurements_data(
        self,
        session_key: str = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
        **filters,
    ) -> Tuple[DataAsync, str]:
        """Open a WebSocket stream of real-time measurements (async).

        You can use the same named parameters for filters as those used in the aget_data method of the current instance
        (Site or Pad). In this case, the WebSocket will first return historical data according to the specified
        parameters and then start sending new real-time data.

        Args:
            session_key: Optional session identifier to resume/associate a stream.
            restart_on_error: If True, the returned DataAsync instance will automatically attempt to reconnect
                to the server if an error occurs.
            restart_on_close: If True, the DataAsync instance will also recreate the underlying async generator when
                the stream completes normally (e.g., clean WebSocket close) and continue streaming.
            **filters: Named parameters available for the aget_data method.

        Returns:
            A tuple of (DataAsync, session_key) where DataAsync lazily yields messages on async iteration.
        """
        query_params = self._build_get_data_params(**filters) if filters else {}
        websocket_kwargs, session_key = self._prepare_measurements_websocket_params(
            self._api_url_data_websocket, session_key, query_params
        )
        data_generator_factory = lambda: Communicator.websocket_generator_async(**websocket_kwargs)
        return (
            DataAsync(
                data_generator_factory,
                auth_token=self._auth_token,
                restart_on_error=restart_on_error,
                restart_on_close=restart_on_close,
                retry_policy=Communicator.create_retry_policy(),
                attempts=None,
            ),
            session_key,
        )

    def _get_real_time_updates_object(
        self,
        endpoint_url: str,
        convert_to: object = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> Data:
        """Build a Data object for a generic real-time WebSocket endpoint (sync).

        Args:
            endpoint_url: URL template for the WebSocket endpoint.
            convert_to: Optional class used to convert each message into a model instance.
            restart_on_error: If True, reconnect automatically on errors.
            restart_on_close: If True, reconnect automatically when the server closes the connection.

        Returns:
            A Data object that lazily yields messages.
        """
        url = self._format_url(endpoint_url, is_websocket=True, id=self.id)
        data_generator_factory = lambda: Communicator.websocket_generator(url, auth_token=self._auth_token)
        return Data(
            data_generator_factory,
            convert_to=convert_to,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            auth_token=self._auth_token,
            retry_policy=Communicator.create_retry_policy(),
            attempts=None,
        )

    async def _aget_real_time_updates_object(
        self,
        endpoint_url: str,
        convert_to: object = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> DataAsync:
        """Build a DataAsync object for a generic real-time WebSocket endpoint (async).

        Args:
            endpoint_url: URL template for the WebSocket endpoint.
            convert_to: Optional class used to convert each message into a model instance.
            restart_on_error: If True, reconnect automatically on errors.
            restart_on_close: If True, reconnect automatically when the server closes the connection.

        Returns:
            A DataAsync object that lazily yields messages.
        """
        url = self._format_url(endpoint_url, is_websocket=True, id=self.id)
        data_generator_factory = lambda: Communicator.websocket_generator_async(url, auth_token=self._auth_token)
        return DataAsync(
            data_generator_factory,
            convert_to=convert_to,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            auth_token=self._auth_token,
            retry_policy=Communicator.create_retry_policy(),
            attempts=None,
        )

    def get_realtime_instance_updates(
        self, as_dict: bool = False, restart_on_error: bool = True, restart_on_close: bool = True
    ) -> Data:
        """Get a Data stream of real-time instance (Pad/Site) updates over WebSocket (sync).
        See the documentation in the overridden methods for more information.

        Args:
            as_dict: If True, yield raw dicts; otherwise yield instances of the current class.
            restart_on_error: If True, reconnect automatically on errors.
            restart_on_close: If True, reconnect automatically when the server closes the connection.

        Returns:
            A Data object that lazily yields updates.
        """
        return self._get_real_time_updates_object(
            self._api_url_instance_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            convert_to=None if as_dict else self.__class__,
        )

    async def aget_realtime_instance_updates(
        self, as_dict: bool = False, restart_on_error: bool = True, restart_on_close: bool = True
    ) -> DataAsync:
        """Get a DataAsync stream of real-time instance (Pad/Site) updates over WebSocket (async).
        See the documentation in the overridden methods for more information.

        Args:
            as_dict: If True, yield raw dicts; otherwise yield instances of the current class.
            restart_on_error: If True, reconnect automatically on errors.
            restart_on_close: If True, reconnect automatically when the server closes the connection.

        Returns:
            A DataAsync object that lazily yields updates.
        """
        return await self._aget_real_time_updates_object(
            self._api_url_instance_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            convert_to=None if as_dict else self.__class__,
        )

    def get_realtime_data_changes_updates(
        self,
        as_dict: bool = False,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> Data:
        """Create a Data object that provides a lazy WebSocket stream of real-time
        data-change events for this Pad/Site.

        Args:
            as_dict (bool): Controls the type of yielded items.
                - False (default): each item is converted to a DataChanges instance.
                - True: each item is returned as a raw dict payload.
            restart_on_error (bool): If True, the returned Data instance will automatically
                attempt to reconnect to the server if an error occurs.
            restart_on_close (bool): If True, the Data instance will also recreate the underlying
                generator when the stream completes normally (e.g., clean WebSocket close) and
                continue streaming.

        Notes:
            - data.data_fetcher is a lazy synchronous generator. Each iteration blocks
              until the next update message is received.
            - Right after the connection is established, the server sends all change events that
              have not been acknowledged yet, then new events as they happen.
            - You can persist the stream to a file via data.save(path: str, overwrite: bool = False).

        Payload schema:
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
            Data: A Data object whose data_fetcher yields data-change messages either as
            DataChanges instances or dictionaries depending on as_dict.
        """
        return self._get_real_time_updates_object(
            self._api_url_changelog_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            convert_to=None if as_dict else DataChanges,
        )

    async def aget_realtime_data_changes_updates(
        self,
        as_dict: bool = False,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> DataAsync:
        """Create a DataAsync object that provides a lazy WebSocket stream of
        real-time data-change events for this Pad/Site (async).

        Args:
            as_dict (bool): Controls the type of yielded items.
                - False (default): each item is converted to a DataChanges instance.
                - True: each item is returned as a raw dict payload.
            restart_on_error (bool): If True, the returned DataAsync instance will automatically
                attempt to reconnect to the server if an error occurs.
            restart_on_close (bool): If True, the DataAsync wrapper will also recreate the underlying
                async generator when the stream completes normally (e.g., clean WebSocket close) and
                continue streaming.

        Notes:
            - data.data_fetcher is a lazy asynchronous generator. Each async iteration awaits
              the next update message.
            - Right after the connection is established, the server sends all change events that
              have not been acknowledged yet, then new events as they happen.
            - You can persist the stream to a file via data.asave(path: str, overwrite: bool = False).

        Payload schema:
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
            DataAsync: A DataAsync object whose data_fetcher yields data-change messages either as
            DataChanges instances or dictionaries depending on as_dict.
        """
        return await self._aget_real_time_updates_object(
            self._api_url_changelog_updates_websocket,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            convert_to=None if as_dict else DataChanges,
        )

    def get_stages_aggregations(self) -> list[dict[str, Any]]:
        """Fetch the stage aggregations (default and custom) defined for this entity (pad/site).

        Stage aggregations are the metrics calculated automatically for every frac stage; their values are
        returned by get_stages_metadata(add_aggregations=True).

        Returns:
            A list of dictionaries describing the aggregations: id, name, config, description,
            imperial_unit and si_unit.
        """
        url = self._format_url(self._api_url_stages_aggregations, id=self.id)
        return Communicator.send_get_request(url, auth_token=self._auth_token)

    async def aget_stages_aggregations(self) -> list[dict[str, Any]]:
        """Asynchronously fetch the stage aggregations (default and custom) defined for this entity (pad/site).

        Stage aggregations are the metrics calculated automatically for every frac stage; their values are
        returned by aget_stages_metadata(add_aggregations=True).

        Returns:
            A list of dictionaries describing the aggregations: id, name, config, description,
            imperial_unit and si_unit.
        """
        url = self._format_url(self._api_url_stages_aggregations, id=self.id)
        return await Communicator.send_get_request_async(url, auth_token=self._auth_token)

    def get_stages_aggregations_descriptions(self) -> list[dict[str, Any]]:
        """Fetch the stage aggregations of this entity (pad/site).
        Alias of get_stages_aggregations kept for backward compatibility.

        Returns:
            A list of dictionaries describing the aggregations.
        """
        return self.get_stages_aggregations()

    async def aget_stages_aggregations_descriptions(self) -> list[dict[str, Any]]:
        """Asynchronously fetch the stage aggregations of this entity (pad/site).
        Alias of aget_stages_aggregations kept for backward compatibility.

        Returns:
            A list of dictionaries describing the aggregations.
        """
        return await self.aget_stages_aggregations()

    def create_stages_aggregations(
        self, aggregations: list[dict[str, Any]], validate_only: bool = False
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Create custom stage aggregations for this entity (pad/site).

        Args:
            aggregations: A list of dictionaries describing the aggregations. Each dictionary should contain:
                - name: The name of the aggregation (letters and underscores, must not start with a digit).
                - config: A dictionary with the configuration of the aggregation: field, aggregation (the function
                  applied to the field, e.g. average) and optional conditions. Units are filled in by the server
                  from the field definition.
                - description: Optional detailed description of the aggregation.
            validate_only: If True, the server validates the aggregations without saving them.

        Example:
                [{
                    "name": "avg_bottomhole_pressure",
                    "config": {
                        "field": "bottom_hole_pressure_customer",
                        "aggregation": "average",
                    },
                    "description": "Detailed description",
                }]

        Returns:
            A list of dictionaries describing the created aggregations, or {"valid": true} when validate_only=True.

        Raises:
            BadRequestError: If the aggregations fail validation on the server.
        """
        url = self._format_url(self._api_url_stages_aggregations, id=self.id)
        return Communicator.send_post_request(
            url, aggregations, auth_token=self._auth_token, validate_only=True if validate_only else None
        )

    async def acreate_stages_aggregations(
        self, aggregations: list[dict[str, Any]], validate_only: bool = False
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Asynchronously create custom stage aggregations for this entity (pad/site).

        Args:
            aggregations: A list of dictionaries describing the aggregations. Each dictionary should contain:
                - name: The name of the aggregation (letters and underscores, must not start with a digit).
                - config: A dictionary with the configuration of the aggregation: field, aggregation (the function
                  applied to the field, e.g. average) and optional conditions. Units are filled in by the server
                  from the field definition.
                - description: Optional detailed description of the aggregation.
            validate_only: If True, the server validates the aggregations without saving them.

        Example:
                [{
                    "name": "avg_bottomhole_pressure",
                    "config": {
                        "field": "bottom_hole_pressure_customer",
                        "aggregation": "average",
                    },
                    "description": "Detailed description",
                }]

        Returns:
            A list of dictionaries describing the created aggregations, or {"valid": true} when validate_only=True.

        Raises:
            BadRequestError: If the aggregations fail validation on the server.
        """
        url = self._format_url(self._api_url_stages_aggregations, id=self.id)
        return await Communicator.send_post_request_async(
            url, aggregations, auth_token=self._auth_token, validate_only=True if validate_only else None
        )

    def get_stages_aggregation(self, aggregation_id: int) -> dict[str, Any]:
        """Fetch a single stage aggregation of this entity (pad/site) by its id.

        Args:
            aggregation_id: Identifier of the aggregation.

        Returns:
            A dictionary describing the aggregation.

        Raises:
            NotFoundError: If the aggregation does not exist for this entity.
        """
        url = self._format_url(self._api_url_stages_aggregations_one, id=self.id, aggregation_id=aggregation_id)
        return Communicator.send_get_request(url, auth_token=self._auth_token)

    async def aget_stages_aggregation(self, aggregation_id: int) -> dict[str, Any]:
        """Asynchronously fetch a single stage aggregation of this entity (pad/site) by its id.

        Args:
            aggregation_id: Identifier of the aggregation.

        Returns:
            A dictionary describing the aggregation.

        Raises:
            NotFoundError: If the aggregation does not exist for this entity.
        """
        url = self._format_url(self._api_url_stages_aggregations_one, id=self.id, aggregation_id=aggregation_id)
        return await Communicator.send_get_request_async(url, auth_token=self._auth_token)

    def update_stages_aggregation(
        self, aggregation_id: int, aggregation: dict[str, Any], partial: bool = True
    ) -> dict[str, Any]:
        """Update a custom stage aggregation of this entity (pad/site).

        Only custom aggregations can be modified; default aggregations are rejected by the server.

        Args:
            aggregation_id: Identifier of the aggregation.
            aggregation: Fields to update. Supported keys: config, description, label, color.
            partial: If True (default), only the provided fields are changed (HTTP PATCH);
                otherwise the aggregation is replaced (HTTP PUT).

        Returns:
            A dictionary describing the updated aggregation.

        Raises:
            BadRequestError: If the aggregation is not custom or the payload fails validation.
            NotFoundError: If the aggregation does not exist for this entity.
        """
        url = self._format_url(self._api_url_stages_aggregations_one, id=self.id, aggregation_id=aggregation_id)
        if partial:
            return Communicator.send_patch_request(url, aggregation, auth_token=self._auth_token)
        return Communicator.send_put_request(url, aggregation, auth_token=self._auth_token)

    async def aupdate_stages_aggregation(
        self, aggregation_id: int, aggregation: dict[str, Any], partial: bool = True
    ) -> dict[str, Any]:
        """Asynchronously update a custom stage aggregation of this entity (pad/site).

        Only custom aggregations can be modified; default aggregations are rejected by the server.

        Args:
            aggregation_id: Identifier of the aggregation.
            aggregation: Fields to update. Supported keys: config, description, label, color.
            partial: If True (default), only the provided fields are changed (HTTP PATCH);
                otherwise the aggregation is replaced (HTTP PUT).

        Returns:
            A dictionary describing the updated aggregation.

        Raises:
            BadRequestError: If the aggregation is not custom or the payload fails validation.
            NotFoundError: If the aggregation does not exist for this entity.
        """
        url = self._format_url(self._api_url_stages_aggregations_one, id=self.id, aggregation_id=aggregation_id)
        if partial:
            return await Communicator.send_patch_request_async(url, aggregation, auth_token=self._auth_token)
        return await Communicator.send_put_request_async(url, aggregation, auth_token=self._auth_token)

    def delete_stages_aggregation(self, aggregation_id: int) -> None:
        """Delete a custom stage aggregation of this entity (pad/site).

        Only custom aggregations can be deleted; default aggregations are rejected by the server.

        Args:
            aggregation_id: Identifier of the aggregation.

        Returns:
            None.

        Raises:
            BadRequestError: If the aggregation is not custom.
            NotFoundError: If the aggregation does not exist for this entity.
        """
        url = self._format_url(self._api_url_stages_aggregations_one, id=self.id, aggregation_id=aggregation_id)
        Communicator.send_delete_request(url, auth_token=self._auth_token)

    async def adelete_stages_aggregation(self, aggregation_id: int) -> None:
        """Asynchronously delete a custom stage aggregation of this entity (pad/site).

        Only custom aggregations can be deleted; default aggregations are rejected by the server.

        Args:
            aggregation_id: Identifier of the aggregation.

        Returns:
            None.

        Raises:
            BadRequestError: If the aggregation is not custom.
            NotFoundError: If the aggregation does not exist for this entity.
        """
        url = self._format_url(self._api_url_stages_aggregations_one, id=self.id, aggregation_id=aggregation_id)
        await Communicator.send_delete_request_async(url, auth_token=self._auth_token)
