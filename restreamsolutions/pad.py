from datetime import datetime
from typing import Any, Tuple

from restreamsolutions import StageNameFilters
from restreamsolutions.base_pad_site import BasePadSite
from restreamsolutions.constants import ENDPOINTS, DataResolutions, DataAggregations, DataFillMethods
from restreamsolutions.data_object import Data, DataAsync
from restreamsolutions.site import Site
from restreamsolutions.exceptions import APICompatibilityError


class Pad(BasePadSite):
    """Pad entity representing a multi-well pad.

    This class provides methods to:
      - get and update properties associated with the pad;
      - navigate to child Sites (wells) and their current States;
      - retrieve the stages history of the child Sites with optional aggregation metrics for each stage;
      - retrieve measurement sources configured for the pad;
      - retrieve data fields that exist in the pad's data;
      - stream time-series data and data changes for the pad.

    Attributes (populated from the API):
      - id (str): Unique identifier of the pad.
      - name (str): Pad name.
      - lease_name (str): Lease name associated with the pad.
      - customer_name (str): Customer name.
      - simops_config (dict): Configuration section with measurement_sources if the pad supports simops.
      - completion_date (datetime): Timezone-aware date of pad completion if all of its sites have completed their operations.
      - wireline_enabled (bool): Whether wireline integration is enabled for this pad.
    """

    _api_url_single_object: str = ENDPOINTS.pads_one.value
    _api_url_multiple_objects: str = ENDPOINTS.pads_many.value
    _api_url_fields_metadata: str = ENDPOINTS.fields_pad.value
    _api_url_stages_metadata: str = ENDPOINTS.stages_pad.value
    _api_url_aggregations_metadata: str = ENDPOINTS.aggregations_pad.value
    _api_url_data: str = ENDPOINTS.data_pad.value
    _api_url_data_changes_single: str = ENDPOINTS.data_changes_pad_one.value
    _api_url_data_changes_multiple: str = ENDPOINTS.data_changes_pad_many.value
    _api_url_data_websocket: str = ENDPOINTS.data_pad_websocket.value
    _api_url_instance_updates_websocket: str = ENDPOINTS.pad_updates_websocket.value
    _api_url_changelog_updates_websocket: str = ENDPOINTS.pad_changelog_updates_websocket.value
    _api_url_pad_parameters: str = ENDPOINTS.pad_parameters

    # These type hints are used by the BaseInterface class to perform automatic type conversion
    # when a new instance is created.
    name: str
    lease_name: str
    customer_name: str
    simops_config: dict
    completion_date: datetime
    wireline_enabled: bool

    @classmethod
    def get_models(
        cls,
        auth_token: str = None,
        as_dict=False,
        complete: bool | None = None,
        well_api: str | None = None,
        site_pk: int | None = None,
        **filters,
    ) -> "list[Pad] | list[dict]":
        """Fetch Pad objects from the API.

        Parameters:
            auth_token: Optional auth token used for API requests; if not provided,
             RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET environment variable will be used to create it.
            as_dict (bool): When True, return plain dicts instead of Pad instances. Default False.
            complete (bool | None): Filter the received pads by the complete attribute. Defaults to None (no filtering).
            well_api (str | None): Optional well API (well identifier) used to filter pads by the site with
            the provided well API.
            site_pk (int | None): Optional site primary key to filter pads by a specific site.
            **filters: Additional query parameters supported by the API.

        Returns:
            list[Pad] | list[dict]: A list of Pad objects (or dicts when as_dict=True).

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is unavailable or the response format is not supported by
                the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        return super().get_models(
            auth_token=auth_token, as_dict=as_dict, complete=complete, well_api=well_api, site_pk=site_pk, **filters
        )

    @classmethod
    async def aget_models(
        cls,
        auth_token: str = None,
        as_dict=False,
        complete: bool | None = None,
        well_api: str | None = None,
        site_pk: int | None = None,
        **filters,
    ) -> "list[Pad] | list[dict]":
        """Asynchronously fetch Pad objects from the API.

        Parameters:
            auth_token: Optional auth token used for API requests; if not provided,
            RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET environment variable will be used to create it.
            as_dict (bool): When True, return plain dicts instead of Pad instances. Default False.
            complete (bool | None): Filter the received pads by the complete attribute. Defaults to None (no filtering).
            well_api (str | None): Optional well API (well identifier) used to filter pads by the site with
            the provided well API.
            site_pk (int | None): Optional site primary key to filter pads by a specific site.
            **filters: Additional query parameters supported by the API.

        Returns:
            list[Pad] | list[dict]: A list of Pad objects (or dicts when as_dict=True).

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is unavailable or the response format is not supported by
                the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        return await super().aget_models(
            auth_token=auth_token, as_dict=as_dict, complete=complete, well_api=well_api, site_pk=site_pk, **filters
        )

    def get_sites(self, as_dict=False, **filters) -> list[Site] | list[dict[str, Any]]:
        """Fetch all Site objects that belong to this Pad.

        Parameters:
            as_dict (bool): If True, return plain dicts instead of Site instances. Default False.
            **filters: Additional query parameters passed through to the HTTP request and supported by the API.

        Returns:
            list[Site] | list[dict]: A list of Site objects (or dicts when as_dict=True).

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
            the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """

        final_filters = {**filters, 'pad__id': self.id}
        return Site.get_models(auth_token=self._auth_token, as_dict=as_dict, **final_filters)

    async def aget_sites(self, as_dict=False, **filters) -> list['Site'] | list[dict[str, Any]]:
        """Asynchronously fetch all Site objects that belong to this Pad.

        Parameters:
            as_dict (bool): If True, return plain dicts instead of Site instances. Default False.
            **filters: Additional query parameters passed through to the HTTP request and supported by the API.

        Returns:
            list[Site] | list[dict]: A list of Site objects (or dicts when as_dict=True).

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
            the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """

        final_filters = {**filters, 'pad__id': self.id}
        return await Site.aget_models(auth_token=self._auth_token, as_dict=as_dict, **final_filters)

    def get_states(
        self, stage_name_filter: StageNameFilters = None, as_dict=False, **filters
    ) -> list['State'] | list[dict]:
        """Fetch site State objects representing current stage info for Sites under this Pad.

        Parameters:
            stage_name_filter (StageNameFilters | None): Optional filter to include only states
                matching a specific stage name pattern.
            as_dict (bool): If True, return plain dicts instead of State instances. Default False.
            **filters: Additional query parameters passed through to the HTTP request and supported by the API.

        Returns:
            list[State] | list[dict]: A list of State objects (or dicts when as_dict=True).

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
            the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        from .state import State

        final_filters = {**filters, 'pad__id': self.id}
        return State.get_models(
            stage_name_filter=stage_name_filter, auth_token=self._auth_token, as_dict=as_dict, **final_filters
        )

    async def aget_states(
        self, stage_name_filter: StageNameFilters = None, as_dict=False, **filters
    ) -> list['State'] | list[dict]:
        """Asynchronously fetch site State objects representing current stage info for Sites under this Pad.

        Parameters:
            stage_name_filter (StageNameFilters | None): Optional filter to include only states
                matching a specific stage name pattern.
            as_dict (bool): If True, return plain dicts instead of State instances. Default False.
            **filters: Additional query parameters passed through to the HTTP request and supported by the API.

        Returns:
            list[State] | list[dict]: A list of State objects (or dicts when as_dict=True).

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
            the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        from .state import State

        final_filters = {**filters, 'pad__id': self.id}
        return await State.aget_models(
            stage_name_filter=stage_name_filter, auth_token=self._auth_token, as_dict=as_dict, **final_filters
        )

    def _get_measurement_sources(self) -> dict:
        """Internal helper to read measurement_sources from simops_config.

        Returns:
            dict: A mapping of measurement source categories to their configurations.

        Raises:
            APICompatibilityError: If the simops_config property format is not supported by
            the current version of this package.
        """
        if getattr(self, 'simops_config') is None:
            return {}
        try:
            return self.simops_config['measurement_sources']
        except KeyError:
            raise APICompatibilityError("measurement_sources section not found.")

    def get_measurement_sources_metadata(self) -> dict:
        """Return measurement sources metadata for this pad.

        Returns:
            dict: Measurement sources metadata grouped by category or an empty dict if this pad doesn't have a simops
            configuration.

        Raises:
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
            the current version of this package.
            AuthError: If authentication fails while getting the updated simops configuration.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        if not hasattr(self, 'simops_config'):
            self.update()
        return self._get_measurement_sources()

    async def aget_measurement_sources_metadata(self) -> dict:
        """Asynchronously return measurement sources metadata for this pad.

        Returns:
            dict: Measurement sources metadata grouped by category or an empty dict if this pad doesn't have a simops
            configuration.

        Raises:
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
            the current version of this package.
            AuthError: If authentication fails while getting the updated simops configuration.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
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
        fields: str | list[str] = None,
        si_units: bool = False,
        measurement_sources_names: str | list[str] = None,
        is_routed: bool = False,
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> Data:
        """Stream time-series data for this pad or save it to a file.

        data.data_fetcher is a lazy synchronous generator. Iterate over it to get timestamped data
        items one by one. data.save(path: str, overwrite: bool = False) will save all the data to a JSON file.

        Parameters:
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            si_units (bool): If True, values are converted to SI units.
            measurement_sources_names (str | list[str] | None): Filter by measurement source names.
            fields: (str | list[str] | None): Optional fields filter.
                Use get_fields_metadata() method to get all available fields for this pad.
            is_routed (bool): Required when stage_number is set. If True, return data routed per site; otherwise each
                element represents data for the entire pad at a given timestamp. If is_routed=False and simops is
                configured for this pad, the simulator-run fields will be prefixed according to configured measurement
                sources, e.g., M1_slurry_flow for measurement source M1 and M2_slurry_flow for M2, instead of just
                slurry_flow. Use get_fields_metadata() to get the full list of available fields and
                get_measurement_sources_metadata() to map measurement sources to sites.
            fill_data_method (DataFillMethods | None): Optional gap-filling method. Use DataFillMethods.FORWARD_FILL
                to fill missing values forward after the last known value, or DataFillMethods.BACKWARD_FILL
                to fill missing values backward before the next known value. If provided, resolution must be
                DataResolutions.SECOND and aggregation must be None.
            fill_data_limit (int | None): Required when fill_data_method is provided. Maximum number of seconds to fill
                per gap: for FORWARD_FILL this applies after a known value; for BACKWARD_FILL this applies before a
                known value. Has no effect if fill_data_method is not provided. Must be a positive integer.
            inside_area_only (bool): Optional. Default True. Controls whether filling is restricted to internal gaps
                between original values (True) or may extend across edges (False). Has effect only when
                fill_data_method is provided.

        Returns:
            Data: A Data object that lazily streams records from the API when iterated over.

        Raises:
            ValueError: If stage_number is provided while is_routed is False (pad-specific rule).
            ValueError: If datetimes are not timezone-aware, or stage_number is provided without
                stage_name_filter (originating from BasePadSite validations).
            AuthError: If authentication fails when the underlying request is performed.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
                the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """

        if not is_routed and stage_number is not None:
            raise ValueError('Please select is_routed = True to query data by stage_number.')

        return super().get_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            fields=fields,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
            is_routed=is_routed,
            fill_data_method=fill_data_method,
            fill_data_limit=fill_data_limit,
            inside_area_only=inside_area_only,
        )

    async def aget_data(
        self,
        start_datetime: datetime = None,
        end_datetime: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        resolution: DataResolutions = DataResolutions.SECOND,
        aggregation: DataAggregations = None,
        fields: str | list[str] = None,
        si_units: bool = False,
        measurement_sources_names: str | list[str] = None,
        is_routed: bool = False,
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> DataAsync:
        """Asynchronously stream time-series data for this pad or save it to a file.

        data.data_fetcher is a lazy asynchronous generator. Iterate over it to get timestamped data items one by one.
        data.asave(path: str, overwrite: bool = False) will asynchronously save all the data to a JSON file.

        Parameters:
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields: (str | list[str] | None): Optional fields filter.
                Use get_fields_metadata() method to get all available fields for this pad.
            si_units (bool): If True, values are converted to SI units.
            measurement_sources_names (str | list[str] | None): Filter by measurement source names.
            is_routed (bool): Required when stage_number is set. If True, return data routed per site; otherwise
                each element represents data for the entire pad at a given timestamp. If is_routed=False and simops is
                configured for this pad, the simulator-run fields will be prefixed according to configured measurement
                sources, e.g., M1_slurry_flow for measurement source M1 and M2_slurry_flow for M2, instead of just
                slurry_flow. Use aget_fields_metadata() to get the full list of available fields and
                aget_measurement_sources_metadata() to map measurement sources to sites.
            fill_data_method (DataFillMethods | None): Optional gap-filling method. Use DataFillMethods.FORWARD_FILL
                to fill missing values forward after the last known value, or DataFillMethods.BACKWARD_FILL
                to fill missing values backward before the next known value. If provided, resolution must be
                DataResolutions.SECOND and aggregation must be None.
            fill_data_limit (int | None): Required when fill_data_method is provided. Maximum number of seconds to fill
                per gap: for FORWARD_FILL this applies after a known value; for BACKWARD_FILL this applies before a
                known value. Has no effect if fill_data_method is not provided. Must be a positive integer.
            inside_area_only (bool): Optional. Default True. Controls whether filling is restricted to internal gaps
                between original values (True) or may extend across edges (False). Has effect only when
                fill_data_method is provided.

        Returns:
            DataAsync: A DataAsync object that lazily streams records from the API when asynchronously iterated over.

        Raises:
            ValueError: If stage_number is provided while is_routed is False (pad-specific rule).
            ValueError: If datetimes are not timezone-aware, or stage_number is provided without
                stage_name_filter (originating from BasePadSite validations).
            AuthError: If authentication fails when the underlying request is performed.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
                the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """

        if not is_routed and stage_number is not None:
            raise ValueError('Please select is_routed = True to query data by stage_number.')

        return await super().aget_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            fields=fields,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
            is_routed=is_routed,
            fill_data_method=fill_data_method,
            fill_data_limit=fill_data_limit,
            inside_area_only=inside_area_only,
        )

    def get_realtime_measurements_data(
        self,
        session_key: str | None = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
        start_datetime: datetime = None,
        end_datetime: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        resolution: DataResolutions = DataResolutions.SECOND,
        aggregation: DataAggregations = None,
        fields: str | list[str] = None,
        si_units: bool = False,
        measurement_sources_names: str | list[str] = None,
        is_routed: bool = False,
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> Tuple[Data, str]:
        """Open a WebSocket stream of real-time measurements for this Pad (sync).

        Works with the same filters as get_data. If filters are provided, the stream first yields
        the historical data matching the filters, then continues with real-time updates.

        Parameters:
            session_key (str | None): Optional session identifier to resume an existing stream.
            restart_on_error (bool): If True, the Data will auto-reconnect on errors.
            restart_on_close (bool): If True, the Data will also restart when the stream closes normally.
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields (str | list[str] | None): Optional fields filter. Use get_fields_metadata() to
                discover all available fields for this pad.
            si_units (bool): If True, values are converted to SI units.
            measurement_sources_names (str | list[str] | None): Filter by measurement source names.
            is_routed (bool): Required when stage_number is set. If True, return data routed per site; otherwise each
                element represents data for the entire pad at a given timestamp. See get_data for details.
            fill_data_method (DataFillMethods | None): Optional gap-filling method. Use DataFillMethods.FORWARD_FILL
                to fill missing values forward after the last known value, or DataFillMethods.BACKWARD_FILL
                to fill missing values backward before the next known value. If provided, resolution must be
                DataResolutions.SECOND and aggregation must be None.
            fill_data_limit (int | None): Required when fill_data_method is provided. Maximum number of seconds to fill
                per gap: for FORWARD_FILL this applies after a known value; for BACKWARD_FILL this applies before a
                known value. Has no effect if fill_data_method is not provided. Must be a positive integer.
            inside_area_only (bool): Optional. Default True. Controls whether filling is restricted to internal gaps
                between original values (True) or may extend across edges (False). Has effect only when
                fill_data_method is provided.

        Returns:
            tuple[Data, str]: A Data object that lazily yields messages and the session_key used.
        """
        if not is_routed and stage_number is not None:
            raise ValueError('Please select is_routed = True to query data by stage_number.')

        data, key = super().get_realtime_measurements_data(
            session_key=session_key,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            fields=fields,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
            is_routed=is_routed,
            fill_data_method=fill_data_method,
            fill_data_limit=fill_data_limit,
            inside_area_only=inside_area_only,
        )
        return data, key

    async def aget_realtime_measurements_data(
        self,
        session_key: str | None = None,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
        start_datetime: datetime = None,
        end_datetime: datetime = None,
        stage_number: int = None,
        stage_name_filter: StageNameFilters = None,
        resolution: DataResolutions = DataResolutions.SECOND,
        aggregation: DataAggregations = None,
        fields: str | list[str] = None,
        si_units: bool = False,
        measurement_sources_names: str | list[str] = None,
        is_routed: bool = False,
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> Tuple[DataAsync, str]:
        """Open a WebSocket stream of real-time measurements for this Pad (async).

        Works with the same filters as aget_data. If filters are provided, the stream first yields
        the historical data matching the filters, then continues with real-time updates.

        Parameters:
            session_key (str | None): Optional session identifier to resume an existing stream.
            restart_on_error (bool): If True, the DataAsync will auto-reconnect on errors.
            restart_on_close (bool): If True, the DataAsync will also restart when the stream closes normally.
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields (str | list[str] | None): Optional fields filter. Use aget_fields_metadata() to
                discover all available fields for this pad.
            si_units (bool): If True, values are converted to SI units.
            measurement_sources_names (str | list[str] | None): Filter by measurement source names.
            is_routed (bool): Required when stage_number is set. If True, return data routed per site; otherwise each
                element represents data for the entire pad at a given timestamp. See aget_data for details.
            fill_data_method (DataFillMethods | None): Optional gap-filling method. Use DataFillMethods.FORWARD_FILL
                to fill missing values forward after the last known value, or DataFillMethods.BACKWARD_FILL
                to fill missing values backward before the next known value. If provided, resolution must be
                DataResolutions.SECOND and aggregation must be None.
            fill_data_limit (int | None): Required when fill_data_method is provided. Maximum number of seconds to fill
                per gap: for FORWARD_FILL this applies after a known value; for BACKWARD_FILL this applies before a
                known value. Has no effect if fill_data_method is not provided. Must be a positive integer.
            inside_area_only (bool): Optional. Default True. Controls whether filling is restricted to internal gaps
                between original values (True) or may extend across edges (False). Has effect only when
                fill_data_method is provided.

        Returns:
            tuple[DataAsync, str]: A DataAsync object that lazily yields messages and the session_key used.
        """
        if not is_routed and stage_number is not None:
            raise ValueError('Please select is_routed = True to query data by stage_number.')

        data_async, key = await super().aget_realtime_measurements_data(
            session_key=session_key,
            restart_on_error=restart_on_error,
            restart_on_close=restart_on_close,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            fields=fields,
            si_units=si_units,
            measurement_sources_names=measurement_sources_names,
            is_routed=is_routed,
            fill_data_method=fill_data_method,
            fill_data_limit=fill_data_limit,
            inside_area_only=inside_area_only,
        )
        return data_async, key

    def get_realtime_instance_updates(
        self, as_dict: bool = False, restart_on_error: bool = True, restart_on_close: bool = True
    ) -> Data:
        """Create a Data object that provides a lazy WebSocket stream of real-time
        updates for this Pad.

        Parameters:
            as_dict (bool): Controls the type of yielded items.
                - False (default): each item is converted to a Pad instance.
                - True: each item is returned as a raw dict payload.
            restart_on_error (bool): If True (default), the returned Data instance will automatically
                attempt to reconnect to the server if an error occurs.
            restart_on_close (bool): If True (default), the Data wrapper will also recreate the underlying
                generator when the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        Notes:
            - data.data_fetcher is a lazy synchronous generator. Each iteration blocks awaiting the next update.
            - Persist the stream with data.save(path: str, overwrite: bool = False).

        Payload schema:
            Each message is a JSON object that reflects the current state of this Pad and may
            include some additional properties obtained from related entities. The payload contains
            fields such as:
              - id (int)
              - name (str)
              - lease_name (str)
              - customer_name (str)
              - simops_config (object | None):
                  - measurement_sources (object): frac, wl, fluid, pumpdown (lists)
                  - exc_fields (list)
              - completion_date (str | None, ISO 8601)
              - wireline_enabled (bool)

        Returns:
            Data: A Data object whose data_fetcher yields items as Pad instances or dicts depending on as_dict.
        """
        return super().get_realtime_instance_updates(
            as_dict=as_dict, restart_on_error=restart_on_error, restart_on_close=restart_on_close
        )

    async def aget_realtime_instance_updates(
        self,
        as_dict: bool = False,
        restart_on_error: bool = True,
        restart_on_close: bool = True,
    ) -> DataAsync:
        """Create a DataAsync object that provides a lazy WebSocket stream of
        real-time updates for this Pad (async).

        Parameters:
            as_dict (bool): Controls the type of yielded items.
                - False (default): each item is converted to a Pad instance.
                - True: each item is returned as a raw dict payload.
            restart_on_error (bool): If True (default), the returned DataAsync instance will automatically
                attempt to reconnect to the server if an error occurs.
            restart_on_close (bool): If True (default), the DataAsync wrapper will also recreate the underlying
                async generator when the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        Notes:
            - data.data_fetcher is a lazy asynchronous generator. Each async iteration awaits the next update.
            - Persist the stream with data.asave(path: str, overwrite: bool = False).

        Payload schema:
            Each message is a JSON object that reflects the current state of this Pad and may
            include some additional properties obtained from related entities. The payload contains
            fields such as:
              - id (int)
              - name (str)
              - lease_name (str)
              - customer_name (str)
              - simops_config (object | None):
                  - measurement_sources (object): frac, wl, fluid, pumpdown (lists)
                  - exc_fields (list)
              - completion_date (str | None, ISO 8601)
              - wireline_enabled (bool)

        Returns:
            DataAsync: A DataAsync object whose data_fetcher yields items as Pad instances or dicts depending on as_dict when asynchronously iterated over.
        """
        return await super().aget_realtime_instance_updates(
            as_dict=as_dict, restart_on_error=restart_on_error, restart_on_close=restart_on_close
        )
