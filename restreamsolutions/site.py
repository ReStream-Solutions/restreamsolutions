from datetime import datetime
from typing import Optional, Any, Tuple

from restreamsolutions.base_pad_site import BasePadSite
from restreamsolutions.constants import ENDPOINTS, StageNameFilters, DataResolutions, DataAggregations, DataFillMethods
from restreamsolutions.data_object import Data, DataAsync
from restreamsolutions.exceptions import APICompatibilityError


class Site(BasePadSite):
    """Site entity representing a single well within a pad.

    This class provides methods to:
      - get and update instance properties associated with the site;
      - navigate to the parent Pad and the current State of this site;
      - retrieve the stages history for this site with optional aggregation metrics for each stage;
      - retrieve measurement sources configured on the parent pad that are attached to this site;
      - retrieve data fields that exist in this site's data;
      - stream time-series data and data changes for this site.

    Attributes (populated from the API):
      - id (str): Unique identifier of the site.
      - name (str): Site name.
      - date_created (datetime): Timezone-aware creation timestamp.
      - latitude (float): Latitude in decimal degrees.
      - longitude (float): Longitude in decimal degrees.
      - formation_name (str): Name of the formation of the site (or of its pad); empty string if unknown.
      - lease_name (str): Lease name associated with the site.
      - operator_name (str): Operator name.
      - crew (str | None): Name of the crew assigned to the site; None if no crew is assigned.
      - division (str | None): Name of the division the site belongs to; None if no division is assigned.
      - metadata (dict): Additional metadata dictionary.
      - well_api (str): Well API number if available.
      - pad_id (int): Identifier of the parent Pad.
      - is_demo_site (bool): Whether this is a demo site.
      - stage_total (int): Total number of stages for this site.
      - timezone (str): Timezone name for this site.
      - current_stage_number (int): Current stage number.
      - current_state (str): Current state for this site ('frac_frac', 'standby_standby', 'wl_*').
      - fleet_id (int): Fleet identifier if the site belongs to a fleet.
      - fleet_name (str): Fleet name if the site belongs to a fleet.
    """

    _api_url_single_object: str = ENDPOINTS.sites_one.value
    _api_url_multiple_objects: str = ENDPOINTS.sites_many.value
    _api_url_fields_metadata: str = ENDPOINTS.fields_site.value
    _api_url_stages_metadata: str = ENDPOINTS.stages_site.value
    _api_url_aggregations_metadata: str = ENDPOINTS.aggregations_site.value
    _api_url_data: str = ENDPOINTS.data_site.value
    _api_url_data_changes_single: str = ENDPOINTS.data_changes_site_one.value
    _api_url_data_changes_multiple: str = ENDPOINTS.data_changes_site_many.value
    _api_url_data_websocket: str = ENDPOINTS.data_site_websocket.value
    _api_url_instance_updates_websocket: str = ENDPOINTS.site_updates_websocket.value
    _api_url_changelog_updates_websocket: str = ENDPOINTS.site_changelog_updates_websocket.value
    _api_url_stages_aggregations: str = ENDPOINTS.stages_aggregations_site.value
    _api_url_stages_aggregations_one: str = ENDPOINTS.stages_aggregations_site_one.value

    # These type hints are used by the BaseInterface class to perform automatic type conversion
    # when a new instance is created.
    name: str
    date_created: datetime
    latitude: float
    longitude: float
    formation_name: str
    lease_name: str
    operator_name: str
    # The API omits crew and division when they are not assigned
    crew: str = None
    division: str = None
    metadata: dict
    well_api: str
    pad_id: int
    is_demo_site: bool
    stage_total: int
    timezone: str
    current_stage_number: int
    current_state: str
    fleet_id: int
    fleet_name: str

    @staticmethod
    def _build_site_filters(
        complete: bool | None = None,
        pad_pk: int | None = None,
        well_api: str | None = None,
        name_search: str | None = None,
        formation_name: str | None = None,
        pad_name: str | None = None,
        latitude_gte: float | None = None,
        latitude_lte: float | None = None,
        longitude_gte: float | None = None,
        longitude_lte: float | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        radius_miles: float | None = None,
        fleet_id: int | None = None,
        fleet_name: str | None = None,
        **filters,
    ) -> dict[str, Any]:
        """Translate the named site filters into query parameters of the sites endpoint.

        Args:
            See get_models for the meaning of every filter.
            **filters: Additional query parameters passed through as-is. The raw lat and long parameters
                of the API are accepted as the center of the radius filter as well.

        Returns:
            A dict of query parameters without None values.

        Raises:
            ValueError: If only some of the radius filter values (center and radius_miles) are provided.
        """
        center_latitude = latitude if latitude is not None else filters.get('lat')
        center_longitude = longitude if longitude is not None else filters.get('long')
        radius_filter = (center_latitude, center_longitude, radius_miles)
        provided_count = sum(value is not None for value in radius_filter)
        if 0 < provided_count < 3:
            raise ValueError('latitude (or lat), longitude (or long) and radius_miles must be provided together.')
        named = {
            'complete': complete,
            'pad_pk': pad_pk,
            'well_api': well_api,
            'name_search': name_search,
            'formation_name': formation_name,
            'pad_name': pad_name,
            'latitude_gte': latitude_gte,
            'latitude_lte': latitude_lte,
            'longitude_gte': longitude_gte,
            'longitude_lte': longitude_lte,
            'lat': latitude,
            'long': longitude,
            'radius_miles': radius_miles,
            'fleet_id': fleet_id,
            'fleet_name': fleet_name,
        }
        # Raw query parameters passed through filters (e.g. lat) must survive when the named value is not set
        params = {key: value for key, value in filters.items() if value is not None}
        params.update({key: value for key, value in named.items() if value is not None})
        return params

    @classmethod
    def get_models(
        cls,
        auth_token: str = None,
        as_dict=False,
        complete: bool | None = None,
        pad_pk: int | None = None,
        well_api: str | None = None,
        name_search: str | None = None,
        formation_name: str | None = None,
        pad_name: str | None = None,
        latitude_gte: float | None = None,
        latitude_lte: float | None = None,
        longitude_gte: float | None = None,
        longitude_lte: float | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        radius_miles: float | None = None,
        fleet_id: int | None = None,
        fleet_name: str | None = None,
        **filters,
    ) -> "list[Site] | list[dict]":
        """Fetch Site objects from the API.

        Args:
            auth_token: Optional auth token used for API requests; if not provided,
                RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET environment variables will be used to create it.
            as_dict (bool): When True, return plain dicts instead of Site instances. Default False.
            complete (bool | None): Filter sites by the completion status of their pad. None means no filtering.
            pad_pk (int | None): Return only sites that belong to the pad with this id.
            well_api (str | None): Return only the site with this well API number.
            name_search (str | None): Fuzzy (trigram) search by site name.
            formation_name (str | None): Filter by formation name (case-insensitive partial match).
            pad_name (str | None): Filter by pad name (case-insensitive partial match).
            latitude_gte (float | None): Minimum latitude in decimal degrees (bounding box filter).
            latitude_lte (float | None): Maximum latitude in decimal degrees (bounding box filter).
            longitude_gte (float | None): Minimum longitude in decimal degrees (bounding box filter).
            longitude_lte (float | None): Maximum longitude in decimal degrees (bounding box filter).
            latitude (float | None): Center latitude of the radius filter. Requires longitude and radius_miles.
            longitude (float | None): Center longitude of the radius filter. Requires latitude and radius_miles.
            radius_miles (float | None): Radius in miles around the center point. Requires latitude and longitude.
            fleet_id (int | None): Filter sites by fleet identifier.
            fleet_name (str | None): Filter sites by fleet name (case-insensitive partial match).
            **filters: Additional query parameters supported by the API.

        Returns:
            list[Site] | list[dict]: A list of Site objects (or dicts when as_dict=True).

        Raises:
            ValueError: If only some of the radius filter values (center and radius_miles) are provided.
            AuthError: If authentication fails.
            ForbiddenError: If the client has no access to the requested resource.
            NotFoundError: If the endpoint does not exist.
            APICompatibilityError: If the response format is not supported by the current version of this package.
            APIConcurrencyLimitError: If the API concurrency limit is reached.
            BadRequestError: For other non-2xx HTTP responses.
        """
        site_filters = cls._build_site_filters(
            complete=complete,
            pad_pk=pad_pk,
            well_api=well_api,
            name_search=name_search,
            formation_name=formation_name,
            pad_name=pad_name,
            latitude_gte=latitude_gte,
            latitude_lte=latitude_lte,
            longitude_gte=longitude_gte,
            longitude_lte=longitude_lte,
            latitude=latitude,
            longitude=longitude,
            radius_miles=radius_miles,
            fleet_id=fleet_id,
            fleet_name=fleet_name,
            **filters,
        )
        return super().get_models(auth_token=auth_token, as_dict=as_dict, **site_filters)

    @classmethod
    async def aget_models(
        cls,
        auth_token: str = None,
        as_dict=False,
        complete: bool | None = None,
        pad_pk: int | None = None,
        well_api: str | None = None,
        name_search: str | None = None,
        formation_name: str | None = None,
        pad_name: str | None = None,
        latitude_gte: float | None = None,
        latitude_lte: float | None = None,
        longitude_gte: float | None = None,
        longitude_lte: float | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        radius_miles: float | None = None,
        fleet_id: int | None = None,
        fleet_name: str | None = None,
        **filters,
    ) -> "list[Site] | list[dict]":
        """Asynchronously fetch Site objects from the API.

        Args:
            auth_token (str | None): Optional auth token used for API requests; if not provided,
                RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET environment variables will be used to create it.
            as_dict (bool): When True, return plain dicts instead of Site instances. Default False.
            complete (bool | None): Filter sites by the completion status of their pad. None means no filtering.
            pad_pk (int | None): Return only sites that belong to the pad with this id.
            well_api (str | None): Return only the site with this well API number.
            name_search (str | None): Fuzzy (trigram) search by site name.
            formation_name (str | None): Filter by formation name (case-insensitive partial match).
            pad_name (str | None): Filter by pad name (case-insensitive partial match).
            latitude_gte (float | None): Minimum latitude in decimal degrees (bounding box filter).
            latitude_lte (float | None): Maximum latitude in decimal degrees (bounding box filter).
            longitude_gte (float | None): Minimum longitude in decimal degrees (bounding box filter).
            longitude_lte (float | None): Maximum longitude in decimal degrees (bounding box filter).
            latitude (float | None): Center latitude of the radius filter. Requires longitude and radius_miles.
            longitude (float | None): Center longitude of the radius filter. Requires latitude and radius_miles.
            radius_miles (float | None): Radius in miles around the center point. Requires latitude and longitude.
            fleet_id (int | None): Filter sites by fleet identifier.
            fleet_name (str | None): Filter sites by fleet name (case-insensitive partial match).
            **filters: Additional query parameters supported by the API.

        Returns:
            list[Site] | list[dict]: A list of Site objects (or dicts when as_dict=True).

        Raises:
            ValueError: If only some of the radius filter values (center and radius_miles) are provided.
            AuthError: If authentication fails.
            ForbiddenError: If the client has no access to the requested resource.
            NotFoundError: If the endpoint does not exist.
            APICompatibilityError: If the response format is not supported by the current version of this package.
            APIConcurrencyLimitError: If the API concurrency limit is reached.
            BadRequestError: For other non-2xx HTTP responses.
        """
        site_filters = cls._build_site_filters(
            complete=complete,
            pad_pk=pad_pk,
            well_api=well_api,
            name_search=name_search,
            formation_name=formation_name,
            pad_name=pad_name,
            latitude_gte=latitude_gte,
            latitude_lte=latitude_lte,
            longitude_gte=longitude_gte,
            longitude_lte=longitude_lte,
            latitude=latitude,
            longitude=longitude,
            radius_miles=radius_miles,
            fleet_id=fleet_id,
            fleet_name=fleet_name,
            **filters,
        )
        return await super().aget_models(auth_token=auth_token, as_dict=as_dict, **site_filters)

    def get_state(self, as_dict: bool = False) -> Optional['State'] | Optional[dict[str, Any]]:
        """Fetch the current State object for this site.

        Args:
            as_dict (bool): If True, return a plain dict instead of a State instance. Default False.

        Returns:
            State | dict | None: The current State for this site (or dict when as_dict=True),
            or None if no state exists.

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is
                not supported by the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        from .state import State

        states = State.get_models(auth_token=self._auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    async def aget_state(self, as_dict: bool = False) -> Optional['State'] | Optional[dict[str, Any]]:
        """Asynchronously fetch the current State object for this site.

        Args:
            as_dict (bool): If True, return a plain dict instead of a State instance. Default False.

        Returns:
            State | dict | None: The current State for this site (or dict when as_dict=True),
            or None if no state exists.

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is
                not supported by the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        from .state import State

        states = await State.aget_models(auth_token=self._auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    def get_pad(self, as_dict: bool = False) -> Optional['Pad'] | Optional[dict[str, Any]]:
        """Fetch the parent Pad object this site belongs to.

        Args:
            as_dict (bool): If True, return a plain dict instead of a Pad instance. Default False.

        Returns:
            Pad | dict | None: The parent Pad (or dict when as_dict=True), or None if pad_id is not set.

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is
                not supported by the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        from .pad import Pad

        if not hasattr(self, 'pad_id'):
            self.update()
        if getattr(self, 'pad_id') is None:
            return None
        return Pad.get_model(id=self.pad_id, auth_token=self._auth_token, as_dict=as_dict)

    async def aget_pad(self, as_dict: bool = False) -> Optional['Pad'] | Optional[dict[str, Any]]:
        """Asynchronously fetch the parent Pad object this site belongs to.

        Args:
            as_dict (bool): If True, return a plain dict instead of a Pad instance. Default False.

        Returns:
            Pad | dict | None: The parent Pad (or dict when as_dict=True), or None if pad_id is not set.

        Raises:
            AuthError: If authentication fails.
            APICompatibilityError: If the endpoint is not available or the response format is
                not supported by the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        from .pad import Pad

        if not hasattr(self, 'pad_id'):
            await self.aupdate()
        if getattr(self, 'pad_id') is None:
            return None
        return await Pad.aget_model(id=self.pad_id, auth_token=self._auth_token, as_dict=as_dict)

    def _extract_site_measurement_sources(self, pad_measurement_sources: dict) -> dict:
        """Internal helper to filter pad measurement_sources to only those attached to this site.

        Args:
            pad_measurement_sources (dict): Metadata returned by Pad.get_measurement_sources_metadata().

        Returns:
            dict: A mapping of measurement source categories to configurations that reference this site,
            or an empty dict if none are attached.

        Raises:
            APICompatibilityError: If the expected keys are absent in the input structure.
        """
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
        """Return measurement sources metadata attached to this site.

        The metadata is derived from the parent pad's measurement sources and filtered to those
        that list this site in their attached_sites.

        Returns:
            dict: Measurement sources metadata grouped by category, filtered for this site. If the pad
            doesn't have a simops configuration or nothing is attached to this site, returns an empty dict.

        Raises:
            APICompatibilityError: If the parent pad's metadata format is not supported by the
                current version of this package.
            AuthError: If authentication fails while fetching the pad or its configuration.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        pad = self.get_pad()
        pad_measurement_sources = pad.get_measurement_sources_metadata()
        return self._extract_site_measurement_sources(pad_measurement_sources)

    async def aget_measurement_sources_metadata(self) -> dict:
        """Asynchronously return measurement sources metadata attached to this site.

        The metadata is derived from the parent pad's measurement sources and filtered to those
        that list this site in their attached_sites.

        Returns:
            dict: Measurement sources metadata grouped by category, filtered for this site. If the pad
            doesn't have a simops configuration or nothing is attached to this site, returns an empty dict.

        Raises:
            APICompatibilityError: If the parent pad's metadata format is not supported by the
                current version of this package.
            AuthError: If authentication fails while fetching the pad or its configuration.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
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
        fields: str | list[str] = None,
        si_units: bool = False,
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> Data:
        """Stream time-series data for this site or save it to a file.

        data.data_fetcher is a lazy synchronous generator. Iterate over it to get timestamped data
        items one by one. data.save(path: str, overwrite: bool = False) will save all the data to a JSON file.

        Args:
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields (str | list[str] | None): Optional fields filter. Use get_fields_metadata() to
                discover all available fields for this site.
            si_units (bool): If True, values are converted to SI units.
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
            ValueError: If datetimes are not timezone-aware, or stage_number is provided without
                stage_name_filter (originating from BasePadSite validations).
            AuthError: If authentication fails when the underlying request is performed.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
                the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        return super().get_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            fields=fields,
            si_units=si_units,
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
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> DataAsync:
        """Asynchronously stream time-series data for this site or save it to a file.

        data.data_fetcher is a lazy asynchronous generator. Iterate over it to get timestamped data items one by one.
        data.asave(path: str, overwrite: bool = False) will asynchronously save all the data to a JSON file.

        Args:
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields (str | list[str] | None): Optional fields filter. Use aget_fields_metadata() to
                discover all available fields for this site.
            si_units (bool): If True, values are converted to SI units.
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
            ValueError: If datetimes are not timezone-aware, or stage_number is provided without
                stage_name_filter (originating from BasePadSite validations).
            AuthError: If authentication fails when the underlying request is performed.
            APICompatibilityError: If the endpoint is not available or the response format is not supported by
                the current version of this package.
            APIConcurrencyLimitError: If the API rate limit is reached.
            HTTPError: For other non-2xx HTTP responses.
        """
        return await super().aget_data(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            stage_number=stage_number,
            stage_name_filter=stage_name_filter,
            resolution=resolution,
            aggregation=aggregation,
            fields=fields,
            si_units=si_units,
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
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> Tuple[Data, str]:
        """Open a WebSocket stream of real-time measurements for this Site (sync).

        Works with the same filters as get_data. If filters are provided, the stream first yields
        the historical data matching the filters, then continues with real-time updates.

        Args:
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
                discover all available fields for this site.
            si_units (bool): If True, values are converted to SI units.
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
        fill_data_method: DataFillMethods = None,
        fill_data_limit: int | None = None,
        inside_area_only: bool = True,
    ) -> Tuple[DataAsync, str]:
        """Open a WebSocket stream of real-time measurements for this Site (async).

        Works with the same filters as aget_data. If filters are provided, the stream first yields
        the historical data matching the filters, then continues with real-time updates.

        Args:
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
                discover all available fields for this site.
            si_units (bool): If True, values are converted to SI units.
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
            fill_data_method=fill_data_method,
            fill_data_limit=fill_data_limit,
            inside_area_only=inside_area_only,
        )
        return data_async, key

    def get_realtime_instance_updates(
        self, as_dict: bool = False, restart_on_error: bool = True, restart_on_close: bool = True
    ) -> Data:
        """Create a Data object that provides a lazy WebSocket stream of real-time
        updates for this Site.

        Args:
            as_dict (bool): Controls the type of yielded items.
                - False (default): each item is converted to a Site instance.
                - True: each item is returned as a raw dict payload.
            restart_on_error (bool): If True (default), the returned Data instance will automatically
                attempt to reconnect to the server if an error occurs.
            restart_on_close (bool): If True (default), the Data wrapper will also recreate the underlying
                generator when the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        Notes:
            - data.data_fetcher is a lazy synchronous generator. Each iteration blocks awaiting the next update.
            - Persist the stream with data.save(path: str, overwrite: bool = False).

        Payload schema:
            Each message is a JSON object that reflects the current state of this Site and
            includes some additional properties obtained from related entities. The payload contains
            fields such as:
              - id (int)
              - name (str)
              - date_created (str, ISO 8601)
              - latitude (float | None)
              - longitude (float | None)
              - lease_name (str)
              - operator_name (str)
              - metadata (object): first_timestamp/last_timestamp (str | None, ISO 8601)
              - well_api (str | None)
              - pad_id (int)
              - is_demo_site (bool)
              - stage_total (int | None)
              - timezone (str | None), e.g. "US/Central"
              - current_stage_number (int | None)
              - current_state (str | None), e.g. "frac_frac"
              - current_data_sources (object): frac, wl, fluid, pumpdown (lists)

        Returns:
            Data: A Data object whose data_fetcher yields items as Site instances or dicts depending on as_dict.
        """
        return super().get_realtime_instance_updates(
            as_dict=as_dict, restart_on_error=restart_on_error, restart_on_close=restart_on_close
        )

    async def aget_realtime_instance_updates(
        self, as_dict: bool = False, restart_on_error: bool = True, restart_on_close: bool = True
    ) -> DataAsync:
        """Create a DataAsync object that provides a lazy WebSocket stream of
        real-time updates for this Site (async).

        Args:
            as_dict (bool): Controls the type of yielded items.
                - False (default): each item is converted to a Site instance.
                - True: each item is returned as a raw dict payload.
            restart_on_error (bool): If True (default), the returned DataAsync instance will automatically
                attempt to reconnect to the server if an error occurs.
            restart_on_close (bool): If True (default), the DataAsync instance will also recreate the underlying
                async generator when the stream completes normally (e.g., clean WebSocket close) and continue streaming.

        Notes:
            - data.data_fetcher is a lazy asynchronous generator. Each async iteration awaits the next update.
            - Persist the stream with data.asave(path: str, overwrite: bool = False).

        Payload schema:
            Each message is a JSON object that reflects the current state of this Site and
            includes some additional properties obtained from related entities. The payload contains
            fields such as:
              - id (int)
              - name (str)
              - date_created (str, ISO 8601)
              - latitude (float | None)
              - longitude (float | None)
              - lease_name (str)
              - operator_name (str)
              - metadata (object): first_timestamp/last_timestamp (str | None, ISO 8601)
              - well_api (str | None)
              - pad_id (int)
              - is_demo_site (bool)
              - stage_total (int | None)
              - timezone (str | None), e.g. "US/Central"
              - current_stage_number (int | None)
              - current_state (str | None), e.g. "frac_frac"
              - current_data_sources (object): frac, wl, fluid, pumpdown (lists)

        Returns:
            DataAsync: A DataAsync object whose data_fetcher yields items as Site instances or dicts depending on as_dict when asynchronously iterated over.
        """
        return await super().aget_realtime_instance_updates(
            as_dict=as_dict, restart_on_error=restart_on_error, restart_on_close=restart_on_close
        )
