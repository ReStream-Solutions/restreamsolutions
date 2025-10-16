from datetime import datetime
from typing import Optional, Any

from datastore_sdk.base_pad_site import BasePadSite
from datastore_sdk.constants import ENDPOINTS, StageNameFilters, DataResolutions, DataAggregations, DataFillMethods
from datastore_sdk.data_object import Data, DataAsync
from datastore_sdk.exceptions import APICompatibilityError


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
      - latitude (int): Latitude in microdegrees.
      - longitude (int): Longitude in microdegrees.
      - lease_name (str): Lease name associated with the site.
      - operator_name (str): Operator name.
      - metadata (dict): Additional metadata dictionary.
      - well_api (str): Well API number if available.
      - pad_id (int): Identifier of the parent Pad.
      - is_demo_site (bool): Whether this is a demo site.
      - stage_total (int): Total number of stages for this site.
      - timezone (str): Timezone name for this site.
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

    # These type hints are used by the BaseInterface class to perform automatic type conversion
    # when a new instance is created.
    name: str
    date_created: datetime
    latitude: int
    longitude: int
    lease_name: str
    operator_name: str
    metadata: dict
    well_api: str
    pad_id: int
    is_demo_site: bool
    stage_total: int
    timezone: str

    def get_state(self, as_dict: bool = False) -> Optional['State'] | Optional[dict[str, Any]]:
        """Fetch the current State object for this site.

        Parameters:
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
        auth_token = self._auth_token if self._auth_token else None
        states = State.get_models(auth_token=auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    async def aget_state(self, as_dict: bool = False) -> Optional['State'] | Optional[dict[str, Any]]:
        """Asynchronously fetch the current State object for this site.

        Parameters:
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
        auth_token = self._auth_token if self._auth_token else None
        states = await State.aget_models(auth_token=auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    def get_pad(self, as_dict: bool = False) -> Optional['Pad'] | Optional[dict[str, Any]]:
        """Fetch the parent Pad object this site belongs to.

        Parameters:
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
        auth_token = self._auth_token if self._auth_token else None
        return Pad.get_model(id=self.pad_id, auth_token=auth_token, as_dict=as_dict)

    async def aget_pad(self, as_dict: bool = False) -> Optional['Pad'] | Optional[dict[str, Any]]:
        """Asynchronously fetch the parent Pad object this site belongs to.

        Parameters:
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
        auth_token = self._auth_token if self._auth_token else None
        return await Pad.aget_model(id=self.pad_id, auth_token=auth_token, as_dict=as_dict)

    def _extract_site_measurement_sources(self, pad_measurement_sources: dict) -> dict:
        """Internal helper to filter pad measurement_sources to only those attached to this site.

        Parameters:
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
            measurement_sources_names: str | list[str] = None,
            fill_data_method: DataFillMethods = None,
            fill_data_limit: int | None = None,
            inside_area_only: bool = True,
    ) -> Data:
        """Stream time-series data for this site or save it to a file.

        data.data_fetcher is a lazy synchronous generator. Iterate over it to get timestamped data
        items one by one. data.save(path: str, overwrite: bool = False) will save all the data to a JSON file.

        Parameters:
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields (str | list[str] | None): Optional fields filter. Use get_fields_metadata() to
                discover all available fields for this site.
            si_units (bool): If True, values are converted to SI units.
            measurement_sources_names (str | list[str] | None): Filter by measurement source names.
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
            measurement_sources_names=measurement_sources_names,
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
            fill_data_method: DataFillMethods = None,
            fill_data_limit: int | None = None,
            inside_area_only: bool = True,
    ) -> DataAsync:
        """Asynchronously stream time-series data for this site or save it to a file.

        data.data_fetcher is a lazy asynchronous generator. Iterate over it to get timestamped data items one by one.
        data.asave(path: str, overwrite: bool = False) will asynchronously save all the data to a JSON file.

        Parameters:
            start_datetime (datetime | None): Inclusive start; must be timezone-aware.
            end_datetime (datetime | None): Inclusive end; must be timezone-aware.
            stage_number (int | None): Optional stage number to filter by (requires stage_name_filter; see below).
            stage_name_filter (StageNameFilters | None): Filter for stage names (frac, wireline, etc.).
            resolution (DataResolutions): Sampling resolution of the output series (seconds, minutes, hours, etc.).
            aggregation (DataAggregations | None): Optional aggregation to apply.
            fields (str | list[str] | None): Optional fields filter. Use aget_fields_metadata() to
                discover all available fields for this site.
            si_units (bool): If True, values are converted to SI units.
            measurement_sources_names (str | list[str] | None): Filter by measurement source names.
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
            measurement_sources_names=measurement_sources_names,
            fill_data_method=fill_data_method,
            fill_data_limit=fill_data_limit,
            inside_area_only=inside_area_only,
        )

    def get_realtime_updates(self) -> Data:
        """Creates a Data class, containing a lazy WebSocket stream of real-time updates for this Site.

        data.data_fetcher is a lazy synchronous generator. Each iteration creates a blocking call, waiting for the next
        update.
        data.save(path: str, overwrite: bool = False) will save all the data to a JSON file.

        Each message is a JSON object that reflects the current state of this Site and
        includes some additional properties obtained from related entities.
        The payload contains the following fields:
          - id (int)
          - name (str)
          - date_created (str, ISO 8601)
          - latitude (float | None)
          - longitude (float | None)
          - lease_name (str)
          - operator_name (str)
          - metadata (object):
              - first_timestamp (str | None, ISO 8601)
              - last_timestamp (str | None, ISO 8601)
          - well_api (str | None)
          - pad_id (int)
          - is_demo_site (bool)
          - stage_total (int | None)
          - timezone (str | None), e.g. "US/Central"
          - current_stage_number (int | None)
          - current_state (str | None), e.g. "frac_frac"
          - current_data_sources (object):
              - frac (list)
              - wl (list)
              - fluid (list)
              - pumpdown (list)

        Returns:
            Data: A Data object whose data_fetcher yields update messages one by one.
        """
        return super().get_realtime_updates()

    async def aget_realtime_updates(self) -> DataAsync:
        """Creates a DataAsync class, containing a lazy WebSocket stream of real-time updates for this Site.

        data.data_fetcher is a lazy asynchronous generator. Each async iteration awaits the next update.
        data.asave(path: str, overwrite: bool = False) will asynchronously save all the data to a JSON file.

        Each message is a JSON object that reflects the current state of this Site and
        includes some additional properties obtained from related entities.
        The payload contains the following fields:
          - id (int)
          - name (str)
          - date_created (str, ISO 8601)
          - latitude (float | None)
          - longitude (float | None)
          - lease_name (str)
          - operator_name (str)
          - metadata (object):
              - first_timestamp (str | None, ISO 8601)
              - last_timestamp (str | None, ISO 8601)
          - well_api (str | None)
          - pad_id (int)
          - is_demo_site (bool)
          - stage_total (int | None)
          - timezone (str | None), e.g. "US/Central"
          - current_stage_number (int | None)
          - current_state (str | None), e.g. "frac_frac"
          - current_data_sources (object):
              - frac (list)
              - wl (list)
              - fluid (list)
              - pumpdown (list)

        Returns:
            DataAsync: A DataAsync object whose data_fetcher yields update messages one by one when asynchronously
            iterated over.
        """
        return await super().aget_realtime_updates()