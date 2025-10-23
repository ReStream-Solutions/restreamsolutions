from datetime import datetime
from typing import Any

from restreamsolutions import StageNameFilters
from restreamsolutions.base_interface import BaseInterface
from restreamsolutions.constants import ENDPOINTS


class State(BaseInterface):
    """Represents the current configuration state for a Site (well) on a Pad.

    This model is returned by Pad.get_states()/aget_states() and Site.get_state()/aget_state()
    and describes which stage a site is in right now, together with supporting
    metadata used by state calculation. The class provides synchronous and asynchronous methods for each use case.

    Typical usage examples:
      - Fetch current states for all sites on a pad: Pad.get_states()
      - Fetch the current state for a specific site: Site.get_state()
      - Filter states by stage name pattern via StageNameFilters.

    Attributes (populated from the API):
      - site (dict): Minimal representation of the owning Site.
      - site_component_configuration (dict): Site-level configuration that feeds state calculation.
      - truth_table (dict): Rules and computed data used to derive the current_state.
      - current_state (str): Human-readable current state/stage name.
      - actual_stage_number (int): Actual stage number.
      - completed_stage_number (int): Completed stage number.
      - calculated_stage_number (int): Calculated stage number.
      - site_name (str): Convenience copy of the site name.
      - date_created (datetime): When this State object was first created.
      - last_updated (datetime): When this State object was last updated by the system.
      - last_state_update (datetime): Datetime of the last state transition that affected current_state.
      - truth_table_calculating (bool): Deprecated; kept for backward compatibility.
      - state_change_primary_field_values (dict): The set of primary configuration field values whose changes
        caused the current state change.
      - last_state_update_system_time (datetime): System time of the last state update recorded by the backend.
      - last_state_confirmation_time (datetime): Timestamp of the most recent confirmation of the current state's validity.
      - previous_state (int): ID of the previous stage history object.
    """

    _api_url_single_object: str = ENDPOINTS.states_one.value
    _api_url_multiple_objects: str = ENDPOINTS.states_many.value

    # These type hints are used by the BaseInterface class to perform automatic type conversion
    # when a new instance is created.
    site: dict
    site_component_configuration: dict
    truth_table: dict
    current_state: str
    actual_stage_number: int
    completed_stage_number: int
    calculated_stage_number: int
    site_name: str
    date_created: datetime
    last_updated: datetime
    last_state_update: datetime
    truth_table_calculating: bool
    state_change_primary_field_values: dict
    last_state_update_system_time: datetime
    last_state_confirmation_time: datetime
    previous_state: int

    @classmethod
    def get_models(
        cls, stage_name_filter: StageNameFilters = None, auth_token: str = None, as_dict=False, **filters
    ) -> list['State'] | list[dict[str, Any]]:
        """Retrieve State models for a site or for a pad.


        Parameters:
          - stage_name_filter (StageNameFilters | None): When provided, restricts results to
            states whose current_state matches the given stage name pattern (e.g., frac, wireline).
          - auth_token (str | None): Explicit auth token. If None, the SDK will attempt to use
            RESTREAM_AUTH_TOKEN environment variable.
          - as_dict (bool): If True, return a list of plain dictionaries instead of State instances.
          - **filters: Additional server-side filters (e.g., pad__id, site__id) passed through
            to the HTTP request and supported by the API.

        Returns:
          - list[State] | list[dict[str, Any]]: A list of State objects, or dicts if as_dict=True.

        Raises:
          - AuthError: If authentication fails.
          - APICompatibilityError: If the endpoint is not available or the response format is not supported.
          - APIConcurrencyLimitError: If the API rate limit is reached.
          - HTTPError: For other non-2xx HTTP responses.
        """

        if stage_name_filter is not None:
            filters = {**filters, 'current_state_search': stage_name_filter.value}
        return super().get_models(auth_token=auth_token, as_dict=as_dict, **filters)

    @classmethod
    async def aget_models(
        cls, stage_name_filter: StageNameFilters = None, auth_token: str = None, as_dict=False, **filters
    ) -> list['State'] | list[dict[str, Any]]:
        """Asynchronously retrieve State models for a site or for a pad.


        Parameters:
          - stage_name_filter (StageNameFilters | None): When provided, restricts results to
            states whose current_state matches the given stage name pattern (e.g., frac, wireline).
          - auth_token (str | None): Explicit auth token. If None, the SDK will attempt to use
            RESTREAM_AUTH_TOKEN environment variable.
          - as_dict (bool): If True, return a list of plain dictionaries instead of State instances.
          - **filters: Additional server-side filters (e.g., pad__id, site__id) passed through
            to the HTTP request and supported by the API.

        Returns:
          - list[State] | list[dict[str, Any]]: A list of State objects, or dicts if as_dict=True.

        Raises:
          - AuthError: If authentication fails.
          - APICompatibilityError: If the endpoint is not available or the response format is not supported.
          - APIConcurrencyLimitError: If the API rate limit is reached.
          - HTTPError: For other non-2xx HTTP responses.
        """

        if stage_name_filter is not None:
            filters = {**filters, 'current_state_search': stage_name_filter.value}
        return await super().aget_models(auth_token=auth_token, as_dict=as_dict, **filters)
