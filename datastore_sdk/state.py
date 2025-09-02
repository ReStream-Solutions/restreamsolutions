from datetime import datetime

from datastore_sdk import StageNameFilter
from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.constants import ENDPOINTS


class State(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.states_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.states_get_many.value

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
    last_state_update:datetime
    truth_table_calculating: bool
    state_change_primary_field_values: dict
    last_state_update_system_time: datetime
    last_state_confirmation_time: datetime
    previous_state: int

    @classmethod
    def get_models(
            cls,
            stage_name_filter: StageNameFilter = None,
            auth_token: str = None,
            as_dict=False,
            **filters
    ) -> list['State']:

        if stage_name_filter is not None:
            filters = {**filters, 'current_state_search': stage_name_filter.value}
        return super().get_models(auth_token=auth_token, as_dict=as_dict, **filters)
