from datetime import datetime

from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.constants import ENDPOINTS


class Pad(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.pads_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.pads_get_many.value

    id: str
    name: str
    lease_name: str
    customer_name: str
    simops_config: dict
    completion_date: datetime
    wireline_enabled: bool
