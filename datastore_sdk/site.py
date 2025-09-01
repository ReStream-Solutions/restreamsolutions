from datetime import datetime

from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.constants import ENDPOINTS


class Site(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.sites_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.sites_get_many.value

    name: str
    date_created: datetime
    latitude: int
    longitude: int
    lease_name: str
    operator_name: str
    crew: str
    division: str
    metadata: dict
    well_api: str
    pad_id: int
    is_demo_site: bool
    stage_total: int
    timezone: str