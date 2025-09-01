from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.constants import ENDPOINTS


class Site(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.sites_get_one.value
    _api_url_multiple_objects: str = ENDPOINTS.sites_get_many.value