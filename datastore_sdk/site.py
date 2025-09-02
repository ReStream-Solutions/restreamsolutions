from datetime import datetime
from typing import Optional

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

    def get_state(self, as_dict: bool = False) -> Optional['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        states = State.get_models(auth_token=auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    async def aget_state(self, as_dict: bool = False) -> Optional['State']:
        from .state import State
        auth_token = self._auth_token if self._auth_token else None
        states = await State.aget_models(auth_token=auth_token, as_dict=as_dict, site__id=self.id)
        if not states:
            return None
        return states[0]

    def get_pad(self, as_dict: bool = False) -> Optional['Pad']:
        from .pad import Pad
        if not hasattr(self, 'pad_id'):
            self.update()
        if getattr(self, 'pad_id') is None:
            return None
        auth_token = self._auth_token if self._auth_token else None
        return Pad.get_model(id=self.pad_id, auth_token=auth_token, as_dict=as_dict)

    async def aget_pad(self, as_dict: bool = False) -> Optional['Pad']:
        from .pad import Pad
        if not hasattr(self, 'pad_id'):
            await self.aupdate()
        if getattr(self, 'pad_id') is None:
            return None
        auth_token = self._auth_token if self._auth_token else None
        return await Pad.aget_model(id=self.pad_id, auth_token=auth_token, as_dict=as_dict)