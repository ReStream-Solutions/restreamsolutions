import warnings
from datetime import datetime
from typing import Any
from dateutil import parser

from datastore_sdk.base_interface import BaseInterface
from datastore_sdk.communicator import Communicator
from datastore_sdk.constants import ENDPOINTS
from datastore_sdk.exceptions import APICompatibilityError
from .data_object import Data


class DataChanges(BaseInterface):
    _api_url_single_object: str = ENDPOINTS.data_changes_site_one.value
    _api_url_multiple_objects: str = ENDPOINTS.data_changes_site_many.value

    created_at: datetime
    modification_type: str
    modification_subtype: str
    start_date: datetime
    end_date: datetime
    site: int
    update_received: bool

    def __init__(self, *args, **kwargs) -> None:
        self.update_received = False
        super().__init__(*args, **kwargs)

    @classmethod
    def get_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    @classmethod
    def get_models(cls, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    @classmethod
    async def aget_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    @classmethod
    async def aget_models(cls, auth_token: str = None, as_dict=False, **filters):
        raise NotImplementedError()

    def _select_token_and_url(self) -> tuple[str, str]:
        current_auth_token = self._select_token(self._auth_token)
        url = self._format_url(self._api_url_multiple_objects, parent_id=self.site)
        return current_auth_token, url

    def _update_self_state(self, response_data: dict[str, Any]) -> None:
        if (not isinstance(response_data, dict) or 'change_log' not in response_data
            or not isinstance(response_data['change_log'], list)
        ):
            raise APICompatibilityError(
                f'change_log must be a list of dicts, but received {response_data["change_log"]}'
            )

        new_instance_dict = {}
        for change_log in response_data['change_log']:
            if change_log['id'] == self.id:
                new_instance_dict = change_log
        if not new_instance_dict:
            self.update_received = True
            return

        for key, value in new_instance_dict.items():
            setattr(self, key, self._try_convert_value(key, value))
        self.update_received = False

    def update(self):
        current_auth_token, url = self._select_token_and_url()
        response_data = Communicator.send_get_request(url, current_auth_token)
        self._update_self_state(response_data)

    async def aupdate(self):
        current_auth_token, url = self._select_token_and_url()
        response_data = await Communicator.send_get_request_async(url, current_auth_token)
        self._update_self_state(response_data)

    def get_data(self) -> 'Data':
        from .site import Site
        site = Site(self.site)
        return site.get_data(start_datetime=self.start_date, end_datetime=self.end_date)

    async def aget_data(self) -> 'DataAsync':
        from .site import Site
        site = Site(self.site)
        return await site.aget_data(start_datetime=self.start_date, end_datetime=self.end_date)

    def _create_confirm_data_received_payload(self):
        return {
            "change_log": [
                {
                    "id": self.id,
                    "update_received": True
                }
            ]
        }

    def confirm_data_received(self) -> bool:
        if self.update_received:
            warnings.warn(f"{str(self)} - confirmation has already been received.")
            return True
        payload = self._create_confirm_data_received_payload()
        current_auth_token, url = self._select_token_and_url()
        response_data = Communicator.send_post_request(url, current_auth_token, payload)
        self._update_self_state(response_data)
        return self.update_received

    async def aconfirm_data_received(self) -> bool:
        if self.update_received:
            warnings.warn(f"{str(self)} - confirmation has already been received.")
            return True
        payload = self._create_confirm_data_received_payload()
        current_auth_token, url = self._select_token_and_url()
        response_data = await Communicator.send_post_request_async(url, current_auth_token, payload)
        self._update_self_state(response_data)
        return self.update_received

    @staticmethod
    def _group_and_merge_intervals_by_site(raw_changes: list[dict[str, Any]]) -> dict[int, list[tuple[datetime, datetime]]]:
        intervals_by_site: dict[int, list[tuple[datetime, datetime]]] = {}
        for item in raw_changes:
            try:
                site_id = int(item['site'])
                start_dt = parser.parse(str(item['start_date']))
                end_dt = parser.parse(str(item['end_date']))
            except Exception:
                raise APICompatibilityError("Wrong data changes format.")
            if site_id not in intervals_by_site:
                intervals_by_site[site_id] = []
            intervals_by_site[site_id].append((start_dt, end_dt))

        for site_id, intervals in intervals_by_site.items():
            if not intervals:
                continue
            intervals.sort(key=lambda x: x[0])
            merged: list[tuple[datetime, datetime]] = []
            cur_start, cur_end = intervals[0]
            for s, e in intervals[1:]:
                if s <= cur_end:
                    cur_end = max(cur_end, e)
                else:
                    merged.append((cur_start, cur_end))
                    cur_start, cur_end = s, e
            merged.append((cur_start, cur_end))
            intervals_by_site[site_id] = merged
        return intervals_by_site

    @staticmethod
    def _build_combined_data_object(raw_changes: list[dict[str, Any]], auth_token: str) -> 'Data':
        intervals_by_site = DataChanges._group_and_merge_intervals_by_site(raw_changes)

        def data_generator_factory():
            from .site import Site
            for site_id in sorted(intervals_by_site.keys()):
                site = Site(id=site_id, auth_token=auth_token)
                for start_dt, end_dt in intervals_by_site[site_id]:
                    data_obj = site.get_data(start_datetime=start_dt, end_datetime=end_dt)
                    for item in data_obj.data_fetcher:
                        yield item

        return Data(data_generator_factory)

    @staticmethod
    def _build_combined_data_async_object(raw_changes: list[dict[str, Any]], auth_token: str) -> 'DataAsync':
        from .data_object import DataAsync
        intervals_by_site = DataChanges._group_and_merge_intervals_by_site(raw_changes)

        async def data_generator_factory():
            from .site import Site
            for site_id in sorted(intervals_by_site.keys()):
                site = Site(id=site_id, auth_token=auth_token)
                for start_dt, end_dt in intervals_by_site[site_id]:
                    data_obj = await site.aget_data(start_datetime=start_dt, end_datetime=end_dt)
                    async for item in data_obj.data_fetcher:
                        yield item

        return DataAsync(data_generator_factory)