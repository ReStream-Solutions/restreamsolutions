import warnings
from datetime import datetime
from typing import Any
from dateutil import parser

from restreamsolutions.base_interface import BaseInterface
from restreamsolutions.communicator import Communicator
from restreamsolutions.constants import ENDPOINTS
from restreamsolutions.exceptions import APICompatibilityError
from .data_object import Data, DataAsync


class DataChanges(BaseInterface):
    """
    Represents a data-change event for a specific site within a time interval
    and provides utilities to retrieve the affected data and confirm receipt.

    Instances carry the site ID, the affected time window [start_date, end_date],
    the modification type and subtype, and the update_received flag indicating
    whether this change was confirmed by a user after fetching it from the API.

    The class exposes synchronous and asynchronous methods to fetch the
    corresponding data for the interval and to acknowledge the event back to the
    API. After acknowledgment (confirm_data_received), the event will no longer
    appear in API responses, and update_received for the instance will be set to
    True.
    """

    _api_url_single_object: str = ENDPOINTS.data_changes_site_one.value
    _api_url_multiple_objects: str = ENDPOINTS.data_changes_site_many.value

    # These type hints are used by the BaseInterface class to perform automatic type conversion
    # when a new instance is created.
    created_at: datetime
    modification_type: str
    modification_subtype: str
    start_date: datetime
    end_date: datetime
    site: int
    update_received: bool

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the DataChanges instance.

        Sets the update_received flag to False indicating that the change event hasn't been confirmed by a user and
        the related data hasn't been fetched yet.
        The rest of the initialization is delegated to the BaseInterface.
        """
        self.update_received = False
        super().__init__(*args, **kwargs)

    @classmethod
    def get_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        """Not supported for DataChanges.

        Instances of the DataChanges class are delivered only by relevant methods
        of the Pad and Site classes. Users should not import this class and call
        its static/class methods directly; only instance methods are available.
        """
        raise NotImplementedError()

    @classmethod
    def get_models(cls, auth_token: str = None, as_dict=False, **filters):
        """Not supported for DataChanges.

        Instances of the DataChanges class are delivered only by relevant methods
        of the Pad and Site classes. Users should not import this class and call
        its static/class methods directly; only instance methods are available.
        """
        raise NotImplementedError()

    @classmethod
    async def aget_model(cls, id: int = None, auth_token: str = None, as_dict=False, **filters):
        """Not supported for DataChanges.

        Instances of the DataChanges class are delivered only by relevant methods
        of the Pad and Site classes. Users should not import this class and call
        its static/class methods directly; only instance methods are available.
        """
        raise NotImplementedError()

    @classmethod
    async def aget_models(cls, auth_token: str = None, as_dict=False, **filters):
        """Not supported for DataChanges.

        Instances of the DataChanges class are delivered only by relevant methods
        of the Pad and Site classes. Users should not import this class and call
        its static/class methods directly; only instance methods are available.
        """
        raise NotImplementedError()

    def _build_url(self) -> str:
        """Builds an endpoint URL for this instance."""
        url = self._format_url(self._api_url_multiple_objects, parent_id=self.site)
        return url

    def _update_self_state(self, response_data: dict[str, Any]) -> None:
        """Update internal fields from a server response payload.

        Expects a dictionary with a "change_log" list of dicts. It finds an
        entry matching this instance's id, applies field values with type
        conversion, and updates update_received accordingly. If no matching entry
        is found, it means the change‑log event has already been confirmed.

        Raises:
            APICompatibilityError: If the response structure is incompatible
            with the expected schema.
        """
        if (
            not isinstance(response_data, dict)
            or 'change_log' not in response_data
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
        """Fetch the latest data-change event for this instance and apply it.

        Performs a synchronous GET request and updates fields and flags based on
        the server response. Sets update_received to True if the data-change
        event associated with the instance has already been confirmed.
        """
        url = self._build_url()
        response_data = Communicator.send_get_request(url, self._auth_token)
        self._update_self_state(response_data)

    async def aupdate(self):
        """Asynchronously fetch the latest data-change event for this instance and apply it.

        Performs an asynchronous GET request and updates fields and flags based on
        the server response. Sets update_received to True if the data-change
        event associated with the instance has already been confirmed.
        """
        url = self._build_url()
        response_data = await Communicator.send_get_request_async(url, self._auth_token)
        self._update_self_state(response_data)

    def get_data(self) -> 'Data':
        """Retrieve data for the affected site and interval (synchronous).

        WARNING: Retrieving data for every separate change-log event is suboptimal
        because events may overlap, causing duplicated data retrieval. Use this
        only if you need data for one or a few change events. If you need data
        for all site/pad change events at once, use the Data object returned by
        the get_data_changes method of a Site/Pad instance.

        Returns:
            Data: A Data object whose iterator yields items for the specified
            [start_date, end_date] interval for this instance's site.
        """
        from .site import Site

        site = Site(self.site, auth_token=self._auth_token)
        return site.get_data(start_datetime=self.start_date, end_datetime=self.end_date)

    async def aget_data(self) -> 'DataAsync':
        """Retrieve data for the affected site and interval (asynchronous).

        WARNING: Retrieving data for every separate change-log event is suboptimal
        because events may overlap, causing duplicated data retrieval. Use this
        only if you need data for one or a few change events. If you need data
        for all site/pad change events at once, use the Data object returned by
        the aget_data_changes method of a Site/Pad instance.

        Returns:
            DataAsync: A DataAsync object whose iterator yields items for the
            specified [start_date, end_date] interval for this instance's site.
        """
        from .site import Site

        site = Site(self.site, auth_token=self._auth_token)
        return await site.aget_data(start_datetime=self.start_date, end_datetime=self.end_date)

    def _create_confirm_data_received_payload(self):
        """Build payload to acknowledge the receipt of this change event.

        Returns:
            dict: Payload with change_log entry marking update_received=True for
            this instance's id.
        """
        return {"change_log": [{"id": self.id, "update_received": True}]}

    def confirm_data_received(self) -> bool:
        """Confirm to the API that this change event was processed.
        Sets update_received on the instance to True on success.

        WARNING: After this call, the associated data-change event will no longer
        appear in future API responses.

        Returns:
            bool: True if the server acknowledges the confirmation (update_received
            becomes True), False otherwise.
        """
        if self.update_received:
            warnings.warn(f"{str(self)} - confirmation has already been received.")
            return True
        payload = self._create_confirm_data_received_payload()
        url = self._build_url()
        response_data = Communicator.send_post_request(url, payload, self._auth_token)
        self._update_self_state(response_data)
        return self.update_received

    async def aconfirm_data_received(self) -> bool:
        """Asynchronously confirm to the API that this change event was processed.
        Sets update_received on the instance to True on success.

        WARNING: After this call, the associated data-change event will no longer
        appear in future API responses.

        Returns:
            bool: True if the server acknowledges the confirmation (update_received
            becomes True), False otherwise.
        """
        if self.update_received:
            warnings.warn(f"{str(self)} - confirmation has already been received.")
            return True
        payload = self._create_confirm_data_received_payload()
        url = self._build_url()
        response_data = await Communicator.send_post_request_async(url, payload, self._auth_token)
        self._update_self_state(response_data)
        return self.update_received

    @staticmethod
    def _group_and_merge_intervals_by_site(
        raw_changes: list[dict[str, Any]],
    ) -> dict[int, list[tuple[datetime, datetime]]]:
        """Group change events entries by site and merge overlapping start_date - end_date intervals.
        This helps to avoid duplicated data retrieval.

        Args:
            raw_changes: List of data change events as dicts with keys: 'site', 'start_date', 'end_date'.

        Returns:
            dict[int, list[tuple[datetime, datetime]]]: Mapping of site ID to a
            list of non-overlapping merged (start, end) datetime intervals.

        Raises:
            APICompatibilityError: If any item has an unexpected format.
        """
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
        """Create a single Data object that iterates over merged intervals by site.

        This utility groups and merges intervals per site, then lazily fetches
        data for each interval and site and yields items through a single Data wrapper.

        Args:
            raw_changes: Raw change entries from the API change log.
            auth_token: Optional auth token for downstream data requests; if not provided,
            RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET environment variable will be used to create it.

        Returns:
            Data: A lazy iterable over all affected data across sites/intervals.
        """
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
        """Create a single DataAsync object that iterates over merged intervals by site.

        This utility groups and merges intervals per site, then lazily fetches
        data for each interval and site and yields items through a single DataAsync wrapper.

        Args:
            raw_changes: Raw change entries from the API change log.
            auth_token: Optional auth token for downstream data requests; if not provided,
            RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET environment variable will be used to create it.

        Returns:
            DataAsync: A lazy async iterable over all affected data across sites/intervals.
        """
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
