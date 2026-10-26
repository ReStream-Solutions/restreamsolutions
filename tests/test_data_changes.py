from datetime import datetime, timezone

import pytest

from restreamsolutions.data_changes import DataChanges
from restreamsolutions.exceptions import APICompatibilityError


def _change(site: int, start: str, end: str, change_id: int = 1) -> dict:
    """Build a raw change event as returned by the API.

    Args:
        site: Id of the site the change belongs to.
        start: Start of the changed interval as an ISO 8601 string.
        end: End of the changed interval as an ISO 8601 string.
        change_id: Id of the change event.

    Returns:
        A dict with the change event fields.
    """
    return {
        'id': change_id,
        'site': site,
        'start_date': start,
        'end_date': end,
        'modification_type': 'transaction',
        'modification_subtype': 'add',
        'created_at': '2025-01-01T00:00:00Z',
    }


def test_intervals_are_grouped_by_site_and_merged():
    """Overlapping and touching intervals are merged per site, and malformed dates raise APICompatibilityError."""
    raw_changes = [
        _change(1, '2025-01-01T00:00:00Z', '2025-01-01T02:00:00Z'),
        _change(1, '2025-01-01T01:00:00Z', '2025-01-01T03:00:00Z'),
        _change(1, '2025-01-01T05:00:00Z', '2025-01-01T06:00:00Z'),
        _change(2, '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z'),
        _change(1, '2025-01-01T03:00:00Z', '2025-01-01T04:00:00Z'),
    ]

    merged = DataChanges._group_and_merge_intervals_by_site(raw_changes)

    utc = lambda h: datetime(2025, 1, 1, h, tzinfo=timezone.utc)
    assert merged == {
        1: [(utc(0), utc(4)), (utc(5), utc(6))],
        2: [(utc(0), utc(1))],
    }
    assert DataChanges._group_and_merge_intervals_by_site([]) == {}
    with pytest.raises(APICompatibilityError):
        DataChanges._group_and_merge_intervals_by_site([{'site': 1, 'start_date': 'bad'}])


def test_instance_state_follows_the_server_response():
    """A change event refreshes its fields while it is pending and is marked as received once the server drops it."""
    change = DataChanges(auth_token='tok', **_change(5, '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z', change_id=7))
    assert change.update_received is False
    assert change.start_date == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert change.site == 5

    # The event is still pending: fields are refreshed from the response
    change._update_self_state({'change_log': [_change(5, '2025-01-01T00:00:00Z', '2025-01-01T02:00:00Z', change_id=7)]})
    assert change.end_date == datetime(2025, 1, 1, 2, tzinfo=timezone.utc)
    assert change.update_received is False

    # The event is absent from the response: it has been acknowledged
    change._update_self_state({'change_log': []})
    assert change.update_received is True

    with pytest.raises(APICompatibilityError):
        change._update_self_state({'unexpected': 1})
    with pytest.raises(NotImplementedError):
        DataChanges.get_models()
    assert change._create_confirm_data_received_payload() == {'change_log': [{'id': 7, 'update_received': True}]}
