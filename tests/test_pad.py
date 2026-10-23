import asyncio

import pytest

from restreamsolutions import Pad, Site, State
from restreamsolutions.constants import StageNameFilters
from restreamsolutions.data_changes import DataChanges
from restreamsolutions.exceptions import APICompatibilityError
from tests.fake_api.server import API_PREFIX, WS_PREFIX, load_fixture
from tests.utils import check_instance_types

PAD_ID = 697
PADS_PATH = f'{API_PREFIX}/pads/'
PAD_PATH = f'{PADS_PATH}{PAD_ID}/'
SITES_PATH = f'{API_PREFIX}/sites/'
PAD_WS_PATH = f'{WS_PREFIX}/pads/{PAD_ID}/'


def run(coroutine):
    """Run a coroutine to completion in a new event loop.

    Args:
        coroutine: The coroutine to run.

    Returns:
        The result of the coroutine.
    """
    return asyncio.run(coroutine)


def test_get_models_with_filters(api):
    """Pad filters are sent as query parameters, and pads are returned as models or dicts, both sync and async."""
    pads = Pad.get_models(
        complete=False, well_api='42', site_pk=1112, name_search='Ar', fleet_id=5, fleet_name='Alpha', custom='1'
    )

    assert api.last('GET', PADS_PATH).query == {
        'complete': ['false'],
        'well_api': ['42'],
        'site_pk': ['1112'],
        'name_search': ['Ar'],
        'fleet_id': ['5'],
        'fleet_name': ['Alpha'],
        'custom': ['1'],
    }
    payload = load_fixture('pad/pad_many.json')
    assert len(pads) == len(payload)
    assert all(isinstance(pad, Pad) for pad in pads)
    check_instance_types(pads[0])
    assert pads[0].wireline_enabled is payload[0]['wireline_enabled']
    assert run(Pad.aget_models(as_dict=True)) == payload
    assert api.last('GET', PADS_PATH).query == {}

    pad = Pad.get_model(id=PAD_ID)
    assert pad.id == PAD_ID and isinstance(pad.simops_config, dict)
    check_instance_types(pad)
    assert run(Pad.aget_model(id=PAD_ID, as_dict=True))['id'] == PAD_ID


def test_get_sites_and_states(api):
    """Sites and states of a pad are requested with the pad id filter, which callers cannot override."""
    pad = Pad(id=PAD_ID)

    sites = pad.get_sites(complete=True, latitude=32.0, longitude=-102.0, radius_miles=5, fleet_name='Alpha')
    assert api.last('GET', SITES_PATH).query == {
        'pad__id': [str(PAD_ID)],
        'complete': ['true'],
        'lat': ['32.0'],
        'long': ['-102.0'],
        'radius_miles': ['5'],
        'fleet_name': ['Alpha'],
    }
    assert sites and all(isinstance(site, Site) for site in sites)
    check_instance_types(sites[0])
    with pytest.raises(ValueError):
        pad.get_sites(latitude=1.0)
    # The pad id filter is set by the pad itself
    with pytest.raises(TypeError, match=r'Pad\.get_sites\(\) does not accept pad__id'):
        pad.get_sites(pad__id=1)
    with pytest.raises(TypeError, match=r'Pad\.aget_sites\(\) does not accept pad__id'):
        run(pad.aget_sites(pad__id=1))
    assert isinstance(run(pad.aget_sites(as_dict=True, name_search='Leto'))[0], dict)
    assert api.last('GET', SITES_PATH).query == {'pad__id': [str(PAD_ID)], 'name_search': ['Leto']}

    states = pad.get_states(stage_name_filter=StageNameFilters.FRAC)
    assert api.last('GET', f'{API_PREFIX}/states/').query == {
        'pad__id': [str(PAD_ID)],
        'current_state_search': ['frac'],
    }
    assert states and all(isinstance(state, State) for state in states)
    check_instance_types(states[0])
    assert isinstance(run(pad.aget_states(as_dict=True))[0], dict)
    assert api.last('GET', f'{API_PREFIX}/states/').query == {'pad__id': [str(PAD_ID)]}


def test_measurement_sources(api):
    """Measurement sources come from the pad simops config, which is loaded from the API only when missing."""
    payload = load_fixture('pad/pad_one.json')

    # The pad is refreshed from the API when simops_config is not loaded yet
    pad = Pad(id=PAD_ID)
    assert pad.get_measurement_sources_metadata() == payload['simops_config']['measurement_sources']
    assert api.count('GET', PAD_PATH) == 1
    assert pad.get_measurement_sources_metadata() == payload['simops_config']['measurement_sources']
    assert api.count('GET', PAD_PATH) == 1
    assert run(Pad(id=PAD_ID).aget_measurement_sources_metadata()) == payload['simops_config']['measurement_sources']

    assert Pad(id=PAD_ID, simops_config=None).get_measurement_sources_metadata() == {}
    with pytest.raises(APICompatibilityError):
        Pad(id=PAD_ID, simops_config={'other': 1}).get_measurement_sources_metadata()


def test_get_data_with_pad_specific_parameters(api):
    """Pad data requests send measurement source and routing parameters, and stage filters require routed data."""
    pad = Pad(id=PAD_ID)
    payload = load_fixture('pad/data_pad.json')

    data = pad.get_data(
        fields=['a', 'b'],
        si_units=True,
        measurement_sources_names='M1',
        is_routed=True,
        stage_number=1,
        stage_name_filter=StageNameFilters.FRAC,
    )
    assert list(data.data_fetcher) == payload
    assert api.last('GET', f'{PAD_PATH}data/').query == {
        'si_units': ['true'],
        'resolution': ['raw'],
        'fields': ['a,b'],
        'stage_number': ['1'],
        'state_imatch': ['frac'],
        'measurement_source': ['M1'],
        'routed': ['true'],
    }

    async def async_flow():
        """Stream the pad data asynchronously and check the measurement source and routed query parameters."""
        data = await pad.aget_data(measurement_sources_names=['M1', 'M2'])
        assert [item async for item in data.data_fetcher] == payload
        assert api.last('GET', f'{PAD_PATH}data/').query['measurement_source'] == ['M1,M2']
        assert api.last('GET', f'{PAD_PATH}data/').query['routed'] == ['false']

    run(async_flow())

    # Querying by stage number requires routed data
    with pytest.raises(ValueError):
        pad.get_data(stage_number=1, stage_name_filter=StageNameFilters.FRAC)
    with pytest.raises(ValueError):
        run(pad.aget_data(stage_number=1, stage_name_filter=StageNameFilters.FRAC))
    with pytest.raises(ValueError):
        pad.get_realtime_measurements_data(stage_number=1, stage_name_filter=StageNameFilters.FRAC)
    with pytest.raises(ValueError):
        run(pad.aget_realtime_measurements_data(stage_number=1, stage_name_filter=StageNameFilters.FRAC))


def test_data_changes_across_sites(api):
    """Pad data changes stream the merged intervals of every affected site and are acknowledged per site."""
    pad = Pad(id=PAD_ID)
    fixture = load_fixture('pad/data_changes_pad_many.json')['change_log']
    intervals = DataChanges._group_and_merge_intervals_by_site(fixture)
    assert len(intervals) > 1, 'the fixture must cover several sites'
    for site_id in intervals:
        api.stream_items[f'{SITES_PATH}{site_id}/data/'] = [{'timestamp': 1, 'site_id': site_id}]

    changes, combined = pad.get_data_changes(as_dict=True)
    assert changes == fixture
    items = list(combined.data_fetcher)
    assert [item['site_id'] for item in items] == [site_id for site_id in sorted(intervals) for _ in intervals[site_id]]

    async def async_flow():
        """Fetch the pad data changes asynchronously, stream the combined data and acknowledge the first change."""
        changes, combined = await pad.aget_data_changes()
        assert all(isinstance(change, DataChanges) for change in changes)
        assert len([item async for item in combined.data_fetcher]) == len(items)
        # A change event is acknowledged through the endpoint of the site it belongs to
        assert await changes[0].aconfirm_data_received() is True
        assert api.last('POST', f'{SITES_PATH}{changes[0].site}/data_changes/').body == {
            'change_log': [{'id': changes[0].id, 'update_received': True}]
        }

    run(async_flow())


def test_realtime_updates(api):
    """Pad realtime instance, data change and measurement streams deliver the scripted websocket messages."""
    pad = Pad(id=PAD_ID, auth_token=None)
    pad_messages = [{'id': PAD_ID, 'name': 'A', 'wireline_enabled': True}, {'id': PAD_ID, 'name': 'B'}]
    change_messages = [{'id': 7, 'site': 1, 'start_date': '2020-01-01T00:00:00Z', 'end_date': '2020-01-01T01:00:00Z'}]
    api.ws_scripts[PAD_WS_PATH] = pad_messages
    api.ws_scripts[f'{PAD_WS_PATH}data_changes/'] = change_messages
    api.ws_scripts[f'{PAD_WS_PATH}data/'] = [{'timestamp': 1, 'site_id': 1}]

    updates = list(pad.get_realtime_instance_updates(restart_on_error=False, restart_on_close=False).data_fetcher)
    assert [update.name for update in updates] == ['A', 'B'] and all(isinstance(u, Pad) for u in updates)
    changes = list(pad.get_realtime_data_changes_updates(restart_on_error=False, restart_on_close=False).data_fetcher)
    assert len(changes) == 1 and isinstance(changes[0], DataChanges) and changes[0].site == 1

    data, key = pad.get_realtime_measurements_data(
        session_key='pad-key', restart_on_error=False, restart_on_close=False, is_routed=True
    )
    assert list(data.data_fetcher) == [{'timestamp': 1, 'site_id': 1}]
    assert api.websockets[-1].param('session_key') == 'pad-key'
    assert api.websockets[-1].param('routed') == 'true'

    async def async_flow():
        """Consume the async realtime streams of the pad and compare them with the scripted messages."""
        stream = await pad.aget_realtime_instance_updates(as_dict=True, restart_on_error=False, restart_on_close=False)
        assert [item async for item in stream.data_fetcher] == pad_messages
        stream = await pad.aget_realtime_data_changes_updates(
            as_dict=True, restart_on_error=False, restart_on_close=False
        )
        assert [item async for item in stream.data_fetcher] == change_messages
        data, _ = await pad.aget_realtime_measurements_data(restart_on_error=False, restart_on_close=False)
        assert [item async for item in data.data_fetcher] == [{'timestamp': 1, 'site_id': 1}]

    run(async_flow())
    assert len(api.websockets) == 6
