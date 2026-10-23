import asyncio
from copy import deepcopy
from datetime import datetime, timezone

import pandas as pd
import pytest

from restreamsolutions import Pad, Site, State
from restreamsolutions.constants import DataAggregations, DataFillMethods, DataResolutions, StageNameFilters
from restreamsolutions.data_changes import DataChanges
from restreamsolutions.exceptions import APICompatibilityError, BadRequestError, ForbiddenError, NotFoundError
from tests.fake_api.server import API_PREFIX, WS_PREFIX, load_fixture
from tests.utils import check_instance_types

SITE_ID = 1112
SITES_PATH = f'{API_PREFIX}/sites/'
SITE_PATH = f'{SITES_PATH}{SITE_ID}/'
SITE_DATA_PATH = f'{SITE_PATH}data/'
SITE_DATA_CHANGES_PATH = f'{SITE_PATH}data_changes/'
SITE_AGGREGATIONS_PATH = f'{SITE_PATH}parameters/'
SITE_WS_PATH = f'{WS_PREFIX}/sites/{SITE_ID}/'
NEW_AGGREGATION = [
    {'name': 'avg_rate', 'config': {'field': 'slurry_flow', 'aggregation': 'average'}, 'description': 'd'}
]


def run(coroutine):
    """Run a coroutine to completion in a new event loop.

    Args:
        coroutine: The coroutine to run.

    Returns:
        The result of the coroutine.
    """
    return asyncio.run(coroutine)


def test_get_models_with_filters(api):
    """Site filters are sent as query parameters, and incomplete radius filters raise ValueError without a request."""
    sites = Site.get_models(
        complete=True,
        pad_pk=697,
        well_api='42',
        name_search='Runner',
        formation_name='Wolf',
        pad_name='Med',
        latitude_gte=30.0,
        latitude_lte=33.0,
        longitude_gte=-103.0,
        longitude_lte=-100.0,
        latitude=32.0,
        longitude=-102.0,
        radius_miles=10,
        fleet_id=5,
        fleet_name='Alpha',
        extra='x',
    )

    assert api.last('GET', SITES_PATH).query == {
        'complete': ['true'],
        'pad_pk': ['697'],
        'well_api': ['42'],
        'name_search': ['Runner'],
        'formation_name': ['Wolf'],
        'pad_name': ['Med'],
        'latitude_gte': ['30.0'],
        'latitude_lte': ['33.0'],
        'longitude_gte': ['-103.0'],
        'longitude_lte': ['-100.0'],
        'lat': ['32.0'],
        'long': ['-102.0'],
        'radius_miles': ['10'],
        'fleet_id': ['5'],
        'fleet_name': ['Alpha'],
        'extra': ['x'],
    }
    payload = load_fixture('site/sites_many.json')
    assert len(sites) == len(payload)
    assert all(isinstance(site, Site) for site in sites)
    check_instance_types(sites[0])
    assert sites[0].latitude == payload[0]['latitude']
    assert isinstance(sites[0].latitude, float)
    assert sites[0].fleet_name == payload[0]['fleet_name']

    assert run(Site.aget_models(as_dict=True, fleet_id=5)) == payload
    assert api.last('GET', SITES_PATH).query == {'fleet_id': ['5']}

    # The raw lat and long names of the API are accepted as the center of the radius filter
    Site.get_models(lat=32.0, long=-102.0, radius_miles=10)
    assert api.last('GET', SITES_PATH).query == {'lat': ['32.0'], 'long': ['-102.0'], 'radius_miles': ['10']}

    # Raw server parameter names passed through **filters are not swallowed by the named parameters
    assert Site._build_site_filters(lat=1.0, long=2.0, radius_miles=3, complete=None) == {
        'lat': 1.0,
        'long': 2.0,
        'radius_miles': 3,
    }
    assert Site._build_site_filters(lat=1.0, latitude=2.0, longitude=3.0, radius_miles=1) == {
        'lat': 2.0,
        'long': 3.0,
        'radius_miles': 1,
    }
    incomplete_radius_filters = (
        {'latitude': 1.0},
        {'latitude': 1.0, 'longitude': 2.0},
        {'radius_miles': 3},
        {'lat': 1.0},
        {'lat': 1.0, 'long': 2.0},
        {'long': 2.0, 'radius_miles': 3},
    )
    for kwargs in incomplete_radius_filters:
        with pytest.raises(ValueError):
            Site.get_models(**kwargs)
        with pytest.raises(ValueError):
            run(Site.aget_models(**kwargs))
    assert api.count('GET', SITES_PATH) == 3


def test_crew_and_division_default_to_none():
    """The API omits crew and division when they are not assigned, so the attributes default to None."""
    site = Site(id=SITE_ID, auth_token='token', name='Well')
    assert site.crew is None
    assert site.division is None
    assert Site(id=SITE_ID, auth_token='token', crew='Crew A').crew == 'Crew A'


def test_get_model_update_and_errors(api):
    """Sites are fetched and refreshed by id, and missing or forbidden ids raise NotFoundError and ForbiddenError."""
    payload = load_fixture('site/site_one.json')

    site = Site.get_model(id=SITE_ID)
    assert site.id == SITE_ID
    assert site.name == payload['name']
    assert site.fleet_id == payload['fleet_id']
    assert isinstance(site.latitude, float) and site.latitude == payload['latitude']
    assert isinstance(site.date_created, datetime)
    check_instance_types(site)
    assert run(Site.aget_model(id=SITE_ID, as_dict=True)) == payload

    lazy_site = Site(id=SITE_ID)
    lazy_site.update()
    assert lazy_site.name == payload['name']
    run(lazy_site.aupdate())
    assert api.count('GET', SITE_PATH) == 4

    api.missing_ids.add(999)
    api.forbidden_ids.add(998)
    with pytest.raises(NotFoundError):
        Site.get_model(id=999)
    with pytest.raises(NotFoundError):
        run(Site.aget_model(id=999))
    with pytest.raises(ForbiddenError):
        Site.get_model(id=998)
    with pytest.raises(ForbiddenError):
        run(Site(id=998).aupdate())


def test_get_state_and_parent_pad(api):
    """The site state and parent pad are fetched, and the site is refreshed only once when its pad id is unknown."""
    site = Site(id=SITE_ID)

    state = site.get_state()
    assert isinstance(state, State)
    check_instance_types(state)
    assert api.last('GET', f'{API_PREFIX}/states/').query == {'site__id': [str(SITE_ID)]}
    assert isinstance(run(site.aget_state(as_dict=True)), dict)

    # The pad id is not known yet, so the site is refreshed first
    pad = site.get_pad()
    assert isinstance(pad, Pad)
    assert pad.id == load_fixture('site/site_one.json')['pad_id']
    check_instance_types(pad)
    assert api.count('GET', SITE_PATH) == 1
    assert run(site.aget_pad(as_dict=True))['id'] == pad.id
    assert api.count('GET', SITE_PATH) == 1

    assert Site(id=SITE_ID, pad_id=None).get_pad() is None
    assert run(Site(id=SITE_ID, pad_id=None).aget_pad()) is None


def test_measurement_sources_are_filtered_for_the_site(api):
    """Measurement sources of the parent pad are limited to those attached to the site, without attached_sites."""
    pad_payload = load_fixture('pad/pad_one.json')
    expected = {}
    for name, sources in deepcopy(pad_payload['simops_config']['measurement_sources']).items():
        expected[name] = []
        for source in sources:
            if SITE_ID in source['attached_sites']:
                source.pop('attached_sites')
                expected[name].append(source)
    assert any(expected.values()), 'the fixture must attach at least one source to the site'

    site = Site(id=SITE_ID, pad_id=pad_payload['id'])
    assert site.get_measurement_sources_metadata() == expected
    assert run(site.aget_measurement_sources_metadata()) == expected
    assert api.count('GET', f'{API_PREFIX}/pads/{pad_payload["id"]}/') == 2


def test_fields_stages_and_aggregations(api):
    """Fields and stages metadata are requested with the given filters, and aggregations are attached to stages."""
    site = Site(id=SITE_ID)
    fields_payload = load_fixture('site/fields_site.json')

    assert site.get_fields_metadata(api_number='42') == fields_payload
    assert api.last('GET', f'{SITE_PATH}fields/').query == {'api_number': ['42']}
    assert run(site.aget_fields_metadata()) == fields_payload

    stages = site.get_stages_metadata(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end=datetime(2020, 1, 2, tzinfo=timezone.utc),
        stage_name_filter=StageNameFilters.FRAC,
        stage_numbers=[5, 6],
        add_aggregations=True,
        aggregations_filter='avg_clean_rate',
    )
    assert api.last('GET', f'{SITE_PATH}history/').query == {
        'start': ['2020-01-01T00:00:00Z'],
        'end': ['2020-01-02T00:00:00Z'],
        'state': ['frac'],
        'stage_numbers': ['5,6'],
        'aggregations_filter': ['avg_clean_rate'],
    }
    filtered_payload = load_fixture('site/stages_site_filtered.json')
    aggregations_request = api.last('GET', f'{SITE_PATH}aggregations/')
    # The current, unfinished stage has no id and is excluded from the aggregations lookup
    assert aggregations_request.query['histories'] == [
        str(stage['id']) for stage in filtered_payload if stage.get('id') is not None
    ]
    assert aggregations_request.query['aggregations_filter'] == ['avg_clean_rate']
    assert len(stages) == len(filtered_payload)
    assert all('aggregations' in stage for stage in stages)
    aggregated_ids = {item['id'] for item in load_fixture('site/aggregations_site.json')[str(SITE_ID)] if 'id' in item}
    assert aggregated_ids & {stage.get('id') for stage in stages}
    for stage in stages:
        if stage.get('id') in aggregated_ids:
            assert isinstance(stage['aggregations'], dict) and 'id' not in stage['aggregations']

    plain = run(site.aget_stages_metadata(stage_number=5, stage_name_filter=StageNameFilters.FRAC))
    assert api.last('GET', f'{SITE_PATH}history/').query == {'stage_number': ['5'], 'state': ['frac']}
    assert len(plain) == len(filtered_payload) and 'aggregations' not in plain[0]
    assert api.count('GET', f'{SITE_PATH}aggregations/') == 1

    # Stages without ids (the current, unfinished stage) do not break the aggregations lookup
    assert site._add_aggregations([{'state': 'frac_frac', 'id': None}]) == [{'state': 'frac_frac', 'id': None}]


def test_stages_aggregations_crud(api):
    """Stage aggregations support the full CRUD cycle sync and async, while default aggregations cannot be changed."""
    site = Site(id=SITE_ID)
    fixture = list(api.stages_aggregations.values())

    assert site.get_stages_aggregations() == fixture
    assert site.get_stages_aggregations_descriptions() == fixture
    assert run(site.aget_stages_aggregations()) == fixture
    assert run(site.aget_stages_aggregations_descriptions()) == fixture

    assert site.create_stages_aggregations(NEW_AGGREGATION, validate_only=True) == {'valid': True}
    assert api.last('POST', SITE_AGGREGATIONS_PATH).query == {'validate_only': ['true']}
    created = site.create_stages_aggregations(NEW_AGGREGATION)
    assert created[0]['name'] == 'avg_rate' and created[0]['description'] == 'd'
    assert api.last('POST', SITE_AGGREGATIONS_PATH).body == NEW_AGGREGATION
    assert api.last('POST', SITE_AGGREGATIONS_PATH).query == {}

    aggregation_id = created[0]['id']
    assert site.get_stages_aggregation(aggregation_id)['id'] == aggregation_id
    updated = site.update_stages_aggregation(aggregation_id, {'description': 'new'})
    assert updated['description'] == 'new'
    assert api.last('PATCH', f'{SITE_AGGREGATIONS_PATH}{aggregation_id}/').body == {'description': 'new'}
    replaced = site.update_stages_aggregation(
        aggregation_id, {'config': {'field': 'f', 'aggregation': 'min'}}, partial=False
    )
    assert replaced['config'] == {'field': 'f', 'aggregation': 'min'} and replaced['description'] == ''
    assert api.last('PUT', f'{SITE_AGGREGATIONS_PATH}{aggregation_id}/').method == 'PUT'
    assert site.delete_stages_aggregation(aggregation_id) is None
    with pytest.raises(NotFoundError):
        site.get_stages_aggregation(aggregation_id)

    async def async_flow():
        """Run the create, read, update, replace and delete cycle of a stage aggregation through the async methods."""
        created = await site.acreate_stages_aggregations(NEW_AGGREGATION)
        aggregation_id = created[0]['id']
        assert (await site.aget_stages_aggregation(aggregation_id))['id'] == aggregation_id
        assert (await site.aupdate_stages_aggregation(aggregation_id, {'label': 'L'}))['label'] == 'L'
        assert (await site.aupdate_stages_aggregation(aggregation_id, {'description': 'x'}, partial=False))[
            'label'
        ] == ''
        assert await site.adelete_stages_aggregation(aggregation_id) is None
        with pytest.raises(NotFoundError):
            await site.aget_stages_aggregation(aggregation_id)
        assert await site.acreate_stages_aggregations(NEW_AGGREGATION, validate_only=True) == {'valid': True}

    run(async_flow())

    default_id = fixture[0]['id']
    with pytest.raises(BadRequestError):
        site.update_stages_aggregation(default_id, {'description': 'x'})
    with pytest.raises(BadRequestError):
        run(site.adelete_stages_aggregation(default_id))
    with pytest.raises(BadRequestError) as excinfo:
        site.create_stages_aggregations([{'name': 'missing-config'}])
    assert excinfo.value.status_code == 400


def test_get_data_and_save(api, tmp_path):
    """Site data is streamed with the expected query parameters and saved to JSON and CSV files."""
    site = Site(id=SITE_ID)
    payload = load_fixture('site/data_site.json')

    data = site.get_data(
        start_datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_datetime=datetime(2020, 1, 1, 1, tzinfo=timezone.utc),
        fields=['a', 'b'],
        si_units=True,
        resolution=DataResolutions.MINUTE,
        aggregation=DataAggregations.MEAN,
    )
    items = list(data.data_fetcher)
    assert items == payload
    assert api.last('GET', SITE_DATA_PATH).query == {
        'si_units': ['true'],
        'resolution': ['minute'],
        'start_datetime': ['2020-01-01 00:00:00'],
        'end_datetime': ['2020-01-01 01:00:00'],
        'fields': ['a,b'],
        'agg': ['mean'],
    }
    assert api.last('GET', SITE_DATA_PATH).headers['Prefer'] == 'streaming'

    json_path = tmp_path / 'data.json'
    data.save(str(json_path))
    assert pd.read_json(json_path, convert_dates=False).shape[0] == len(payload)
    csv_path = tmp_path / 'data.csv'
    data.save(str(csv_path))
    frame = pd.read_csv(csv_path)
    assert len(frame) == len(payload)
    assert set(frame.columns) == set(pd.DataFrame(payload).columns)

    with pytest.raises(ValueError):
        site.get_data(fill_data_method=DataFillMethods.FORWARD_FILL)
    with pytest.raises(ValueError):
        site.get_data(start_datetime=datetime(2020, 1, 1))


async def test_aget_data_and_asave(api, tmp_path):
    """Site data is streamed asynchronously with gap filling parameters and saved to JSON and CSV files."""
    site = Site(id=SITE_ID)
    payload = load_fixture('site/data_site.json')

    data = await site.aget_data(
        fill_data_method=DataFillMethods.BACKWARD_FILL, fill_data_limit=30, inside_area_only=False
    )
    items = [item async for item in data.data_fetcher]
    assert items == payload
    # Site.aget_data passes fields=None, so the server default (all customer fields) applies
    assert api.last('GET', SITE_DATA_PATH).query == {
        'si_units': ['false'],
        'resolution': ['raw'],
        'fill_data_method': ['bfill'],
        'fill_data_limit': ['30'],
        'inside_area_only': ['false'],
    }

    json_path = tmp_path / 'data.json'
    await data.asave(str(json_path))
    assert pd.read_json(json_path, convert_dates=False).shape[0] == len(payload)
    csv_path = tmp_path / 'data.csv'
    await data.asave(str(csv_path))
    assert len(pd.read_csv(csv_path)) == len(payload)


def test_data_changes_with_an_unexpected_body_raise_api_compatibility_error(api):
    """An empty or non-object change log response is reported as an API compatibility problem, sync and async."""
    site = Site(id=SITE_ID)

    api.plan('GET', SITE_DATA_CHANGES_PATH, {'status': 200, 'body': ''})
    with pytest.raises(APICompatibilityError):
        site.get_data_changes()

    api.plan('GET', SITE_DATA_CHANGES_PATH, {'status': 200, 'body': []})
    with pytest.raises(APICompatibilityError):
        run(site.aget_data_changes())


def test_data_changes_flow(api):
    """Site data changes are listed, streamed per merged interval and acknowledged only once, sync and async."""
    site = Site(id=SITE_ID)
    fixture = load_fixture('site/data_changes_site_many.json')['change_log']
    intervals = DataChanges._group_and_merge_intervals_by_site(fixture)
    for site_id in intervals:
        api.stream_items[f'{SITES_PATH}{site_id}/data/'] = [{'timestamp': 1, 'site_id': site_id}]

    changes, combined = site.get_data_changes(get_acknowledged=False)
    assert api.last('GET', SITE_DATA_CHANGES_PATH).query == {'get_acknowledged': ['false']}
    assert len(changes) == len(fixture)
    assert all(isinstance(change, DataChanges) and not change.update_received for change in changes)
    assert isinstance(changes[0].start_date, datetime) and changes[0].start_date.tzinfo is not None

    # The combined stream requests each merged interval of each site exactly once
    combined_items = list(combined.data_fetcher)
    assert len(combined_items) == sum(len(ranges) for ranges in intervals.values())
    data_requests = [r for r in api.requests if r.path.endswith('/data/')]
    assert len(data_requests) == len(combined_items)
    first_site = sorted(intervals)[0]
    first_start, first_end = intervals[first_site][0]
    assert data_requests[0].path == f'{SITES_PATH}{first_site}/data/'
    assert data_requests[0].param('start_datetime') == first_start.strftime('%Y-%m-%d %H:%M:%S')
    assert data_requests[0].param('end_datetime') == first_end.strftime('%Y-%m-%d %H:%M:%S')

    first = changes[0]
    assert first.confirm_data_received() is True
    assert api.last('POST', SITE_DATA_CHANGES_PATH).body == {'change_log': [{'id': first.id, 'update_received': True}]}
    assert first.update_received is True
    assert first.confirm_data_received() is True
    assert api.count('POST', SITE_DATA_CHANGES_PATH) == 1

    remaining, _ = site.get_data_changes(as_dict=True)
    assert len(remaining) == len(fixture) - 1
    everything, _ = site.get_data_changes(as_dict=True, get_acknowledged=True)
    assert len(everything) == len(fixture)

    async def async_flow():
        """Fetch the remaining data changes asynchronously, acknowledge one and stream the data of its interval."""
        changes, combined = await site.aget_data_changes()
        assert len(changes) == len(fixture) - 1
        assert len([item async for item in combined.data_fetcher]) == len(combined_items)
        second = changes[0]
        assert await second.aconfirm_data_received() is True
        await second.aupdate()
        assert second.update_received is True
        # A single change event can stream the data of its own interval
        single = await second.aget_data()
        assert [item async for item in single.data_fetcher] == [{'timestamp': 1, 'site_id': second.site}]

    run(async_flow())
    assert len(api.acknowledged_change_logs) == 2


def test_realtime_instance_and_data_changes_updates(api):
    """Realtime site updates and data change events are delivered as models or dicts over authorized websockets."""
    site = Site(id=SITE_ID)
    site_messages = [{'id': SITE_ID, 'name': 'A', 'latitude': 1.5}, {'id': SITE_ID, 'name': 'B', 'latitude': 2.5}]
    change_messages = [
        [
            {'id': 1, 'site': SITE_ID, 'start_date': '2020-01-01T00:00:00Z', 'end_date': '2020-01-01T01:00:00Z'},
            {'id': 2, 'site': SITE_ID, 'start_date': '2020-01-02T00:00:00Z', 'end_date': '2020-01-02T01:00:00Z'},
        ],
        {'id': 3, 'site': SITE_ID, 'start_date': '2020-01-03T00:00:00Z', 'end_date': '2020-01-03T01:00:00Z'},
    ]
    api.ws_scripts[SITE_WS_PATH] = site_messages
    api.ws_scripts[f'{SITE_WS_PATH}data_changes/'] = change_messages

    updates = list(site.get_realtime_instance_updates(restart_on_error=False, restart_on_close=False).data_fetcher)
    assert [update.name for update in updates] == ['A', 'B']
    assert all(isinstance(update, Site) and isinstance(update.latitude, float) for update in updates)
    dicts = list(
        site.get_realtime_instance_updates(as_dict=True, restart_on_error=False, restart_on_close=False).data_fetcher
    )
    assert dicts == site_messages

    changes = list(site.get_realtime_data_changes_updates(restart_on_error=False, restart_on_close=False).data_fetcher)
    assert [change.id for change in changes] == [1, 2, 3]
    assert all(isinstance(change, DataChanges) and isinstance(change.start_date, datetime) for change in changes)

    async def async_flow():
        """Consume the async realtime instance and data change streams and compare them with the scripted messages."""
        stream = await site.aget_realtime_instance_updates(as_dict=True, restart_on_error=False, restart_on_close=False)
        assert [item async for item in stream.data_fetcher] == site_messages
        stream = await site.aget_realtime_data_changes_updates(restart_on_error=False, restart_on_close=False)
        assert [item.id async for item in stream.data_fetcher] == [1, 2, 3]

    run(async_flow())
    assert len(api.websockets) == 5
    assert all(ws.headers['Authorization'].startswith('Bearer ') for ws in api.websockets)


def test_realtime_measurements_data(api):
    """Realtime measurements use the given or a generated session key and send the data query parameters."""
    site = Site(id=SITE_ID)
    path = f'{SITE_WS_PATH}data/'
    api.ws_scripts[path] = [[{'timestamp': 1, 'a': 1}, {'timestamp': 2, 'a': 2}], {'timestamp': 3, 'a': 3}]

    data, key = site.get_realtime_measurements_data(
        session_key='my-key',
        restart_on_error=False,
        restart_on_close=False,
        si_units=True,
        start_datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert key == 'my-key'
    assert list(data.data_fetcher) == [{'timestamp': 1, 'a': 1}, {'timestamp': 2, 'a': 2}, {'timestamp': 3, 'a': 3}]
    ws = api.websockets[-1]
    assert ws.path == path
    assert ws.param('session_key') == 'my-key'
    assert ws.param('si_units') == 'true'
    assert ws.param('resolution') == 'raw'
    assert ws.param('start_datetime') == '2020-01-01 00:00:00'

    _, generated_key = site.get_realtime_measurements_data(restart_on_error=False, restart_on_close=False)
    assert len(generated_key) == 32

    async def async_flow():
        """Stream the realtime measurements asynchronously and check the session key passed to the websocket."""
        data, key = await site.aget_realtime_measurements_data(
            session_key='k2', restart_on_error=False, restart_on_close=False
        )
        assert key == 'k2'
        assert [item['a'] async for item in data.data_fetcher] == [1, 2, 3]
        assert api.websockets[-1].param('session_key') == 'k2'

    run(async_flow())
