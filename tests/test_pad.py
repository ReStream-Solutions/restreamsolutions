import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pytest

from restreamsolutions.constants import (
    RESTREAM_HOST,
    ENDPOINTS,
    StageNameFilters,
    DataResolutions,
    DataAggregations,
    DataFillMethods,
)

from restreamsolutions.communicator import Communicator
from restreamsolutions.pad import Pad
from restreamsolutions.site import Site
from restreamsolutions.state import State
from tests.utils import check_instance_types

# Helpers
BASE = Path(__file__).parent / 'mock_responses' / 'pad'


def load_json(name: str):
    with open(BASE / name, 'r', encoding='utf-8') as f:
        return json.load(f)


# ------------------------
# Class-level methods (inherited from BaseInterface)
# ------------------------


def test_pad_get_models(monkeypatch):
    token = 'tok'
    payload = load_json('pad_many.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.pads_many.value}"

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out_models = Pad.get_models(auth_token=token)
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], Pad)
    assert out_models[0].id == payload[0]['id']
    check_instance_types(out_models[0])

    out_dicts = Pad.get_models(auth_token=token, as_dict=True)
    assert isinstance(out_dicts, list)
    assert out_dicts == payload


@pytest.mark.asyncio
async def test_pad_aget_models(monkeypatch):
    token = 'tok'
    payload = load_json('pad_many.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.pads_many.value}"

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out_models = await Pad.aget_models(auth_token=token)
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], Pad)
    check_instance_types(out_models[0])

    out_dicts = await Pad.aget_models(auth_token=token, as_dict=True)
    assert isinstance(out_dicts, list)
    assert out_dicts == payload


def test_pad_get_model(monkeypatch):
    token = 'tok'
    pad_id = 697
    payload = load_json('pad_one.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=pad_id)

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out_model = Pad.get_model(id=pad_id, auth_token=token)
    check_instance_types(out_model)

    out_dict = Pad.get_model(id=pad_id, auth_token=token, as_dict=True)
    assert out_dict == payload


@pytest.mark.asyncio
async def test_pad_aget_model(monkeypatch):
    token = 'tok'
    pad_id = 697
    payload = load_json('pad_one.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=pad_id)

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out_model = await Pad.aget_model(id=pad_id, auth_token=token)
    check_instance_types(out_model)

    out_dict = await Pad.aget_model(id=pad_id, auth_token=token, as_dict=True)
    assert out_dict == payload


# ------------------------
# Instance-level: navigation to Sites and States
# ------------------------


def test_pad_get_sites(monkeypatch):
    token = 'tok'
    payload = load_json('sites_many_by_pad.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.sites_many.value}"

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        assert 'pad__id' in params
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    pad = Pad(id=679, auth_token=token)
    out_models = pad.get_sites()
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], Site)
    check_instance_types(out_models[0])

    out_dicts = pad.get_sites(as_dict=True)
    assert out_dicts == payload


@pytest.mark.asyncio
async def test_pad_aget_sites(monkeypatch):
    token = 'tok'
    payload = load_json('sites_many_by_pad.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.sites_many.value}"

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        assert 'pad__id' in params
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    pad = Pad(id=679, auth_token=token)
    out_models = await pad.aget_sites()
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], Site)
    check_instance_types(out_models[0])

    out_dicts = await pad.aget_sites(as_dict=True)
    assert out_dicts == payload


def test_pad_get_states(monkeypatch):
    token = 'tok'
    payload = load_json('states_many_by_pad_frac.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.states_many.value}"

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        assert params['pad__id'] == 697
        assert params['current_state_search'] == StageNameFilters.FRAC.value
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    pad = Pad(id=697, auth_token=token)
    out_models = pad.get_states(stage_name_filter=StageNameFilters.FRAC)
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], State)
    check_instance_types(out_models[0])

    # dicts path
    out_dicts = pad.get_states(stage_name_filter=StageNameFilters.FRAC, as_dict=True)
    assert out_dicts == payload


@pytest.mark.asyncio
async def test_pad_aget_states(monkeypatch):
    token = 'tok'
    payload = load_json('states_many_by_pad.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.states_many.value}"

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        assert params['pad__id'] == 697
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    pad = Pad(id=697, auth_token=token)

    out_models = await pad.aget_states()
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], State)
    check_instance_types(out_models[0])

    # dicts path
    out_dicts = await pad.aget_states(as_dict=True)
    assert out_dicts == payload


# ------------------------
# Metadata endpoints (inherited from BasePadSite)
# ------------------------


def test_pad_get_fields_metadata(monkeypatch):
    token = 'tok'
    pad = Pad(id=697, auth_token=token)
    payload = load_json('fields_pad.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.fields_pad.value}".format(id=pad.id)

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out = pad.get_fields_metadata()
    assert out == payload


@pytest.mark.asyncio
async def test_pad_aget_fields_metadata(monkeypatch):
    token = 'tok'
    pad = Pad(id=697, auth_token=token)
    payload = load_json('fields_pad.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.fields_pad.value}".format(id=pad.id)

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out = await pad.aget_fields_metadata()
    assert out == payload


def test_pad_get_stages_metadata(monkeypatch):
    token = 'tok'
    pad = Pad(id=697, auth_token=token)
    stages_payload = load_json('stages_pad.json')
    aggs_payload = load_json('aggregations_pad.json')

    url_stages = f"{RESTREAM_HOST}{ENDPOINTS.stages_pad.value}".format(id=pad.id)
    url_aggs = f"{RESTREAM_HOST}{ENDPOINTS.aggregations_pad.value}".format(id=pad.id)

    def fake_get(u, auth_token, **params):
        # First call is for stages, second for aggregations
        if u == url_stages:
            # verify filters composed
            assert params['start'] == '2020-01-01T00:00:00Z'
            assert params['end'] == '2020-01-02T00:00:00Z'
            return stages_payload
        if u == url_aggs:
            assert 'histories' in params and isinstance(params['histories'], list)
            return aggs_payload
        raise AssertionError("Unexpected URL: " + u)

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out = pad.get_stages_metadata(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end=datetime(2020, 1, 2, tzinfo=timezone.utc),
        add_aggregations=True,
    )
    assert isinstance(out, list)
    # Verify that aggregations merged: if any stage id present in aggs, it should have 'aggregations' key
    assert all('aggregations' in s for s in out)


@pytest.mark.asyncio
async def test_pad_aget_stages_metadata(monkeypatch):
    token = 'tok'
    pad = Pad(id=697, auth_token=token)
    stages_payload = load_json('stages_pad_filtered.json')

    url_stages = f"{RESTREAM_HOST}{ENDPOINTS.stages_pad.value}".format(id=pad.id)

    async def fake_get(u, auth_token, **params):
        assert u == url_stages
        # ensure filter mapping
        assert params['stage_number'] == 5
        assert params['state'] == StageNameFilters.FRAC.value
        return stages_payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out = await pad.aget_stages_metadata(stage_number=5, stage_name_filter=StageNameFilters.FRAC)
    assert isinstance(out, list)
    assert len(out) == len(stages_payload)


# ------------------------
# Measurement sources (Pad override)
# ------------------------


def test_pad_get_measurement_sources_metadata(monkeypatch):
    token = 'tok'
    pad = Pad(id=123, auth_token=token)
    payload = load_json('pad_one.json')

    url = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=pad.id)

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out = pad.get_measurement_sources_metadata()
    assert out == payload['simops_config']['measurement_sources']


@pytest.mark.asyncio
async def test_pad_aget_measurement_sources_metadata(monkeypatch):
    # Case when simops_config is absent on instance -> aupdate should be called and perform GET
    token = 'tok'
    pad = Pad(id=456, auth_token=token)
    payload = load_json('pad_one.json')

    url = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=pad.id)

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out = await pad.aget_measurement_sources_metadata()
    assert out == payload['simops_config']['measurement_sources']


# ------------------------
# Data endpoints (inherited from BasePadSite but with additional pad-specific validation)
# ------------------------


def test_pad_get_data(monkeypatch):
    token = 'tok'
    pad = Pad(id=697, auth_token=token)
    url = f"{RESTREAM_HOST}{ENDPOINTS.data_pad.value}".format(id=pad.id)

    payload = load_json('data_pad.json')

    # happy path: patch streaming generator to yield mock items
    def fake_streaming(url_in, auth_token, **params):
        assert url_in == url
        assert auth_token == token
        # Check params mapping
        assert params['si_units'] == 'true'
        assert params['resolution'] == DataResolutions.SECOND.value
        assert params['fields'] == 'a,b'
        assert params['measurement_source'] == 'M1'
        assert params['routed'] == 'true'
        # No start/end provided here
        return iter(payload)

    monkeypatch.setattr(Communicator, 'steaming_get_generator', fake_streaming)

    # Method parameters are just to test API query parameters. The response is fixed by the mock.
    data_obj = pad.get_data(
        fields=['a', 'b'],
        si_units=True,
        measurement_sources_names=['M1'],
        is_routed=True,
    )

    # Ensure Data wrapper built and yields items
    items = list(data_obj.data_fetcher)
    assert isinstance(items, list)
    assert items == payload

    # And Data.save writes the same content
    out_path = BASE / 'tmp_data_sync.json'
    try:
        data_obj.save(str(out_path), overwrite=True)
        with open(out_path, 'r', encoding='utf-8') as f:
            saved = json.load(f)
        assert saved == payload
    finally:
        try:
            out_path.unlink()
        except FileNotFoundError:
            pass

    # Also verify CSV save
    out_csv = BASE / 'tmp_data_sync.csv'
    try:
        data_obj.save(str(out_csv), overwrite=True)
        df_csv = pd.read_csv(out_csv)
        df_expected = pd.DataFrame(payload)
        assert len(df_csv) == len(df_expected)
        assert set(df_csv.columns) == set(df_expected.columns)
    finally:
        try:
            out_csv.unlink()
        except FileNotFoundError:
            pass

    # pad-specific rule: stage_number requires is_routed=True
    with pytest.raises(ValueError):
        pad.get_data(stage_number=1, is_routed=False)

    # fill_data validation path from BasePadSite
    with pytest.raises(ValueError):
        pad.get_data(fill_data_method=DataFillMethods.FORWARD_FILL, fill_data_limit=None)


@pytest.mark.asyncio
async def test_pad_aget_data(monkeypatch):
    token = 'tok'
    pad = Pad(id=321, auth_token=token)
    url = f"{RESTREAM_HOST}{ENDPOINTS.data_pad.value}".format(id=pad.id)

    payload = load_json('data_pad.json')

    def fake_streaming(url_in, auth_token, **params):
        assert url_in == url
        assert auth_token == token
        assert params['agg'] == DataAggregations.MEAN.value
        # Check datetime conversion to UTC format
        assert params['start_datetime'] == '2020-01-01 00:00:00'
        assert params['end_datetime'] == '2020-01-01 01:00:00'

        async def _agen():
            for item in payload:
                yield item

        return _agen()

    monkeypatch.setattr(Communicator, 'steaming_get_generator_async', fake_streaming)

    out = await pad.aget_data(
        start_datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_datetime=datetime(2020, 1, 1, 1, tzinfo=timezone.utc),
        aggregation=DataAggregations.MEAN,
    )
    items = []
    async for i in out.data_fetcher:
        items.append(i)
    assert items == payload

    # And DataAsync.asave writes the same content
    out_path = BASE / 'tmp_data_async.json'
    try:
        await out.asave(str(out_path), overwrite=True)
        with open(out_path, 'r', encoding='utf-8') as f:
            saved = json.load(f)
        assert saved == payload
    finally:
        try:
            out_path.unlink()
        except FileNotFoundError:
            pass


# ------------------------
# Data changes endpoints
# ------------------------


def test_pad_get_realtime_updates(monkeypatch):
    token = 'tok'
    pad = Pad(id=999, auth_token=token)
    # Build expected wss URL
    url_http = f"{RESTREAM_HOST}{ENDPOINTS.pad_updates_websocket.value}".format(id=pad.id)
    url_wss = url_http.replace('https://', 'wss://').replace('http://', 'ws://')

    messages = [{'id': 1, 'k1': 'v1'}, {'id': 2, 'k2': 2}, {'id': 3, 'k3': None}]

    def fake_ws(url_in, auth_token, params=None, ack_message=None, additional_headers=None):
        assert url_in == url_wss
        assert auth_token == token
        return iter(messages)

    monkeypatch.setattr(Communicator, 'websocket_generator', fake_ws)

    # as_dict=True returns raw dicts
    data_obj = pad.get_realtime_instance_updates(restart_on_close=False, restart_on_error=False, as_dict=True)
    out = list(data_obj.data_fetcher)
    assert out == messages

    # as_dict=False returns Pad instances with propagated _auth_token
    data_obj2 = pad.get_realtime_instance_updates(restart_on_close=False, restart_on_error=False, as_dict=False)
    out2 = list(data_obj2.data_fetcher)
    assert all(isinstance(it, Pad) for it in out2)
    assert [it.id for it in out2] == [m['id'] for m in messages]
    assert all(getattr(it, '_auth_token') == token for it in out2)


@pytest.mark.asyncio
async def test_pad_aget_realtime_updates(monkeypatch):
    token = 'tok'
    pad = Pad(id=1001, auth_token=token)
    url_http = f"{RESTREAM_HOST}{ENDPOINTS.pad_updates_websocket.value}".format(id=pad.id)
    url_wss = url_http.replace('https://', 'wss://').replace('http://', 'ws://')

    messages = [{'id': 10, 'k1': 'v1'}, {'id': 20, 'k2': 2}, {'id': 30, 'k3': None}]

    def fake_ws_async(url_in, auth_token, params=None, ack_message=None, additional_headers=None):
        assert url_in == url_wss
        assert auth_token == token

        async def _agen():
            for m in messages:
                yield m

        return _agen()

    monkeypatch.setattr(Communicator, 'websocket_generator_async', fake_ws_async)

    # as_dict=True
    data_async = await pad.aget_realtime_instance_updates(restart_on_close=False, restart_on_error=False, as_dict=True)
    collected = []
    async for item in data_async.data_fetcher:
        collected.append(item)
    assert collected == messages

    # as_dict=False -> Pad instances with auth token
    data_async2 = await pad.aget_realtime_instance_updates(
        restart_on_close=False, restart_on_error=False, as_dict=False
    )
    collected2 = []
    async for item in data_async2.data_fetcher:
        collected2.append(item)
    assert all(isinstance(it, Pad) for it in collected2)
    assert [it.id for it in collected2] == [m['id'] for m in messages]
    assert all(getattr(it, '_auth_token') == token for it in collected2)


def test_pad_get_data_changes(monkeypatch):
    token = 'tok'
    pad = Pad(id=777, auth_token=token)
    many_payload = load_json('data_changes_pad_many.json')

    url_many = f"{RESTREAM_HOST}{ENDPOINTS.data_changes_pad_many.value}".format(parent_id=pad.id)

    def fake_get(u, auth_token, **params):
        assert u == url_many
        # Ensure the communicator returns an envelope with 'change_log'
        if isinstance(many_payload, dict) and 'change_log' in many_payload:
            return many_payload
        return {'change_log': many_payload}

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    changes, combined = pad.get_data_changes(as_dict=True)
    assert isinstance(changes, list)
    # combined should be a Data object with data_fetcher attribute
    assert hasattr(combined, 'data_fetcher')


@pytest.mark.asyncio
async def test_pad_aget_data_changes(monkeypatch):
    token = 'tok'
    pad = Pad(id=1, auth_token=token)
    many_payload = load_json('data_changes_pad_many.json')

    url_many = f"{RESTREAM_HOST}{ENDPOINTS.data_changes_pad_many.value}".format(parent_id=pad.id)

    def fake_get(u, auth_token, **params):
        assert u == url_many
        assert auth_token == token
        if isinstance(many_payload, dict) and 'change_log' in many_payload:
            return many_payload
        return {'change_log': many_payload}

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    changes, combined = await pad.aget_data_changes(as_dict=True)
    assert isinstance(changes, list)
    assert hasattr(combined, 'data_fetcher')


def test_pad_get_realtime_data_changes_updates(monkeypatch):
    token = 'tok'
    pad = Pad(id=55, auth_token=token)
    url_http = f"{RESTREAM_HOST}{ENDPOINTS.pad_changelog_updates_websocket.value}".format(id=pad.id)
    url_wss = url_http.replace('https://', 'wss://').replace('http://', 'ws://')

    # minimal valid DataChanges payloads must include id and site
    messages = [
        {
            "id": 1,
            "site": 10,
            "modification_type": "transaction",
            "modification_subtype": "add",
            "start_date": "2020-01-01T00:00:00Z",
            "end_date": "2020-01-01T01:00:00Z",
        },
        {
            "id": 2,
            "site": 11,
            "modification_type": "translation_layer",
            "modification_subtype": "change",
            "start_date": "2020-01-02T00:00:00Z",
            "end_date": "2020-01-02T01:00:00Z",
        },
    ]

    def fake_ws(url_in, auth_token, params=None, ack_message=None, additional_headers=None):
        assert url_in == url_wss
        assert auth_token == token
        return iter(messages)

    monkeypatch.setattr(Communicator, 'websocket_generator', fake_ws)

    # as_dict=True
    data_obj = pad.get_realtime_data_changes_updates(restart_on_close=False, restart_on_error=False, as_dict=True)
    out = list(data_obj.data_fetcher)
    assert out == messages

    # as_dict=False -> DataChanges instances with auth
    data_obj2 = pad.get_realtime_data_changes_updates(restart_on_close=False, restart_on_error=False, as_dict=False)
    out2 = list(data_obj2.data_fetcher)
    from restreamsolutions.data_changes import DataChanges

    assert all(isinstance(it, DataChanges) for it in out2)
    assert [it.id for it in out2] == [m['id'] for m in messages]
    assert all(getattr(it, '_auth_token') == token for it in out2)


@pytest.mark.asyncio
async def test_pad_aget_realtime_data_changes_updates(monkeypatch):
    token = 'tok'
    pad = Pad(id=56, auth_token=token)
    url_http = f"{RESTREAM_HOST}{ENDPOINTS.pad_changelog_updates_websocket.value}".format(id=pad.id)
    url_wss = url_http.replace('https://', 'wss://').replace('http://', 'ws://')

    messages = [
        {
            "id": 10,
            "site": 100,
            "modification_type": "transaction",
            "modification_subtype": "add",
            "start_date": "2020-01-03T00:00:00Z",
            "end_date": "2020-01-03T01:00:00Z",
        },
        {
            "id": 20,
            "site": 101,
            "modification_type": "translation_layer",
            "modification_subtype": "change",
            "start_date": "2020-01-04T00:00:00Z",
            "end_date": "2020-01-04T01:00:00Z",
        },
    ]

    def fake_ws_async(url_in, auth_token, params=None, ack_message=None, additional_headers=None):
        assert url_in == url_wss
        assert auth_token == token

        async def _agen():
            for m in messages:
                yield m

        return _agen()

    monkeypatch.setattr(Communicator, 'websocket_generator_async', fake_ws_async)

    # as_dict=True
    data_async = await pad.aget_realtime_data_changes_updates(
        restart_on_close=False, restart_on_error=False, as_dict=True
    )
    collected = []
    async for item in data_async.data_fetcher:
        collected.append(item)
    assert collected == messages

    # as_dict=False -> DataChanges instances with auth
    data_async2 = await pad.aget_realtime_data_changes_updates(
        restart_on_close=False, restart_on_error=False, as_dict=False
    )
    collected2 = []
    async for item in data_async2.data_fetcher:
        collected2.append(item)
    from restreamsolutions.data_changes import DataChanges

    assert all(isinstance(it, DataChanges) for it in collected2)
    assert [it.id for it in collected2] == [m['id'] for m in messages]
    assert all(getattr(it, '_auth_token') == token for it in collected2)
