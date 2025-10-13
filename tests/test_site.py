import json
from copy import deepcopy
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pytest

from datastore_sdk.constants import (
    RESTREAM_HOST,
    ENDPOINTS,
    StageNameFilters,
    DataResolutions,
    DataAggregations,
    DataFillMethods,
)

from datastore_sdk.communicator import Communicator
from datastore_sdk.pad import Pad
from datastore_sdk.site import Site
from datastore_sdk.state import State
from tests.utils import check_instance_types

# Helpers
BASE = Path(__file__).parent / 'mock_responses' / 'site'
SITE_ID = 1112


def load_json(name: str):
    with open(BASE / name, 'r', encoding='utf-8') as f:
        return json.load(f)

# ------------------------
# Class-level methods (inherited from BaseInterface)
# ------------------------

def test_site_get_models(monkeypatch):
    token = 'tok'
    payload = load_json('sites_many.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.sites_many.value}"

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out_models = Site.get_models(auth_token=token)
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], Site)
    assert out_models[0].id == payload[0]['id']
    check_instance_types(out_models[0])

    out_dicts = Site.get_models(auth_token=token, as_dict=True)
    assert isinstance(out_dicts, list)
    assert out_dicts == payload


@pytest.mark.asyncio
async def test_site_aget_models(monkeypatch):
    token = 'tok'
    payload = load_json('sites_many.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.sites_many.value}"

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out_models = await Site.aget_models(auth_token=token)
    assert isinstance(out_models, list)
    assert len(out_models) == len(payload)
    assert isinstance(out_models[0], Site)
    check_instance_types(out_models[0])

    out_dicts = await Site.aget_models(auth_token=token, as_dict=True)
    assert isinstance(out_dicts, list)
    assert out_dicts == payload


def test_site_get_model(monkeypatch):
    token = 'tok'
    site_id = SITE_ID
    payload = load_json('site_one.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.sites_one.value}".format(id=site_id)

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out_model = Site.get_model(id=site_id, auth_token=token)
    check_instance_types(out_model)

    out_dict = Site.get_model(id=site_id, auth_token=token, as_dict=True)
    assert out_dict == payload


@pytest.mark.asyncio
async def test_site_aget_model(monkeypatch):
    token = 'tok'
    site_id = SITE_ID
    payload = load_json('site_one.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.sites_one.value}".format(id=site_id)

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out_model = await Site.aget_model(id=site_id, auth_token=token)
    check_instance_types(out_model)

    out_dict = await Site.aget_model(id=site_id, auth_token=token, as_dict=True)
    assert out_dict == payload


# ------------------------
# Instance-level: navigation to Pad and State
# ------------------------

def test_site_get_state(monkeypatch):
    token = 'tok'
    payload = load_json('states_many_by_site_frac.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.states_many.value}"

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        # The Site.get_state uses State.get_models with site__id filter
        assert params['site__id'] == SITE_ID
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    site = Site(id=SITE_ID, auth_token=token)
    out = site.get_state()
    assert isinstance(out, State)
    check_instance_types(out)

    out_dict = site.get_state(as_dict=True)
    assert isinstance(out_dict, dict)


@pytest.mark.asyncio
async def test_site_aget_state(monkeypatch):
    token = 'tok'
    payload = load_json('states_many_by_site.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.states_many.value}"

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        assert params['site__id'] == SITE_ID
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    site = Site(id=SITE_ID, auth_token=token)

    out = await site.aget_state()
    assert isinstance(out, State)
    check_instance_types(out)

    out_dict = await site.aget_state(as_dict=True)
    assert isinstance(out_dict, dict)


def test_site_get_pad(monkeypatch):
    token = 'tok'
    site_payload = load_json('site_one.json')
    pad_payload = load_json('pad_one_by_site.json')

    url_site = f"{RESTREAM_HOST}{ENDPOINTS.sites_one.value}".format(id=SITE_ID)
    url_pad = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=site_payload['pad_id'])

    def fake_get(u, auth_token, **params):
        assert auth_token == token
        if u == url_site:
            return site_payload
        if u == url_pad:
            return pad_payload
        raise AssertionError("Unexpected URL: " + u)

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    # Ensure pad_id is "fetched" via update() and then Pad.get_model is called
    site = Site(id=SITE_ID, auth_token=token)
    out_model = site.get_pad()
    assert isinstance(out_model, Pad)
    check_instance_types(out_model)

    out_dict = site.get_pad(as_dict=True)
    assert isinstance(out_dict, dict)
    assert out_dict['id'] == pad_payload['id']


@pytest.mark.asyncio
async def test_site_aget_pad(monkeypatch):
    token = 'tok'
    site_payload = load_json('site_one.json')
    pad_payload = load_json('pad_one_by_site.json')

    url_site = f"{RESTREAM_HOST}{ENDPOINTS.sites_one.value}".format(id=SITE_ID)
    url_pad = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=site_payload['pad_id'])

    async def fake_get(u, auth_token, **params):
        assert auth_token == token
        if u == url_site:
            return site_payload
        if u == url_pad:
            return pad_payload
        raise AssertionError("Unexpected URL: " + u)

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    site = Site(id=SITE_ID, auth_token=token)
    out_model = await site.aget_pad()
    assert isinstance(out_model, Pad)
    check_instance_types(out_model)

    out_dict = await site.aget_pad(as_dict=True)
    assert isinstance(out_dict, dict)
    assert out_dict['id'] == pad_payload['id']


# ------------------------
# Metadata endpoints (inherited from BasePadSite)
# ------------------------

def test_site_get_fields_metadata(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    payload = load_json('fields_site.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.fields_site.value}".format(id=site.id)

    def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out = site.get_fields_metadata()
    assert out == payload


@pytest.mark.asyncio
async def test_site_aget_fields_metadata(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    payload = load_json('fields_site.json')
    url = f"{RESTREAM_HOST}{ENDPOINTS.fields_site.value}".format(id=site.id)

    async def fake_get(u, auth_token, **params):
        assert u == url
        assert auth_token == token
        return payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out = await site.aget_fields_metadata()
    assert out == payload


def test_site_get_stages_metadata(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    stages_payload = load_json('stages_site.json')
    aggs_payload = load_json('aggregations_site.json')

    url_stages = f"{RESTREAM_HOST}{ENDPOINTS.stages_site.value}".format(id=site.id)
    url_aggs = f"{RESTREAM_HOST}{ENDPOINTS.aggregations_site.value}".format(id=site.id)

    def fake_get(u, auth_token, **params):
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

    out = site.get_stages_metadata(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end=datetime(2020, 1, 2, tzinfo=timezone.utc),
        add_aggregations=True,
    )
    assert isinstance(out, list)
    assert len(out) > 0
    assert all('aggregations' in s for s in out)


@pytest.mark.asyncio
async def test_site_aget_stages_metadata(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    stages_payload = load_json('stages_site_filtered.json')

    url_stages = f"{RESTREAM_HOST}{ENDPOINTS.stages_site.value}".format(id=site.id)

    async def fake_get(u, auth_token, **params):
        assert u == url_stages
        assert params['stage_number'] == 5
        assert params['state'] == StageNameFilters.FRAC.value
        return stages_payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out = await site.aget_stages_metadata(stage_number=5, stage_name_filter=StageNameFilters.FRAC)
    assert isinstance(out, list)
    assert len(out) > 0
    assert len(out) == len(stages_payload)


# ------------------------
# Measurement sources (Site override)
# ------------------------

def test_site_get_measurement_sources_metadata(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    # Avoid an extra GET to the Site endpoint inside get_pad(); set pad_id directly for this test
    site.pad_id = load_json('site_one.json')['pad_id']
    pad_payload = load_json('pad_one_by_site.json')
    pad_ms = deepcopy(pad_payload['simops_config']['measurement_sources'])

    url_pad = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=load_json('site_one.json')['pad_id'])

    def fake_get(u, auth_token, **params):
        assert u == url_pad
        assert auth_token == token
        return pad_payload

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    out = site.get_measurement_sources_metadata()
    # Only sources that have this site id in attached_sites should be returned and without 'attached_sites' key
    site_ms = dict()
    for type_name, sources in pad_ms.items():
        site_ms[type_name] = list()
        for source in sources:
            if SITE_ID in source['attached_sites']:
                source.pop('attached_sites')
                site_ms[type_name].append(source)

    assert site_ms == out


@pytest.mark.asyncio
async def test_site_aget_measurement_sources_metadata(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    # Avoid an extra GET to the Site endpoint inside aget_pad(); set pad_id directly for this test
    site.pad_id = load_json('site_one.json')['pad_id']
    pad_payload = load_json('pad_one_by_site.json')
    pad_ms = deepcopy(pad_payload['simops_config']['measurement_sources'])

    url_pad = f"{RESTREAM_HOST}{ENDPOINTS.pads_one.value}".format(id=load_json('site_one.json')['pad_id'])

    async def fake_get(u, auth_token, **params):
        assert u == url_pad
        assert auth_token == token
        return pad_payload

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    out = await site.aget_measurement_sources_metadata()

    site_ms = dict()
    for type_name, sources in pad_ms.items():
        site_ms[type_name] = list()
        for source in sources:
            if SITE_ID in source['attached_sites']:
                source.pop('attached_sites')
                site_ms[type_name].append(source)

    assert site_ms == out

# ------------------------
# Data endpoints (inherited from BasePadSite)
# ------------------------

def test_site_get_data(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    url = f"{RESTREAM_HOST}{ENDPOINTS.data_site.value}".format(id=site.id)

    payload = load_json('data_site.json')

    def fake_streaming(url_in, auth_token, **params):
        assert url_in == url
        assert auth_token == token
        # Check params mapping
        assert params['si_units'] == 'true'
        assert params['resolution'] == DataResolutions.SECOND.value
        assert params['fields'] == ['a', 'b']
        assert params['measurement_source'] == ['M1']
        # No start/end provided here
        return iter(payload)

    monkeypatch.setattr(Communicator, 'steaming_get_generator', fake_streaming)

    data_obj = site.get_data(
        fields=['a', 'b'],
        si_units=True,
        measurement_sources_names=['M1'],
    )

    items = list(data_obj.data_fetcher)
    assert isinstance(items, list)
    assert items == payload

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

    # fill_data validation path from BasePadSite
    with pytest.raises(ValueError):
        site.get_data(fill_data_method=DataFillMethods.FORWARD_FILL, fill_data_limit=None)


@pytest.mark.asyncio
async def test_site_aget_data(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    url = f"{RESTREAM_HOST}{ENDPOINTS.data_site.value}".format(id=site.id)

    payload = load_json('data_site.json')

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

    out = await site.aget_data(
        start_datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_datetime=datetime(2020, 1, 1, 1, tzinfo=timezone.utc),
        aggregation=DataAggregations.MEAN,
    )
    items = []
    async for i in out.data_fetcher:
        items.append(i)
    assert items == payload

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

def test_site_get_data_changes(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    many_payload = load_json('data_changes_site_many.json')

    url_many = f"{RESTREAM_HOST}{ENDPOINTS.data_changes_site_many.value}".format(parent_id=site.id)

    def fake_get(u, auth_token, **params):
        assert u == url_many
        if isinstance(many_payload, dict) and 'change_log' in many_payload:
            return many_payload
        return {'change_log': many_payload}

    monkeypatch.setattr(Communicator, 'send_get_request', fake_get)

    changes, combined = site.get_data_changes(as_dict=True, page=1)
    assert isinstance(changes, list)
    assert hasattr(combined, 'data_fetcher')


@pytest.mark.asyncio
async def test_site_aget_data_changes(monkeypatch):
    token = 'tok'
    site = Site(id=SITE_ID, auth_token=token)
    many_payload = load_json('data_changes_site_many.json')

    url_many = f"{RESTREAM_HOST}{ENDPOINTS.data_changes_site_many.value}".format(parent_id=site.id)

    async def fake_get(u, auth_token, **params):
        assert u == url_many
        assert auth_token == token
        if isinstance(many_payload, dict) and 'change_log' in many_payload:
            return many_payload
        return {'change_log': many_payload}

    monkeypatch.setattr(Communicator, 'send_get_request_async', fake_get)

    changes, combined = await site.aget_data_changes(as_dict=True)
    assert isinstance(changes, list)
    assert hasattr(combined, 'data_fetcher')
