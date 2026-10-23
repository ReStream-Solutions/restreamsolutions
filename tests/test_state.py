import asyncio

from restreamsolutions.constants import StageNameFilters
from restreamsolutions.state import State
from tests.fake_api.server import API_PREFIX
from tests.utils import check_instance_types

STATES_PATH = f'{API_PREFIX}/states/'


def test_get_models_applies_stage_filter(api):
    """The stage name filter and extra filters are sent as query parameters, and aget_models can return dicts."""
    states = State.get_models(stage_name_filter=StageNameFilters.FRAC, pad__id=697)

    assert states and all(isinstance(state, State) for state in states)
    check_instance_types(states[0])
    assert api.last('GET', STATES_PATH).query == {'current_state_search': ['frac'], 'pad__id': ['697']}

    dicts = asyncio.run(State.aget_models(as_dict=True, site__id=1112))
    assert isinstance(dicts, list) and isinstance(dicts[0], dict)
    assert api.last('GET', STATES_PATH).query == {'site__id': ['1112']}


def test_get_model_by_id(api):
    """A single state is fetched by id without query parameters, both as a model and as a dict."""
    state = State.get_model(id=204)
    assert state.id == 204
    check_instance_types(state)
    assert api.last('GET', f'{STATES_PATH}204/').query == {}

    assert asyncio.run(State.aget_model(id=204, as_dict=True))['id'] == 204
