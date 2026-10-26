from datetime import datetime, timedelta, timezone

import pytest

from restreamsolutions.base_pad_site import BasePadSite
from restreamsolutions.constants import DataAggregations, DataFillMethods, DataResolutions, StageNameFilters


class _Entity(BasePadSite):
    pass


def test_get_data_params_are_normalized():
    """Data request parameters are converted to API query values; fields is omitted when it is not given."""
    entity = _Entity(id=1)
    params = entity._build_get_data_params(
        start_datetime=datetime(2020, 1, 1, 12, tzinfo=timezone(timedelta(hours=2))),
        end_datetime=datetime(2020, 1, 1, 13, tzinfo=timezone.utc),
        fields=['a', 'b'],
        si_units=True,
        resolution=DataResolutions.MINUTE,
        stage_number=3,
        stage_name_filter=StageNameFilters.FRAC,
        aggregation=DataAggregations.MEAN,
        measurement_sources_names=['M1', 'M2'],
        is_routed=True,
    )
    assert params == {
        'si_units': 'true',
        'resolution': 'minute',
        'start_datetime': '2020-01-01 10:00:00',
        'end_datetime': '2020-01-01 13:00:00',
        'fields': 'a,b',
        'stage_number': 3,
        'state_imatch': 'frac',
        'agg': 'mean',
        'measurement_source': 'M1,M2',
        'routed': 'true',
    }

    fill_params = entity._build_get_data_params(fill_data_method=DataFillMethods.FORWARD_FILL, fill_data_limit=10)
    assert fill_params['fill_data_method'] == 'ffill'
    assert fill_params['fill_data_limit'] == '10'
    assert fill_params['inside_area_only'] == 'true'
    # Without fields the parameter is omitted so that the server applies its own default
    assert 'fields' not in fill_params
    assert entity._build_get_data_params(fields='slurry_flow')['fields'] == 'slurry_flow'


@pytest.mark.parametrize(
    'kwargs',
    [
        {'start_datetime': datetime(2020, 1, 1)},
        {'end_datetime': datetime(2020, 1, 1)},
        {'stage_number': 1},
        {'fill_data_method': DataFillMethods.FORWARD_FILL},
        {'fill_data_method': 'ffill', 'fill_data_limit': 1},
        {'fill_data_method': DataFillMethods.FORWARD_FILL, 'fill_data_limit': 1, 'resolution': DataResolutions.HOUR},
        {'fill_data_method': DataFillMethods.FORWARD_FILL, 'fill_data_limit': 1, 'aggregation': DataAggregations.MAX},
        {'fill_data_limit': 0},
        {'measurement_sources_names': 5},
    ],
)
def test_get_data_params_validation(kwargs):
    """Invalid or inconsistent data request parameters raise ValueError.

    Args:
        kwargs: The invalid parameters passed to _build_get_data_params.
    """
    with pytest.raises(ValueError):
        _Entity(id=1)._build_get_data_params(**kwargs)


def test_stage_metadata_filters():
    """Stage metadata filters are formatted as UTC ISO strings and comma-separated lists, or omitted when unset."""
    entity = _Entity(id=1)
    filters = entity._mix_stage_metadata_filters(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end=datetime(2020, 1, 2, tzinfo=timezone.utc),
        stage_number=0,
        stage_name_filter=StageNameFilters.WIRELINE,
        stage_numbers=[1, 2, 3],
        aggregations_filter='a,b',
    )
    assert filters == {
        'start': '2020-01-01T00:00:00Z',
        'end': '2020-01-02T00:00:00Z',
        'stage_number': 0,
        'stage_numbers': '1,2,3',
        'state': 'wl',
        'aggregations_filter': 'a,b',
    }
    assert entity._mix_stage_metadata_filters() == {}


def test_aggregations_are_merged_into_stages_by_history_id():
    """Aggregations are attached to stages by history id; unmatched stages and site error messages get None."""
    stages = [{'id': 1, 'state': 'frac_frac'}, {'id': 2, 'state': 'frac_frac'}, {'state': 'frac_frac'}]
    aggregations = {
        '10': [{'id': 1, 'avg_rate': 5.0}, {'id': 99, 'avg_rate': 1.0}],
        '11': 'Got incorrect aggregations data.',
    }

    merged = BasePadSite._merge_aggregations_with_stages(stages, aggregations)

    assert merged[0]['aggregations'] == {'avg_rate': 5.0}
    assert merged[1]['aggregations'] is None
    assert merged[2]['aggregations'] is None


@pytest.mark.parametrize('stage_numbers', ['abc', '1,2', [1, 'b'], [True], 5, [1.5]])
def test_stage_numbers_must_be_a_list_of_integers(stage_numbers):
    """Anything but a list of integers passed as stage_numbers raises ValueError before any request is made.

    Args:
        stage_numbers: The invalid value.
    """
    with pytest.raises(ValueError):
        _Entity(id=1)._mix_stage_metadata_filters(stage_numbers=stage_numbers)


def test_stage_numbers_tuple_is_accepted_and_empty_list_is_omitted():
    """A tuple of integers is accepted and an empty list sends no stage_numbers parameter."""
    entity = _Entity(id=1)
    assert entity._mix_stage_metadata_filters(stage_numbers=(4, 5)) == {'stage_numbers': '4,5'}
    assert entity._mix_stage_metadata_filters(stage_numbers=[]) == {}
