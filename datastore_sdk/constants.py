"""Constants and enumerations used across the datastore SDK."""

from enum import Enum

# Base URL for the Restream Solutions external API
RESTREAM_HOST = 'https://app.restreamsolutions.com'

class ENDPOINTS(Enum):
    """REST API endpoint path templates.

    Notes:
        Many endpoints include placeholders that must be formatted:
        - {id}: Identifier of the target resource (site, pad, or state).
        - {parent_id}: Identifier of the parent resource (site or pad) when
          working with nested resources like data changes.
    """

    sites_one = '/external/thirdparty/v1/sites/{id}/'
    sites_many = '/external/thirdparty/v1/sites/'
    pads_one = '/external/thirdparty/v1/pads/{id}/'
    pads_many = '/external/thirdparty/v1/pads/'
    states_one = '/external/thirdparty/v1/states/{id}/'
    states_many = '/external/thirdparty/v1/states/'
    fields_pad = '/external/thirdparty/v1/pads/{id}/fields/'
    fields_site = '/external/thirdparty/v1/sites/{id}/fields/'
    stages_pad = '/external/thirdparty/v1/pads/{id}/history/'
    stages_site = '/external/thirdparty/v1/sites/{id}/history/'
    aggregations_pad = '/external/thirdparty/v1/pads/{id}/aggregations/'
    aggregations_site = '/external/thirdparty/v1/sites/{id}/aggregations/'
    data_pad = '/external/thirdparty/v1/pads/{id}/data/'
    data_site = '/external/thirdparty/v1/sites/{id}/data/'
    data_changes_site_many = '/external/thirdparty/v1/sites/{parent_id}/data_changes/'
    data_changes_site_one = '/external/thirdparty/v1/sites/{parent_id}/data_changes/{id}/'
    data_changes_pad_many = '/external/thirdparty/v1/pads/{parent_id}/data_changes/'
    data_changes_pad_one = '/external/thirdparty/v1/pads/{parent_id}/data_changes/{id}/'

class StageNameFilters(Enum):
    """Short codes used to filter stage history by operation type."""

    FRAC = 'frac'       # Frac operations
    WIRELINE = 'wl'     # Wireline operations
    STANDBY = 'standby' # Non-operational/standby periods

class DataResolutions(Enum):
    """Allowed sampling resolutions for time-series data requests."""

    SECOND = 'raw'
    TEN_SECOND = 'ten-second'
    MINUTE = 'minute'
    TEN_MINUTE = 'ten-minute'
    HOUR = 'hour'

class DataAggregations(Enum):
    """Aggregation functions supported by the API when querying time-series data."""

    MIN = 'min'
    MAX = 'max'
    MEAN = 'mean'

class DataFillMethods(Enum):
    BACKWARD_FILL = 'bfill'
    FORWARD_FILL = 'ffill'