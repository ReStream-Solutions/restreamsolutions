from enum import Enum

RESTREAM_HOST = 'https://app.restreamsolutions.com'

class ENDPOINTS(Enum):
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
    FRAC = 'frac'
    WIRELINE = 'wl'
    STANDBY = 'standby'

class DataResolutions(Enum):
    SECOND = 'raw'
    TEN_SECOND = 'ten-second'
    MINUTE = 'minute'
    TEN_MINUTE = 'ten-minute'
    HOUR = 'hour'

class DataAggregations(Enum):
    MIN = 'min'
    MAX = 'max'
    MEAN = 'mean'