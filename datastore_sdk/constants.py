from enum import Enum

RESTREAM_HOST = 'https://app.restreamsolutions.com'

class ENDPOINTS(Enum):
    sites_get_one = '/external/thirdparty/v1/sites/{id}/'
    sites_get_many = '/external/thirdparty/v1/sites/'
    pads_get_one = '/external/thirdparty/v1/pads/{id}/'
    pads_get_many = '/external/thirdparty/v1/pads/'
    states_get_one = '/external/thirdparty/v1/states/{id}/'
    states_get_many = '/external/thirdparty/v1/states/'
    fields_for_pad = '/external/thirdparty/v1/pads/{id}/fields/'
    fields_for_site = '/external/thirdparty/v1/sites/{id}/fields/'
    stages_for_pad = '/external/thirdparty/v1/pads/{id}/history/'
    stages_for_site = '/external/thirdparty/v1/sites/{id}/history/'
    aggregations_for_pad = '/external/thirdparty/v1/pads/{id}/aggregations/'
    aggregations_for_site = '/external/thirdparty/v1/sites/{id}/aggregations/'

class StageNameFilter(Enum):
    FRAC = 'frac'
    WIRELINE = 'wl'
    STANDBY = 'standby'