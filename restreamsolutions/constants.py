"""Constants and enumerations used across the datastore SDK."""

from enum import Enum

# Base URL for the Restream Solutions external API
RESTREAM_HOST = 'https://app.restreamsolutions.com'

# The number of concurrent calls to the same endpoint (does not include query parameters)
MAX_CONCURRENT_CALLS_TO_ENDPOINT = 3

# The number of concurrent calls to the websocket URL (includes query parameters)
MAX_CONCURRENT_CALLS_TO_WEBSOCKET = 1

# Total timeout for regular (non-streaming) HTTP requests, in seconds
REQUEST_TIMEOUT_SECONDS = 60

# Total timeout for the OAuth2 token request, in seconds
AUTH_REQUEST_TIMEOUT_SECONDS = 10

# Connect timeout for streaming HTTP requests, in seconds. The read phase is unbounded because
# the server may keep a streaming response open for a long time.
STREAMING_CONNECT_TIMEOUT_SECONDS = 5

# A cached access token is treated as expired this many seconds before its real expiration
TOKEN_EXPIRY_MARGIN_SECONDS = 10

# Total number of attempts (including the first call) for retryable requests
MAX_RETRY_ATTEMPTS = 5

# Upper bound of attempts for the OAuth2 token request. Frequent token requests may lead to an auto-ban,
# so this endpoint is retried less aggressively than the others.
AUTH_MAX_RETRY_ATTEMPTS = 3

# Environment variable that overrides MAX_RETRY_ATTEMPTS at runtime
MAX_RETRY_ATTEMPTS_ENV_VAR = 'RESTREAM_MAX_RETRY_ATTEMPTS'

# Delay before the first retry, in seconds
RETRY_INITIAL_DELAY_SECONDS = 1.0

# Multiplier applied to the delay after each failed attempt
RETRY_BACKOFF_FACTOR = 3.0

# Upper bound for a single retry delay, in seconds, including the jitter
RETRY_MAX_DELAY_SECONDS = 30.0

# Upper bound for the delay taken from the Retry-After header, in seconds. Larger values are clamped so that
# a server cannot block the caller for an arbitrary time
RETRY_AFTER_MAX_SECONDS = 120.0

# If True, each retry delay is randomized within [delay / 2, delay * 1.5] to avoid thundering herds;
# the randomized delay never exceeds RETRY_MAX_DELAY_SECONDS
RETRY_JITTER = True


class ENDPOINTS(Enum):
    """REST API endpoint path templates.

    Notes:
        Many endpoints include placeholders that must be formatted:
        - {id}: Identifier of the target resource (site, pad, or state).
        - {parent_id}: Identifier of the parent resource (site or pad) when
          working with nested resources like data changes.
        - {aggregation_id}: Identifier of a stage aggregation.
    """

    auth_access_token = '/o/token/'
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
    data_site_websocket = '/external/thirdparty/v2/ws/sites/{id}/data/'
    data_pad_websocket = '/external/thirdparty/v2/ws/pads/{id}/data/'
    site_updates_websocket = '/external/thirdparty/v2/ws/sites/{id}/'
    pad_updates_websocket = '/external/thirdparty/v2/ws/pads/{id}/'
    site_changelog_updates_websocket = '/external/thirdparty/v2/ws/sites/{id}/data_changes/'
    pad_changelog_updates_websocket = '/external/thirdparty/v2/ws/pads/{id}/data_changes/'
    stages_aggregations_pad = '/external/thirdparty/v1/pads/{id}/parameters/'
    stages_aggregations_site = '/external/thirdparty/v1/sites/{id}/parameters/'
    stages_aggregations_pad_one = '/external/thirdparty/v1/pads/{id}/parameters/{aggregation_id}/'
    stages_aggregations_site_one = '/external/thirdparty/v1/sites/{id}/parameters/{aggregation_id}/'


class StageNameFilters(Enum):
    """Short codes used to filter stage history by operation type."""

    FRAC = 'frac'  # Frac operations
    WIRELINE = 'wl'  # Wireline operations
    STANDBY = 'standby'  # Non-operational/standby periods


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
    """Fill methods supported by the API when querying time-series data."""

    BACKWARD_FILL = 'bfill'
    FORWARD_FILL = 'ffill'
