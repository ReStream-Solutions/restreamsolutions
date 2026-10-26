# Changelog

## [v0.5.0] - 2026-10-20

### New Features

- `Site.get_models`/`aget_models` and `Pad.get_sites`/`aget_sites` accept explicit filters: `well_api`, `name_search`,
  `formation_name`, `pad_name`, `fleet_id`, `fleet_name`, a bounding box (`latitude_gte`, `latitude_lte`,
  `longitude_gte`, `longitude_lte`) and a radius filter (`latitude`, `longitude`, `radius_miles`, validated on the
  client to be provided together; the raw API names `lat` and `long` are accepted as well).
  `Pad.get_models`/`aget_models` accept `name_search`, `fleet_id` and `fleet_name`.
- `get_stages_metadata`/`aget_stages_metadata` accept `stage_numbers` (a list of stage numbers, validated on the
  client: anything but a list of integers raises `ValueError`).
- `get_data_changes`/`aget_data_changes` accept `get_acknowledged` to include already confirmed events.
- Full management of stage aggregations (the metrics calculated for every frac stage, whose values are returned
  by `get_stages_metadata`): `get_stages_aggregations`, `create_stages_aggregations` (sync, with `validate_only`),
  `get_stages_aggregation`, `update_stages_aggregation` (PATCH or PUT), `delete_stages_aggregation` and their
  async counterparts. `get_stages_aggregations_descriptions` is kept as an alias of `get_stages_aggregations`.
- New exceptions `ForbiddenError` (HTTP 403, subclass of `AuthError`), `NotFoundError` (HTTP 404, subclass of
  `APICompatibilityError`) and `BadRequestError` (HTTP 400 and other unmapped 4xx). All SDK exceptions now inherit
  from `RestreamError`. `ServerError` covers every 5xx status and carries `status_code`; `APIConcurrencyLimitError`
  carries the `Retry-After` delay.
- Reworked retry policy (`restreamsolutions.retry.RetryPolicy`) shared by requests, HTTP streams and WebSockets:
  credential and compatibility errors, 400/403/404, `ValueError` and `TypeError` are never retried; 401 (and 403 on
  a WebSocket handshake, which the server returns for rejected tokens) is retried once after a token refresh, which
  does not consume an attempt; 429 waits at least for `Retry-After` (capped by `RETRY_AFTER_MAX_SECONDS`); any other
  error (network failures, timeouts, 5xx, etc.) is retried with exponential backoff and jitter, with a single delay
  never exceeding `RETRY_MAX_DELAY_SECONDS`. POST requests are non-idempotent unless marked otherwise and are repeated
  only when the server could not have processed them (creating stage aggregations); acknowledging data changes is
  idempotent. WebSocket streams that keep closing without sending data reconnect with a growing delay. The number of
  attempts is configured by `MAX_RETRY_ATTEMPTS` in `constants.py` and the `RESTREAM_MAX_RETRY_ATTEMPTS`
  environment variable.
- Retries and reconnections are reported through the `logging` module (`restreamsolutions` logger) instead of
  `warnings`.

### Bug Fixes

- Security: a cached access token could be returned for a `client_id` without a valid `client_secret`. Credentials
  are now validated before the cache is consulted and a cached token is reused only for the credentials it was
  issued with.
- `Site.latitude`/`Site.longitude` were converted to `int`, truncating the coordinates returned by the API; they are
  now floats in decimal degrees. `Site` also declares `formation_name`, `crew` and `division` (`None` when not
  assigned); `Pad` declares `coil_tubing_enabled`.
- A string passed to the `fields` parameter of the data methods was split into single characters.
- `stage_number=0` was ignored by `get_stages_metadata`.
- Query parameters passed through `**filters` with the same name as a named filter (e.g. `lat`) were dropped.
- HTTP streams returned by `get_data()`/`aget_data()` no longer restart after they started delivering records, which
  produced duplicated records in the saved files.
- Requests rejected at the same time with the same token each requested a new token; now a single token request is
  made and the other requests reuse the new token.
- The concurrency limiter with a limit of 1 (token requests, WebSocket connections) could let two callers in at the
  same time when a new caller arrived while another one was still waiting.
- Synchronous GET/POST requests had no timeout. All regular requests (GET, POST, PATCH, PUT,
  DELETE) now use a total timeout of `REQUEST_TIMEOUT_SECONDS` in both the synchronous and the asynchronous API;
  the OAuth2 token request uses `AUTH_REQUEST_TIMEOUT_SECONDS`.

### Breaking Changes

- Python 3.11 or newer is required.
- `httpx` is no longer used: asynchronous requests, streaming and WebSockets are implemented with `aiohttp`, while
  synchronous code keeps using `requests` and `websocket-client`. Dependency version bounds were raised to exclude
  releases with known vulnerabilities.
- HTTP 403 raises `ForbiddenError` and HTTP 404 raises `NotFoundError` (both subclasses of the previously raised
  exceptions); a WebSocket handshake rejected with HTTP 403 still raises `AuthError`. Other 4xx responses raise
  `BadRequestError` instead of the HTTP client's own exception.
- `Communicator.steaming_get_generator`/`steaming_get_generator_async` were renamed to
  `streaming_get_generator`/`streaming_get_generator_async`; `Data`/`DataAsync` take a `retry_policy` and
  `restart_after_partial` instead of `initial_delay`, `factor` and `jitter`.
- `acreate_pad_parameters` was renamed to `acreate_stages_aggregations`.

## [v0.4.0] - 2026-03-18

### New Features

- Added `acreate_pad_parameters` method for creating pad parameters for a pad or site. Accepts a list of parameter
  definitions, each specifying a `name`, a `config` (including field, units, and aggregation type), and a `description`.

## [v0.3.1] - 2026-01-27

### Bug Fixes

- Fixed a bug in `get_stages_metadata` and `aget_stages_metadata` where `**filters` were not passed to the aggregations 
  request when `add_aggregations=True`.

## [v0.3.0] - 2025-12-09

### New Features

- Added `get_stages_aggregations_descriptions` and `aget_stages_aggregations_descriptions` methods for retrieving the
  list of available aggregations that are automatically calculated for each frac stage. The aggregation values can be
  obtained using the `get_stages_metadata` and `aget_stages_metadata` methods.

### Bug Fixes

- Fixed a bug in `get_stages_metadata` and `aget_stages_metadata` where an error occurred if the history list was empty.

## [v0.2.0] - 2025-11-25

### New Features

- Enabled streaming support for asynchronous requests.
- Extended the `Authorization` class to support token management and storage for multiple `client_id`/`client_secret`
  pairs, so previously issued tokens can be reused across sessions instead of being regenerated for every tool call.
- Added `ConcurrencyLimiter` and `AsyncConcurrencyLimiter` to track and enforce concurrency limits independently
  per client.

### Bug Fixes

- Fixed a bug where the SDK retried requests on `AuthError` even when an explicit token was provided; retries on
  authorization failure now only occur when token generation is handled by the SDK itself.

## [v0.1.1] - 2025-10-28

### New Features

- New `Authorization` class for OAuth2 client‑credentials flow with token caching.
- `get_realtime_data_changes_updates`/`get_realtime_instance_updates` (and their async versions) now support the 
`as_dict` parameter.

### Breaking Changes

- Methods `get_realtime_data_changes_updates`, `aget_realtime_data_changes_updates`, `get_realtime_instance_updates`,
`aget_realtime_instance_updates` now have an `as_dict` parameter. When `as_dict=False` (default), the `Data` and 
`DataAsync` objects returned by these methods yield model instances (`Site`, `Pad`, `DataChanges`) instead of Python 
`dict`. Use `as_dict=True` to receive raw dictionaries.
- The SDK now uses `RESTREAM_CLIENT_ID` and `RESTREAM_CLIENT_SECRET` environment variables to obtain an access token 
automatically when needed. The variable `RESTREAM_AUTH_TOKEN` is deprecated. You can optionally set `RESTREAM_HOST` to 
override the default host used for authorization.

## [v0.1.0] - 2025-10-23

### Miscellaneous

- First pre-release that is ready for testing