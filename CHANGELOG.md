# Changelog

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