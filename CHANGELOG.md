# Changelog

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