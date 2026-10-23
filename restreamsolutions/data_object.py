import asyncio
import json
import logging
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Generator, Optional

import aiofiles
import pandas as pd

from restreamsolutions.exceptions import APICompatibilityError
from restreamsolutions.retry import RetryPolicy

logger = logging.getLogger(__name__)


class BaseData:
    """Common utilities for data streaming classes."""

    def __init__(
        self,
        data_generator_factory: Callable[[], Any],
        restart_on_error: bool = False,
        restart_on_close: bool = False,
        convert_to: Any = None,
        auth_token: str = None,
        *,
        attempts: Optional[int] = None,
        retry_policy: Optional[RetryPolicy] = None,
        restart_after_partial: bool = True,
    ) -> None:
        """Store the streaming configuration shared by the sync and async wrappers.

        Args:
            data_generator_factory: A callable that returns a new (async) generator yielding dict items.
                A new generator must be created on each call so that the wrapper can reconnect.
            restart_on_error: If True, the underlying generator is recreated when a retryable error
                occurs and streaming continues. If False, the error is propagated.
            restart_on_close: If True, the underlying generator is also recreated when it completes
                normally (e.g. a WebSocket closes cleanly). If False, normal completion ends the stream.
                The reconnection delay grows while the server keeps closing the stream without sending items.
            convert_to: Optional class used to convert each yielded dict into a model instance.
            auth_token: Token passed to converted instances. If set to a non-empty value, authorization errors are
                never retried because the SDK cannot regenerate a user-provided token.
            attempts: Maximum number of consecutive runs of the underlying generator before an error
                is propagated. None means unlimited reconnection attempts.
            retry_policy: Policy that classifies errors and computes reconnection delays.
                Defaults to a policy without token refresh.
            restart_after_partial: If False, an error that happens after at least one item was yielded
                by the current run is propagated instead of restarting. Use it for finite HTTP streams
                where a restart would re-send items that were already delivered.
        """
        if attempts is not None and attempts < 1:
            raise ValueError("attempts must be >= 1 or None")
        self._data_generator_factory = data_generator_factory
        self._restart_on_error = restart_on_error
        self._restart_on_close = restart_on_close
        self._convert_to = convert_to
        self._auth_token = auth_token
        self._attempts = attempts
        self._retry_policy = retry_policy or RetryPolicy()
        self._restart_after_partial = restart_after_partial

    def _convert_item(self, item: dict, convert_if_needed: bool) -> Any:
        """Convert a raw item into the configured model class when requested.

        Args:
            item: Raw dict item yielded by the underlying generator.
            convert_if_needed: If False, the item is returned as-is even when convert_to is configured.

        Returns:
            The converted model instance or the original item.

        Raises:
            APICompatibilityError: If the item cannot be converted.
        """
        if not (self._convert_to and convert_if_needed):
            return item
        try:
            return self._convert_to(**item, auth_token=self._auth_token)
        except Exception as exc:
            raise APICompatibilityError(f'Failed to convert {item} to {self._convert_to}') from exc

    def _prepare_restart(
        self, exc: Exception, yielded_in_run: bool, failures: int, token_refreshed: bool, can_refresh: bool
    ) -> tuple[float, bool]:
        """Decide whether the stream may be restarted after an error and compute the delay.

        Args:
            exc: The exception raised by the underlying generator.
            yielded_in_run: True if the failed run delivered at least one item.
            failures: Number of consecutive failed runs including the current one, counted as if the current run
                is a regular failure. A run that failed with a refreshable 401 does not consume an attempt.
            token_refreshed: True if the access token was already refreshed during the current failure streak.
            can_refresh: True if the retry policy has a token refresh callback for the caller's execution mode.

        Returns:
            A tuple (delay_seconds, refresh_needed). When refresh_needed is True the caller must refresh
            the access token before the next run and the delay is zero.

        Raises:
            The original exception if the stream must not be restarted.
        """
        policy = self._retry_policy
        if not self._restart_on_error or (yielded_in_run and not self._restart_after_partial):
            raise exc
        if policy.can_refresh_token(exc, bool(self._auth_token)):
            # Refresh the SDK-managed token once per failure streak; a second 401 means the credentials are wrong.
            # The refresh corrects the request instead of repeating it, so it is allowed even when attempts is 1
            if token_refreshed or not can_refresh:
                raise exc
            logger.warning('Authorization failed, requesting a new access token and reconnecting')
            return 0.0, True
        if self._attempts is not None and failures >= self._attempts:
            raise exc
        if not policy.is_retryable(exc):
            raise exc
        delay = policy.delay_for(failures - 1, getattr(exc, 'retry_after', None))
        logger.warning('Got exception %r, reconnecting in %.2f seconds', exc, delay)
        return delay, False

    def _close_delay(self, empty_closes: int) -> float:
        """Return the delay before reconnecting after the server closed the stream.

        Args:
            empty_closes: Number of consecutive runs that were closed without yielding any item.

        Returns:
            Delay in seconds. It grows with empty_closes, so a server that keeps closing the stream
            without sending data is not reconnected to in a tight loop.
        """
        delay = self._retry_policy.delay_for(max(empty_closes - 1, 0))
        logger.warning('The connection was closed, reconnecting in %.2f seconds', delay)
        return delay

    @classmethod
    def _check_and_prepare_path(cls, path: str, overwrite: bool) -> Path:
        """Validate and prepare an output path for saving.

        - Ensures parent directories exist.
        - Verifies the extension is either .json or .csv.
        - Removes the existing file if overwrite=True; otherwise raises FileExistsError.

        Args:
            path: Destination file path as a string.
            overwrite: Whether to overwrite an existing file.

        Returns:
            A pathlib.Path instance.
        """
        path = Path(path)
        if path.suffix not in [".json", ".csv"]:
            raise ValueError(f"Invalid file extension '{path.suffix}', must be 'json' or 'csv'")
        if path.exists():
            if not overwrite:
                raise FileExistsError(f"File {path} already exists")
            else:
                path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _get_temp_json_path(json_path: Path) -> Path:
        """Build the path of the hidden temporary JSON file used while producing a CSV file.

        Args:
            json_path: Target file path.

        Returns:
            A path in the same directory with a leading dot and a .json extension.
        """
        return json_path.parent / f".{json_path.stem}.json"

    @staticmethod
    def convert_json_csv(json_path: Path, csv_path: Path):
        """Convert a JSON array file into a CSV file using pandas.

        Args:
            json_path: Source JSON file containing an array of objects.
            csv_path: Destination CSV file path.
        """
        df = pd.read_json(json_path, convert_dates=False)
        df.to_csv(csv_path, index=False)


class Data(BaseData):
    """Synchronous wrapper around a data generator that streams data from an HTTP or WebSocket source.

    It lets you process the incoming stream item by item without loading a potentially
    large response fully into memory and without opening a connection to the server
    until consumption actually begins. The class both exposes a fresh generator via
    the data_fetcher property and allows redirecting the stream to a file for
    persistence via save() in either JSON or CSV format.

    A factory must be provided that returns a new generator on each access. The
    generator should yield dictionaries.
    """

    data_fetcher: Generator[dict, None, None]

    @property
    def data_fetcher(self) -> Generator[dict, None, None]:
        """Return a fresh data generator that fetches data from the selected sites or pads.

        The returned generator is a wrapper over the underlying generator from the
        factory. If restart_on_error=True, it will recreate the underlying generator
        when a retryable exception occurs and continue yielding items. If restart_on_close=True,
        it will also recreate the generator when it completes normally (e.g., clean WebSocket close)
        and continue streaming.

        Returns:
            Generator that yields dictionaries (or converted model instances) representing records
            for sites or pads for a specific timestamp.
        """
        return self._data_fetcher(convert_if_needed=True)

    def _data_fetcher(self, convert_if_needed: bool = False) -> Generator[dict, None, None]:
        """Return a fresh data generator with the reconnection logic applied.

        Args:
            convert_if_needed: If True, items are converted with convert_to when it is configured.

        Returns:
            Generator that yields dictionaries representing records for sites or pads for a specific timestamp.
        """

        def _wrapper():
            """Iterate over the underlying generator, recreating it according to the restart settings.

            Yields:
                Items of the underlying generator, converted with convert_to when requested.
            """
            policy = self._retry_policy
            failures = 0
            empty_closes = 0
            token_refreshed = False
            while True:
                gen = self._data_generator_factory()
                yielded_in_run = False
                try:
                    for item in gen:
                        yielded_in_run = True
                        failures = 0
                        empty_closes = 0
                        token_refreshed = False
                        yield self._convert_item(item, convert_if_needed)
                    if not self._restart_on_close:
                        return
                    if not yielded_in_run:
                        empty_closes += 1
                    delay = self._close_delay(empty_closes)
                except Exception as exc:
                    delay, refresh_needed = self._prepare_restart(
                        exc, yielded_in_run, failures + 1, token_refreshed, policy.refresh_token is not None
                    )
                    if refresh_needed:
                        policy.refresh_token(getattr(exc, 'rejected_token', None))
                        token_refreshed = True
                    else:
                        # A run that ended with a refreshable 401 does not consume an attempt
                        failures += 1
                finally:
                    if hasattr(gen, 'close'):
                        gen.close()
                if delay > 0:
                    policy.sleep(delay)

        return _wrapper()

    def _save_json(self, path: Path) -> None:
        """Write the streamed items to a JSON file as a single array.

        Items are written one by one to avoid loading the entire dataset
        into memory at once.

        Args:
            path: Target file path (should have a .json extension).
        """
        try:
            with path.open("w", encoding='utf-8') as f:
                f.write('[')
                for i, item in enumerate(self._data_fetcher(convert_if_needed=False)):
                    if i > 0:
                        f.write(',\n')
                    f.write(json.dumps(item))
                f.write(']')
        except Exception as e:
            path.unlink(missing_ok=True)
            raise e

    def _save_csv(self, path: Path) -> None:
        """Write the streamed items to a temporary JSON file and then convert it to a CSV file.

        Args:
            path: Target file path (should have a .csv extension).
        """
        temp_json_path = self._get_temp_json_path(path)
        self._save_json(temp_json_path)
        try:
            self.__class__.convert_json_csv(temp_json_path, path)
        except Exception as e:
            path.unlink(missing_ok=True)
            raise e
        finally:
            temp_json_path.unlink(missing_ok=True)

    def save(self, path: str, overwrite: bool = False):
        """Save all selected pad/site data to a JSON or CSV file.

        The method writes all selected pad/sites data either to a JSON file (as a single
        JSON array of objects) or to a CSV file. Choose the output by providing a path with a .json or .csv extension.

        Parent directories are created if missing. If the target file
        exists and overwrite is False, a FileExistsError is raised. If any
        exception occurs during writing, the partially written file is removed.

        Args:
            path: Destination file path ending with .json or .csv.
            overwrite: If True, replaces an existing file; otherwise raises FileExistsError.
        """
        path = self.__class__._check_and_prepare_path(path, overwrite)
        if path.suffix == ".json":
            self._save_json(path)
        elif path.suffix == ".csv":
            self._save_csv(path)
        else:
            raise ValueError(f"Invalid file extension '{path.suffix}', must be 'json' or 'csv'")


class DataAsync(BaseData):
    """Asynchronous wrapper around an async data generator that streams data from an HTTP or WebSocket source.

    All operations are asynchronous: the connection to the server is not opened
    until consumption of the async stream begins, and items are handled one by one
    without loading a potentially large response fully into memory. The class exposes
    a fresh async generator via the data_fetcher property and allows redirecting the
    stream to a file using the asynchronous asave() method, in either JSON or CSV format.

    A factory must be provided that returns a new async generator on each access.
    The async generator should yield dictionaries. For CSV output, the header (field names)
    is derived from the keys of the first yielded item.
    """

    data_fetcher: AsyncGenerator[dict, None]

    @property
    def data_fetcher(self) -> AsyncGenerator[dict, None]:
        """Return a fresh async data generator that fetches data from the selected sites or pads.

        The returned async generator is a wrapper over the underlying generator from the
        factory. If restart_on_error=True, it will recreate the underlying generator when
        a retryable exception occurs and continue yielding items. If restart_on_close=True, it will also
        recreate the generator when it completes normally (e.g., clean WebSocket close) and continue streaming.

        Returns:
            Async generator that yields dictionaries (or converted model instances) representing records
            for sites or pads for a specific timestamp.
        """
        return self._data_fetcher(convert_if_needed=True)

    def _data_fetcher(self, convert_if_needed: bool = False) -> AsyncGenerator[dict, None]:
        """Return a fresh async data generator with the reconnection logic applied.

        Args:
            convert_if_needed: If True, items are converted with convert_to when it is configured.

        Returns:
            Async generator that yields dictionaries representing records for sites or pads for a specific timestamp.
        """

        async def _wrapper():
            """Iterate over the underlying async generator, recreating it according to the restart settings.

            Yields:
                Items of the underlying async generator, converted with convert_to when requested.
            """
            policy = self._retry_policy
            failures = 0
            empty_closes = 0
            token_refreshed = False
            while True:
                agen = self._data_generator_factory()
                yielded_in_run = False
                try:
                    async for item in agen:
                        yielded_in_run = True
                        failures = 0
                        empty_closes = 0
                        token_refreshed = False
                        yield self._convert_item(item, convert_if_needed)
                    if not self._restart_on_close:
                        return
                    if not yielded_in_run:
                        empty_closes += 1
                    delay = self._close_delay(empty_closes)
                except Exception as exc:
                    delay, refresh_needed = self._prepare_restart(
                        exc, yielded_in_run, failures + 1, token_refreshed, policy.arefresh_token is not None
                    )
                    if refresh_needed:
                        await policy.arefresh_token(getattr(exc, 'rejected_token', None))
                        token_refreshed = True
                    else:
                        # A run that ended with a refreshable 401 does not consume an attempt
                        failures += 1
                finally:
                    # Close the underlying async generator before reconnecting or when the consumer stops
                    # iterating, so that aiohttp sessions and websockets do not leak
                    if hasattr(agen, 'aclose'):
                        try:
                            await agen.aclose()
                        except Exception:
                            pass
                if delay > 0:
                    await policy.asleep(delay)

        return _wrapper()

    async def _asave_json(self, path: Path):
        """Asynchronously write the streamed items to a JSON file as a single array.

        Items are written incrementally to avoid holding the entire dataset in memory.

        Args:
            path: Target file path (should have a .json extension).
        """
        try:
            async with aiofiles.open(path, 'w', encoding='utf-8') as f:
                await f.write('[')
                first = True
                async for item in self._data_fetcher(convert_if_needed=False):
                    if not first:
                        await f.write(',\n')
                    await f.write(json.dumps(item))
                    first = False
                await f.write(']')
        except Exception as e:
            path.unlink(missing_ok=True)
            raise e

    async def _asave_csv(self, path: Path):
        """Asynchronously write the streamed items to a temporary JSON file and then convert it to a CSV file.

        Args:
            path: Target file path (should have a .csv extension).
        """
        temp_json_path = self._get_temp_json_path(path)
        await self._asave_json(temp_json_path)
        try:
            await asyncio.to_thread(self.__class__.convert_json_csv, temp_json_path, path)
        except Exception as e:
            path.unlink(missing_ok=True)
            raise e
        finally:
            temp_json_path.unlink(missing_ok=True)

    async def asave(self, path: str, overwrite: bool = False):
        """Asynchronously save all selected pad/site data to a JSON or CSV file.

        The method writes all selected pad/sites data either to a JSON file (as a single
        JSON array of objects) or to a CSV file. Choose the output by providing a path with a .json or .csv extension.

        Parent directories are created if missing. If the target file
        exists and overwrite is False, a FileExistsError is raised. If any
        exception occurs during writing, the partially written file is removed.

        Args:
            path: Destination file path ending with .json or .csv.
            overwrite: If True, replaces an existing file; otherwise raises FileExistsError.
        """
        path = self.__class__._check_and_prepare_path(path, overwrite)
        if path.suffix == ".json":
            await self._asave_json(path)
        elif path.suffix == ".csv":
            await self._asave_csv(path)
        else:
            raise ValueError(f"Invalid file extension '{path.suffix}', must be 'json' or 'csv'")
