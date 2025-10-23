import asyncio
import json
import time
import warnings
from typing import Generator, AsyncGenerator, Callable
from pathlib import Path

import aiofiles
import pandas as pd

from datastore_sdk.exceptions import AuthError, APICompatibilityError, APIConcurrencyLimitError


class BaseData:
    """Common utilities for data streaming classes."""

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
        return json_path.parent / f".{json_path.stem}.json"

    @staticmethod
    def convert_json_csv(json_path: Path, csv_path: Path):
        df = pd.read_json(json_path, convert_dates=False)
        df.to_csv(csv_path, index=False)


class Data(BaseData):
    """Synchronous wrapper around a data generator that streams data from an HTTP source.

    It lets you process the incoming stream item by item without loading a potentially
    large response fully into memory and without opening a connection to the server
    until consumption actually begins. The class both exposes a fresh generator via
    the data_fetcher property and allows redirecting the stream to a file for
    persistence via save() in either JSON or CSV format.

    A factory must be provided that returns a new generator on each access. The
    generator should yield dictionaries.
    """

    data_fetcher: Generator[dict, None, None]

    def __init__(
        self,
        data_generator_factory: Callable[[], Generator[dict, None, None]],
        restart_on_error: bool = False,
        restart_on_close: bool = False,
    ) -> None:
        """Initialize the Data wrapper.

        Args:
            data_generator_factory: A callable that returns a new generator which
                yields dict items to be saved or processed. A new generator should be created
                on each call so that the instance of the Data class could be reusable.
            restart_on_error: If True, the data_fetcher will automatically recreate the
                underlying generator via the factory when an error occurs during iteration,
                and continue yielding subsequent items. If False, the error is propagated.
            restart_on_close: If True, the wrapper will also recreate the underlying generator
                when it completes normally (e.g., a WebSocket closes cleanly) and continue
                streaming. If False, normal completion will finish the stream.
        """
        self._data_generator_factory = data_generator_factory
        self._restart_on_error = restart_on_error
        self._restart_on_close = restart_on_close

    @property
    def data_fetcher(self) -> Generator[dict, None, None]:
        """Return a fresh data generator that fetches data from the selected sites or pads.

        The returned generator is a wrapper over the underlying generator from the
        factory. If restart_on_error=True, it will recreate the underlying generator
        when an exception occurs and continue yielding items. If restart_on_close=True,
        it will also recreate the generator when it completes normally (e.g., clean WebSocket close)
        and continue streaming.

        Returns:
            Generator that yields dictionaries representing records for sites or pads for a specific timestamp.
        """

        def _wrapper():
            while True:
                gen = self._data_generator_factory()
                try:
                    for item in gen:
                        yield item
                    if not self._restart_on_close:
                        break  # normal completion
                except (AuthError, APICompatibilityError, APIConcurrencyLimitError):
                    # Do not retry on these errors
                    raise
                except Exception as e:
                    if not self._restart_on_error:
                        raise
                    # else: recreate and continue loop
                    warnings.warn(f"Got exception: {e}, reconnecting...", RuntimeWarning)
                    time.sleep(1)
                    continue
                warnings.warn(f"The connection was closed. Reconnecting", RuntimeWarning)

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
                for i, item in enumerate(self.data_fetcher):
                    if i > 0:
                        f.write(',\n')
                    f.write(json.dumps(item))
                f.write(']')
        except Exception as e:
            path.unlink(missing_ok=True)
            raise e

    def _save_csv(self, path: Path) -> None:
        """Write the streamed items to a JSON file and then convert it to a CSV file.

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
    """Asynchronous wrapper around an async data generator that streams data from an HTTP source.

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

    def __init__(
        self,
        data_generator_factory: Callable[[], AsyncGenerator[dict, None]],
        restart_on_error: bool = False,
        restart_on_close: bool = False,
    ) -> None:
        """Initialize the asynchronous Data wrapper.

        Args:
            data_generator_factory: A callable that returns a new async generator which
                yields dict items to be saved or processed. A new generator should be created
                on each call so that the instance of the DataAsync class could be reusable.
            restart_on_error: If True, the data_fetcher will automatically recreate the
                underlying async generator via the factory when an error occurs during
                iteration, and continue yielding subsequent items. If False, the error is propagated.
            restart_on_close: If True, the wrapper will also recreate the underlying async generator
                when it completes normally (e.g., a WebSocket closes cleanly) and continue
                streaming. If False, normal completion will finish the stream.
        """
        self._data_generator_factory = data_generator_factory
        self._restart_on_error = restart_on_error
        self._restart_on_close = restart_on_close

    @property
    def data_fetcher(self) -> AsyncGenerator[dict, None]:
        """Return a fresh  async data generator that fetches data from the selected sites or pads.

        The returned async generator is a wrapper over the underlying generator from the
        factory. If restart_on_error=True, it will recreate the underlying generator when
        an exception occurs and continue yielding items. If restart_on_close=True, it will also
        recreate the generator when it completes normally (e.g., clean WebSocket close) and continue streaming.

        Returns:
            Async Generator that yields dictionaries representing records for sites or pads for a specific timestamp.
        """

        async def _wrapper():
            while True:
                agen = self._data_generator_factory()
                try:
                    async for item in agen:
                        yield item
                    if not self._restart_on_close:
                        break  # normal completion
                except (AuthError, APICompatibilityError, APIConcurrencyLimitError):
                    # Do not retry on these errors
                    raise
                except Exception as e:
                    if not self._restart_on_error:
                        raise
                    # else: recreate and continue loop
                    warnings.warn(f"Got exception: {e}, reconnecting...", RuntimeWarning)
                    await asyncio.sleep(1)
                    continue
                finally:
                    # Ensure the underlying async generator is properly closed when the
                    # consumer stops iterating or when we plan to reconnect. This avoids
                    # leaking aiohttp sessions/websockets and prevents 'Unclosed client session'.
                    try:
                        await agen.aclose()
                    except Exception:
                        pass
                warnings.warn(f"The connection was closed. Reconnecting", RuntimeWarning)

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
                async for item in self.data_fetcher:
                    if not first:
                        await f.write(',\n')
                    await f.write(json.dumps(item))
                    first = False
                await f.write(']')
        except Exception as e:
            path.unlink(missing_ok=True)
            raise e

    async def _asave_csv(self, path: Path):
        """Asynchronously write the streamed items to a JSON file and then convert it to a CSV file.

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
