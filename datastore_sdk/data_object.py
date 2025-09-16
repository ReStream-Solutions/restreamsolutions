import json
from typing import Generator, AsyncGenerator, Callable
from pathlib import Path

import aiofiles


class Data:
    """Synchronous wrapper around a data generator that streams JSON data from an HTTP source.

    It lets you process the incoming stream item by item without loading a potentially
    large response fully into memory and without opening a connection to the server
    until consumption actually begins. The class both exposes a fresh generator via
    the data_fetcher property and allows redirecting the stream to a file for
    persistence via save().

    A factory must be provided that returns a new generator on each access. The
    generator should yield dictionaries.
    """

    data_fetcher: Generator[dict, dict, None]

    def __init__(self, data_generator_factory: Callable[[], Generator[dict, dict, None]]) -> None:
        """Initialize the Data wrapper.

        Args:
            data_generator_factory: A callable that returns a new generator which
                yields dict items to be saved or processed. A new generator should be created
                on each call so that the instance of the Data class could be reusable.
        """
        self._data_generator_factory = data_generator_factory

    @property
    def data_fetcher(self) -> Generator[dict, dict, None]:
        """Return a fresh data generator that fetches data from the selected sites or pads.

        Returns:
            Generator that yields dictionaries representing records for sites or pads for a specific timestamp.
        """
        return self._data_generator_factory()

    def save(self, path: str, overwrite: bool=False):
        """Save all selected pad/site data to a JSON file.

        The method writes all selected pad/sites data to a JSON file. The JSON file will have
        a list of dictionaries representing records for a specific site/pad and a timestamp.

        Parent directories are created if missing. If the target file
        exists and overwrite is False, a FileExistsError is raised. If any
        exception occurs during writing, the partially written file is removed.

        Args:
            path: Destination file path.
            overwrite: If True, replaces an existing file; otherwise raises FileExistsError exception.
        """
        path = Path(path)
        if path.exists():
            if not overwrite:
                raise FileExistsError(f"File {path} already exists")
            else:
                path.unlink()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
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


class DataAsync:
    """Asynchronous wrapper around an async data generator that streams JSON data from an HTTP source.

    All operations are asynchronous: the connection to the server is not opened
    until consumption of the async stream begins, and items are handled one by one
    without loading a potentially large response fully into memory. The class exposes
    a fresh async generator via the data_fetcher property and allows redirecting the
    stream to a file using the asynchronous asave() method.

    A factory must be provided that returns a new async generator on each access.
    The async generator should yield dictionaries.
    """

    data_fetcher: AsyncGenerator[dict, None]

    def __init__(self, data_generator_factory: Callable[[], AsyncGenerator[dict, None]]) -> None:
        """Initialize the asynchronous Data wrapper.

        Args:
            data_generator_factory: A callable that returns a new async generator which
                yields dict items to be saved or processed. A new generator should be created
                on each call so that the instance of the DataAsync class could be reusable.
        """
        self._data_generator_factory = data_generator_factory

    @property
    def data_fetcher(self) -> AsyncGenerator[dict, None]:
        """Return a fresh  async data generator that fetches data from the selected sites or pads.

        Returns:
            Async Generator that yields dictionaries representing records for sites or pads for a specific timestamp.
        """
        return self._data_generator_factory()

    async def asave(self, path: str, overwrite: bool = False):
        """Asynchronously save all selected pad/site data to a JSON file.

        The method writes all selected pad/sites data to a JSON file. The JSON file will have
        a list of dictionaries representing records for a specific site/pad and a timestamp.

        Parent directories are created if missing. If the target file
        exists and overwrite is False, a FileExistsError is raised. If any
        exception occurs during writing, the partially written file is removed.

        Args:
            path: Destination file path.
            overwrite: If True, replaces an existing file; otherwise raises FileExistsError exception.
        """
        path = Path(path)
        if path.exists():
            if not overwrite:
                raise FileExistsError(f"File {path} already exists")
            else:
                path.unlink()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(path, "w", encoding='utf-8') as f:
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
