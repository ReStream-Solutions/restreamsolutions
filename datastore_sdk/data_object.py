import json
from typing import Generator, AsyncGenerator, Callable
from pathlib import Path

import aiofiles


class Data:
    data_fetcher: Generator[dict, dict, None]

    def __init__(self, data_generator_factory: Callable[[], Generator[dict, dict, None]]) -> None:
        self._data_generator_factory = data_generator_factory

    @property
    def data_fetcher(self) -> Generator[dict, dict, None]:
        return self._data_generator_factory()

    def save(self, path: str, overwrite: bool=False):
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
    data_fetcher: AsyncGenerator[dict, None]

    def __init__(self, data_generator_factory: Callable[[], AsyncGenerator[dict, None]]) -> None:
        self._data_generator_factory = data_generator_factory

    @property
    def data_fetcher(self) -> AsyncGenerator[dict, None]:
        return self._data_generator_factory()

    async def asave(self, path: str, overwrite: bool = False):
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
