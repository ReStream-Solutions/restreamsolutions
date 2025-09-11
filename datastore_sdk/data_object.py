import json
from typing import Generator
from pathlib import Path


class Data:
    data_fetcher: Generator[dict, dict, None]

    def __init__(self, data_generator: Generator[dict, dict, None]) -> None:
        self.data_fetcher = data_generator

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