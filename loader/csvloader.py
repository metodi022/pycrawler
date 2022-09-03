import csv
from pathlib import Path
from typing import Tuple, Iterator

from loader.loader import Loader


class CSVLoader(Loader):
    def __init__(self, source: Path) -> None:
        super().__init__(source)

    def __iter__(self) -> Iterator[Tuple[int, str]]:
        def result(source: Path) -> Iterator[Tuple[int, str]]:
            with open(source, mode='r', encoding='utf-8', newline='') as file:
                reader = csv.reader(file)
                for line in reader:
                    yield int(line[0], 10), line[1]

        return result(self._source)

    def __next__(self) -> Tuple[int, str]:
        raise NotImplementedError
