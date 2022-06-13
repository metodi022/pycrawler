import csv
from typing import Tuple, Iterator

from loader.loader import Loader


class CSVLoader(Loader):
    def __init__(self, source: str) -> None:
        self.source: str = source

    def __iter__(self) -> Iterator[Tuple[int, str]]:
        def result(source: str) -> Iterator[Tuple[int, str]]:
            with open(source, mode='r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f)
                for line in reader:
                    yield int(line[0], 10), line[1]

        return result(self.source)

    def __next__(self) -> Tuple[int, str]:
        raise NotImplementedError
