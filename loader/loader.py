from pathlib import Path
from typing import Tuple, Iterator


class Loader:
    def __init__(self, source: Path) -> None:
        """Initialize a loader for URLs.

        Args:
            source (Path): path of file with URLs
        """
        self._source: Path = source

    def __iter__(self) -> Iterator[Tuple[int, str]]:
        raise NotImplementedError

    def __next__(self) -> Tuple[int, str]:
        raise NotImplementedError
