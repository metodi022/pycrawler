from typing import Tuple, Iterator


class Loader:
    def __init__(self, source: str) -> None:
        """Initialize a loader for URLs.

        Args:
            source (str): path of source file with URLs
        """

    def __iter__(self) -> Iterator[Tuple[int, str]]:
        raise NotImplementedError

    def __next__(self) -> Tuple[int, str]:
        raise NotImplementedError
