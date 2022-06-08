from collections import deque
from typing import Optional, Tuple, Deque, MutableSet


class DequeDB:
    def __init__(self) -> None:
        self._data: Deque[Tuple[str, int]] = deque()
        self._seen: MutableSet[str] = set()

    def get_url(self) -> Optional[Tuple[str, int]]:
        if len(self._data) == 0:
            self._seen.clear()
            return None

        return self._data.popleft()

    def add_seen(self, url: str):
        self._seen.add(url)
        if url[-1] == '/':
            self._seen.add(url[:-1])
        else:
            self._seen.add(url + '/')

    def add_url(self, url: str, depth: int) -> None:
        if url in self._seen or len(url) == 0:
            return

        self.add_seen(url)
        self._data.append((url, depth))
