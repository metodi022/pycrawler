from collections import deque
from typing import Optional, Tuple, Deque, MutableSet
from database.database import Database


class DequeDB(Database):
    def __init__(self, job_id: int, crawler_id: int) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._data: Deque[Tuple[str, int]] = deque()
        self._seen: MutableSet[str] = set()

    def get_url(self, job_id: int, crawler_id: int) -> Optional[Tuple[str, int]]:
        if self.job_id != job_id or self.crawler_id != crawler_id:
            raise RuntimeError('Wrong job or crawler.')

        if len(self._data) == 0:
            return None

        return self._data.popleft()

    def add_url(self, job_id: int, url: str, depth: int) -> None:
        if self.job_id != job_id:
            raise RuntimeError('Wrong job or crawler.')

        if url in self._seen:
            return

        self._seen.add(url)
        self._data.append((url, depth))
