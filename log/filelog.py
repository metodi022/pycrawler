from datetime import datetime
from .log import Log


class FileLog(Log):
    def __init__(self, job_id: int, crawler_id: int, path: str) -> None:
        self._job_id = job_id
        self._crawler_id = crawler_id
        self._path = path
        self._file = open(path, mode='a')

    def append(self, message: str) -> None:
        self._file.write(
            f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} Job {self._job_id} Crawler {self._crawler_id} {message}\n")

    def close(self) -> None:
        self._file.close()
