from collections import deque
from datetime import datetime
from typing import Deque, List, MutableSet, Optional, Tuple

from peewee import DateTimeField, IntegerField, Model, PostgresqlDatabase, TextField

from config import Config

# Change the database to whatever you want
database = PostgresqlDatabase(Config.DATABASE,
                              user=Config.USER,
                              password=Config.PASSWORD,
                              host=Config.HOST,
                              port=Config.PORT,
                              autorollback=True)


class BaseModel(Model):
    created = DateTimeField(default=datetime.now)
    updated = DateTimeField(default=datetime.now)
    note = TextField(default=None, null=True)

    def save(self, *args, **kwargs):
        self.updated = datetime.now()
        return super(BaseModel, self).save(*args, **kwargs)

    class Meta:
        database = database


class URL(BaseModel):
    job = TextField()
    crawler = IntegerField(null=True)
    site = TextField()
    url = TextField()
    landing_page = TextField()
    rank = IntegerField()
    state = TextField(default='free')
    code = IntegerField(null=True)
    error = TextField(null=True)


class DequeDB:
    def __init__(self) -> None:
        self._data: Deque[Tuple[str, int, int, List[Tuple[str, str]]]] = deque()
        self._seen: MutableSet[str] = set()

    def get_url(self) -> Optional[Tuple[str, int, int, List[Tuple[str, str]]]]:
        if len(self._data) == 0:
            self._seen.clear()
            return None

        return self._data.popleft()

    def get_seen(self, url: str) -> bool:
        return url in self._seen

    def add_seen(self, url: str):
        self._seen.add(url)
        if url[-1] == '/':
            self._seen.add(url[:-1])
        else:
            self._seen.add(url + '/')

    def add_url(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        if url[0] in self._seen:
            return

        self.add_seen(url[0])
        self._data.append(url)

    def add_url_force(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        self.add_seen(url[0])
        self._data.append(url)

    def clear_urls(self) -> None:
        self._seen.clear()
        self._data.clear()

    def __len__(self):
        return len(self._data)
