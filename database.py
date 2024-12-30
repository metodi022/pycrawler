from datetime import datetime
from typing import Optional

from peewee import BlobField, CharField, DateTimeField, DeferredForeignKey, ForeignKeyField, IntegerField, Model, PostgresqlDatabase, TextField

import utils
from config import Config

database = PostgresqlDatabase(
    Config.DATABASE,
    user=Config.USER,
    password=Config.PASSWORD,
    host=Config.HOST,
    port=Config.PORT,
    autorollback=True
)


class BaseModel(Model):
    created = DateTimeField(default=datetime.now)
    updated = DateTimeField(default=datetime.now)

    def save(self, *args, **kwargs):
        self.updated = datetime.now()
        return super(BaseModel, self).save(*args, **kwargs)

    class Meta:
        database = database


class Site(BaseModel):
    scheme = CharField(primary_key=True, index=True)
    site = TextField(primary_key=True, index=True)
    rank = IntegerField(default=None, null=True, index=True)
    categories = TextField(default=None, null=True)

class Task(BaseModel):
    job = TextField(index=True)
    crawler = IntegerField(default=None, null=True, index=True)
    site = ForeignKeyField(Site, index=True)
    landing = DeferredForeignKey("URL", default=None, null=True, backref='tasks', index=True)
    state = TextField(default="free", index=True)
    code = IntegerField(default=None, null=True, index=True)
    error = TextField(default=None, null=True)
    crawlerstate = BlobField(default=None, null=True)

class URL(BaseModel):
    task = ForeignKeyField(Task, backref='urls', index=True)
    site = ForeignKeyField(Site, index=True)
    url = TextField(index=True)
    urlfinal = TextField(default=None, null=True)
    fromurl = ForeignKeyField("self", default=None, null=True, backref="from_url", index=True)
    depth = IntegerField(index=True)
    code = IntegerField(default=None, null=True, index=True)
    repetition = IntegerField(index=True)
    state = TextField(default="free", index=True)


class URLDB:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler

        self._seen: set[str] = self.crawler.state.get('URLDB', set())
        self.crawler.state['URLDB'] = self._seen

    def get_url(self, repetition: int) -> Optional[URL]:
        url: Optional[URL] = None

        query = [
            URL.task == self.crawler.task,
            URL.repetition == repetition
        ]

        if repetition == 1:
            query.append(URL.state == "free")

            if Config.BREADTHFIRST:
                query.append(URL.depth == self.crawler.depth)

            url = URL.get_or_none(*query)

            if (not url) and Config.BREADTHFIRST:
                url = URL.get_or_none(*(query[:-1]))
        else:
            query.append(URL.state == "waiting")
            query.append(URL.url == self.crawler.url.url)
            query.append(URL.depth == self.crawler.depth)
            url = URL.get_or_none(*query)

        if url:
            url.state = "progress"
            url.save()

        return url

    def get_seen(self, url: str) -> bool:
        url = url.rstrip('/')
        return (url in self._seen) or ((url + '/') in self._seen)

    def add_seen(self, url: str, query_fragment: bool):
        if query_fragment:
            self._seen.add(url)
        else:
            url = url.rstrip('/')
            self._seen.add(url)
            self._seen.add(url + '/')

    def add_url(self, url: str, depth: int, query_fragment: bool, fromurl: Optional[URL], force: bool = False) -> None:
        if (not force) and self.get_seen(url):
            return

        self.add_seen(url, query_fragment)

        url_parsed = utils.get_tld_object(url)
        if url_parsed is None:
            return

        site = Site.get_or_create(
            scheme=utils.get_url_scheme(url_parsed),
            site=utils.get_url_site(url_parsed)
        )[0]

        url_data = {
            "task": self.crawler.task,
            "site": site,
            "url": url,
            "fromurl": fromurl,
            "depth": depth
        }

        with database.atomic():
            URL.create(**url_data, repetition=1)

            for repetition in range(2, Config.REPETITIONS + 1):
                URL.create(**url_data, repetition=repetition, state="waiting")

    def get_state(self, state: str) -> list[int]:
        return list(URL.select(URL.id).where(URL.task==self.crawler.task, URL.state==state))
