from datetime import datetime
from typing import Optional

from peewee import BlobField, DateTimeField, DeferredForeignKey, ForeignKeyField, IntegerField, Model, PostgresqlDatabase, TextField

import utils
from config import Config

database = PostgresqlDatabase(Config.DATABASE,
                              user=Config.USER,
                              password=Config.PASSWORD,
                              host=Config.HOST,
                              port=Config.PORT,
                              autorollback=True)


class BaseModel(Model):
    created = DateTimeField(default=datetime.now)
    updated = DateTimeField(default=datetime.now)

    def save(self, *args, **kwargs):
        self.updated = datetime.now()
        return super(BaseModel, self).save(*args, **kwargs)

    class Meta:
        database = database


class Site(BaseModel):
    site = TextField(primary_key=True, unique=True, index=True)
    bucket = IntegerField(default=None, null=True)
    rank = IntegerField(default=None, null=True)
    category = TextField(default=None, null=True)

class Task(BaseModel):
    job = TextField()
    crawler = IntegerField(null=True)
    site = ForeignKeyField(Site)
    landing = DeferredForeignKey("URL", default=None, null=True, backref='tasks')
    state = TextField(default="free")
    code = IntegerField(default=None, null=True)
    error = TextField(default=None, null=True)
    crawlerstate = BlobField(default=None, null=True)

class URL(BaseModel):
    task = ForeignKeyField(Task, backref='urls')
    site = ForeignKeyField(Site)
    url = TextField()
    urlfinal = TextField(default=None, null=True)
    scheme = TextField()
    fromurl = ForeignKeyField("self", default=None, null=True, backref="from_url")
    depth = IntegerField()
    code = IntegerField(default=None, null=True)
    repetition = IntegerField()
    state = TextField(default="free")


class URLDB:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler
        self._seen: set[str] = self.crawler.state.get('URLDB', set(URL.select(URL.url).where(URL.task==crawler.task, URL.repetition==1)))  # TODO check
        self.crawler.state['URLDB'] = self._seen

    def get_url(self, repetition: int) -> Optional[URL]:
        url: Optional[URL] = None

        query = [
            URL.task == self.crawler.task,
            URL.repetition == repetition
        ]

        if (repetition == 1):
            query.append(URL.state == "free")

            if Config.BREADTHFIRST:
                query.append(URL.depth == self.crawler.depth)

            url = URL.select().where(*query).order_by(URL.created.asc()).first()

            query = query[:-1]
            url = URL.select().where(*query).order_by(URL.depth.asc()).first() if not url else url
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

    def add_seen(self, url: str):
        url = url.rstrip('/')
        self._seen.add(url)
        self._seen.add(url + '/')

    def add_url(self, url: str, depth: int, fromurl: Optional[URL], force: bool = False) -> None:
        if (not force) and self.get_seen(url):
            return

        self.add_seen(url)

        url = (url.rstrip('/') + '/') if url[-1] == '/' else url

        site = utils.get_tld_object(url)
        if site is None:
            return

        site = Site.get_or_create(site=site.fld)[0]

        url_data = {
            "task": self.crawler.task,
            "site": site,
            "url": url,
            "scheme": url[:url.find(':')],
            "fromurl": fromurl,
            "depth": depth
        }

        with database.atomic():
            URL.create(**url_data, repetition=1)

            for repetition in range(2, Config.REPETITIONS + 1):
                URL.create(**url_data, repetition=repetition, state="waiting")

    def get_state(self, state: str) -> list[int]:
        return list(URL.select(URL.id).where(URL.task==self.crawler.task, URL.state==state))
