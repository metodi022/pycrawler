from datetime import datetime
from typing import MutableSet, Optional

from config import Config
from peewee import BlobField, DateTimeField, ForeignKeyField, IntegerField, Model, PostgresqlDatabase, TextField


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

class Task(BaseModel):
    job = TextField()
    crawler = IntegerField(null=True)
    site = TextField()
    url = TextField()
    rank = IntegerField()
    state = TextField(default="free")
    code = IntegerField(null=True)
    error = TextField(null=True)
    crawlerState = BlobField(null=True, default=None)

class URL(BaseModel):
    task = ForeignKeyField(Task)
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    url = TextField()
    urlfinal = TextField(default=None, null=True)
    fromurl = ForeignKeyField("self", default=None, null=True, backref="children")
    depth = IntegerField()
    code = IntegerField(default=None, null=True)
    repetition = IntegerField()
    state = TextField(default="free")


class URLDB:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler
        self._seen: MutableSet[str] = set(URL.select(URL.url).where(URL.task==crawler.task, URL.repetition==1))

    def get_url(self, repetition: int) -> Optional[URL]:
        url: Optional[URL] = None

        query = [
            URL.task == self.crawler.task,
            URL.job == self.crawler.job_id,
            URL.crawler == self.crawler.crawler_id,
            URL.site == self.crawler.site,
            URL.repetition == repetition
        ]

        if (repetition == 1):
            if Config.BREADTHFIRST:
                query.append(URL.depth == self.crawler.depth)

            url = URL.select().where(*query, URL.state == "free").order_by(URL.created.asc()).first()
        else:
            url = URL.get_or_none(*query, url=self.crawler.currenturl, depth=self.crawler.depth, state="waiting")

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
        if self.get_seen(url) and (not force):
            return

        self.add_seen(url)

        url = (url.rstrip('/') + '/') if url[-1] == '/' else url

        url_data = {
            "task": self.crawler.task,
            "job": self.crawler.job_id,
            "crawler": self.crawler.crawler_id,
            "site": self.crawler.site,
            "url": url,
            "fromurl": fromurl,
            "depth": depth
        }

        with database.atomic():
            URL.create(**url_data, repetition=1)

            for repetition in range(2, Config.REPETITIONS + 1):
                URL.create(**url_data, repetition=repetition, state="waiting")
