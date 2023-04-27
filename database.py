from datetime import datetime
from typing import MutableSet, Optional

from peewee import DateTimeField, ForeignKeyField, IntegerField, Model, PostgresqlDatabase, TextField

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


class Task(BaseModel):
    job = TextField()
    crawler = IntegerField(null=True)
    site = TextField()
    url = TextField()
    landing_page = TextField()
    rank = IntegerField()
    state = TextField(default='free')
    code = IntegerField(null=True)
    error = TextField(null=True)


class URL(BaseModel):
    task = ForeignKeyField(Task)
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    url = TextField()
    urlfinal = TextField(default=None, null=True)
    fromurl = TextField(null=True)
    fromurlfinal = TextField(null=True)
    depth = IntegerField()
    code = IntegerField(default=None, null=True)
    repetition = IntegerField()
    start = DateTimeField()
    end = DateTimeField()
    state = TextField(default='free')


class URLDB:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler
        self._seen: MutableSet[str] = set()

    def get_url(self) -> Optional[URL]:
        url: Optional[URL] = URL.get_or_none(job=self.crawler.job_id,
                                             crawler=self.crawler.crawler_id,
                                             site=self.crawler.site,
                                             repetition=self.crawler.repetition,
                                             state=('free' if self.crawler.repetition == 1 else 'waiting'))
        
        if self.crawler.repetition > 1:
            assert(url is not None)

        if url is None:
            self._seen.clear()
            return None
        
        url.state = 'progress'
        url.save()
        return url

    def get_seen(self, url: str) -> bool:
        return url in self._seen

    def add_seen(self, url: str):
        self._seen.add(url)
        if url[-1] == '/':
            self._seen.add(url[:-1])
        else:
            self._seen.add(url + '/')

    def add_url(self, url: str, fromurl: Optional[str], fromurlfinal: Optional[str], force: bool = False) -> None:
        if url[0] in self._seen and not force:
            return
        
        self.add_seen(url)

        with database.atomic():
            URL.create(task=self.crawler.task,
                       job=self.crawler.job_id,
                       crawler=self.crawler.crawler_id,
                       site=self.crawler.site,
                       url=self.crawler.url,
                       fromurl=fromurl,
                       fromurlfinal=fromurlfinal,
                       depth=(self.crawler.depth+1),
                       repetition=1)
            
            for repetition in range(2, Config.REPETITIONS):
                URL.create(task=self.crawler.task,
                           job=self.crawler.job_id,
                           crawler=self.crawler.crawler_id,
                           site=self.crawler.site,
                           url=self.crawler.url,
                           fromurl=fromurl,
                           fromurlfinal=fromurlfinal,
                           depth=(self.crawler.depth+1),
                           repetition=repetition,
                           state='waiting')

    def __len__(self) -> int:
        return URL.select().where(URL.job==self.crawler.job_id, URL.crawler==self.crawler.crawler_id, URL.site==self.crawler.site, URL.state=='free').count()
    