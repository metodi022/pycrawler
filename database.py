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
    fromurl = ForeignKeyField('self', default=None, null=True, backref='children')
    depth = IntegerField()
    code = IntegerField(default=None, null=True)
    repetition = IntegerField()
    state = TextField(default='free')


class URLDB:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler
        self._seen: MutableSet[str] = set()

    def get_active(self) -> int:
        return URL.select().where(URL.task == self.crawler.task,
                                  URL.job == self.crawler.job_id,
                                  URL.crawler == self.crawler.crawler_id,
                                  URL.site == self.crawler.site,
                                  URL.state != 'complete').count()

    def get_url(self, repetition: int) -> Optional[URL]:
        url: Optional[URL] = None

        if repetition == 1:
            if Config.BREADTHFIRST:
                url = URL.select().where(
                    URL.task == self.crawler.task,
                    URL.job == self.crawler.job_id,
                    URL.crawler == self.crawler.crawler_id,
                    URL.site == self.crawler.site,
                    URL.depth == self.crawler.depth,
                    URL.repetition == repetition,
                    URL.state == 'free'
                ).order_by(URL.created.asc()).first()


            url = url or URL.select().where(
                URL.task == self.crawler.task,
                URL.job == self.crawler.job_id,
                URL.crawler == self.crawler.crawler_id,
                URL.site == self.crawler.site,
                URL.repetition == repetition,
                URL.state == 'free'
            ).order_by(URL.created.asc()).first()
        else:
            url = URL.get_or_none(task=self.crawler.task,
                                  job=self.crawler.job_id,
                                  crawler=self.crawler.crawler_id,
                                  site=self.crawler.site,
                                  url=self.crawler.currenturl,
                                  depth=self.crawler.depth,
                                  repetition=repetition,
                                  state='waiting')

        if url is None:
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

    def add_url(self, url: str, depth: int, fromurl: Optional[URL], force: bool = False) -> None:
        if (url in self._seen) and (not force):
            return
        
        self.add_seen(url)

        with database.atomic():
            URL.create(task=self.crawler.task,
                       job=self.crawler.job_id,
                       crawler=self.crawler.crawler_id,
                       site=self.crawler.site,
                       url=url,
                       fromurl=fromurl,
                       depth=depth,
                       repetition=1)
            
            for repetition in range(2, Config.REPETITIONS + 1):
                URL.create(task=self.crawler.task,
                           job=self.crawler.job_id,
                           crawler=self.crawler.crawler_id,
                           site=self.crawler.site,
                           url=url,
                           fromurl=fromurl,
                           depth=depth,
                           repetition=repetition,
                           state='waiting')
