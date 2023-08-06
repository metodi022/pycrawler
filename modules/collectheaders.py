from asyncio import CancelledError
from logging import Logger
from typing import Optional

from peewee import CharField, ForeignKeyField, IntegerField, TextField
from playwright.sync_api import Response

from database import BaseModel, Task, URL, database
from modules.module import Module


class Header(BaseModel):
    task = ForeignKeyField(Task)
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    depth = IntegerField()
    repetition = IntegerField()
    frame = TextField()
    method = CharField()
    code = IntegerField()
    codetext = TextField()
    content = CharField(null=True)
    resource = CharField()
    fromurl = ForeignKeyField(URL)
    tourl = TextField()
    tourlfinal = TextField()
    headers = TextField(null=True)


class CollectHeaders(Module):
    """
    Module to automatically collect all headers.
    """

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create header table')
        with database:
            database.create_tables([Header])

    def add_handlers(self, url: URL) -> None:
        super().add_handlers(url)

        def handler(response: Response):
            headers: Optional[str] = None
            try:
                headers = str(response.headers_array())
            except (Exception, CancelledError):
                # Ignored
                pass

            try:
                Header.create(task=self.crawler.task,
                              job=self.crawler.job_id,
                              crawler=self.crawler.crawler_id,
                              site=self.crawler.site,
                              depth=self.crawler.depth,
                              repetition=self.crawler.repetition,
                              frame=response.frame.name,
                              method=response.request.method,
                              code=response.status,
                              codetext=response.status_text,
                              content=response.headers.get('content-type', None),
                              resource=response.request.resource_type,
                              fromurl=url,
                              tourl=response.request.url,
                              tourlfinal=response.url,
                              headers=headers)
            except (Exception, CancelledError):
                # Ignored
                pass

        # Register response handler
        self.crawler.page.on('response', handler)
