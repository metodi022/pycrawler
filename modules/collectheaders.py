import json
from logging import Logger
from typing import List, Optional, Tuple

from peewee import CharField, IntegerField, TextField
from playwright.sync_api import Browser, BrowserContext, Page, Response

from database import DequeDB, BaseModel, database
from modules.module import Module


class Header(BaseModel):
    rank = IntegerField()
    job = IntegerField()
    crawler = IntegerField()
    site = CharField()
    depth = IntegerField()
    code = IntegerField()
    method = CharField()
    type = CharField()
    fromurl = CharField()
    fromurlfinal = CharField()
    tourl = CharField()
    headers = TextField()


class CollectHeaders(Module):
    """
    Module to automatically collect all headers.
    """

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create header table')
        with database:
            database.create_tables([Header])

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List['Module']) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        def handler(response: Response):
            headers: Optional[str]
            try:
                headers = json.dumps(response.headers_array())
            except ValueError:
                headers = None

            Header.create(rank=self.rank, job=self.job_id, crawler=self.crawler_id, site=self.site,
                          depth=self.depth, code=response.status, method=response.request.method,
                          type=response.headers.get('content-type', response.request.resource_type),
                          fromurl=self.currenturl, fromurlfinal=response.frame.url,
                          tourl=response.url, headers=headers)

        page.on('response', handler)
