from logging import Logger
from typing import List, Optional, Tuple

from peewee import CharField, IntegerField, TextField
from playwright.sync_api import Browser, BrowserContext, Page, Response

from database import BaseModel, DequeDB, database
from modules.module import Module


class Header(BaseModel):
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    url = TextField()
    depth = IntegerField()
    code = IntegerField()
    method = CharField()
    content = CharField(null=True)
    resource = CharField()
    fromurl = TextField()
    fromurlfinal = TextField()
    tourl = TextField()
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

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List['Module']) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        def handler(response: Response):
            headers: Optional[str]
            try:
                headers = str(response.headers_array())
            except Exception as error:
                self._log.warning(f"Get headers fail: {error}")
                headers = None

            Header.create(job=self.job_id, crawler=self.crawler_id, site=self.site, url=self.url,
                          depth=self.depth, code=response.status, method=response.request.method,
                          content=response.headers.get('content-type', None),
                          resource=response.request.resource_type,
                          fromurl=self.currenturl, fromurlfinal=page.url, tourl=response.url,
                          headers=headers)

        page.on('response', handler)
