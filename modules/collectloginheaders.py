from asyncio import CancelledError
from logging import Logger
from typing import List, Optional, Tuple

from playwright.sync_api import Response

from database import database
from modules.collectheaders import Header
from modules.login import Login


class CollectLoginHeaders(Login):
    def __init__(self, crawler) -> None:
        super().__init__(crawler)

        if not self.loginsuccess:
            self.crawler.stop = True

    @staticmethod
    def register_job(log: Logger) -> None:
        Login.register_job(log)

        log.info('Create login headers table')
        with database:
            database.create_tables([Header])

    def add_handlers(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        super().add_handlers(url)

        # Create response listener that saves all headers
        def handler(response: Response):
            headers: Optional[str]
            try:
                headers = str(response.headers_array())
            except (Exception, CancelledError) as error:
                self.crawler.log.warning(f"Get headers fail: {error}")
                headers = None

            Header.create(job=self.crawler.job_id, crawler=self.crawler.crawler_id,
                          site=self.crawler.site, url=self.crawler.url, depth=self.crawler.depth,
                          code=response.status, method=response.request.method,
                          content=response.headers.get('content-type', None),
                          resource=response.request.resource_type, fromurl=self.crawler.currenturl,
                          fromurlfinal=self.crawler.page.url, tourl=response.url, headers=headers,
                          repetition=self.crawler.repetition)

        # Register response handlers
        self.crawler.page.on('response', handler)
