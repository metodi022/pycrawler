import json
from logging import Logger
from typing import List, Optional, Tuple

from playwright.sync_api import Browser, BrowserContext, Page, Response

from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class CollectHeaders(Module):
    """
    Module to automatically collect all headers.
    """

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS HEADERS (rank INT NOT NULL, job INT NOT NULL,"
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, fromurl TEXT NOT NULL, "
            "fromurlfinal TEXT NOT NULL, tourl TEXT NOT NULL, code INT NOT NULL, headers TEXT)",
            None, False)
        log.info('Create HEADERS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List['Module']) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        # TODO improve with response information as well

        def handler(response: Response):
            headers: Optional[str]
            try:
                headers = json.dumps(response.headers_array())
            except ValueError:
                headers = None

            self._database.invoke_transaction(
                "INSERT INTO HEADERS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                    self.rank, self.job_id, self.crawler_id, self.domainurl, self.currenturl,
                    page.url, response.url, response.status, headers), False)

        page.on('response', handler)
