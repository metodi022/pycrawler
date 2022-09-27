import json
from datetime import datetime
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

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS HEADERS (rank INT NOT NULL, job INT NOT NULL,"
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, fromurl TEXT NOT NULL, "
            "tourl TEXT NOT NULL, code INT NOT NULL, headers TEXT)",
            None, False)
        log.info('Create HEADERS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List['Module']) -> None:
        self._url = url[0]
        self._rank = url[2]

        def handler(response: Response):
            headers: Optional[str]
            try:
                headers = json.dumps(response.all_headers())
            except ValueError:
                headers = None

            self._database.invoke_transaction(
                "INSERT INTO HEADERS VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (
                    self._rank, self.job_id, self.crawler_id, self._url, page.url, response.url,
                    response.status, headers), False)

        page.on('response', handler)

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List['Module'], repetition: int) -> None:
        pass
