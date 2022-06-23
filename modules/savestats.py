from datetime import datetime
from logging import Logger
from typing import Type, List

from playwright.sync_api import Browser, BrowserContext, Page, Response

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class SaveStats(Module):
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS URLSFEEDBACK (rank INT NOT NULL, job INT NOT NULL, "
            "crawler INT NOT NULL, url TEXT NOT NULL, finalurl TEXT NOT NULL, depth INT NOT NULL, "
            "code INT NOT NULL, started TIMESTAMP NOT NULL, ended TIMESTAMP NOT NULL);",
            None, False)
        log.info('Create URLSFEEDBACK database IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: str, rank: int) -> None:
        self._rank = rank

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Response], context_database: DequeDB, url: str,
                         final_url: str, depth: int, start: List[datetime]) -> None:
        end: datetime = datetime.now()
        for i, response in enumerate(responses if len(responses) > 0 else [None]):
            self._database.invoke_transaction(
                "INSERT INTO URLSFEEDBACK VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (
                    self._rank, self.job_id, self.crawler_id, url, final_url, depth,
                    response.status if response is not None else -1,
                    start[i].strftime('%Y-%m-%d %H:%M:%S'),
                    end.strftime('%Y-%m-%d %H:%M:%S')), False)
