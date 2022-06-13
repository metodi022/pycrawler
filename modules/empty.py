from logging import Logger
from typing import Optional

from playwright.sync_api import Browser, BrowserContext, Page, Response

from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class Empty(Module):
    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, context_database: DequeDB,
                     url: str, rank: int) -> None:
        # Empty
        pass

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Optional[Response],
                         context_database: DequeDB, url: str, depth: int) -> Optional[Response]:
        # Empty
        return response
