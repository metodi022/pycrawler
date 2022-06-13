from logging import Logger

from playwright.sync_api import Browser, BrowserContext, Page, Response

from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class Empty(Module):
    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, context_database: DequeDB,
                     url: str, rank: int) -> None:
        pass

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response,
                         context_database: DequeDB, url: str, depth: int) -> Response:
        return response
