from playwright.sync_api import Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log
from modules.module import Module
from typing import Tuple


class TestModule(Module):
    @staticmethod
    def initialize_data(database: Database) -> None:
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, url: Tuple[str, int], database: Database, log: Log) -> None:
        pass

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, database: Database, log: Log, response: Response) -> None:
        pass
