from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log
from modules.module import Module


class TestModule(Module):
    @staticmethod
    def initialize_data(database: Database) -> None:
        pass

    @staticmethod
    def add_handlers(browser: Browser, context: BrowserContext, page: Page, database: Database, log: Log) -> None:
        pass

    @staticmethod
    def receive_response(browser: Browser, context: BrowserContext, page: Page, database: Database, log: Log, response: Response) -> None:
        pass
