from playwright.sync_api import Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log
from modules.module import Module
from typing import Tuple


class EmptyModule(Module):
    def __init__(self, database: Database, log: Log) -> None:
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, url: Tuple[str, int], ) -> None:
        pass

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response) -> None:
        pass
