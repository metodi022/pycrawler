from logging import Logger
from typing import Optional, Type

from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import wait_after_load


class AcceptCookies(Module):
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)
        self.url: str = ''

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, context_database: DequeDB, url: str,
                     rank: int) -> None:
        self.url = url

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Optional[Response],
                         context_database: DequeDB, url: str, final_url: str, depth: int) -> Optional[Response]:
        if self.url == url:
            check: str = "accept|okay|ok|consent|stimm|verstanden|versteh|akzeptier|agree|ja"
            buttons: Locator = page.locator('button:visible', has=page.locator(f"text=/{check}/i"))

            for i in range(buttons.count()):
                buttons.nth(i).click()
                wait_after_load(page, 1000)

            page.goto(final_url)
            wait_after_load(page, self._config.WAIT_AFTER_LOAD)

        return response
