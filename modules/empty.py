from logging import Logger
from typing import Type, List

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class Empty(Module):
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, context_database: DequeDB) -> None:
        return

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response,
                         context_database: DequeDB) -> None:
        return

    def filter_urls(self, urls: List[tld.utils.Result], context_database: DequeDB) -> List[tld.utils.Result]:
        return urls
