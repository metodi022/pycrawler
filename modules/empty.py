from playwright.sync_api import Browser, BrowserContext, Page, Response
from config import Config
from database.database import Database
from logging import Logger
from modules.module import Module
from typing import Tuple, Type, List
import tld


class Empty(Module):
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Database, log: Logger) -> None:
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, url: Tuple[str, int], ) -> None:
        pass

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response) -> None:
        pass

    def filter_urls(self, urls: List[tld.utils.Result]) -> List[tld.utils.Result]:
        return urls
