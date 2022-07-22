import re
from datetime import datetime
from logging import Logger
from typing import List, Tuple, Callable, Optional

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response

from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_url_full


class FindLogin(Module):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        pass

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB,
                     url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        pass

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime]) -> None:
        pass

    @staticmethod
    def add_url_filter_out(filters: List[Callable[[tld.utils.Result], bool]]):
        def filt(url: tld.utils.Result) -> bool:
            return re.match(r'log.?out|sign.?out|log.?off|sign.?off|exit|quit|invalidate',
                            get_url_full(url), re.I) is not None

        filters.append(filt)
