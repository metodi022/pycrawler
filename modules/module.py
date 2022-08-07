from datetime import datetime
from logging import Logger
from typing import List, Tuple, Callable, Optional

import tld.utils
from playwright.sync_api import Browser, BrowserContext, Page, Response

from database.dequedb import DequeDB
from database.postgres import Postgres


class Module:
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        """Initializes module instance.

        Args:
            job_id (int): job id
            crawler_id (int): crawler id
            database (Postgres): database
            log (Logger): log
        """
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._database: Postgres = database
        self._log: Logger = log

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        """Initialize job preparations, for example creation of database.

        Args:
            database (Postgres): database
            log (Logger): log
        """
        raise NotImplementedError

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB,
                     url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        """Add event handlers before navigating to a page.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            context_database (DequeDB): context database
            url (Tuple[str, int, int, List[Tuple[str, str]]]): URL, depth, rank, previous URL
        """
        raise NotImplementedError

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime]) -> None:
        """Receive response from server.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            responses (List[Optional[Response]]): list of responses from crawler and modules
            context_database (DequeDB): context database
            url (Tuple[str, int, int, List[Tuple[str, str]]]): URL, depth, rank, previous URL
            final_url (str): final url after redirections
            start (List[datetime]): start times for crawl and for each module response initiation
        """
        raise NotImplementedError

    @staticmethod
    def add_url_filter_out(filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        pass
