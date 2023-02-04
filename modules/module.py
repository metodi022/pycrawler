from datetime import datetime
from logging import Logger
from typing import List, Tuple, Callable, Optional, Dict, Any

import tld.utils
from playwright.sync_api import Browser, BrowserContext, Page, Response

from database import DequeDB
from utils import get_url_origin


class Module:
    """
    A baseclass from which all modules inherit.
    """

    def __init__(self, job_id: str, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        """
        Initializes module instance.

        Args:
            job_id (str): job id
            crawler_id (int): crawler id
            log (Logger): log
            state (Dict[str, Any]): state
        """
        self.job_id: str = job_id
        self.crawler_id: int = crawler_id
        self.site: str = ''
        self.origin: str = ''
        self.url: str = ''
        self.currenturl: str = ''
        self.depth: int = 0
        self.rank: int = 0
        self.ready: bool = False
        self._log: Logger = log
        self._state: Dict[str, Any] = state

    @staticmethod
    def register_job(log: Logger) -> None:
        """
        Initialize job preparations, for example creation of database.

        Args:
            log (Logger): log
        """
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List['Module']) -> None:
        """
        Add event handlers before navigating to a page.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            context_database (DequeDB): context database
            url (Tuple[str, int, int, List[Tuple[str, str]]]): URL, depth, rank, previous URL
            modules (List[Module]): list of modules currently active modules
        """
        if not self.ready:
            self.url = self._state.get('Module', url[0])
            self.site = tld.get_tld(self.url, as_object=True).fld
            self.origin = get_url_origin(tld.get_tld(self.url, as_object=True))
            self.rank = url[2]
            self._state['Module'] = self.url

        self.currenturl = url[0]
        self.depth = url[1]

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List['Module'], repetition: int) -> None:
        """
        Receive response from server.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            responses (List[Optional[Response]]): list of responses from crawler and modules
            context_database (DequeDB): context database
            url (Tuple[str, int, int, List[Tuple[str, str]]]): URL, depth, rank, previous URL
            final_url (str): final url after redirections
            start (List[datetime]): start times for crawl and for each module response initiation
            modules (List[Module]): list of modules currently active modules
            repetition (int): current URL visited repetition
        """
        self.ready = True

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        """
        Remove certain urls when gethering links. Add a filtering function to the list of existing
        filters.

        Args:
            filters (List[Callable[[tld.utils.Result], bool]]): shared list of already existing filters
        """
        pass
