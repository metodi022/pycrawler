import os
import pickle
from datetime import datetime
from logging import Logger
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

import tld
from playwright.sync_api import Browser, BrowserContext, Error, Page, Playwright, Response, sync_playwright

from config import Config
from database import URL, DequeDB
from modules.acceptcookies import AcceptCookies
from modules.collecturls import CollectURLs
from modules.module import Module
from utils import get_screenshot, get_url_origin


class Crawler:
    def __init__(self, job_id: str, crawler_id: int, url: str, rank: int, log: Logger,
                 modules: List[Type[Module]]) -> None:
        # Prepare variables
        self.log: Logger = log
        self.job_id: str = job_id
        self.crawler_id: int = crawler_id
        self.state: Dict[str, Any] = {}

        # Load previous state
        if Config.RESTART and (Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache").exists():
            self.log.debug("Loading old cache")
            with open(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache", mode="rb") as file:
                self.state = pickle.load(file)

        # Prepare rest of variables
        self.url: str = self.state.get('Module', url)
        self.state['Module'] = self.url
        self.scheme: str = 'https' if url.startswith('https') else 'http'
        self.site: str = tld.get_tld(self.url, as_object=True).fld
        self.origin: str = get_url_origin(tld.get_tld(self.url, as_object=True))
        self.currenturl: str = url

        self.rank: int = rank
        self.depth: int = 0
        self.repetition: int = 1

        self.stop: bool = False

        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.context_database: DequeDB = DequeDB()

        if 'DequeDB' in self.state:
            self.context_database._data = self.state['DequeDB'][0]
            self.context_database._seen = self.state['DequeDB'][1]
        else:
            self.state['DequeDB'] = [self.context_database._data, self.context_database._seen]

        self.context_database.add_url((self.url, 0, self.rank, []))

        # Prepare modules
        self.modules: List[Module] = []
        self.modules += [AcceptCookies(self)] if Config.ACCEPT_COOKIES else []
        self.modules += [CollectURLs(self)] if Config.RECURSIVE else []
        for module in modules:
            self.modules.append(module(self))
        self.log.debug(f"Prepared modules: {self.modules}")

        # Prepare filters
        url_filter_out: List[Callable[[tld.utils.Result], bool]] = []
        for module in self.modules:
            module.add_url_filter_out(url_filter_out)
        self.log.debug("Prepared filters")

    def start_crawl(self):
        # Stop crawler earlier if stop flag is set
        if self.stop:
            if Config.RESTART and (Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache").exists():
                self.log.debug("Deleting cache")
                os.remove(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
            return

        if self.url is None:
            self.log.info("Get URL None")
            return

        # Initiate playwright, browser, context, and page
        self.playwright = sync_playwright().start()

        if Config.BROWSER == 'firefox':
            self.browser = self.playwright.firefox.launch(headless=Config.HEADLESS)
        elif Config.BROWSER == 'webkit':
            self.browser = self.playwright.webkit.launch(headless=Config.HEADLESS)
        else:
            self.browser = self.playwright.chromium.launch(headless=Config.HEADLESS)

        self.log.debug(f"Start {Config.BROWSER.capitalize()} {self.browser.version}")

        self.context = self.browser.new_context(
            storage_state=self.state.get('Context', None),
            **self.playwright.devices[Config.DEVICE],
            locale=Config.LOCALE,
            timezone_id=Config.TIMEZONE
        )

        self.page = self.context.new_page()

        url: Optional[Tuple[str, int, int, List[Tuple[str, str]]]] = self.context_database.get_url()
        self.log.info(f"Get URL {url[0] if url is not None else url}")

        # Main loop
        while url is not None and not self.stop:
            # Update variables
            self.currenturl = url[0]
            self.depth = url[1]

            # Initiate modules
            self._invoke_page_handler(url)

            # Repetition loop
            for repetition in range(1, Config.REPETITIONS + 1):
                self.repetition = repetition

                # Navigate to page
                response: Optional[Response] = self._open_url(url)
                self.log.info(f"Response status {response if response is None else response.status} repetition {repetition + 1}")

                # Wait after page is loaded
                self.page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                get_screenshot(self.page, (Config.LOG / f"screenshots/job{self.job_id}-{tld.get_tld(self.url, as_object=True).fld}.png"), False)

                # Run modules response handler
                self._invoke_response_handler([response], url, [datetime.now()], repetition + 1)

            # Get next URL to crawl
            url = self.context_database.get_url()
            self.log.info(f"Get URL {url[0] if url is not None else url}")

            # Save state
            try:
                self.state['Context'] = self.context.storage_state()
            except Exception as error:
                self.log.warning(f"Get context fail: {error}")

            if Config.RESTART:
                with open(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache", mode='wb') as file:
                    pickle.dump(self.state, file)

            # Close everything (to avoid memory issues)
            self.page.close()
            self.context.close()
            self.browser.close()

            # Re-open stuff
            if Config.BROWSER == 'firefox':
                self.browser = self.playwright.firefox.launch(headless=Config.HEADLESS)
            elif Config.BROWSER == 'webkit':
                self.browser = self.playwright.webkit.launch(headless=Config.HEADLESS)
            else:
                self.browser = self.playwright.chromium.launch(headless=Config.HEADLESS)

            context = self.browser.new_context(
                storage_state=self.state.get('Context', None),
                **self.playwright.devices[Config.DEVICE],
                locale=Config.LOCALE,
                timezone_id=Config.TIMEZONE
            )

            self.page = context.new_page()

        # Close everything
        self.page.close()
        self.context.close()
        self.browser.close()
        self.playwright.stop()

        # Delete old cache
        if Config.RESTART and (Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache").exists():
            self.log.debug("Deleting cache")
            os.remove(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")

    def _open_url(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> Optional[Response]:
        response: Optional[Response] = None
        error_message: Optional[str] = None

        try:
            response = self.page.goto(url[0], timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
        except Error as error:
            error_message = error.message
            self.log.warning(error_message)

        if url[1] == 0 and self.url == url[0]:
            tempurl: URL = URL.get(job=self.job_id, crawler=self.crawler_id, state='progress')
            tempurl.landing_page = self.page.url
            tempurl.code = response.status if response is not None else Config.ERROR_CODES['response_error']
            tempurl.error = error_message
            tempurl.save()

        return response

    def _invoke_page_handler(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        self.log.debug('Invoke module page handler')

        for module in self.modules:
            module.add_handlers(url)

    def _invoke_response_handler(self, responses: List[Optional[Response]],
                                 url: Tuple[str, int, int, List[Tuple[str, str]]],
                                 start: List[datetime], repetition: int) -> None:
        self.log.debug('Invoke module response handler')

        for module in self.modules:
            module.receive_response(responses, url, self.page.url, start, repetition)
