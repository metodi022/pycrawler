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
from utils import get_screenshot


class Crawler:
    def __init__(self, job_id: str, crawler_id: int,
                 url: Tuple[str, int, int, List[Tuple[str, str]]], log: Logger,
                 modules: List[Type[Module]]) -> None:
        # Prepare database and log
        self.job_id: str = job_id
        self.crawler_id: int = crawler_id
        self._site = tld.get_tld(url[0], as_object=True).fld
        self._url: Tuple[str, int, int, List[Tuple[str, str]]] = url
        self._log: Logger = log
        self._state: Dict[str, Any] = dict()

        # Load previous state
        self._state['Crawler'] = None
        if Config.RESTART and (Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache").exists():
            with open(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache", mode="rb") as file:
                self._state = pickle.load(file)

        # Prepare modules
        self._modules: List[Module] = []
        self._modules += [AcceptCookies(job_id, crawler_id, log, self._state)] if Config.ACCEPT_COOKIES else []
        self._modules += [CollectURLs(job_id, crawler_id, log, self._state)] if Config.RECURSIVE else []
        self._modules += self._initialize_modules(modules, job_id, crawler_id, log, self._state)

        # Prepare filters
        url_filter_out: List[Callable[[tld.utils.Result], bool]] = []
        for module in self._modules:
            module.add_url_filter_out(url_filter_out)

    def start_crawl(self):
        url: Optional[Tuple[str, int, int, List[Tuple[str, str]]]] = self._url
        if url is None:
            self._log.info("Get URL None")
            return

        # Initiate playwright, browser, context, and page
        playwright: Playwright = sync_playwright().start()

        browser: Browser
        if Config.BROWSER == 'firefox':
            browser = playwright.firefox.launch(headless=Config.HEADLESS)
        elif Config.BROWSER == 'webkit':
            browser = playwright.webkit.launch(headless=Config.HEADLESS)
        else:
            browser = playwright.chromium.launch(headless=Config.HEADLESS)

        context: BrowserContext = browser.new_context(
            storage_state=self._state.get('Crawler', None),
            **playwright.devices[Config.DEVICE],
            locale=Config.LOCALE,
            timezone_id=Config.TIMEZONE
        )

        context_database: DequeDB = DequeDB()
        page: Page = context.new_page()
        self._log.info(f"Start {Config.BROWSER.capitalize()} {browser.version}")

        if 'DequeDB' in self._state:
            context_database._data = self._state['DequeDB'][0]
            context_database._seen = self._state['DequeDB'][1]
        else:
            self._state['DequeDB'] = [context_database._data, context_database._seen]

        context_database.add_url(url)
        url = context_database.get_url()
        self._log.info(f"Get URL {url[0] if url is not None else url}")

        # Main loop
        while url is not None:
            # Initiate modules
            self._invoke_page_handler(browser, context, page, url, context_database)

            # Repetition loop
            for repetition in range(Config.REPETITIONS):
                # Navigate to page
                response: Optional[Response] = self._open_url(page, url)
                self._log.info(f"Response status {response if response is None else response.status} repetition {repetition + 1}")

                # Wait after page is loaded
                page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                get_screenshot(page, (Config.LOG / f"screenshots/job{self.job_id}-{self._site}.png"), False)

                # Run modules response handler
                self._invoke_response_handler(browser, context, page, [response], url, context_database, [datetime.now()], repetition + 1)

            # Get next URL to crawl
            url = context_database.get_url()
            self._log.info(f"Get URL {url[0] if url is not None else url}")

            # Save state
            try:
                self._state['Crawler'] = context.storage_state()
            except Exception as e:
                self._log.warning(e)

            if Config.RESTART:
                with open(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache", mode='wb') as file:
                    pickle.dump(self._state, file)

            # Close everything (to avoid memory issues)
            page.close()
            context.close()
            browser.close()
            
            # Re-open stuff
            if Config.BROWSER == 'firefox':
                browser = playwright.firefox.launch(headless=Config.HEADLESS)
            elif Config.BROWSER == 'webkit':
                browser = playwright.webkit.launch(headless=Config.HEADLESS)
            else:
                browser = playwright.chromium.launch(headless=Config.HEADLESS)
            
            context = browser.new_context(
                storage_state=self._state.get('Crawler', None),
                **playwright.devices[Config.DEVICE],
                locale=Config.LOCALE,
                timezone_id=Config.TIMEZONE
            )
            
            page = context.new_page()

        # Close everything
        page.close()
        context.close()
        browser.close()
        playwright.stop()
        self._log.info(f"Close Chromium")

        # Delete old cache
        if Config.RESTART and (Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache").exists():
            os.remove(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")

    def _open_url(self, page: Page, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> \
            Optional[Response]:
        response: Optional[Response] = None
        error_message: Optional[str] = None

        try:
            response = page.goto(url[0], timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
        except Error as error:
            error_message = error.message
            self._log.warning(error_message)

        if url[1] == 0 and self._url[0] == url[0]:
            code: int = response.status if response is not None else Config.ERROR_CODES['response_error']
            URL.update(landing_page=page.url, code=code, error=error_message).where((URL.job == self.job_id) & (URL.crawler == self.crawler_id) & (URL.url == url[0]) & (URL.rank == url[2])).execute()

        return response

    def _invoke_page_handler(self, browser: Browser, context: BrowserContext, page: Page,
                             url: Tuple[str, int, int, List[Tuple[str, str]]],
                             context_database: DequeDB) -> None:
        self._log.debug('Invoke module page handler')

        for module in self._modules:
            module.add_handlers(browser, context, page, context_database, url, self._modules)

    def _invoke_response_handler(self, browser: Browser, context: BrowserContext, page: Page,
                                 responses: List[Optional[Response]],
                                 url: Tuple[str, int, int, List[Tuple[str, str]]],
                                 context_database: DequeDB, start: List[datetime],
                                 repetition: int) -> None:
        self._log.debug('Invoke module response handler')

        for module in self._modules:
            module.receive_response(browser, context, page, responses, context_database, url, page.url, start, self._modules, repetition)

    def _initialize_modules(self, modules: List[Type[Module]], job_id: str, crawler_id: int,
                            log: Logger, state: Dict[str, Any]) -> List[Module]:
        result: List[Module] = []
        for module in modules:
            result.append(module(job_id, crawler_id, log, state))
        return result
