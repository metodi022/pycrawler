import os
import pathlib
import pickle
from datetime import datetime
from logging import Logger
import shutil
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
        self.cache: pathlib.Path = Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache"

        # Load previous state
        if Config.RESTART and self.cache.exists():
            self.log.debug("Loading old cache")
            with open(self.cache, mode="rb") as file:
                self.state = pickle.load(file)

        # Prepare rest of variables
        self.url: str = url
        self.scheme: str = 'https' if url.startswith('https') else 'http'
        self.site: str = tld.get_tld(self.url, as_object=True).fld
        self.origin: str = get_url_origin(tld.get_tld(self.url, as_object=True))
        self.currenturl: str = (self.state.get('Crawler')[0]) if 'Crawler' in self.state else url

        self.rank: int = rank
        self.depth: int = (self.state.get('Crawler')[1]) if 'Crawler' in self.state else 0
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

        self.context_database.add_url((self.url, self.depth, self.rank, []))

        # Prepare modules
        self.modules: List[Module] = []
        self.modules += [AcceptCookies(self)] if (Config.COOKIES != 'Ignore') else []
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
            if Config.RESTART and self.cache.exists():
                self.log.debug("Deleting cache")
                os.remove(self.cache)
            return

        if self.url is None:
            self.log.info(f"Get URL None depth {self.depth}")
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
        self.log.info(f"Get URL {url[0] if url is not None else url} depth {url[1] if url is not None else self.depth}")

        # Update variables
        if url is not None:
            self.currenturl = url[0]
            self.depth = url[1]
            self.state['Crawler'] = (self.currenturl, self.depth)

        # Main loop
        while url is not None and not self.stop:
            # Initiate modules
            self.log.debug('Invoke module page handler')
            self._invoke_page_handler(url)

            # Repetition loop
            for repetition in range(1, Config.REPETITIONS + 1):
                self.repetition = repetition

                # Navigate to page
                response: Optional[Response] = self._open_url(url)
                self.log.info(f"Response status {response if response is None else response.status} repetition {repetition}")

                # Run modules response handler
                self.log.debug('Invoke module response handler')
                self._invoke_response_handler([response], url, [datetime.now()], repetition)

            # Get next URL to crawl
            url = self.context_database.get_url()
            self.log.info(f"Get URL {url[0] if url is not None else url} depth {url[1] if url is not None else self.depth}")

            # Update variables
            if url is not None:
                self.currenturl = url[0]
                self.depth = url[1]
                self.state['Crawler'] = (self.currenturl, self.depth)

            # Save state if needed
            if (Config.RESTART and Config.RESTARTCONTEXT) or (Config.RESTART and ('Context' not in self.state)):
                _module: Optional[AcceptCookies] = next((module for module in self.modules if isinstance(module, AcceptCookies)), None)
                if _module is None or not _module.extension:
                    try:
                        self.state['Context'] = self.context.storage_state()
                    except Exception as error:
                        self.log.warning(f"Get main context fail: {error}")

            if Config.RESTART:
                with open(self.cache, mode='wb') as file:
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

            self.context = self.browser.new_context(
                storage_state=self.state.get('Context', None),
                **self.playwright.devices[Config.DEVICE],
                locale=Config.LOCALE,
                timezone_id=Config.TIMEZONE
            )

            self.page = self.context.new_page()

        # Close everything
        self.page.close()
        self.context.close()
        self.browser.close()
        self.playwright.stop()

        # Delete old cache
        if Config.RESTART and self.cache.exists():
            self.log.debug("Deleting cache")
            os.remove(self.cache)
        
        # Delete old persistent storage
        path = Config.LOG / f"{Config.BROWSER}{self.job_id}{self.crawler_id}"
        if path.exists():
            self.log.debug('Deleting old persistent storage')
            shutil.rmtree(path)

    def _open_url(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> Optional[Response]:
        response: Optional[Response] = None
        error_message: Optional[str] = None

        try:
            response = self.page.goto(url[0], timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            self.page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error as error:
            error_message = ((error.name + ' ') if error.name else '') + error.message
            self.log.warning(error)

        if url[1] == 0 and self.url == url[0] and self.repetition == 1 and self.depth == 0:
            tempurl: URL = URL.get(job=self.job_id, crawler=self.crawler_id, state='progress')
            tempurl.landing_page = self.page.url
            tempurl.code = response.status if response is not None else Config.ERROR_CODES['response_error']
            tempurl.error = error_message
            tempurl.save()
            get_screenshot(self.page, (Config.LOG / f"screenshots/{self.site}-{self.job_id}.png"), False)

        return response

    def _invoke_page_handler(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        for module in self.modules:
            module.add_handlers(url)

    def _invoke_response_handler(self, responses: List[Optional[Response]],
                                 url: Tuple[str, int, int, List[Tuple[str, str]]],
                                 start: List[datetime], repetition: int) -> None:
        for module in self.modules:
            module.receive_response(responses, url, self.page.url, start, repetition)
