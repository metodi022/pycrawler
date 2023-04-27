import os
import pathlib
import pickle
import shutil
from datetime import datetime
from logging import Logger
from typing import Any, Callable, Dict, List, Optional, Type, cast

import tld
from playwright.sync_api import Browser, BrowserContext, Error, Page, Playwright, Response, sync_playwright

from config import Config
from database import URL, URLDB, Task
from modules.acceptcookies import AcceptCookies
from modules.collecturls import CollectURLs
from modules.feedbackurl import FeedbackURL
from modules.module import Module
from utils import get_screenshot, get_url_origin


class Crawler:
    def __init__(self, job: str, crawler_id: int, task: int, log: Logger, modules: List[Type[Module]]) -> None:
        # Prepare variables
        self.log: Logger = log
        self.job_id: str = job
        self.crawler_id: int = crawler_id
        self.state: Dict[str, Any] = {}
        self.cache: pathlib.Path = Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache"

        # Load previous state
        if Config.RESTART and self.cache.exists():
            self.log.debug("Loading old cache")
            with open(self.cache, mode="rb") as file:
                self.state = pickle.load(file)

        # Prepare rest of variables
        self.task: Task = cast(Task, Task.get(task))
        self.url: str = cast(str, self.task.url)
        self.scheme: str = 'https' if self.url.startswith('https') else 'http'
        self.site: str = cast(str, tld.get_tld(self.url, as_object=True).fld)
        self.origin: str = get_url_origin(tld.get_tld(self.url, as_object=True))
        self.currenturl: str = cast(str, self.state.get('Crawler')[0] if 'Crawler' in self.state else self.url)

        self.rank: int = cast(int, self.task.rank)
        self.depth: int = cast(int, self.state.get('Crawler')[1] if 'Crawler' in self.state else 0)
        self.repetition: int = 1

        self.stop: bool = False

        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.urldb: URLDB = URLDB(self)

        if 'URLDB' in self.state:
            self.urldb._seen = self.state['URLDB']
        else:
            self.state['URLDB'] = self.urldb._seen

        self.urldb.add_url(self.url, None, None)

        # Prepare modules
        self.modules: List[Module] = []
        self.modules += [AcceptCookies(self)] if (Config.COOKIES != 'Ignore') else []
        self.modules += [CollectURLs(self)] if Config.RECURSIVE else []
        for module in modules:
            self.modules.append(module(self))
        self.modules += [FeedbackURL(self)] if Config.RECURSIVE else []
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

        url: Optional[URL] = self.urldb.get_url()
        self.log.info(f"Get URL {url.url if url is not None else url} depth {url.depth if url is not None else self.depth}")

        # Update variables
        if url is not None:
            self.currenturl = url.url
            self.depth = url.depth
            self.state['Crawler'] = (self.currenturl, self.depth)

        # Main loop
        while url is not None and not self.stop:
            # Initiate modules
            self.log.debug('Invoke module page handler')
            self._invoke_page_handler(url)

            # Repetition loop
            for repetition in range(1, Config.REPETITIONS + 1):
                self.repetition = repetition

                if repetition > 1:
                    url = self.urldb.get_url()
                    assert(url is not None)

                # Navigate to page
                response: Optional[Response] = self._open_url(url)
                self.log.info(f"Response status {response if response is None else response.status} repetition {repetition}")

                # Run modules response handler
                self.log.debug('Invoke module response handler')
                self._invoke_response_handler([response], url, [datetime.now()], repetition)

            # Get next URL to crawl
            url = self.urldb.get_url()
            self.log.info(f"Get URL {url.url if url is not None else url} depth {url.depth if url is not None else self.depth}")

            # Update variables
            if url is not None:
                self.currenturl = url.url
                self.depth = url.depth
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

    def _open_url(self, url: URL) -> Optional[Response]:
        response: Optional[Response] = None
        error_message: Optional[str] = None

        try:
            response = self.page.goto(url.url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            self.page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error as error:
            error_message = ((error.name + ' ') if error.name else '') + error.message
            self.log.warning(error)

        if url.depth == 0 and self.url == url.url and self.repetition == 1 and self.depth == 0:
            self.task = cast(Task, Task.get(self.task.id))
            self.task.landing_page = self.page.url
            self.task.code = response.status if response is not None else Config.ERROR_CODES['response_error']
            self.task.error = error_message
            self.task.save()
            get_screenshot(self.page, (Config.LOG / f"screenshots/{self.site}-{self.job_id}.png"), False)

        return response

    def _invoke_page_handler(self, url: URL) -> None:
        for module in self.modules:
            module.add_handlers(url)

    def _invoke_response_handler(self, responses: List[Optional[Response]], url: URL, start: List[datetime], repetition: int) -> None:
        for module in self.modules:
            module.receive_response(responses, url, self.page.url, start, repetition)
