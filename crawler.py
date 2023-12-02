from datetime import datetime
import pickle
from logging import Logger
from typing import Any, Callable, Dict, List, Optional, Type, cast

import tld
from config import Config
from playwright.sync_api import Browser, BrowserContext, Error, Page, Playwright, Response, sync_playwright

from database import URL, URLDB, Task, database
from modules.collecturls import CollectURLs
from modules.feedbackurl import FeedbackURL
from modules.module import Module
from utils import get_screenshot, get_tld_object, get_url_origin


class Crawler:
    def _update_cache(self) -> None:
        self.log.info("Updating cache")

        with database.atomic():
            self.task.updated = datetime.today()
            self.task.crawlerState = pickle.dumps(self.state)
            database.execute_sql("UPDATE task SET updated=%s, crawlerState=%s WHERE id=%s", (self.task.updated, self.task.crawlerState, self.task.get_id()))

    def _delete_cache(self) -> None:
        self.log.info("Deleting cache")

        with database.atomic():
            self.task.updated = datetime.today()
            self.state = {}
            database.execute_sql("UPDATE task SET updated=%s, crawlerState=NULL WHERE id=%s", (self.task.updated, self.task.get_id()))

    def _init_browser(self) -> None:
        self.log.info("Initializing browser")

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
    
    def _close_borwser(self) -> None:
        self.log.info("Closing browser")

        self.page.close()
        self.context.close()
        self.browser.close()

    def _invoke_page_handlers(self, url: URL) -> None:
        self.log.info("Invoking page handlers")

        for module in self.modules:
            module.add_handlers(url)

    def _invoke_response_handlers(self, responses: List[Optional[Response]], url: URL, repetition: int) -> None:
        self.log.info("Invoking response handlers")

        final_url: str = self.page.url
        for module in self.modules:
            module.receive_response(responses, url, final_url, repetition)
    
    def _open_url(self, url: URL) -> Optional[Response]:
        self.log.info(f"Navigating to URL: {url.url}")

        response: Optional[Response] = None
        error_message: Optional[str] = None

        try:
            response = self.page.goto(cast(str, url.url), timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            self.page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error as error:
            error_message = ((error.name + ' ') if error.name else '') + error.message
            self.log.warning(error)

        # On first visit, also update the task
        if self.initial and (self.repetition == 1):
            with database.atomic():
                self.task.updated = datetime.today()
                self.task.code = response.status if response is not None else Config.ERROR_CODES['response_error']
                self.task.error = error_message
                database.execute_sql("UPDATE task SET updated=%s, code=%s, error=%s WHERE id=%s", (self.task.updated, self.task.code, self.task.error, self.task.get_id()))

            get_screenshot(self.page, (Config.LOG / f"screenshots/{self.site}-{self.job_id}.png"))

        self.log.info(f"Response status {response if response is None else response.status} repetition {self.repetition}")
        return response

    def __init__(self, job: str, crawler_id: int, taskid: int, log: Logger, modules: List[Type[Module]]) -> None:
        log.info("Crawler initializing")

        # Prepare variables
        self.stop: bool = False
        self.log: Logger = log
        self.job_id: str = job
        self.crawler_id: int = crawler_id
        self.task: Task = cast(Task, Task.get_by_id(taskid))
        self.state: Dict[str, Any] = cast(Dict[str, Any], self.task.crawlerState or {})
        self.repetition: int = 1

        # Load previous state
        self.restart: bool = False
        if Config.RESTART and self.state:
            self.restart = True
            self.state = pickle.loads(self.state)
            self.log.warning(f"Loading old state: {self.state}")

        # Load state-dependent variables
        self.depth: int = self.state.get('Crawler', (None,0))[1]
        self.initial: bool = self.state.get('Crawler', (None,None,True))[2]

        # Validate URL
        self.url: str = cast(str, self.task.url)
        url_object: Optional[tld.utils.Result] = get_tld_object(self.url)
        if url_object is None:
            self.log.error(f"Can't parse URL {self.url}")
            self._delete_cache()
        url_object = cast(tld.utils.Result, url_object)

        # Unpack URL
        self.scheme: str = self.url[:self.url.find(':')]
        self.site: str = url_object.fld
        self.origin: str = get_url_origin(url_object)
        self.currenturl: str = self.state.get('Crawler', (self.url,))[0]

        # Prepare browser variables
        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.urldb: URLDB = URLDB(self)

        # Add URL to database
        if self.restart or self.urldb.get_seen(self.currenturl):
            # Crawler previously crashed on current URL
            # Therefore, invalidate the current URL
            self.log.warning(f"Invalidating latest URL: {self.currenturl}")
            URL.update(code=Config.ERROR_CODES['browser_error'], state='complete').where(URL.task==self.task, URL.url==self.currenturl, URL.depth==self.depth).execute()
        else:
            self.urldb.add_url(self.currenturl, self.depth, None)

        # Initialize modules
        self.modules: List[Module] = [CollectURLs(self)] if Config.RECURSIVE else []
        for module in modules:
            self.modules.append(module(self))
        self.modules += [FeedbackURL(self)]
        self.log.debug(f"Prepared modules: {self.modules}")

        # Initialize URL filters
        url_filter_out: List[Callable[[tld.utils.Result], bool]] = []
        for module in self.modules:
            module.add_url_filter_out(url_filter_out)

    def start_crawl(self):
        if self.stop:
            self._delete_cache()
            return

        # Initiate playwright, browser, context, and page
        self.playwright = sync_playwright().start()
        self._init_browser()

        self.log.info(f"Start {Config.BROWSER} {self.browser.version}")

        # Get URL
        url: Optional[URL] = self.urldb.get_url(1)
        self.log.info(f"Get URL {url.url if url is not None else url} depth {url.depth if url is not None else self.depth}")

        # Update state
        if url is not None:
            self.currenturl = cast(str, url.url)
            self.depth = cast(int, url.depth)
            self.state['Crawler'] = (self.currenturl, self.depth, self.initial)

        if Config.RESTART:
            self._update_cache()

        # Main loop
        while (url is not None) and (not self.stop):
            # Invoke module page handlers
            self._invoke_page_handlers(url)

            # Repetition loop
            for repetition in range(1, Config.REPETITIONS + 1):
                self.repetition = repetition

                if repetition > 1:
                    url = self.urldb.get_url(repetition)

                # Navigate to page
                response: Optional[Response] = self._open_url(url)

                # Run modules response handler
                self._invoke_response_handlers([response], url, repetition)

            # Get next URL to crawl
            url = self.urldb.get_url(1)
            self.initial = False
            self.log.info(f"Get URL {url.url if url is not None else url} depth {url.depth if url is not None else self.depth}")

            # Update state
            if url is not None:
                self.currenturl = cast(str, url.url)
                self.depth = cast(int, url.depth)
                self.state['Crawler'] = (self.currenturl, self.depth, self.initial)

            # Save state if needed
            if Config.RESTART:
                if Config.RESTART_CONTEXT:
                    try:
                        self.state['Context'] = self.context.storage_state()
                    except Exception as error:
                        self.log.warning(f"Get main context fail: {error}")

                self._update_cache()

            # Close everything (to avoid memory issues)
            self._close_borwser()

            # Re-open stuff
            self._init_browser()

        # Close everything
        self._close_borwser()
        self.playwright.stop()

        # Delete old cache
        if Config.RESTART and self.state:
            self._delete_cache()
