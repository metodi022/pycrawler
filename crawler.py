import pathlib
import pickle
import shutil
from datetime import datetime
from logging import Logger
from typing import Any, Callable, Dict, List, Optional, Type, cast

import tld
from playwright.sync_api import Browser, BrowserContext, Error, Page, Playwright, Response, sync_playwright

import utils
from config import Config
from database import URL, URLDB, Site, Task, database
from modules.AcceptCookies import AcceptCookies
from modules.CollectUrls import CollectUrls
from modules.Module import Module
from modules.SaveUrl import SaveUrl


class Crawler:
    def _update_cache(self) -> None:
        self.log.info("Updating cache")

        with database.atomic():
            self.task.updated = datetime.today()
            self.task.crawlerstate = pickle.dumps(self.state)
            database.execute_sql("UPDATE task SET updated=%s, crawlerstate=%s WHERE id=%s", (self.task.updated, self.task.crawlerstate, self.task.get_id()))

    def _delete_cache(self) -> None:
        self.log.info("Deleting cache")

        browser_cache: pathlib.Path = Config.LOG / f"browser-{self.task.job}-{self.task.crawler}"
        if browser_cache.exists():
            shutil.rmtree(browser_cache)

        with database.atomic():
            self.task.updated = datetime.today()
            self.state = None
            database.execute_sql("UPDATE task SET updated=%s, crawlerstate=NULL WHERE id=%s", (self.task.updated, self.task.get_id()))

    def _init_browser(self) -> None:
        self.log.info("Initializing browser")

        if Config.BROWSER == 'firefox':
            self.browser = self.playwright.firefox.launch(headless=Config.HEADLESS)
        elif Config.BROWSER == 'webkit':
            self.browser = self.playwright.webkit.launch(headless=Config.HEADLESS)
        else:
            self.browser = self.playwright.chromium.launch(headless=Config.HEADLESS)

        if Config.HAR:
            self.context = self.browser.new_context(
                storage_state=self.state.get('Context', None),
                **self.playwright.devices[Config.DEVICE],
                locale=Config.LOCALE,
                timezone_id=Config.TIMEZONE,
                record_har_content='embed',
                record_har_mode='full',
                record_har_path=(Config.HAR / f"{self.task.job}-{self.task.crawler}.har")
            )
        else:
            self.context = self.browser.new_context(
                storage_state=self.state.get('Context', None),
                **self.playwright.devices[Config.DEVICE],
                locale=Config.LOCALE,
                timezone_id=Config.TIMEZONE
            )

        self.page = self.context.new_page()

    def _init_browser_extensions(self) -> None:
        self.log.info("Initializing browser with extensions")

        (Config.LOG / f"browser-{self.task.job}-{self.task.crawler}").mkdir(parents=True, exist_ok=True)

        self.browser = None

        if Config.HAR:
            self.context = self.playwright.chromium.launch_persistent_context(
                Config.LOG / f"browser-{self.task.job}-{self.task.crawler}",
                headless=Config.HEADLESS,
                record_har_content='embed',
                record_har_mode='full',
                record_har_path=(Config.HAR / f"{self.task.job}.har"),
                args=[
                    "--disable-extensions-except=./extensions/consent-o-matic-v1.1.3-chromium",
                    "--load-extension=./extensions/consent-o-matic-v1.1.3-chromium",
                ]
            )
        else:
            self.context = self.playwright.chromium.launch_persistent_context(
                Config.LOG / f"browser-{self.task.job}-{self.task.crawler}",
                headless=Config.HEADLESS,
                args=[
                    "--disable-extensions-except=./extensions/consent-o-matic-v1.1.3-chromium",
                    "--load-extension=./extensions/consent-o-matic-v1.1.3-chromium",
                ]
            )

        self.page = self.context.new_page()

    def _close_browser(self) -> None:
        self.log.info("Closing browser")

        if len(self.urldb.get_state('free')) == 0:
            utils.get_screenshot(
                self.page,
                (Config.LOG / f"screenshots/{datetime.now().strftime('%Y-%m-%d')}-{self.task.job}-2-{self.site.scheme}-{self.site.site}.png")
            )

        self.page.close()
        self.context.close()

        if self.browser:
            self.browser.close()

    def _invoke_page_handlers(self) -> None:
        self.log.info("Invoking page handlers")

        for module in self.modules:
            module.add_handlers()

    def _invoke_response_handlers(self, responses: List[Optional[Response]], repetition: int) -> None:
        self.log.info("Invoking response handlers")

        final_url: str = self.page.url
        for module in self.modules:
            module.receive_response(responses, final_url, repetition)

    def _open_url(self) -> Optional[Response]:
        self.log.info(f"Navigating to URL: {self.url.url}")

        response: Optional[Response] = None
        error_message: Optional[str] = None

        try:
            response = self.page.goto(cast(str, self.url.url), timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            self.page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error as error:
            error_message = ((error.name + ' ') if error.name else '') + error.message
            self.log.error("Error navigating to %s", self.url.url, exc_info=True)

        # On first visit, also update the task
        if self.state['Initial'] and (self.repetition == 1):
            with database.atomic():
                self.task.updated = datetime.today()
                self.task.code = response.status if response is not None else Config.ERROR_CODES['response_error']
                self.task.error = error_message

                database.execute_sql(
                    "UPDATE task SET updated=%s, code=%s, error=%s WHERE id=%s",
                    (self.task.updated, self.task.code, self.task.error, self.task.get_id())
                )

            utils.get_screenshot(
                self.page,
                (Config.LOG / f"screenshots/{datetime.now().strftime('%Y-%m-%d')}-{self.task.job}-1-{self.site.scheme}-{self.site.site}.png")
            )

        self.log.info(f"Response status {response if response is None else response.status} repetition {self.repetition}")
        return response

    def __init__(self, taskid: int, log: Logger, modules: List[Type[Module]]) -> None:
        log.info("Crawler initializing")

        # Prepare variables
        self.stop: bool = cast(bool, False)
        self.log: Logger = log
        self.task: Task = cast(Task, Task.get_by_id(taskid))
        self.site: Site = cast(Site, self.task.site)
        self.landing: URL = cast(URL, self.task.landing)
        self.origin: str = utils.get_url_origin(utils.get_tld_object(self.landing.url))
        self.repetition: int = cast(int, 1)

        # Load previous state
        self.state: Dict[str, Any] = cast(Dict[str, Any], self.task.crawlerstate or {})

        if self.state:
            self.state = pickle.loads(self.state)
            self.log.warning("Loading old state")
            self.log.debug(self.state)

        self.state['Initial'] = self.state.get('Initial', True)

        # Load state-dependent variables
        self.url: URL = URL.get_by_id(self.state.get('Crawler', cast(URL, self.task.landing)))
        self.depth: int = self.url.depth

        # Prepare browser variables
        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.urldb: URLDB = URLDB(self)

        # Add URL to database
        if not self.state['Initial']:
            # Crawler previously crashed on current URL
            # Therefore, invalidate the current URL
            self.log.warning("Invalidating latest URL %s", self.url.url)
            URL.update(code=Config.ERROR_CODES['browser_error'], state='complete').where(URL.task==self.task, URL.url==self.url.url, URL.depth==self.depth).execute()

        # Initialize modules
        self.modules: List[Module] = [AcceptCookies(self)] if Config.ACCEPT_COOKIES else []
        self.modules += [CollectUrls(self)] if Config.RECURSIVE else []
        for module in modules:
            self.modules.append(module(self))
        self.modules += [SaveUrl(self)]
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
        try:
            self.playwright = sync_playwright().start()
            self._init_browser_extensions()
        except Exception as error:
            self.log.error(error)
            self.stop = True
            self._delete_cache()
            return

        if self.browser:
            self.log.info(f"Start {Config.BROWSER} {self.browser.version}")
        else:
            self.log.info("Start Chromium with Extensions")

        # Get URL
        self.url: URL = self.urldb.get_url(1) if (not self.state['Initial']) else self.url
        self.log.info(f"Get URL {self.url.url if self.url is not None else self.url} depth {self.url.depth if self.url is not None else self.depth}")

        # Update state
        if self.url is not None:
            self.url = cast(URL, self.url)
            self.depth = cast(int, self.url.depth)
            self.state['Crawler'] = self.url.get_id()

        self.state['Initial'] = False
        self._update_cache()

        # Main loop
        while (self.url is not None) and (not self.stop):
            # Invoke module page handlers
            self._invoke_page_handlers()

            # Repetition loop
            for repetition in range(1, Config.REPETITIONS + 1):
                self.repetition = repetition

                if repetition > 1:
                    self.url = cast(URL, self.urldb.get_url(repetition))

                # Navigate to page
                response: Optional[Response] = self._open_url()

                # Run modules response handler
                self._invoke_response_handlers([response], repetition)

            # Get next URL to crawl
            self.url = self.urldb.get_url(1)
            self.log.info(f"Get URL {self.url.url if self.url is not None else self.url} depth {self.url.depth if self.url is not None else self.depth}")

            # Update state
            if self.url is not None:
                self.url = cast(URL, self.url)
                self.depth = cast(int, self.url.depth)
                self.state['Crawler'] = self.url.get_id()

            # Save state if needed
            if Config.SAVE_CONTEXT and self.browser:
                try:
                    self.state['Context'] = self.context.storage_state()
                except Exception:
                    self.log.error("Get main context fail", exc_info=True)

                self._update_cache()

            # Delete browser cache if needed
            if (not Config.SAVE_CONTEXT) and (not self.browser):
                browser_cache: pathlib.Path = Config.LOG / f"browser-{self.task.job}-{self.task.crawler}"
                if browser_cache.exists():
                    shutil.rmtree(browser_cache)

            # Close everything (to avoid memory issues)  # TODO do I need that?
            self._close_browser()

            # Re-open stuff
            try:
                self._init_browser_extensions()
            except Exception as error:
                self.log.error(error)
                self.stop = True

        # Close everything
        self._close_browser()
        self.playwright.stop()

        # Delete old cache
        self._delete_cache()
