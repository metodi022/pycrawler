import pathlib
import pickle
import shutil
import time
import traceback
from datetime import datetime
from logging import Logger
from typing import Any, Callable, Dict, List, Optional, Type, cast

import tld
from playwright.sync_api import Browser, BrowserContext, CDPSession, Error, Page, Playwright, Response, sync_playwright

import utils
from config import Config
from database import URL, URLDB, Site, Task, database
from modules.AcceptCookies import AcceptCookies
from modules.CollectUrls import CollectUrls
from modules.Module import Module
from modules.SaveURL import SaveURL


class Crawler:
    def _update_cache(self) -> None:
        self.log.info("Updating cache")

        with database.atomic():
            self.task.updated = datetime.today()
            self.task.crawlerstate = pickle.dumps(self.state)
            database.execute_sql(f"UPDATE task SET updated={database.param}, crawlerstate={database.param} WHERE id={database.param}", (self.task.updated, self.task.crawlerstate, self.task.get_id()))

    def _delete_cache(self) -> None:
        self.log.info("Deleting cache")

        browser_cache: pathlib.Path = Config.LOG / f"browser-{self.task.job}-{self.task.crawler}"
        if browser_cache.exists():
            shutil.rmtree(browser_cache)

        with database.atomic():
            self.task.updated = datetime.today()
            self.state = None
            database.execute_sql(f"UPDATE task SET updated={database.param}, crawlerstate=NULL WHERE id={database.param}", (self.task.updated, self.task.get_id()))

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
        self.cdp = self.context.new_cdp_session(self.page) if Config.BROWSER == 'chromium' else None

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
                    *["--disable-extensions-except=" + str(extension) for extension in Config.EXTENSIONS],
                    *["--load-extension" + str(extension) for extension in Config.EXTENSIONS],
                ]
            )

        self.page = self.context.new_page()
        self.cdp = self.context.new_cdp_session(self.page) if Config.BROWSER == 'chromium' else None

    def _close_browser(self) -> None:
        self.log.info("Closing browser")

        if len(self.urldb.get_state('free')) == 0:
            utils.get_screenshot(
                self.page,
                (Config.LOG / f"screenshots/{datetime.now().strftime('%Y-%m-%d')}-{self.task.job}-2-{self.site.scheme}-{self.site.site}.png")
            )

        self.cdp.detach()
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

        # Prepare response chain if there are redirections
        response: Optional[Response] = responses[-1]
        while (response is not None) and (response.request.redirected_from is not None):
            responses.append(response.request.redirected_from.response())
            response: Optional[Response] = responses[-1]
        responses.reverse()

        final_url: str = self.page.url

        for module in self.modules:
            module.receive_response(responses, final_url, repetition)

    def _open_url(self) -> Optional[Response]:
        self.log.info(f"Navigating to URL: {self.url.url}")

        response: Optional[Response] = None
        error_message: Optional[str] = None

        self.page.wait_for_timeout(Config.WAIT_BEFORE_LOAD)

        try:
            response = self.page.goto(cast(str, self.url.url), timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            self.page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error as error:
            error_message = ((error.name + ' ') if error.name else '') + error.message
            self.log.error('crawler.py:%s %s', traceback.extract_stack()[-1].lineno, error)

        # On first visit, also update the task
        if (cast(URL, self.task.landing).code is None) and (self.repetition == 1):
            with database.atomic():
                self.task.updated = datetime.today()
                self.task.error = error_message

                database.execute_sql(
                    f"UPDATE task SET updated={database.param}, error={database.param} WHERE id={database.param}",
                    (self.task.updated, self.task.error, self.task.get_id())
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

        # Load state-dependent variables
        self.url: URL = URL.get_by_id(self.state.get('URL', cast(URL, self.task.landing)))
        self.depth: int = self.url.depth

        # Prepare browser variables
        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self.cdp: Optional[CDPSession] = None
        self.urldb: URLDB = URLDB(self)

        # Add URL to database
        if 'URL' in self.state:
            # Crawler previously crashed on current URL
            # Therefore, invalidate the current URL
            self.log.warning("Invalidating latest crashed URL %s", self.url.url)
            URL.update(code=Config.ERROR_CODES['crawler_error'], state='complete').where(URL.task==self.task, URL.url==self.url.url, URL.depth==self.depth).execute()
            self.url = URL.get_by_id(self.state.get('URL', cast(URL, self.task.landing)))

        # Initialize modules
        self.modules: List[Module] = []  # TODO [AcceptCookies(self)] if Config.ACCEPT_COOKIES else []
        self.modules += [CollectUrls(self)] if Config.RECURSIVE else []
        for module in modules:
            self.modules.append(module(self))
        self.modules += [SaveURL(self)]
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
            if Config.EXTENSIONS:
                self._init_browser_extensions()
            else:
                self._init_browser()
        except Exception as error:
            self.log.error('crawler.py:%s %s', traceback.extract_stack()[-1].lineno, error)
            self.stop = True
            self._delete_cache()
            return

        if self.browser:
            self.log.info(f"Start {Config.BROWSER} {self.browser.version}")
        else:
            self.log.info(f"Start {Config.BROWSER} with extensions")

        # Get URL
        self.url: URL = self.urldb.get_url(1) if self.url.code is not None else self.url
        self.log.info(f"Get URL {self.url.url if self.url is not None else self.url} depth {self.url.depth if self.url is not None else self.depth}")

        # Update state
        if self.url is not None:
            self.url = cast(URL, self.url)
            self.depth = cast(int, self.url.depth)
            self.state['URL'] = self.url.get_id()

        # Manual setup here
        if Config.MANUAL_SETUP and (self.url is not None):
            self.log.info("Initiate manual setup")

            response: Optional[Response] = self._open_url()

            print("Close the browser after you are done with the manual setup to continue.")
            print("Waiting for browser to close...")

            while not self.page.is_closed():
                try:
                    self.page.bring_to_front()
                    time.sleep(10)
                except Exception:
                    pass

            self.log.info("Finish manual setup")

            self.state['Context'] = self.context.storage_state()
            self.page = self.context.new_page()

        self._update_cache()

        # Main loop
        _count = 0
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
                self.state['URL'] = self.url.get_id()

            # Save state if needed
            if Config.SAVE_CONTEXT and self.browser:
                try:
                    self.state['Context'] = self.context.storage_state()
                except Exception as error:
                    self.log.error('crawler.py:%s %s', traceback.extract_stack()[-1].lineno, error)

            self._update_cache()

            # Delete browser cache if needed
            if (not Config.SAVE_CONTEXT) and (not self.browser):
                browser_cache: pathlib.Path = Config.LOG / f"browser-{self.task.job}-{self.task.crawler}"
                if browser_cache.exists():
                    shutil.rmtree(browser_cache)

            # Restart browser to to avoid memory issues
            if _count % Config.RESTART_BROWSER == 0:
                self._close_browser()

                # Re-open stuff
                try:
                    if Config.EXTENSIONS:
                        self._init_browser_extensions()
                    else:
                        self._init_browser()
                except Exception as error:
                    self.log.error('crawler.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                    self.stop = True

            _count += 1

        # Close everything
        self._close_browser()
        self.playwright.stop()

        # Delete old cache
        self._delete_cache()
