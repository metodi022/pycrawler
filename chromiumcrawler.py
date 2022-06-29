from datetime import datetime
from logging import Logger
from typing import Type, Optional, Tuple, List

from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, \
    Response, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.acceptcookies import AcceptCookies
from modules.collecturls import CollectUrls
from modules.module import Module
from modules.savestats import SaveStats
from utils import get_tld_object, get_url_full, get_screenshot


class ChromiumCrawler:
    # noinspection PyTypeChecker
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger, modules: List[Module]) -> None:
        # Prepare database and log
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._config: Type[Config] = config
        self._database: Postgres = database
        self._log: Logger = log

        # Prepare modules
        self._modules: List[Module] = []
        self._modules += [AcceptCookies(job_id, crawler_id, config, database,
                                        log)] if config.ACCEPT_COOKIES else []
        self._modules += [
            CollectUrls(job_id, crawler_id, config, database, log)] if config.RECURSIVE else []
        self._modules += modules
        self._modules += [SaveStats(job_id, crawler_id, config, database, log)]

        # Prepare browser instances
        self._playwright: Playwright = None  # type: ignore
        self._browser: Browser = None  # type: ignore

    def start_crawl_chromium(self) -> None:
        self._playwright: Playwright = sync_playwright().start()
        self._browser: Browser = self._playwright.chromium.launch(headless=self._config.HEADLESS)
        self._log.info(f"Start crawl, Chromium {self._browser.version}")
        self._start_crawl()

    def stop_crawl_chromium(self) -> None:
        self._browser = self._browser.close() or None  # type: ignore
        self._playwright = self._playwright.stop() or None  # type: ignore
        self._log.info('End crawl')

    def _start_crawl(self):
        start: datetime = datetime.now()

        url: Optional[Tuple[str, int, int, List[Tuple[str, str]]]] = self._database.get_url(
            self.job_id, self.crawler_id)
        self._log.info(f"Get URL {str(url[0]) if url is not None else url}")

        if url is None:
            return

        context: BrowserContext = self._browser.new_context()
        context_database: DequeDB = DequeDB()
        page: Page = context.new_page()
        self._log.debug('New context')

        self._invoke_page_handler(context, page, url, context_database)

        while url is not None:
            # Navigate to page
            response: Optional[Response] = self._open_url(page, url)

            # Check response status
            response = self._confirm_response(response, url)

            # Wait after page is loaded
            page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)
            get_screenshot(page,
                           (self._config.LOG / f"screenshots/job{self.job_id}rank{url[2]}.png"))

            # Run module response handler and exit if errors occur
            self._invoke_response_handler(context, page, [response] if response is not None else [],
                                          url, context_database, [start])

            # Get next URL to crawl
            start = datetime.now()
            url = context_database.get_url()
            context_switch: bool = self._config.SAME_CONTEXT
            if url is None:
                url = self._database.get_url(self.job_id, self.crawler_id)
                context_switch = False
            self._log.info(f"Get URL {str(url)}")

            # Reload context if need be
            if not context_switch and url:
                # Open a new page
                context.close()
                context = self._browser.new_context()
                page = context.new_page()
                self._log.debug('New context')

                # Run module
                self._invoke_page_handler(context, page, url, context_database)

        context.close()

    def _open_url(self, page: Page, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> \
            Optional[Response]:
        response: Optional[Response] = None

        try:
            response = page.goto(url[0], timeout=self._config.LOAD_TIMEOUT,
                                 wait_until=self._config.WAIT_LOAD_UNTIL)
        except Error as error:
            self._log.warning(str(error.message))

        return response

    def _confirm_response(self, response: Optional[Response],
                          url: Tuple[str, int, int, List[Tuple[str, str]]]) -> Optional[Response]:
        self._database.update_url(self.job_id, self.crawler_id, url[0],
                                  response.status if response is not None else -1)

        if response is None:
            return None

        self._log.info(f"Receive response status {response.status}")

        if response.status < 400:
            return response

        return response

    def _invoke_page_handler(self, context: BrowserContext, page: Page,
                             url: Tuple[str, int, int, List[Tuple[str, str]]],
                             context_database: DequeDB) -> None:
        self._log.debug('Invoke module page handler')

        for module in self._modules:
            module.add_handlers(self._browser, context, page, context_database, url)

    def _invoke_response_handler(self, context: BrowserContext, page: Page,
                                 responses: List[Response],
                                 url: Tuple[str, int, int, List[Tuple[str, str]]],
                                 context_database: DequeDB, start: List[datetime]) -> None:
        self._log.debug('Invoke module response handler')

        final_url: str = get_url_full(get_tld_object(page.url)) if get_tld_object(  # type: ignore
            page.url) is not None else url[0]
        for module in self._modules:
            module.receive_response(self._browser, context, page, responses, context_database, url,
                                    final_url, start)
