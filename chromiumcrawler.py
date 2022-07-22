from datetime import datetime
from logging import Logger
from typing import Optional, Tuple, List, Type, Callable

import tld
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
    def __init__(self, job_id: int, crawler_id: int,
                 url: Tuple[str, int, int, List[Tuple[str, str]]], database: Postgres, log: Logger,
                 modules: List[Type[Module]]) -> None:
        # Prepare database and log
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._url: Tuple[str, int, int, List[Tuple[str, str]]] = url
        self._database: Postgres = database
        self._log: Logger = log

        # Prepare filters
        url_filter_out: List[Callable[[tld.utils.Result], bool]] = []
        for module in modules:
            module.add_url_filter_out(url_filter_out)

        # Prepare modules
        self._modules: List[Module] = []
        self._modules += [
            AcceptCookies(job_id, crawler_id, database, log)] if Config.ACCEPT_COOKIES else []
        self._modules += [CollectUrls(job_id, crawler_id, database, log,
                                      url_filter_out)] if Config.RECURSIVE else []
        self._modules += self._initialize_modules(modules, job_id, crawler_id, database, log)
        self._modules += [SaveStats(job_id, crawler_id, database, log)]

    def start_crawl(self):
        url: Optional[Tuple[str, int, int, List[Tuple[str, str]]]] = self._url
        self._log.info(f"Get URL {url[0] if url is not None else url}")
        if url is None:
            return

        start: datetime = datetime.now()
        playwright: Playwright = sync_playwright().start()
        browser: Browser = playwright.chromium.launch(headless=Config.HEADLESS)
        context: BrowserContext = browser.new_context()
        context_database: DequeDB = DequeDB()
        page: Page = context.new_page()
        self._log.info(f"Start Chromium {browser.version}")
        self._log.info('New context')

        self._invoke_page_handler(browser, context, page, url, context_database)

        while url is not None:
            # Navigate to page
            response: Optional[Response] = self._open_url(page, url)

            # Wait after page is loaded
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
            get_screenshot(page,
                           (Config.LOG / f"screenshots/job{self.job_id}rank{url[2]}.png"))

            # Run module response handler
            self._invoke_response_handler(browser, context, page,
                                          [response] if response is not None else [], url,
                                          context_database, [start])

            # Get next URL to crawl
            start = datetime.now()
            url = context_database.get_url()
            self._log.info(f"Get URL {url[0] if url is not None else url}")

            # Reload context if need be
            if not Config.SAME_CONTEXT and url:
                # Open a new page
                page.close()
                context.close()
                browser.close()
                browser = playwright.chromium.launch(headless=Config.HEADLESS)
                context = browser.new_context()
                page = context.new_page()
                self._log.info('New context')

                # Run module
                self._invoke_page_handler(browser, context, page, url, context_database)

        # Close everything
        page.close()
        context.close()
        browser.close()
        playwright.stop()
        self._log.info(f"Close Chromium")

    def _open_url(self, page: Page, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> \
            Optional[Response]:
        response: Optional[Response] = None
        error_message: Optional[str] = None

        error: Error
        try:
            response = page.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                 wait_until=Config.WAIT_LOAD_UNTIL)
        except Error as error:
            error_message = error.message
            self._log.warning(error_message)

        if url[1] == 0:
            self._database.update_url(self.job_id, self.crawler_id, url[0],
                                      response.status if response is not None else
                                      Config.ERROR_CODES['response_error'], error_message)

        return response

    def _invoke_page_handler(self, browser: Browser, context: BrowserContext, page: Page,
                             url: Tuple[str, int, int, List[Tuple[str, str]]],
                             context_database: DequeDB) -> None:
        self._log.debug('Invoke module page handler')

        for module in self._modules:
            module.add_handlers(browser, context, page, context_database, url)

    def _invoke_response_handler(self, browser: Browser, context: BrowserContext, page: Page,
                                 responses: List[Optional[Response]],
                                 url: Tuple[str, int, int, List[Tuple[str, str]]],
                                 context_database: DequeDB, start: List[datetime]) -> None:
        self._log.debug('Invoke module response handler')

        final_url: str = get_url_full(get_tld_object(page.url)) if get_tld_object(  # type: ignore
            page.url) is not None else url[0]
        for module in self._modules:
            module.receive_response(browser, context, page, responses, context_database, url,
                                    final_url, start)

    def _initialize_modules(self, modules: List[Type[Module]], job_id: int, crawler_id: int,
                            database: Postgres, log: Logger) -> List[Module]:
        result: List[Module] = []
        for module in modules:
            result.append(module(job_id, crawler_id, database, log))
        return result
