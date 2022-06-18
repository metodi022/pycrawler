from datetime import datetime
from logging import Logger
from typing import Type, Optional, Tuple, List

from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, Response

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.acceptcookies import AcceptCookies
from modules.collecturls import CollectUrls
from modules.module import Module
from modules.savestats import SaveStats
from utils import get_tld_object, get_url_full


class ChromiumCrawler:
    # noinspection PyTypeChecker
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger, modules: List[Module]) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._config: Type[Config] = config
        self._database: Postgres = database
        self._log: Logger = log

        # Prepare modules
        self._modules: List[Module] = []
        self._modules += [AcceptCookies(job_id, crawler_id, config, database, log)]
        self._modules += [
            CollectUrls(job_id, crawler_id, config, database, log)] if config.RECURSIVE else []
        self._modules += modules
        self._modules += [SaveStats(job_id, crawler_id, config, database, log)]

        self._playwright: Playwright = None
        self._browser: Browser = None

    def start_crawl_chromium(self) -> None:
        self._playwright: Playwright = sync_playwright().start()
        self._browser: Browser = self._playwright.chromium.launch(headless=self._config.HEADLESS)

        self._log.info(f"Start crawl, Chromium {self._browser.version}")

        # TODO Ask Jannis for browser and context options

        # Start crawl process here
        self._start_crawl()

        self._browser.close()
        self._browser = None
        self._playwright.stop()
        self._playwright = None
        self._log.info('End crawl')

    def _start_crawl(self):
        start: datetime = datetime.now()

        url: Optional[Tuple[str, int, int]] = self._database.get_url(self.job_id, self.crawler_id)
        self._log.info(f"Get URL {str(url)}")

        if url is None:
            return

        context: BrowserContext = self._browser.new_context()
        context_database: DequeDB = DequeDB()
        context_switch: bool = self._config.SAME_CONTEXT
        page: Page = context.new_page()
        self._log.debug('New context')

        if not self._invoke_page_handler(context, page, url, context_database):
            context.close()
            return

        while url is not None:
            # Navigate to page
            response: Optional[Response] = self._open_url(page, url, context_switch)

            # Check response status
            response = self._confirm_response(page, response, url, context_switch)

            # Wait after page is loaded
            page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)

            # Run module response handler and exit if errors occur
            if not self._invoke_response_handler(context, page,
                                                 [response] if response is not None else [], url,
                                                 context_switch, context_database, [start]):
                break

            # Get next URL to crawl
            start = datetime.now()
            url = context_database.get_url()
            context_switch = self._config.SAME_CONTEXT
            if url is None:
                url = self._database.get_url(self.job_id, self.crawler_id)
                context_switch = False
            self._log.info(f"Get URL {str(url)}")

            if not context_switch and url:
                # Open a new page
                context.close()
                context = self._browser.new_context()
                page = context.new_page()
                self._log.debug('New context')

                # Run module and exit if errors occur
                if not self._invoke_page_handler(context, page, url, context_database):
                    break

        context.close()

    def _open_url(self, page: Page, url: Tuple[str, int, int], context_switch: bool) -> \
            Optional[Response]:
        response: Optional[Response] = None

        try:
            response = page.goto(url[0], timeout=self._config.LOAD_TIMEOUT,
                                 wait_until=self._config.WAIT_LOAD_UNTIL)
            if response is None:
                self._log.warning('Response is None')
        except Exception as error:
            self._log.warning(str(error))
            if not context_switch:
                final_url: str = get_url_full(get_tld_object(page.url))
                self._database.update_url(self.job_id, self.crawler_id, url[0], final_url, -2)

        return response

    def _confirm_response(self, page: Page, response: Optional[Response], url: Tuple[str, int, int],
                          context_switch: bool) -> Optional[Response]:
        if response is None:
            return None

        self._log.info(f"Receive response status {response.status}")

        if response.status < 400:
            return response

        if not context_switch:
            final_url: str = get_url_full(get_tld_object(page.url))
            self._database.update_url(self.job_id, self.crawler_id, url[0], final_url,
                                      response.status)

        return response

    def _invoke_page_handler(self, context: BrowserContext, page: Page, url: Tuple[str, int, int],
                             context_database: DequeDB) -> bool:
        self._log.debug('Invoke module page handler')

        for module in self._modules:
            try:
                module.add_handlers(self._browser, context, page, context_database, url[0], url[2])
            except Exception as error:
                self._log.error(str(error))
                final_url: str = get_url_full(get_tld_object(page.url))
                self._database.update_url(self.job_id, self.crawler_id, url[0], final_url, -1)
                return False

        return True

    def _invoke_response_handler(self, context: BrowserContext, page: Page,
                                 responses: List[Response], url: Tuple[str, int, int],
                                 context_switch: bool, context_database: DequeDB,
                                 start: List[datetime]) -> bool:
        self._log.debug('Invoke module response handler')

        code: bool = True
        final_url: str = get_url_full(get_tld_object(page.url))
        for module in self._modules:
            try:
                module.receive_response(self._browser, context, page, responses, context_database,
                                        url[0], final_url, url[1], start)
            except Exception as error:
                self._log.error(str(error))
                code = False
                break

        if not context_switch:
            final_url: str = get_url_full(get_tld_object(page.url))
            self._database.update_url(self.job_id, self.crawler_id, url[0], final_url, (
                responses[0].status if responses[0] is not None else -2) * code or -1 * (not code))

        return code
