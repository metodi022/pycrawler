from logging import Logger
from typing import Type, Optional, Tuple, List

from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, Response

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.acceptcookies import AcceptCookies
from modules.collecturls import CollectUrls
from modules.module import Module
from utils import get_tld_object, get_url_full, wait_after_load


class ChromiumCrawler:
    # noinspection PyTypeChecker
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres, log: Logger,
                 modules: List[Module]) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._config: Type[Config] = config
        self._database: Postgres = database
        self._log: Logger = log

        # Prepare modules
        self._modules: List[Module] = []
        self._modules += [AcceptCookies(job_id, crawler_id, config, database, log)]
        self._modules += [CollectUrls(job_id, crawler_id, config, database, log)]
        self._modules += modules

        self._playwright: Playwright = None
        self._browser: Browser = None
        self._blank: Page = None

    def start_crawl_chromium(self) -> None:
        self._playwright: Playwright = sync_playwright().start()
        self._browser: Browser = self._playwright.chromium.launch(headless=self._config.HEADLESS)
        self._blank: Page = self._browser.new_page()

        self._log.info(f"Start crawl, Chromium {self._browser.version}")
        self._blank.goto('about:blank')

        # TODO Ask Jannis for browser and context options
        # TODO including routing

        # Start crawl process here
        self._start_crawl()

        self._blank.close()
        self._blank = None
        self._browser.close()
        self._browser = None
        self._playwright.stop()
        self._playwright = None
        self._log.info('End crawl')

    def _start_crawl(self):
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
            wait_after_load(self._blank, self._config.WAIT_AFTER_LOAD)

            # Run module response handler and exit if errors occur
            if not self._invoke_response_handler(context, page, response, url, context_switch, context_database):
                break

            # Get next URL to crawl
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
            # TODO referer?
            response = page.goto(url[0], timeout=self._config.LOAD_TIMEOUT, wait_until=self._config.WAIT_LOAD_UNTIL)
            if response is None:
                self._log.warning('Response is None')
        except Exception as e:
            self._log.warning(str(e))
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
            self._database.update_url(self.job_id, self.crawler_id, url[0], final_url, response.status)

        return response

    def _invoke_page_handler(self, context: BrowserContext, page: Page, url: Tuple[str, int, int],
                             context_database: DequeDB) -> bool:
        self._log.debug('Invoke module page handler')

        for module in self._modules:
            try:
                module.add_handlers(self._browser, context, page, context_database, url[0], url[2])
            except Exception as e:
                self._log.error(str(e))
                final_url: str = get_url_full(get_tld_object(page.url))
                self._database.update_url(self.job_id, self.crawler_id, url[0], final_url, -1)
                return False

        return True

    def _invoke_response_handler(self, context: BrowserContext, page: Page, response: Optional[Response],
                                 url: Tuple[str, int, int], context_switch: bool, context_database: DequeDB) -> bool:
        self._log.debug('Invoke module response handler')

        code: bool = True
        for module in self._modules:
            try:
                final_url: str = get_url_full(get_tld_object(page.url))
                response = module.receive_response(self._browser, context, page, response, context_database, url[0],
                                                   final_url, url[1])
            except Exception as e:
                self._log.error(str(e))
                code = False
                break

        if not context_switch:
            final_url: str = get_url_full(get_tld_object(page.url))
            self._database.update_url(self.job_id, self.crawler_id, url[0], final_url,
                                      (response.status if response is not None else -2) * code or -1 * (not code))

        return code