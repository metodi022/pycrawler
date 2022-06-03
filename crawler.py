from typing import Type, Optional, Tuple, List
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, Response, Locator
from database.database import Database
from database.dequedb import DequeDB
from logging import Logger
from config import Config
from modules.module import Module
from utils import get_url_from_href, get_origin, get_tld_object, get_url_full
import tld


class Crawler:
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Database, log: Logger, modules: List[Module]) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._config: Type[Config] = config
        self._database: Database = database
        self._log: Logger = log
        self._modules: List[Module] = modules

    def start_crawl_chromium(self) -> None:
        self._playwright: Playwright = sync_playwright().start()
        self._browser: Browser = self._playwright.chromium.launch()
        self._blank: Page = self._browser.new_page()
        self._blank.goto('about:blank')
        self._log.info(f"Start Chromium {self._browser.version}")

        # TODO Ask Jannis for browser and context options
        # TODO including routing

        # Start crawl process here
        self._start_crawl()

        self._blank.close()
        self._browser.close()
        self._playwright.stop()
        self._log.info('Crawler ended, closed all browser instances')

    def _start_crawl(self):
        url: Optional[Tuple[str, int]] = self._database.get_url(
            self.job_id, self.crawler_id)
        self._log.info(f"Get URL {str(url)}")

        if url is None:
            return

        context: BrowserContext = self._browser.new_context()
        context_database: Database = DequeDB(self.job_id, self.crawler_id)
        context_switch: bool = False
        page: Page = context.new_page()
        self._log.info('New context')

        context_database.add_url(self.job_id, url[0], url[1])
        context_database.get_url(self.job_id, self.crawler_id)

        if not self._invoke_page_handler(context, page, url):
            context.close()
            return

        while url:
            # Navigate to page
            response: Optional[Response] = self._open_url(
                page, url, context_switch)

            # Check response status
            response = self._confirm_response(response, url, context_switch)

            if response is not None:
                # Wait after page is loaded
                self._wait(self._config.WAIT_AFTER_LOAD)

                # Collect URLs if needed
                context_switch = self._get_urls(
                    page, url, context_database, context_switch)

                # Run module response handler and exit if errors occur
                if not self._invoke_response_handler(context, page, response, url, context_switch):
                    break

            # Get next URL to crawl
            url = context_database.get_url(self.job_id, self.crawler_id)
            if url is None:
                url = self._database.get_url(self.job_id, self.crawler_id)
                context_switch = False
            self._log.info(f"Get URL {str(url)}")

            if not context_switch and url is not None:
                # Open a new page
                context.close()
                context = self._browser.new_context()
                page = context.new_page()
                self._log.info('New context')

                context_database.add_url(self.job_id, url[0], url[1])
                context_database.get_url(self.job_id, self.crawler_id)

                # Run module and exit if errors occur
                if not self._invoke_page_handler(context, page, url):
                    break

        context.close()

    def _open_url(self, page: Page, url: Tuple[str, int], context_switch: bool) -> Optional[Response]:
        response: Optional[Response] = None
        try:
            response = page.goto(url[0], timeout=self._config.LOAD_TIMEOUT,
                                 wait_until=self._config.WAIT_LOAD_UNTIL)  # TODO referer?
            if response is None:
                self._log.warning("Response is None.")
        except Exception as e:
            self._log.warning(str(e))
            if not context_switch:
                self._database.update_url(
                    self.job_id, self.crawler_id, url[0], -2)

        return response

    def _confirm_response(self, response: Optional[Response], url: Tuple[str, int], context_switch: bool) -> Optional[Response]:
        if response is None:
            return None

        self._log.info(f"Receive response status {response.status}")

        if response.status < 400:
            return response

        if not context_switch:
            self._database.update_url(
                self.job_id, self.crawler_id, url[0], response.status)

        return None

    def _wait(self, amount: int) -> None:
        self._blank.evaluate(
            'window.x = 0; setTimeout(() => { window.x = 1 }, ' + str(amount) + ');')
        self._blank.wait_for_function('() => window.x > 0')

    def _get_urls(self, page: Page, url: Tuple[str, int], context_database: Database, context_switch: bool) -> bool:
        if not self._config.RECURSIVE or url[1] >= self._config.DEPTH:
            return context_switch

        parsed_url: Optional[tld.utils.Result] = get_tld_object(url[0])
        if parsed_url is None:
            return context_switch

        links: Locator = page.locator('a')
        links_gathered: List[tld.utils.Result] = []

        # Iterate over each <a> tag and add its href
        for i in range(links.count()):
            # Get href attribute
            link: Optional[str] = links.nth(i).get_attribute('href')
            if not link or not link.strip():
                continue

            # Parse attribute
            parsed_link: Optional[tld.utils.Result] = get_url_from_href(
                link.strip(), parsed_url)
            if parsed_link is None:
                continue

            # Check for same origin
            if self._config.SAME_ORIGIN and get_origin(parsed_url) != get_origin(parsed_link):
                continue

            # Check for same ETLD+1
            if self._config.SAME_ETLDP1 and parsed_url.fld != parsed_link.fld:
                continue

            # Add link
            self._log.debug(f"Find {link}")
            context_switch = self._config.SAME_CONTEXT
            links_gathered.append(parsed_link)

        # Apply filter from each module
        for module in self._modules:
            links_gathered = module.filter_urls(links_gathered)

        # Add urls to database
        for link_gathered in links_gathered:
            context_database.add_url(
                self.job_id, get_url_full(link_gathered), url[1] + 1)

        return context_switch

    def _invoke_page_handler(self, context: BrowserContext, page: Page, url: Tuple[str, int]) -> bool:
        self._log.info('Invoke module page handler')

        try:
            map(lambda module: module.add_handlers(
                self._browser, context, page, url), self._modules)
        except Exception as e:
            self._log.error(str(e))
            self._database.update_url(self.job_id, self.crawler_id, url[0], -1)
            return False

        return True

    def _invoke_response_handler(self, context: BrowserContext, page: Page, response: Response, url: Tuple[str, int], context_switch: bool) -> bool:
        self._log.info('Invoke module response handler')

        code: bool = True
        try:
            map(lambda module: module.receive_response(
                self._browser, context, page, response), self._modules)
        except Exception as e:
            self._log.error(str(e))
            code = False

        if not context_switch:
            self._database.update_url(
                self.job_id, self.crawler_id, url[0], response.status * code or -1 * (not code))

        return code
