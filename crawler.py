from typing import Type, Optional, Tuple
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, Response
from database.database import Database
from database.dequedb import DequeDB
from log.log import Log
from config import Config
from modules.module import Module
from utils import href_to_url, get_origin, get_tld_object
import tld


class Crawler:
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Database, log: Log, module: Module) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._config: Type[Config] = config
        self._database: Database = database
        self._log: Log = log
        self._module: Module = module

        self._playwright: Playwright = None
        self._browser: Browser = None
        self._context: BrowserContext = None
        self._page: Page = None

    def start_crawl_chromium(self) -> None:
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch()
        self._log.add_message(
            f"Start Chromium {self._browser.version}")

        # TODO Ask Jannis for browser and context options
        # TODO including routing

        # Start crawl process here
        self._start_crawl()

        self._browser.close()
        self._playwright.stop()

    def _start_crawl(self):
        # TODO if self._config.SAME_CONTEXT -> ???

        while True:
            # Get next URL to crawl
            url: Optional[Tuple[str, int]] = self._database.get_url(
                self.job_id, self.crawler_id)
            self._log.add_message(f"Get URL {str(url)}")

            # If no URL was found -> stop crawl
            if not url:
                break

            # Open a new page
            self._context: BrowserContext = self._browser.new_context()
            self._page: Page = self._context.new_page()

            # Open a blank page for internal use later
            blank_page = self._context.new_page()
            blank_page.goto('about:blank')

            # Run module and exit if errors occur
            self._log.add_message('Invoke add_handlers')
            try:
                self._module.add_handlers(
                    self._browser, self._context, self._page, url)
            except Exception as e:
                self._log.add_message(str(e))
                self._database.update_url(
                    self.job_id, self.crawler_id, url[0], -1)
                self._context.close()
                break

            # Navigate to page
            self._log.add_message('Navigate')
            try:
                response: Optional[Response] = self._page.goto(
                    url[0], timeout=self._config.LOAD_TIMEOUT, wait_until=self._config.WAIT_LOAD_UNTIL)  # TODO referer?
                if response is None:
                    raise RuntimeError("Response is None.")
            except Exception as e:
                self._log.add_message(str(e))
                self._database.update_url(
                    self.job_id, self.crawler_id, url[0], -2)
                self._context.close()
                continue

            # Check response status
            self._log.add_message(f"Receive response status {response.status}")
            if response.status >= 400:
                self._database.update_url(
                    self.job_id, self.crawler_id, url[0], response.status)
                self._context.close()
                continue

            # Wait after page is loaded
            blank_page.evaluate(
                'window.x = 0; setTimeout(() => { window.x = 1 }, ' + str(self._config.WAIT_AFTER_LOAD) + ');')
            blank_page.wait_for_function('() => window.x > 0')

            # Collect URLs if needed
            if self._config.RECURSIVE and url[1] < self._config.DEPTH:
                parsed_url: Optional[tld.utils.Result] = get_tld_object(url[0])
                if parsed_url is None:
                    continue

                links = self._page.locator('a')

                # Iterate over each <a> tag
                for i in range(links.count()):
                    # Get href attribute
                    link: Optional[str] = links.nth(i).get_attribute('href')
                    if not link or not link.strip():
                        continue

                    # Parse attribute
                    parsed_link: Optional[tld.utils.Result] = href_to_url(
                        link.strip(), parsed_url)
                    if parsed_link is None:
                        continue

                    # Check for same origin
                    if self._config.SAME_ORIGIN and get_origin(parsed_url.parsed_url) != get_origin(parsed_link.parsed_url):
                        continue

                    # Check for same ETLD+1
                    if self._config.SAME_ETLDP1 and parsed_url.fld != parsed_link.fld:
                        continue

                    # Add link to database
                    self._log.add_message(f"Find {link}")
                    self._database.add_url(self.job_id, link, url[1] + 1)

            # Run module and exit if errors occur
            self._log.add_message('Send response to module')
            try:
                self._module.receive_response(
                    self._browser, self._context, self._page, response)
            except Exception as e:
                self._log.add_message(str(e))
                self._database.update_url(
                    self.job_id, self.crawler_id, url[0], -1)
                self._context.close()
                break

            # Update database
            self._database.update_url(
                self.job_id, self.crawler_id, url[0], response.status)

            # Get next URL to crawl and close pages
            self._context.close()
