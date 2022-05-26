from typing import Type, Optional, Tuple
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, Response
from database.database import Database
from database.dequedb import DequeDB
from log.log import Log
from config import Config
from modules.module import Module
from utils import href_to_url
import tld


class Crawler:
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Database, log: Log, module: Module) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self.config: Type[Config] = config
        self.database: Database = DequeDB(
            job_id, crawler_id, database) if config.SAME_CONTEXT else database
        self.log: Log = log
        self.module: Module = module

        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None

    def start_crawl_chromium(self) -> None:
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch()
        self.log.add_message(
            f"Start Chromium {self.browser.version}")

        # TODO Ask Jannis for browser and context options
        # TODO including routing

        # Start crawl process here
        self._start_crawl()

        self.browser.close()
        self.playwright.stop()

    def _start_crawl(self):
        while True:  # TODO fix
            # Get next URL to crawl
            url: Optional[Tuple[str, int]] = self.database.get_url(
                self.job_id, self.crawler_id)
            self.log.add_message(f"Get URL {str(url)}")

            # If no URL was found -> stop crawl
            if not url:
                break

            # Open a new page
            self.context: BrowserContext = self.browser.new_context()
            self.page: Page = self.context.new_page()

            # Open a blank page for internal use later
            blank_page = self.context.new_page()
            blank_page.goto('about:blank')

            # Run module and exit if errors occur
            self.log.add_message('Invoke add_handlers')
            try:
                self.module.add_handlers(
                    self.browser, self.context, self.page, url, self.database, self.log)
            except Exception as e:
                self.log.add_message(str(e))
                self.database.update_url(
                    self.job_id, self.crawler_id, url[0], -1)
                self.context.close()
                break

            # Navigate to page
            self.log.add_message('Navigate to URL')
            try:
                response: Optional[Response] = self.page.goto(
                    url[0], timeout=self.config.LOAD_TIMEOUT, wait_until=self.config.WAIT_LOAD_UNTIL)  # TODO referer?

                if response is None:
                    raise RuntimeError("Response is None.")
            except Exception as e:
                self.log.add_message(str(e))
                self.database.update_url(
                    self.job_id, self.crawler_id, url[0], -2)
                self.context.close()
                continue

            # Check response status
            self.log.add_message(f"Receive response status {response.status}")
            if (response.status >= 400):
                self.database.update_url(
                    self.job_id, self.crawler_id, url[0], response.status)
                self.context.close()
                continue

            # Wait after page is loaded
            blank_page.evaluate(
                'window.x = 0; setTimeout(() => { window.x = 1 }, ' + str(self.config.WAIT_AFTER_LOAD) + ');')
            blank_page.wait_for_function('() => window.x > 0')

            # Collect URLs if needed
            if (self.config.RECURSIVE):
                parsed_url: Optional[tld.utils.Result] = tld.get_tld(
                    url[0], as_object=True)  # type: ignore
                if parsed_url is None:
                    continue

                links = self.page.locator('a')

                # Iterate over each <a> tag
                for i in range(links.count()):
                    link: Optional[str] = links.nth(i).get_attribute('href')
                    if link is None:
                        continue

                    href_to_url(link, parsed_url)

                    if (self.config.SAME_ORIGIN):
                        continue

                    if (self.config.SAME_ETLDP1):
                        continue

                    # TODO search for URLs if config

            # Run module and exit if errors occur
            self.log.add_message('Send response to module')
            try:
                self.module.receive_response(
                    self.browser, self.context, self.page, self.database, self.log, response)
            except Exception as e:
                self.log.add_message(str(e))
                self.database.update_url(
                    self.job_id, self.crawler_id, url[0], -1)
                self.context.close()
                break

            # Update database
            self.database.update_url(
                self.job_id, self.crawler_id, url[0], response.status)

            # Get next URL to crawl and close pages
            self.context.close()
