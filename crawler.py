from typing import Optional, Type
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log
from config import Config
from modules.module import Module


class Crawler:
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Database, log: Log, module: Type[Module]) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self.config: Type[Config] = config
        self.database: Database = database
        self.log: Log = log
        self.module: Type[Module] = module

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

        # Crawl loop
        while True:  # TODO fix
            # Get next URL to crawl
            url: Optional[str] = self.database.get_url(
                self.job_id, self.crawler_id)
            self.log.add_message(f"Get URL {url}")

            # Stop crawler if there is no URL
            if not url:
                break

            # Open a new page
            self.context: BrowserContext = self.browser.new_context()  # TODO base_url=raw_url ?
            self.page: Page = self.context.new_page()

            # Open a blank page for internal use later
            blank_page = self.context.new_page()
            blank_page.goto('about:blank')

            # Run module and exit if errors occur
            self.log.add_message('Invoke add_handlers')
            try:
                self.module.add_handlers(
                    self.browser, self.context, self.page, self.database, self.log)
            except Exception as e:
                self.log.add_message(e)
                self.database.update_url(self.job_id, self.crawler_id, url, -1)
                self.context.close()
                break

            # Navigate to page
            self.log.add_message('Navigate to URL')
            try:
                response: Response = self.page.goto(
                    url, timeout=self.config.LOAD_TIMEOUT, wait_until=self.config.WAIT_UNTIL)  # TODO referer?
            except Exception as e:
                self.log.add_message(e)
                self.database.update_url(self.job_id, self.crawler_id, url, -2)
                self.context.close()
                continue

            # Check response status
            self.log.add_message(f"Receive response status {response.status}")
            if (response.status >= 400):
                self.database.update_url(
                    self.job_id, self.crawler_id, url, response.status)
                self.context.close()
                continue

            # Wait after page is loaded
            blank_page.evaluate(
                'window.x = 0; setTimeout(() => { window.x = 100 }, ' + str(self.config.AFTER_LOAD_WAIT) + ');')
            blank_page.wait_for_function('() => window.x > 0')

            # TODO search for URLs if config

            # Run module and exit if errors occur
            self.log.add_message('Send response to module')
            try:
                self.module.receive_response(
                    self.browser, self.context, self.page, self.database, self.log, response)
            except Exception as e:
                self.log.add_message(e)
                self.database.update_url(self.job_id, self.crawler_id, url, -1)
                self.context.close()
                break

            # Update database
            self.database.update_url(
                self.job_id, self.crawler_id, url, response.status)

            # Get next URL to crawl and close pages
            self.context.close()

        self.browser.close()
        self.playwright.stop()
