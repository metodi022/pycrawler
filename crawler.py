from typing import Optional
from urllib.parse import ParseResult
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page
from database.database import Database
from log.log import Log


class Crawler:
    def __init__(self, job: int, id: int, database: Database, log: Log) -> None:
        self.job = job
        self.id = id
        self.database = database
        self.log = log
        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None

    def start_chromium(self):
        self.playwright: Playwright = sync_playwright().start()
        self.browser: Browser = self.playwright.chromium.launch()

        # TODO Ask Jannis for browser and context options

        url: Optional[ParseResult] = self.database.get_url()
        while (url):
            self.context: BrowserContext = Browser.new_context()  # TODO base_url=raw_url ?
            self.page: Page = self.context.new_page()

            # TODO finish

            url = self.database.get_url()

        self.browser.close()
        self.playwright.stop()
