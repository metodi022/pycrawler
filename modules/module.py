from playwright.sync_api import Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log
from typing import Tuple


class Module:
    def __init__(self, database: Database, log: Log) -> None:
        """Initializes database for module.

        Args:
            database (Database): database
            log (Log): log
        """
        raise NotImplementedError

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, url: Tuple[str, int]) -> None:
        """Add event handlers before navigating to a page.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            url (str): URL
        """
        raise NotImplementedError

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response) -> None:
        """Receive response from server.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            response (Response): response
        """
        raise NotImplementedError
