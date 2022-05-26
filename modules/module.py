from playwright.sync_api import Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log
from typing import Tuple


class Module:
    @staticmethod
    def initialize_data(database: Database) -> None:
        """Initializes module database.

        Args:
            database (Database): database
        """
        raise NotImplementedError

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, url: Tuple[str, int], database: Database, log: Log) -> None:
        """Add event handlers before navigating to a page.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            url (str): URL
            database (Database): database
            log (Log): log
        """
        raise NotImplementedError

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, database: Database, log: Log, response: Response) -> None:
        """Receive response from server.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            database (Database): database
            log (Log): log
            response (Response): response
        """
        raise NotImplementedError
