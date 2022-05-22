from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Response
from database.database import Database
from log.log import Log


class Module:
    @staticmethod
    def initialize_data(database: Database) -> None:
        """Initializes module database.

        Args:
            database (Database): database
        """
        raise NotImplementedError

    @staticmethod
    def add_handlers(browser: Browser, context: BrowserContext, page: Page, database: Database, log: Log) -> None:
        """Add event handlers before navigating to a page.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            database (Database): database
            log (Log): log
        """
        raise NotImplementedError

    @staticmethod
    def receive_response(browser: Browser, context: BrowserContext, page: Page, database: Database, log: Log, response: Response) -> None:
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
