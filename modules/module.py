from playwright.sync_api import Browser, BrowserContext, Page, Response
from database.database import Database
from logging import Logger
from typing import Tuple, Type, List
from config import Config
import tld


class Module:
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Database, log: Logger) -> None:
        """Initializes database for module.

        Args:
            job_id (int): job id
            crawler_id (int): crawler id
            config (Type[Config]): configuration
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

    def filter_urls(self, urls: List[tld.utils.Result]) -> List[tld.utils.Result]:
        """Filter recursively gathered urls.

        Args:
            urls (List[tld.utils.Result]): urls input list

        Returns:
            List[tld.utils.Result]: urls output list
        """
        raise NotImplementedError
