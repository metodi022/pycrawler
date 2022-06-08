from logging import Logger
from typing import Type, List

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres


class Module:
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres, log: Logger) -> None:
        """Initializes module instance.

        Args:
            job_id (int): job id
            crawler_id (int): crawler id
            config (Type[Config]): configuration
            database (Postgres): database
            log (Log): log
        """

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, context_database: DequeDB) -> None:
        """Add event handlers before navigating to a page.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            context_database (DequeDB): context database
        """
        raise NotImplementedError

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response,
                         context_database: DequeDB) -> None:
        """Receive response from server.

        Args:
            browser (Browser): browser
            context (BrowserContext): context
            page (Page): page
            response (Response): response
            context_database (DequeDB): context database
        """
        raise NotImplementedError

    def filter_urls(self, urls: List[tld.utils.Result], context_database: DequeDB) -> List[tld.utils.Result]:
        """Filter recursively gathered urls.

        Args:
            urls (List[tld.utils.Result]): urls input list
            context_database (DequeDB): context database

        Returns:
            List[tld.utils.Result]: urls output list
        """
        raise NotImplementedError
