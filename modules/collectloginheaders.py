import json
import sys
from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple, Callable, cast

from playwright.sync_api import Browser, BrowserContext, Page, Response, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.acceptcookies import AcceptCookies
from modules.login import Login
from modules.module import Module


class CollectLoginHeaders(Login):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._context_alt: Optional[BrowserContext] = None
        self._page_alt: Optional[Page] = None
        self._cookies: Optional[AcceptCookies] = None

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        Login.register_job(database, log)

        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINHEADERS (rank INT NOT NULL, job INT NOT NULL,"
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, fromurl TEXT NOT NULL, "
            "tourl TEXT NOT NULL, code INT NOT NULL, headers TEXT, login BOOLEAN NOT NULL)",
            None, False)
        log.info('Create LOGINHEADERS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        if Config.ACCEPT_COOKIES:
            self._cookies = cast(AcceptCookies, modules[0])

        super().add_handlers(browser, context, page, context_database, url, modules)

        if not self.success:
            self._log.info('Login failed')
            sys.exit()

        self._context_alt = browser.new_context()
        self._page_alt = self._context_alt.new_page()

        try:
            response_alt = self._page_alt.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                               wait_until=Config.WAIT_LOAD_UNTIL)
        except Error as error:
            self._log.error(error.message)
            sys.exit()

        self._page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)

        if Config.ACCEPT_COOKIES:
            self._cookies.receive_response(browser, self._context_alt, self._page_alt,
                                           [response_alt], context_database, url, page.url, [], [],
                                           force=True)

        def handler(login: bool) -> Callable[[Response], None]:
            def helper(response: Response):
                headers: Optional[str]
                try:
                    headers = json.dumps(response.all_headers())
                except ValueError:
                    headers = None

                self._database.invoke_transaction(
                    "INSERT INTO LOGINHEADERS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, page.url, response.url,
                        response.status, headers, login), False)

            return helper

        page.on('response', handler(True))
        self._page_alt.on('response', handler(False))

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module]) -> None:
        try:
            self._page_alt.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                wait_until=Config.WAIT_LOAD_UNTIL)
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error:
            # Ignored
            pass
