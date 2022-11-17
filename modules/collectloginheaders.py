import sys
from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple, Callable, cast, Dict, Any

from playwright.sync_api import Browser, BrowserContext, Page, Response, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.acceptcookies import AcceptCookies
from modules.login import Login
from modules.module import Module
from utils import clear_cache


class CollectLoginHeaders(Login):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger,
                 state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, database, log, state)
        self._context_alt: Optional[BrowserContext] = None
        self._page_alt: Optional[Page] = None
        self._cookies: Optional[AcceptCookies] = None
        self._repetition: int = 1

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        Login.register_job(database, log)

        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINHEADERS (rank INT NOT NULL, job INT NOT NULL,"
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, fromurl TEXT NOT NULL, "
            "tourl TEXT NOT NULL, code INT NOT NULL, headers TEXT, login BOOLEAN NOT NULL, "
            "repetition INT NOT NULL)",
            None, False)
        log.info('Create LOGINHEADERS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        if not self.setup:
            if Config.ACCEPT_COOKIES:
                self._cookies = cast(AcceptCookies, modules[0])

            # Log in
            super().add_handlers(browser, context, page, context_database, url, modules)

            # Check if login is successful
            if not self.login:
                self._log.info('Login failed')
                self._log.info('Close Browser')
                page.close()
                context.close()
                browser.close()
                clear_cache(Config.RESTART,
                            Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
                sys.exit()

            # Create a fresh context to emulate a not logged-in user
            self._context_alt = browser.new_context(storage_state=(
                self._state[
                    'CollectLoginHeaders'] if 'CollectLoginHeaders' in self._state else None))
            self._page_alt = self._context_alt.new_page()

            response_alt: Optional[Response] = None
            try:
                response_alt = self._page_alt.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                                   wait_until=Config.WAIT_LOAD_UNTIL)
            except Error as error:
                self._log.error(error.message)
                pass

            self._page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)

            if Config.ACCEPT_COOKIES:
                self._cookies = cast(AcceptCookies, self._cookies)
                self._cookies.receive_response(browser, self._context_alt, self._page_alt,
                                               [response_alt], context_database, url, page.url, [],
                                               [],
                                               1, force=True)

            self._state['CollectLoginHeaders'] = self._context_alt.storage_state()

        # Create response listener that saves all headers
        def handler(login: bool) -> Callable[[Response], None]:
            def helper(response: Response):
                headers: Optional[str]
                try:
                    headers = str(response.headers_array())
                except Exception:
                    headers = None

                self._database.invoke_transaction(
                    "INSERT INTO LOGINHEADERS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, page.url, response.url,
                        response.status, headers, login, self._repetition), False)

            return helper

        # Register response listener
        page.on('response', handler(True))
        self._page_alt.on('response', handler(False))

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        self._page_alt = cast(Page, self._page_alt)

        super().receive_response(browser, context, page, responses, context_database, url,
                                 final_url, start, modules, repetition)

        # Navigate with the fresh context to the same page
        try:
            self._page_alt.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                wait_until=Config.WAIT_LOAD_UNTIL)
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error:
            # Ignored
            pass

        # Save state
        self._state['CollectLoginHeaders'] = self._context_alt.storage_state()

        # Close the context (to avoid memory issues)
        self._page_alt.close()
        self._context_alt.close()

        # Check if we are at the end of the crawl
        if len(context_database) == 0 and repetition == Config.REPETITIONS:
            return

        # Open the context again
        self._context_alt = browser.new_context(storage_state=self._state['CollectLoginHeaders'])
        self._page_alt = self._context_alt.new_page()

        # Update repetition
        self._repetition = (self._repetition % Config.REPETITIONS) + 1
