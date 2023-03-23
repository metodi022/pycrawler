from datetime import datetime
from logging import Logger
from typing import Callable, List, Optional, Tuple

from peewee import CharField
from playwright.sync_api import BrowserContext, Error, Page, Response

from config import Config
from database import database
from modules.acceptcookies import AcceptCookies
from modules.collectheaders import Header
from modules.collecturls import CollectURLs
from modules.login import Login


class LoginHeader(Header):
    state = CharField()


class CollectLoginHeaders(Login):
    def __init__(self, crawler) -> None:
        super().__init__(crawler)
        self._context_alt: BrowserContext = None
        self._page_alt: Page = None
        self._repetition: int = 1

        # Login
        self.setup()

        if not self.loginsuccess:
            self.crawler.stop = True

    @staticmethod
    def register_job(log: Logger) -> None:
        Login.register_job(log)

        log.info('Create login headers table')
        with database:
            database.create_tables([LoginHeader])

    def add_handlers(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        super().add_handlers(url)

        # Create response listener that saves all headers
        def handler(login: bool) -> Callable[[Response], None]:
            def helper(response: Response):
                headers: Optional[str]
                try:
                    headers = str(response.headers_array())
                except Exception as error:
                    self.crawler.log.warning(f"Get headers fail for {'login' if login else 'logout'}: {error}")
                    headers = None

                LoginHeader.create(job=self.crawler.job_id, crawler=self.crawler.crawler_id,
                                   site=self.crawler.site, url=self.crawler.url,
                                   depth=self.crawler.depth, code=response.status,
                                   method=response.request.method,
                                   content=response.headers.get('content-type', None),
                                   resource=response.request.resource_type,
                                   fromurl=self.crawler.currenturl,
                                   fromurlfinal=self.crawler.page.url, tourl=response.url,
                                   headers=headers, repetition=self.crawler.repetition,
                                   state=('login' if login else 'logout'))

            return helper

        # Create a fresh context and page to emulate a not logged-in user
        self._context_alt = self.crawler.browser.new_context(
            storage_state=self.crawler.state.get('CollectLoginHeaders', None)
            **self.crawler.playwright.devices[Config.DEVICE],
            locale=Config.LOCALE,
            timezone_id=Config.TIMEZONE
        )

        self._page_alt = self._context_alt.new_page()

        # TODO Accept cookies once?
        # try:
        #     response_alt: Optional[Response] = self._page_alt.goto(self.url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
        #     if response_alt is None or response_alt.status >= 400:
        #         raise Exception(f"Response status {response_alt.status if response_alt is not None else None}")

        #     self._page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)

        #     if Config.ACCEPT_COOKIES:
        #         AcceptCookies.accept(self._page_alt, self.url)
        # except Exception as error:
        #     self._log.warning(f"Could not accept cookies for alt page. {error}")

        # Register handlers
        self.crawler.page.on('response', handler(True))
        self._page_alt.on('response', handler(False))

    def receive_response(self, responses: List[Optional[Response]],
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], repetition: int) -> None:
        super().receive_response(responses, url, final_url, start, repetition)

        # Navigate with the fresh context to the same page
        try:
            response_alt: Optional[Response] = self._page_alt.goto(url[0], timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            self._page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)

            # Collect URLs from logout variant
            if Config.RECURSIVE:
                module: CollectURLs = next((module for module in self.crawler.modules if isinstance(module, CollectURLs)), None)
                module.receive_response([response_alt], url, self._page_alt.url, [], self.crawler.repetition)
        except Error:
            # Ignored
            pass

        # Save state
        self.crawler.state['CollectLoginHeaders'] = self._context_alt.storage_state()

        # Close the context (to avoid memory issues)
        self._page_alt.close()
        self._context_alt.close()
