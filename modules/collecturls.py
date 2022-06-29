from datetime import datetime
from logging import Logger
from typing import Optional, List, Tuple

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_tld_object, get_url_from_href, get_url_origin, get_url_full, \
    get_locator_count, get_locator_nth, get_locator_attribute


class CollectUrls(Module):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB,
                     url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        context_database.add_seen(url[0])
        self._url = url[0]
        self._rank = url[2]

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Response], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime]) -> None:
        context_database.add_seen(final_url)

        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None and url[0] == self._url:
            context_database.clear_urls()
        if response is None or response.status >= 400:
            return

        # Check if depth exceeded
        if url[1] >= Config.DEPTH:
            return

        parsed_url: Optional[tld.utils.Result] = get_tld_object(self._url)
        if parsed_url is None:
            return

        # Iterate over each <a> tag and add its href
        try:
            links: Locator = page.locator('a[href]')
        except Error:
            return

        for i in range(get_locator_count(links)):
            # Get href attribute
            link: Optional[str] = get_locator_attribute(get_locator_nth(links, i), 'href')

            if link is None or not link.strip():
                continue

            # Parse attribute
            parsed_link: Optional[tld.utils.Result] = get_url_from_href(link.strip(), parsed_url)
            if parsed_link is None:
                continue

            # Check for same origin
            if Config.SAME_ORIGIN and get_url_origin(parsed_url) != get_url_origin(
                    parsed_link):
                continue

            # Check for same ETLD+1
            if Config.SAME_ETLDP1 and parsed_url.fld != parsed_link.fld:
                continue

            # Add link
            self._log.debug(
                f"Find {context_database.get_seen(get_url_full(parsed_link))} "
                f"{get_url_full(parsed_link)}")
            context_database.add_url(
                (get_url_full(parsed_link), url[1] + 1, url[2], url[3] + [(url[0], final_url)]))
