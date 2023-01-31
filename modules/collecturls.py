from datetime import datetime
from logging import Logger
from typing import Optional, List, Tuple, Callable, Dict, Any

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database import DequeDB
from modules.module import Module
from utils import get_tld_object, get_url_from_href, get_url_origin, get_url_full, \
    get_locator_count, get_locator_nth, get_locator_attribute, get_url_full_with_query_fragment, \
    get_url_entity, get_url_full_with_query


class CollectURLs(Module):
    """
    Module to automatically collect links to crawl further.
    """

    def __init__(self, job_id: int, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, log, state)
        self._max_urls: int = 0
        self._url_filter_out: List[Callable[[tld.utils.Result], bool]] = []

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        self._max_urls = self._state.get('CollectUrls', Config.MAX_URLS)
        self._state['CollectUrls'] = self._max_urls

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url,
                                 final_url, start, modules, repetition)

        # Make sure to add page as seen
        parsed_url_final: Optional[tld.utils.Result] = get_tld_object(final_url)
        context_database.add_seen(
            get_url_full(parsed_url_final) if parsed_url_final is not None else final_url)

        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if depth or max URLs exceeded
        if self.depth >= Config.DEPTH or self._max_urls < 1:
            return

        parsed_url: Optional[tld.utils.Result] = get_tld_object(self.currenturl)
        if parsed_url is None or parsed_url_final is None:
            return

        # Get all <a> tags with a href
        try:
            links: Locator = page.locator('a[href]')
        except Error:
            return

        # Iterate over each href
        urls: List[tld.utils.Result] = []
        for i in range(get_locator_count(links)):
            # Get href attribute
            link: Optional[str] = get_locator_attribute(get_locator_nth(links, i), 'href')

            if link is None or not link.strip():
                continue

            # Parse attribute
            parsed_link: Optional[tld.utils.Result] = get_url_from_href(link.strip(),
                                                                        parsed_url_final)
            if parsed_link is None:
                continue

            # Check for same origin
            if Config.SAME_ORIGIN and get_url_origin(parsed_url) != get_url_origin(parsed_link):
                continue

            # Check for same ETLD+1
            if Config.SAME_ETLDP1 and parsed_url.fld != parsed_link.fld:
                continue

            # Check for same entity
            if Config.SAME_ENTITY and get_url_entity(parsed_url) != get_url_entity(parsed_link):
                continue

            # Check seen
            parsed_link_full: str = get_url_full_with_query(parsed_link) if Config.QUERY else get_url_full(parsed_link)
            if context_database.get_seen(parsed_link_full):
                continue
            context_database.add_seen(parsed_link_full)

            # Filter out unwanted entries
            filter_out: bool = False
            for filt in self._url_filter_out:
                if filt(parsed_link):
                    filter_out = True
                    break
            if filter_out:
                continue

            # Add link
            urls.append(parsed_link)

        self._log.info(f"Find {min(len(urls), self._max_urls)} URLs at depth {self.depth}")

        # Shuffle the URLs, so that we prioritize visiting the URLs that appear in the beginning and in the end of the page
        urls = urls[:int(len(urls) * 0.15)] + urls[int(len(urls) * 0.85):] + urls[int(len(urls) * 0.15):int(len(urls) * 0.85)]

        # For each found URL, add it to the database, while making sure not to exceed the max URL limit
        for parsed_link in urls:
            context_database.add_url_force((get_url_full_with_query_fragment(parsed_link), self.depth + 1, self.rank, url[3] + [(self.currenturl, final_url)]))

            self._max_urls -= 1
            if self._max_urls < 1:
                break

        self._state['CollectUrls'] = self._max_urls

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        self._url_filter_out = filters
