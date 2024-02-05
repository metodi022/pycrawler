import random
from typing import Callable, List, Optional

import tld
from playwright.sync_api import Error, Locator, Response

import utils
from config import Config
from modules.Module import Module


class CollectUrls(Module):
    """
    Module to automatically collect links to crawl further.
    """

    def __init__(self, crawler) -> None:
        super().__init__(crawler)

        self._max_urls: int = self.crawler.state.get('CollectUrls', Config.MAX_URLS)
        self.crawler.state['CollectUrls'] = self._max_urls

        self._url_filter_out: List[Callable[[tld.utils.Result], bool]] = []

    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        # Speedup by ignoring repetitive URL collection from the same page
        if self.crawler.repetition > 1:
            return

        # Make sure to add page as seen
        parsed_url_final: Optional[tld.utils.Result] = utils.get_tld_object(self.crawler.page.url)
        self.crawler.urldb.add_seen(final_url)

        # Checks
        if self.crawler.depth >= Config.DEPTH:
            return

        if self._max_urls < 1:
            return

        if parsed_url_final is None:
            return

        # TODO add other checks

        # TODO improve link gather
        try:
            links: Locator = self.crawler.page.locator('a[href]')
        except Error:
            return

        urls: List[tld.utils.Result] = []

        # Iterate over each link
        for i in range(utils.get_locator_count(links)):
            # Get href attribute
            link: Optional[str] = utils.get_locator_attribute(utils.get_locator_nth(links, i), 'href')

            if (link is None) or (not link.strip()):
                continue

            # Parse attribute
            parsed_link: Optional[tld.utils.Result] = utils.get_url_from_href(link.strip(), parsed_url_final)
            if not parsed_link:
                continue

            # Check for same origin
            if Config.SAME_ORIGIN and (self.crawler.origin != utils.get_url_origin(parsed_link)):
                continue

            # Check for same ETLD+1
            if Config.SAME_ETLDP1 and (self.crawler.site.site != parsed_link.fld):
                continue

            # TODO: Check for same entity

            # Check seen
            parsed_link_full: str = utils.get_url_str(parsed_link)
            if self.crawler.urldb.get_seen(parsed_link_full):
                continue
            self.crawler.urldb.add_seen(parsed_link_full)

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

        self.crawler.log.info(f"Find {min(len(urls), self._max_urls)} URLs")

        # Shuffle the URLs
        if Config.FOCUS_FILTER:
            urls = urls[:int(len(urls) * 0.15)] + urls[int(len(urls) * 0.85):] + urls[int(len(urls) * 0.15):int(len(urls) * 0.85)]
        else:
            random.shuffle(urls)

        # For each found URL, add it to the database, while making sure not to exceed the max URL limit
        for parsed_link in urls:
            self.crawler.urldb.add_url(utils.get_url_str_with_query_fragment(parsed_link), self.crawler.depth + 1, self.crawler.url, force = True)

            self._max_urls -= 1
            if self._max_urls < 1:
                break

        self.crawler.state['CollectUrls'] = self._max_urls

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        self._url_filter_out = filters
