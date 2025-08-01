import random
from typing import Callable, Optional

import tld
from playwright.sync_api import Error, Locator, Response

import utils
from config import Config
from database import Site
from modules.Module import Module


class CollectUrls(Module):
    """
    Module to automatically collect links to crawl further.
    """

    def __init__(self, crawler) -> None:
        super().__init__(crawler)

        self._max_urls: int = self.crawler.state.get('CollectUrls', Config.MAX_URLS)
        self.crawler.state['CollectUrls'] = self._max_urls

        self._adult_sites = list(Site.select(Site.site).where(Site.adult is True)) if Config.ADULT_FILTER else []

        self._url_filter_out: list[Callable[[tld.utils.Result], bool]] = []

    def receive_response(self, responses: list[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        # Speedup by ignoring repetitive URL collection from the same page
        if self.crawler.repetition > 1:
            return

        # Checks
        if self.crawler.depth >= Config.DEPTH:
            return

        if self._max_urls < 1:
            return

        parsed_url_final: Optional[tld.utils.Result] = utils.get_tld_object(final_url)
        if parsed_url_final is None:
            return

        # Make sure to add page as seen
        self.crawler.urldb.add_seen(
            utils.get_url_str_with_query_fragment(parsed_url_final)
        )

        # Force collect URLs if page didn't load correctly
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if (response is None) and (not Config.FORCE_COLLECT):
            return

        # TODO add other checks
        # pass

        # TODO improve link collection?
        try:
            links: Locator = self.crawler.page.locator('a[href]')
        except Error:
            return

        urls: list[tld.utils.Result] = []

        # Iterate over each link
        for i in range(utils.get_locator_count(links)):
            # Get element
            elem: Optional[Locator] = utils.get_locator_nth(links, i)
            if elem is None:
                continue

            # Get href attribute
            link: Optional[str] = utils.get_locator_attribute(elem, 'href')

            if (link is None) or (not link.strip()):
                continue

            # Parse attribute
            parsed_link: Optional[tld.utils.Result] = utils.get_url_from_href(link.strip(), parsed_url_final)
            if not parsed_link:
                continue

            # Check for same scheme
            if Config.SAME_SCHEME and (self.crawler.site.scheme != utils.get_url_scheme(parsed_link)):
                continue

            # Check for same origin
            if Config.SAME_ORIGIN and (self.crawler.origin != utils.get_url_origin(parsed_link)):
                continue

            # Check for same ETLD+1
            if Config.SAME_ETLDP1 and (self.crawler.site.site != utils.get_url_site(parsed_link)):
                continue

            # TODO: Check for same entity
            # pass

            # Filter out unwanted entries
            if any(filt(parsed_link) for filt in self._url_filter_out):
                continue

            # Check seen
            if self.crawler.urldb.get_seen(utils.get_url_str_with_query_fragment(parsed_link)):
                continue

            # Add to seen
            self.crawler.urldb.add_seen(
                utils.get_url_str_with_query_fragment(parsed_link)
            )

            # Add link
            urls.append(parsed_link)

        self.crawler.log.info(f"Find {min(len(urls), self._max_urls)} URLs")

        # Prioritize URLs at the beginning and end of the HTML document
        if Config.FIRST_AND_LAST:
            urls_1 = urls[:int(len(urls) * 0.15)] + urls[int(len(urls) * 0.85):]
            urls_2 = urls[int(len(urls) * 0.15):int(len(urls) * 0.85)]
            random.shuffle(urls_1)
            random.shuffle(urls_2)
            urls = urls_1 + urls_2
        # Shuffle the URLs
        else:
            random.shuffle(urls)

        # For each found URL, add it to the database, while making sure not to exceed the max URL limit
        with self.crawler.database.atomic():
            for parsed_link in urls[:self._max_urls]:
                self.crawler.urldb.add_url(
                    utils.get_url_str_with_query_fragment(parsed_link),
                    self.crawler.depth + 1,
                    self.crawler.url,
                    force = True
                )

        self._max_urls -= len(urls)
        self._max_urls = max(0, self._max_urls)
        self.crawler.state['CollectUrls'] = self._max_urls
        self.crawler._update_cache()

    def add_url_filter_out(self, filters: list[Callable[[tld.utils.Result], bool]]) -> None:
        self._url_filter_out = filters

        # Add adult filter if enabled
        if Config.ADULT_FILTER:
            self._url_filter_out.append((
                lambda parsed_url: (utils.get_url_site(parsed_url) in self._adult_sites) if parsed_url else False
            ))
