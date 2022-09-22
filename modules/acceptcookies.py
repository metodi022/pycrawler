import re
from datetime import datetime
from logging import Logger
from typing import List, MutableSet, Optional, Tuple

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error, Frame

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_url_origin, get_tld_object, get_locator_nth, invoke_click, CLICKABLES, \
    get_outer_html, SSO, get_locator_count


class AcceptCookies(Module):
    """
    Module to automatically accepts cookie banners.
    """

    # Keywords for accept buttons
    CHECK_ENG: str = '/(\\W|^)(accept|okay|ok|consent|agree|allow|understand|continue|yes|' \
                     'got it|fine)(\\W|$)/i'
    CHECK_GER: str = '/(\\W|^)(stimm|verstanden|versteh|akzeptier|ja(\\W|$)|weiter(\\W|$)|' \
                     'annehm|bestätig|willig|zulassen(\\W|$)|lasse)/i'

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._urls: MutableSet[str] = set()
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        self._url = url[0]
        self._rank = url[2]
        self._urls.clear()

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page | Frame,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], frames=True,
                         force=False) -> None | bool:
        # Verify that response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = get_tld_object(final_url)
        if (url_origin is None or get_url_origin(
                url_origin) in self._urls) and frames and not force:
            return
        self._urls.add(get_url_origin(url_origin))

        # Recursively do the same for all frames found on the page, but only once
        refresh: bool = False
        if frames:
            for iframe in page.frames[1:]:
                refresh = self.receive_response(browser, context, iframe, [response],
                                                context_database, (iframe.url, 0, 0, []),
                                                iframe.url, [], [], frames=False) or refresh

        # Check for buttons with certain keywords
        locator_count: int = 0
        try:
            # First check for english keywords
            check: Locator = page.locator(f"text={AcceptCookies.CHECK_ENG}")
            buttons: Locator = page.locator(CLICKABLES, has=check)
            locator_count = get_locator_count(buttons, page)

            if locator_count == 0:
                buttons = page.locator(f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG}")
                locator_count = get_locator_count(buttons, page)

            # Then check for german keywords
            if locator_count == 0:
                check = page.locator(f"text={AcceptCookies.CHECK_GER}")
                buttons = page.locator(CLICKABLES, has=check)
                locator_count = get_locator_count(buttons, page)

                if locator_count == 0:
                    buttons = page.locator(f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG}")
                    locator_count = get_locator_count(buttons, page)
        except Error:
            return

        # If no accept buttons were found -> abort
        if locator_count == 0 and not refresh:
            return

        self._log.info(f"Find {locator_count} possible cookie accept buttons")

        # Click on each possible cookie accept button
        if locator_count > 0:
            for i in range(locator_count):
                button: Optional[Locator] = get_locator_nth(buttons, i)
                if button is None:
                    continue

                if re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                    continue

                try:
                    invoke_click(page, button, timeout=2000)
                except Error:
                    continue

            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

        # Stop earlier if we are in a frame
        if not frames:
            return True

        # Refresh the page
        temp: datetime = datetime.now()
        try:
            response = page.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                 wait_until=Config.WAIT_LOAD_UNTIL)
        except Error:
            start.append(temp)
            responses.append(None)
            return

        # Verify that response is valid
        if response is None or response.status >= 400:
            # Make sure to add the new response for the following models
            start.append(temp)
            responses.append(None)
            return

        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

        # Make sure to add the new response for the following models
        start.append(temp)
        responses.append(response)
