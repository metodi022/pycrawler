import re
from datetime import datetime
from logging import Logger
from typing import List, MutableSet, Optional, Tuple

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_url_origin, get_tld_object, get_locator_count, \
    get_locator_nth, invoke_click, CLICKABLES, get_outer_html, SSO


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

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], frames=True) -> None:
        # Verify that response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = get_tld_object(final_url)
        if (url_origin is None or get_url_origin(url_origin) in self._urls) and frames:
            return
        self._urls.add(get_url_origin(url_origin))

        # Recursively do the same for all frames found on the page, but only once
        if frames:
            for iframe in page.frames[1:]:
                self.receive_response(browser, context, iframe, [response], context_database,
                                      (iframe.url, 0, 0, []), iframe.url, [], [], frames=False)

        # Check for buttons with certain keywords
        try:
            # First check for english keywords
            check: Locator = page.locator(f"text={AcceptCookies.CHECK_ENG}")
            buttons: Locator = page.locator(CLICKABLES, has=check)
            buttons = page.locator(
                f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG}") if get_locator_count(
                buttons) == 0 else buttons

            # Then check for german keywords
            if get_locator_count(buttons) == 0:
                check = page.locator(f"text={AcceptCookies.CHECK_GER}")
                buttons = page.locator(CLICKABLES, has=check)
                buttons = page.locator(
                    f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG}") if get_locator_count(
                    buttons) == 0 else buttons
        except Error:
            return

        # If no accept buttons were found -> abort
        self._log.info(f"Find {get_locator_count(buttons)} possible cookie accept buttons")
        if get_locator_count(buttons) == 0:
            return

        # Click on each possible cookie accept button
        for i in range(get_locator_count(buttons)):
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
