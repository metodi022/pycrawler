import re
from datetime import datetime
from logging import Logger
from typing import List, MutableSet, Optional, Tuple, cast, Dict, Any

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error, Frame

from config import Config
from database import DequeDB
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

    def __init__(self, job_id: int, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, log, state)
        self._urls: MutableSet[str] = set()

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        self._urls = self._state.get('AcceptCookies', self._urls)
        self._state['AcceptCookies'] = self._urls

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page | Frame,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int, frames=True,
                         force=False) -> None | bool:
        super().receive_response(browser, context, page, responses, context_database, url,
                                 final_url, start, modules, repetition)

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
            page = cast(Page, page)
            for iframe in page.frames[1:]:
                refresh = self.receive_response(browser, context, iframe, [response],
                                                context_database, (iframe.url, 0, 0, []),
                                                iframe.url, [], [], repetition,
                                                frames=False) or refresh

        # Check for buttons with certain keywords
        locator_count: int = 0
        try:
            # First check for english keywords
            buttons: Locator = page.locator(f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG} >> "
                                            f"visible=true")
            locator_count = get_locator_count(buttons, page)

            # Then check for german keywords
            if locator_count == 0:
                buttons = page.locator(f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG} >> "
                                       f"visible=true")
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
            response = page.goto(self.currenturl, timeout=Config.LOAD_TIMEOUT,
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
