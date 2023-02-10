import re
from datetime import datetime
from logging import Logger
from typing import List, MutableSet, Optional, Tuple, cast, Dict, Any

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error, Frame

from database import DequeDB
from modules.module import Module
from utils import get_url_origin, get_tld_object, get_locator_nth, invoke_click, CLICKABLES, \
    get_outer_html, SSO, get_locator_count, refresh_page


class AcceptCookies(Module):
    """
    Module to automatically accepts cookie banners.
    """

    # Keywords for accept buttons
    CHECK_ENG: str = '/(\\W|^)(accept|okay|ok|consent|agree|allow|understand|continue|yes|' \
                     'got it|fine)(\\W|$)/i'
    CHECK_GER: str = '/(\\W|^)(stimm|verstanden|versteh|akzeptier|ja(\\W|$)|weiter(\\W|$)|' \
                     'annehm|bestätig|willig|zulassen(\\W|$)|lasse)/i'

    def __init__(self, job_id: str, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
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
                         start: List[datetime], modules: List[Module], repetition: int, force=False) -> None | bool:
        super().receive_response(browser, context, page, responses, context_database, url, final_url, start, modules, repetition)

        # Verify that response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = get_tld_object(final_url)
        if ((url_origin is None) or (get_url_origin(url_origin) in self._urls)) and not force:
            return
        self._urls.add(get_url_origin(url_origin))

        # Accept cookies for origin
        AcceptCookies.accept(page, url[0], responses=responses, start=start)

    @staticmethod
    def accept(page: Page | Frame, url: str, frame: bool = False,
               responses: List[Optional[Response]] = None, start: List[datetime] = None):
        # Check for cookies in first depth frames
        if not frame:
            page = cast(Page, page)
            for frame in page.frames[1:]:
                AcceptCookies.accept(frame, frame.url, frame=True)

        # Check for buttons with certain keywords
        try:
            # First check for english keywords
            buttons: Locator = page.locator(f"{CLICKABLES} >> text={AcceptCookies.CHECK_ENG} >> visible=true")
            locator_count: int = get_locator_count(buttons, page)

            # Then check for german keywords
            if locator_count == 0:
                buttons = page.locator(f"{CLICKABLES} >> text={AcceptCookies.CHECK_GER} >> visible=true")
                locator_count = get_locator_count(buttons, page)
        except Error:
            return

        # Click on each possible cookie accept button
        for i in range(locator_count):
            button: Optional[Locator] = get_locator_nth(buttons, i)
            if button is None:
                continue

            if re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                continue

            invoke_click(page, button, timeout=2000)

        # Stop earlier if we are in a frame
        if frame:
            return

        # Refresh the page
        temp_time: datetime = datetime.now()
        temp_response: Optional[Response] = refresh_page(page, url)

        # Update if needed
        if responses is not None and start is not None:
            start.append(temp_time)
            responses.append(temp_response)
