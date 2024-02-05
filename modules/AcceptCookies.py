import re
from typing import List, MutableSet, Optional

import tld
from playwright.sync_api import Error, Frame, Locator, Page, Response

import utils
from config import Config
from database import URL
from modules.Module import Module


class AcceptCookies(Module):
    """
    Module to automatically accepts cookie banners.
    """

    # Keywords for accept buttons
    CHECK_ENG: str = '/(\\W|^)(accept|okay|ok|consent|agree|allow|understand|continue|yes|' \
                     'got.?it|fine)(\\W|$)/i'
    CHECK_GER: str = '/(\\W|^)(stimm|verstanden|versteh|akzeptier|ja(\\W|$)|weiter(\\W|$)|' \
                     'annehm|bestätig|willig|lasse)/i'

    # Keywords to avoid clicking non-accepting buttons
    CHECK_IGNORE: str = r'(\W|^)(no|not|nicht|nein|limit)(\W|$)'

    def __init__(self, crawler) -> None:
        super().__init__(crawler)

        self._seen: MutableSet[str] = self.crawler.state.get('AcceptCookies', set())
        self.crawler.state['AcceptCookies'] = self._seen

    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        # Verify that response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if (response is None) or (response.status >= 400):
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = utils.get_tld_object(final_url)
        if (url_origin is None) or (utils.get_url_origin(url_origin) in self._seen):
            return
        self._seen.add(utils.get_url_origin(url_origin))

        # Accept cookies for origin
        for frame in self.crawler.page.frames[1:]:
            self.accept(frame, self.crawler.url)
        self.accept(self.crawler.page, self.crawler.url, inframe=False, responses=responses)

        self.crawler.state['Context'] = self.crawler.context.storage_state()

        # Update the screenshot of the landing page
        if self.crawler.initial and (self.crawler.repetition == 1):
            utils.get_screenshot(self.crawler.page, (Config.LOG / f"screenshots/{self.crawler.site.site}-{self.crawler.job_id}.png"), force=True)

    def accept(self, page: Page | Frame, url: URL, inframe: bool = True, responses: Optional[List[Optional[Response]]] = None) -> Optional[Response]:
        """Automatically searches buttons with accept keywords and presses them.

        Args:
            page (Page | Frame): The page or frame
            url (URL): The URL object
            inframe (bool, optional): If the page argument is a page. Defaults to True.
        """

        # Get all buttons with certain keywords
        try:
            # First check for english keywords
            buttons: Locator = page.locator(f"{utils.CLICKABLES} >> text={AcceptCookies.CHECK_ENG} >> visible=true")
            locator_count: int = utils.get_locator_count(buttons)

            # Then check for german keywords
            if locator_count == 0:
                buttons = page.locator(f"{utils.CLICKABLES} >> text={AcceptCookies.CHECK_GER} >> visible=true")
                locator_count = utils.get_locator_count(buttons)
        except Error:
            return None

        # Click on each possible cookie accept button
        for i in range(locator_count):
            button: Optional[Locator] = utils.get_locator_nth(buttons, i)
            if button is None:
                continue

            button_html: str = utils.get_locator_outer_html(button) or ''

            if re.search(utils.SSO, button_html, flags=re.I) is not None:
                continue

            if re.search(AcceptCookies.CHECK_IGNORE, button_html, flags=re.I) is not None:
                continue

            self.crawler.log.info(f"Found a cookiebanner in {'frame' if inframe else 'main'} {page.url}")

            utils.invoke_click(page, button, timeout=2000)

        # Update responses if needed
        response: Optional[Response] = None

        if not inframe:
            response = page.goto(url.url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

            if responses is not None:
                responses.append(response)

        return response
