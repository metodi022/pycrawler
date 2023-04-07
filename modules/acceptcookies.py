import pathlib
import re
from datetime import datetime
import shutil
from typing import List, MutableSet, Optional, Tuple, cast

import tld
from playwright.sync_api import Error, Frame, Locator, Page, Response

from config import Config
from modules.module import Module
from utils import CLICKABLES, SSO, get_locator_count, get_locator_nth, get_outer_html, get_tld_object, get_url_origin, invoke_click, refresh_page


class AcceptCookies(Module):
    """
    Module to automatically accepts cookie banners.
    """

    # Keywords for accept buttons
    CHECK_ENG: str = '/(\\W|^)(accept|okay|ok|consent|agree|allow|understand|continue|yes|' \
                     'got it|fine)(\\W|$)/i'
    CHECK_GER: str = '/(\\W|^)(stimm|verstanden|versteh|akzeptier|ja(\\W|$)|weiter(\\W|$)|' \
                     'annehm|bestätig|willig|zulassen(\\W|$)|lasse)/i'

    def __init__(self, crawler) -> None:
        super().__init__(crawler)
        self._urls: MutableSet[str] = self.crawler.state.get('AcceptCookies', set())
        self.extension: bool = False

        self.crawler.state['AcceptCookies'] = self._urls

        # Don't activate extension if we restart context
        if Config.RESTARTCONTEXT:
            return

        # Initialize manifest
        path: pathlib.Path = (pathlib.Path(__file__).parent.parent / 'extensions/I-Still-Dont-Care-About-Cookies/src')
        if Config.BROWSER == 'chromium' and path.exists():
            self.extension = True
            if not (path / 'manifest.json').exists():
                shutil.copy((path / 'manifest_v2.json'), (path / 'manifest.json'))
        
        # Check if it's the start of crawl
        if self.crawler.url == self.crawler.currenturl and self.crawler.repetition == 1 and self.crawler.depth == 0:
            path = (Config.LOG / f"persistentchromium{self.crawler.job_id}{self.crawler.crawler_id}")
            if path.exists():
                self.crawler.log.debug('Deleting old Chromium persistent user data')
                shutil.rmtree(path)


    def add_handlers(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        super().add_handlers(url)

        if self.extension:
            self.crawler.page.close()
            self.crawler.context.close()
            
            path: str = str((pathlib.Path(__file__).parent.parent / 'extensions/I-Still-Dont-Care-About-Cookies/src').resolve())

            self.crawler.context = self.crawler.playwright.chromium.launch_persistent_context(
                (Config.LOG / f"persistentchromium{self.crawler.job_id}{self.crawler.crawler_id}"),
                user_agent=self.crawler.playwright.devices[Config.DEVICE]['user_agent'],
                screen=self.crawler.playwright.devices[Config.DEVICE].get('screen', {"width": 1920, "height": 1080}),
                viewport=self.crawler.playwright.devices[Config.DEVICE]['viewport'],
                device_scale_factor=self.crawler.playwright.devices[Config.DEVICE]['device_scale_factor'],
                is_mobile=self.crawler.playwright.devices[Config.DEVICE]['is_mobile'],
                has_touch=self.crawler.playwright.devices[Config.DEVICE]['has_touch'],
                locale=Config.LOCALE,
                timezone_id=Config.TIMEZONE,
                headless=False,
                args=([f"--disable-extensions-except={path}", f"--load-extension={path}"] + (["--headless=new"] if Config.HEADLESS else [])),
            )

            self.crawler.page = self.crawler.context.pages[0]


    def receive_response(self, responses: List[Optional[Response]],
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], repetition: int) -> None:
        super().receive_response(responses, url, final_url, start, repetition)

        # Verify that response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = get_tld_object(final_url)
        if (url_origin is None) or (get_url_origin(url_origin) in self._urls):
            return
        self._urls.add(get_url_origin(url_origin))

        # Accept cookies for origin
        AcceptCookies.accept(self.crawler.page, url[0], inframe=False, responses=responses, start=start, extension=self.extension)

    @staticmethod
    def accept(page: Page | Frame, url: str, inframe: bool = False,
               responses: Optional[List[Optional[Response]]] = None,
               start: Optional[List[datetime]] = None, extension: bool = False):
        if extension:
            page.wait_for_load_state(Config.WAIT_LOAD_UNTIL)
        else:
            # Check for cookies in first depth frames
            if not inframe:
                page = cast(Page, page)
                for frame in page.frames[1:]:
                    AcceptCookies.accept(frame, frame.url, inframe=True, responses=responses, start=start)

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
            if inframe:
                return

        # Refresh the page
        temp_time: datetime = datetime.now()
        temp_response: Optional[Response] = refresh_page(page, url)

        # Update if needed
        if responses is not None and start is not None:
            start.append(temp_time)
            responses.append(temp_response)
