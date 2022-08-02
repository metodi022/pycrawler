from datetime import datetime
from logging import Logger
from typing import List, MutableSet, Optional, Tuple

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_url_origin, get_tld_object, get_screenshot, get_locator_count, get_locator_nth


class AcceptCookies(Module):
    CHECK_ENG: str = '(accept|okay|ok|consent|agree|allow|understand|continue|yes|got it)'
    CHECK_GER: str = '(stimm|verstanden|versteh|akzeptier|ja|weiter|annehm|bestätig|willig|' \
                     'zulassen|lasse) '
    CHECK_TEX: str = f"^{CHECK_ENG}|\\W{CHECK_ENG}|^{CHECK_GER}|\\W{CHECK_GER}"
    ELEM_SEL: str = 'button:visible,a:visible,*[role="button"]:visible,*[onclick]:visible,' \
                    'input[type="button"]:visible,input[type="submit"]:visible'

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._urls: MutableSet[str] = set()
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB,
                     url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        self._url = url[0]
        self._rank = url[2]
        self._urls.clear()

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime]) -> None:
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = get_tld_object(final_url)
        if url_origin is None or get_url_origin(url_origin) in self._urls:
            return
        self._urls.add(get_url_origin(url_origin))

        # Check for buttons with certain keywords
        try:
            check: Locator = page.locator(f"text=/{AcceptCookies.CHECK_TEX}/i")
            buttons: Locator = page.locator(AcceptCookies.ELEM_SEL, has=check)
        except Error:
            return

        self._log.info(f"Find {get_locator_count(buttons)} possible cookie accept buttons")
        if get_locator_count(buttons) == 0:
            return

        # Check for topmost z-index button with keywords
        z_max: int = 0
        for i in range(get_locator_count(buttons)):
            button: Optional[Locator] = get_locator_nth(buttons, i)
            if button is None:
                continue

            try:
                z_temp = button.evaluate(
                    "node => getComputedStyle(node).getPropertyValue('z-index')")
            except Error:
                continue

            z_max = max(z_max, 0 if z_temp == 'auto' else int(z_temp))

        # Click on first cookie button that works and wait some time
        for i in range(get_locator_count(buttons)):
            button: Optional[Locator] = get_locator_nth(buttons, i)
            if button is None:
                continue

            try:
                z_temp = button.evaluate(
                    "node => getComputedStyle(node).getPropertyValue('z-index')")
            except Error:
                continue

            if (0 if z_temp == 'auto' else int(z_temp)) < z_max:
                continue

            try:
                button.hover(timeout=Config.WAIT_AFTER_LOAD)
                page.wait_for_timeout(500)
                button.click(timeout=Config.WAIT_AFTER_LOAD, delay=500)
                break
            except Error:
                # Empty
                pass

        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        temp: datetime = datetime.now()
        try:
            response = page.goto(url[0], timeout=Config.LOAD_TIMEOUT,
                                 wait_until=Config.WAIT_LOAD_UNTIL)
        except Error:
            start.append(temp)
            responses.append(None)
            return

        if response is None:
            start.append(temp)
            responses.append(None)
            return

        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        if self._url == url[0]:
            get_screenshot(page, (
                    Config.LOG / f"screenshots/job{self.job_id}rank{self._rank}cookie.png"))
        start.append(temp)
        responses.append(response)
