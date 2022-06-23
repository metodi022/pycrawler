from datetime import datetime
from logging import Logger
from typing import Type, List, MutableSet, Optional

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_url_origin, get_tld_object, get_screenshot


class AcceptCookies(Module):
    CHECK_SEL: str = 'cookie|cookies|consent|banner|gdpr|modal|popup|policy|overlay|privacy' \
                     '|notification|notice|info|footer|message|block|disclaimer|dialog|warning' \
                     '|backdrop|accept|law|cookiebar|cookieconsent|dismissible|compliance' \
                     '|agreement|notify|legal|tracking|GDPR'

    CHECK_ENG: str = 'accept|okay|\\Wok|^ok|consent|agree|allow|understand|continue|yes'
    CHECK_GER: str = 'stimm|verstanden|versteh|akzeptier|ja|weiter|annehm'
    CHECK_TEX: str = '(' + CHECK_ENG + '|' + CHECK_GER + ')'

    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)
        self._urls: MutableSet[str] = set()
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: str, rank: int) -> None:
        self._urls = set()
        self._rank = rank

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Response], context_database: DequeDB, url: str,
                         final_url: str, depth: int, start: List[datetime]) -> None:
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: Optional[tld.utils.Result] = get_tld_object(final_url)
        if url_origin is None or get_url_origin(url_origin) in self._urls:
            return
        self._urls.add(get_url_origin(url_origin))

        # Check for buttons with certain keywords
        check: Locator = page.locator(f"text=/{AcceptCookies.CHECK_SEL}/i",
                                      has=page.locator(f"text=/{AcceptCookies.CHECK_TEX}/i"))
        buttons: Locator = page.locator(
            'button:visible,a:visible,div[role="button"]:visible,input[type="button"]:visible',
            has=check)
        button_nth: int = 0

        # Check for topmost z-index button with less restrictive keywords
        if buttons.count() == 0:
            buttons: Locator = page.locator(
                'button:visible,a:visible,div[role="button"]:visible,input[type="button"]:visible',
                has=page.locator(f"text=/{AcceptCookies.CHECK_TEX}/i"))

            z_max: int = 0
            for i in range(buttons.count()):
                z_temp = buttons.nth(i).evaluate(
                    "node => getComputedStyle(node).getPropertyValue('z-index')")
                z_max = max(z_max, 0 if z_temp == 'auto' else int(z_temp))

            for i in range(buttons.count()):
                z_temp = buttons.nth(i).evaluate(
                    "node => getComputedStyle(node).getPropertyValue('z-index')")
                z_temp = 0 if z_temp == 'auto' else int(z_temp)
                if z_temp >= z_max:
                    button_nth = i
                    break

        # TODO frame locators if buttons.count() == 0 ?

        # If no buttons found -> just exit
        self._log.info(
            f"Find {buttons.count()} possible cookie accept buttons")
        if buttons.count() == 0:
            return

        # Click on cookie button and wait some time
        buttons.nth(button_nth).click()
        page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)
        temp: datetime = datetime.now()
        response = page.goto(url, timeout=self._config.LOAD_TIMEOUT,  # type: ignore
                             wait_until=self._config.WAIT_LOAD_UNTIL)
        page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)

        get_screenshot(page, (
                self._config.LOG / f"screenshots/job{self.job_id}rank{self._rank}cookie.png"))

        if response is not None:
            start.append(temp)
            responses.append(response)
