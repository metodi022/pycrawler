from datetime import datetime
from logging import Logger
from typing import Type, List, MutableSet

from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Cookie

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_url_origin, get_tld_object


class AcceptCookies(Module):
    CHECK_SEL: str = 'cookie|cookies|consent|banner|gdpr|modal|popup|policy|overlay|privacy' \
                     '|notification|notice|info|footer|message|block|disclaimer|dialog|warning' \
                     '|backdrop|accept|law|cookiebar|cookieconsent|dismissible|compliance' \
                     '|agreement|notify|legal|tracking|GDPR'

    CHECK_ENG: str = 'accept|okay|ok|consent|agree|allow|understand|continue|yes'
    CHECK_GER: str = "stimm|verstanden|versteh|akzeptier|ja|weiter|annehm"
    CHECK_TEX: str = CHECK_ENG + '|' + CHECK_GER

    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)
        self._urls: MutableSet[str] = set()

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: str, rank: int) -> None:
        self._urls = set()

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Response], context_database: DequeDB, url: str,
                         final_url: str, depth: int, start: List[datetime]) -> None:
        response: Response = responses[-1]
        if response is None or response.status >= 400:
            return

        # Check if we already accepted cookies for origin
        url_origin: str = get_url_origin(get_tld_object(final_url))
        if url_origin in self._urls:
            return
        self._urls.add(url_origin)

        # Save cookies initially
        cookies: List[Cookie] = context.cookies()

        # Check for buttons with certain keywords
        check: Locator = page.locator(f"text=/{AcceptCookies.CHECK_SEL}/i",
                                      has=page.locator(f"text=/{AcceptCookies.CHECK_TEX}/i"))
        buttons: Locator = page.locator('button:visible', has=check)
        button_nth: int = 0

        # Check for topmost z-index button with less restrictive keywords
        if buttons.count() == 0:
            buttons: Locator = page.locator('button:visible',
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
        if buttons.count() == 0:
            return

        # Click on cookie button and wait some time
        buttons.nth(button_nth).click()
        page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)

        # After clicking on cookie accept, check if cookie is changed and only then reload
        if not AcceptCookies._compare_cookies(cookies, context.cookies()):
            temp: datetime = datetime.now()
            response = page.goto(url)
            page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)
            if response is not None:
                start.append(temp)
                responses.append(response)

    @staticmethod
    def _compare_cookies(cookies1: List[Cookie], cookies2: List[Cookie]) -> bool:
        if len(cookies1) != len(cookies2):
            return False

        seen: bool = False
        equal: bool = True
        for cookie1 in cookies1:
            for cookie2 in cookies2:
                if cookie1.get("name") == cookie2.get("name"):
                    seen = True
                    equal = cookie1.get("value") == cookie2.get("value")

            if not seen or not equal:
                return False

            seen = False
            equal = True

        return True
