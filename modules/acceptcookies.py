from datetime import datetime
from logging import Logger
from typing import Optional, Type

from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class AcceptCookies(Module):
    CHECK_SEL: str = 'cookie|com|cookies|de|consent|banner|pl|gdpr|modal|bar|data|popup|policy' \
                     '|overlay|privacy|uk|notification|nl|notice|info|co|footer|message|it|fr' \
                     '|domain|fixed|eu|app|org|root|box|block|net|disclaimer|es|dialog|se|content' \
                     '|at|warning|widget|cc|ui|backdrop|accept|dk|hu|cz|wrap|__next|no|be|ng|bg' \
                     '|page|ru|law|layer|io|show|min|panel|toast|cmp|site|cookiebar|cnil|fi|hr|m' \
                     '|cookieconsent|ch|aria|dismissible|button|is|module|msg|component|ro|active' \
                     '|global|br|sticky|rodo|role|dismissable|in|visible|gr|messages|wp|elementor' \
                     '|pt|cp|manager|compliance|b|settings|agreement|first|notify|si|home|legal' \
                     '|tracking|last|system|GDPR|dsgvo|plugins|gov|___gatsby|tv|sk|form'

    CHECK_ENG: str = 'accept|okay|ok|consent|agree|allow|understand|continue|yes'
    CHECK_GER: str = "stimm|verstanden|versteh|akzeptier|ja|weiter|annehm"
    CHECK_TEX: str = CHECK_ENG + '|' + CHECK_GER

    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)
        self.url: str = ''

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        # Empty
        pass

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: str, rank: int) -> None:
        self.url = url

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         response: Optional[Response], context_database: DequeDB, url: str,
                         final_url: str, depth: int, start: datetime) -> Optional[Response]:
        if self.url == url:
            check: Locator = page.locator(f"text=/{AcceptCookies.CHECK_SEL}/i",
                                          has=page.locator(f"text=/{AcceptCookies.CHECK_TEX}/i"))
            buttons: Locator = page.locator('button:visible', has=check)

            if buttons.count() > 0:
                buttons.nth(0).click()
                page.wait_for_timeout(5000)

            page.goto(final_url)
            page.wait_for_timeout(self._config.WAIT_AFTER_LOAD)

        return response
