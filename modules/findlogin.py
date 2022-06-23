import re
from datetime import datetime
from logging import Logger
from typing import Type, List, Optional

import tld.utils
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_tld_object, get_url_origin


class FindLogin(Module):
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres,
                 log: Logger) -> None:
        super().__init__(job_id, crawler_id, config, database, log)
        self._url: str = ''
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINFORMS (rank INT NOT NULL, job INT NOT NULL, crawler "
            "INT NOT NULL, url VARCHAR(255) NOT NULL, loginform TEXT NOT NULL, depth INT NOT "
            "NULL);", None, False)
        log.info('Create LOGINFORMS database IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: str, rank: int) -> None:
        self._rank = rank
        self._url = url

        temp: Optional[tld.utils.Result] = get_tld_object(url)
        if temp is None:
            return
        url = get_url_origin(temp)

        context_database.add_url(url + '/login/', self._config.DEPTH, rank)
        context_database.add_url(url + '/signin/', self._config.DEPTH, rank)
        context_database.add_url(url + '/account/', self._config.DEPTH, rank)
        context_database.add_url(f"https://www.google.com/search?q=site:{url}+login+OR+signin",
                                 self._config.DEPTH - 1, 0)

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Response], context_database: DequeDB, url: str,
                         final_url: str, depth: int, start: List[datetime]) -> None:
        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        if 'google.com/search' in final_url:
            return

        forms: Locator = page.locator('form')
        for i in range(forms.count()):
            if FindLogin._find_login_form(forms.nth(i), url, page.url):
                self._database.invoke_transaction(
                    "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s)",
                    (self._rank, self.job_id, self.crawler_id, self._url, url, depth), False)

    @staticmethod
    def _find_login_form(form: Locator, url: str, final_url: str) -> bool:
        password_fields: int = form.locator('input[type="password"]').count()
        text_fields: int = form.locator('input[type="email"]:visible').count() + form.locator(
            'input[type="text"]:visible').count() + form.locator(
            'input[type="tel"]:visible').count()

        if password_fields == 1:
            return True

        if password_fields > 1:
            return False

        if text_fields != 1:
            return False

        check: str = r"log.?in|sign.?in|passwor|user.?name|account|user|melde|logg|meldung"
        result: bool = form.locator(f"text=/{check}/i").count() > 0
        result = result or re.search(check, url, re.I) is not None
        result = result or re.search(check, final_url, re.I) is not None
        return result
