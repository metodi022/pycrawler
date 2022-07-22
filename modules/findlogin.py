import re
from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple

import tld.utils
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_tld_object, get_url_origin, get_locator_count, get_locator_nth


class FindLogin(Module):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0

        # TODO add warning if not SAME_CONTEXT

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINFORMS (rank INT NOT NULL, job INT NOT NULL, "
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, loginform TEXT NOT NULL, "
            "loginformfinal TEXT NOT NULL, depth INT NOT NULL, fromurl TEXT, fromurlfinal TEXT);",
            None, False)
        log.info('Create LOGINFORMS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB,
                     url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        self._url = url[0]
        self._rank = url[2]

        temp: Optional[tld.utils.Result] = get_tld_object(self._url)
        if temp is None:
            return
        url_origin: str = get_url_origin(temp)

        context_database.add_url((url_origin + '/login/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/signin/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/account/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/profile/', Config.DEPTH, self._rank, []))
        # TODO search engine fallback ?

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime]) -> None:
        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        try:
            forms: Locator = page.locator('form:visible', has=page.locator('input[type]:visible'))
        except Error:
            return

        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)

            if form is None or not FindLogin._find_login_form(form, url[0], page.url):
                continue

            self._database.invoke_transaction(
                "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (self._rank, self.job_id, self.crawler_id, self._url, url[0], final_url, url[1],
                 url[3][-1][0] if len(url[3]) > 0 else None,
                 url[3][-1][1] if len(url[3]) > 0 else None), False)

    @staticmethod
    def _find_login_form(form: Locator, url: str, final_url: str) -> bool:
        try:
            password_fields: int = form.locator('input[type="password"]').count()
            text_fields: int = form.locator('input[type="email"]:visible').count() + form.locator(
                'input[type="text"]:visible').count() + form.locator(
                'input[type="tel"]:visible').count()
        except Error:
            return False

        if password_fields > 1 or text_fields != 1:
            return False

        if password_fields == 1:
            return True

        check_login: str = r"(log.?in|sign.?in|account|profile)"
        check_register: str = r"(sign.?up|register)"

        result: bool = ((re.search(check_login, url, re.I) is not None) and (
                re.search(check_register, url, re.I) is None))
        result = result or ((re.search(check_login, final_url, re.I) is not None) and (
                re.search(check_register, final_url, re.I) is None))
        return result
