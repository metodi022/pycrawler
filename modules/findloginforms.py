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
from utils import get_tld_object, get_url_origin, get_locator_count, get_locator_nth, \
    invoke_click, CLICKABLES


class FindLoginForms(Module):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0
        self._found_site: bool = False

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
        self._found_site = False

        temp: Optional[tld.utils.Result] = get_tld_object(self._url)
        if temp is None:
            return
        url_origin: str = get_url_origin(temp)

        context_database.add_url((url_origin + '/login/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/signin/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/account/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/profile/', Config.DEPTH, self._rank, []))

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

        found_page: bool = False
        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)

            if form is None or not FindLoginForms._find_login_form(form, url[0], page.url):
                continue

            self._database.invoke_transaction(
                "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (self._rank, self.job_id, self.crawler_id, self._url, url[0], final_url, url[1],
                 url[3][-1][0] if len(url[3]) > 0 else None,
                 url[3][-1][1] if len(url[3]) > 0 else None), False)

            found_page = True
            self._found_site = True
            self._log.info(f"Found a possible login form")

            break

        check1: str = r"(log.?in|sign.?in|account|profile|auth|connect)"
        if found_page or (url[0] != self._url and re.search(check1, url[0], re.I) is None
                          and re.search(check1, page.url, re.I) is None):
            return

        buttons: Optional[Locator] = None
        try:
            check2_str: str = r'/log.?in|sign.?in|melde|logge|user.?name|e.?mail|nutzer.?name/i'
            check2: Locator = page.locator(f"text={check2_str}")
            buttons = page.locator(CLICKABLES, has=check2)
            buttons = page.locator(
                f"{CLICKABLES} >> text={check2_str}") if get_locator_count(
                buttons) == 0 else buttons
        except Error:
            # Ignored
            pass

        if buttons is not None and get_locator_count(buttons) > 0:
            self._found_site = True
            self._log.info(f"Found a possible login button")

            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)

                if button is None or re.search(
                        r"Facebook|Twitter|Google|Yahoo|Windows.?Live|LinkedIn|GitHub|PayPal|Amazon|vKontakte|Yandex|37signals|Box|Salesforce|Fitbit|Baidu|RenRen|Weibo|AOL|Shopify|WordPress|Dwolla|miiCard|Yammer|SoundCloud|Instagram|The City|Planning Center|Evernote|Exact",
                        button.text_content(), flags=re.I) is not None:
                    continue

                try:
                    invoke_click(page, get_locator_nth(buttons, i))
                    page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                    page.wait_for_load_state(timeout=Config.LOAD_TIMEOUT,
                                             state=Config.WAIT_LOAD_UNTIL)
                    page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                except Error:
                    # Ignored
                    continue

                break

            if page.url == final_url:
                self._database.invoke_transaction(
                    "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (self._rank, self.job_id, self.crawler_id, self._url, url[0], final_url, url[1],
                     url[3][-1][0] if len(url[3]) > 0 else None,
                     url[3][-1][1] if len(url[3]) > 0 else None), False)
            else:
                self._database.invoke_transaction(
                    "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (self._rank, self.job_id, self.crawler_id, self._url, page.url,
                     page.url, url[1] + 1, url[0], final_url), False)

        if self._found_site or len(context_database) > 0:
            return

        # TODO and no entries for login for Web site
        pass

    @staticmethod
    def _find_login_form(form: Locator, url: str, final_url: str) -> bool:
        try:
            password_fields: int = get_locator_count(form.locator('input[type="password"]:visible'))
            text_fields: int = get_locator_count(
                form.locator('input[type="email"]:visible')) + get_locator_count(
                form.locator('input[type="text"]:visible')) + get_locator_count(
                form.locator('input[type="tel"]:visible')) + get_locator_count(
                form.locator('input:not([type]):visible'))
        except Error:
            return False

        if password_fields > 1 or text_fields != 1:
            return False

        if password_fields == 1:
            return True

        check1: str = r"(log.?in|sign.?in|account|profile|auth|connect)"

        result: bool = re.search(check1, url, re.I) is not None
        result = result or re.search(check1, final_url, re.I) is not None

        try:
            check2_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge)/i'
            check2: Locator = form.locator(f"text={check2_str}")
            button: Locator = form.locator(CLICKABLES, has=check2)
            button = form.locator(
                f"{CLICKABLES} >> text={check2_str}") if get_locator_count(button) == 0 else button
        except Error:
            return result

        check3: str = r"search|feedback"

        return (result or get_locator_count(button) > 0) and re.search(check3, form.inner_html(),
                                                                       re.I) is None
