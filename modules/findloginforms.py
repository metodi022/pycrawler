import re
from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple, Callable

import tld.utils
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_tld_object, get_url_origin, get_locator_count, get_locator_nth, \
    invoke_click, CLICKABLES, get_url_full, SSO, get_outer_html


class FindLoginForms(Module):
    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0
        self._landing_page: bool = True
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
        self._landing_page = True
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

        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)

            if form is None or not FindLoginForms.find_login_form(form):
                continue

            self._database.invoke_transaction(
                "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (self._rank, self.job_id, self.crawler_id, self._url, url[0], final_url, url[1],
                 url[3][-1][0] if len(url[3]) > 0 else None,
                 url[3][-1][1] if len(url[3]) > 0 else None), False)

            self._landing_page = False
            self._found_site = True
            self._log.info(f"Found a possible login form")

            return

        if not self._landing_page:
            return
        self._landing_page = False

        buttons: Optional[Locator] = None
        try:
            check1_str: str = r'/log.?in|sign.?in|melde|logge|user.?name|e.?mail|nutzer|next|' \
                              r'continue|proceed/i'
            check1: Locator = page.locator(f"text={check1_str}")
            buttons = page.locator(CLICKABLES, has=check1)
            buttons = page.locator(
                f"{CLICKABLES} >> text={check1_str}") if get_locator_count(
                buttons) == 0 else buttons
        except Error:
            return

        if buttons is not None and get_locator_count(buttons) > 0:
            self._found_site = True
            self._log.info(f"Found a possible login button")

            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)

                if button is None or re.search(SSO, get_outer_html(button), flags=re.I) is not None:
                    continue

                try:
                    invoke_click(page, get_locator_nth(buttons, i))
                    page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                    page.wait_for_load_state(timeout=Config.LOAD_TIMEOUT,
                                             state=Config.WAIT_LOAD_UNTIL)
                    page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                except Error:
                    # Ignored
                    pass

                break
            else:
                return

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

        # TODO no entries for login for Web site -> search engine

    @staticmethod
    def find_login_form(form: Locator) -> bool:
        try:
            password_fields: int = get_locator_count(form.locator('input[type="password"]:visible'))
            text_fields: int = get_locator_count(
                form.locator('input[type="email"]:visible')) + get_locator_count(
                form.locator('input[type="text"]:visible')) + get_locator_count(
                form.locator('input[type="tel"]:visible')) + get_locator_count(
                form.locator('input:not([type]):visible'))
        except Error:
            return False

        if password_fields > 1 or text_fields == 0 or text_fields > 2:
            return False

        if password_fields == 1:
            return True

        try:
            check1_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge|proceed)/i'
            check1: Locator = form.locator(f"text={check1_str}")
            button: Locator = form.locator(CLICKABLES, has=check1)
            button = form.locator(
                f"{CLICKABLES} >> text={check1_str}") if get_locator_count(button) == 0 else button
        except Error:
            return False

        # TODO can also check HTML for keywords or ignore certain keywords

        return get_locator_count(button) > 0

    @staticmethod
    def add_url_filter_out(filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        def filt(url: tld.utils.Result) -> bool:
            return re.match(
                r'(\.js|\.mp3|\.wav|\.aif|\.aiff|\.wma|\.csv|\.pdf|\.jpg|\.png|\.gif|\.tif|\.svg'
                r'|\.bmp|\.psd|\.tiff|\.ai|\.lsm|\.3gp|\.avi|\.flv|\.gvi|\.m2v|\.m4v|\.mkv|\.mov'
                r'|\.mp4|\.mpg|\.ogv|\.wmv|\.xml|\.otf|\.ttf|\.css|\.rss|\.ico|\.cfg|\.ogg|\.mpa'
                r'|\.jpeg|\.webm|\.mpeg|\.webp)$',
                get_url_full(url), flags=re.I) is not None

        filters.append(filt)
