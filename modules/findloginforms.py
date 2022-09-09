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
    """
        Module to automatically find login forms.
    """

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0
        self._found: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINFORMS (rank INT NOT NULL, job INT NOT NULL, "
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, loginform TEXT NOT NULL, "
            "loginformfinal TEXT NOT NULL, depth INT NOT NULL, fromurl TEXT, fromurlfinal TEXT);",
            None, False)
        log.info('Create LOGINFORMS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        self._url = url[0]
        self._rank = url[2]
        self._found = 0

        temp: Optional[tld.utils.Result] = get_tld_object(self._url)
        if temp is None:
            return

        # Add common URLs with logins
        url_origin: str = get_url_origin(temp)
        context_database.add_url((url_origin + '/login/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/signin/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/account/', Config.DEPTH, self._rank, []))
        context_database.add_url((url_origin + '/profile/', Config.DEPTH, self._rank, []))

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module]) -> None:
        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Get all visible forms on the page
        try:
            forms: Locator = page.locator('form:visible', has=page.locator('input:visible'))
        except Error:
            return

        # Iterate over each form
        for i in range(get_locator_count(forms)):
            # Get and verify form
            form: Optional[Locator] = get_locator_nth(forms, i)
            if form is None or not FindLoginForms.find_login_form(form):
                continue

            # Add form to the database
            self._database.invoke_transaction(
                "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (self._rank, self.job_id, self.crawler_id, self._url, url[0], final_url, url[1],
                 url[3][-1][0] if len(url[3]) > 0 else None,
                 url[3][-1][1] if len(url[3]) > 0 else None), False)

            self._found += 1
            return

        # Next step searches for login buttons, but only do it if we haven't seen a login form
        if self._found > 3:
            return

        # Get all buttons with login keywords
        buttons: Optional[Locator]
        try:
            check1_str: str = r'/log.?in|sign.?in|melde|logge|user.?name|e.?mail|nutzer|next|' \
                              r'continue|proceed|fortfahren/i'
            check1: Locator = page.locator(f"text={check1_str}")
            buttons = page.locator(CLICKABLES, has=check1)
            buttons = page.locator(
                f"{CLICKABLES} >> text={check1_str}") if get_locator_count(
                buttons) == 0 else buttons
        except Error:
            return

        if buttons is not None and get_locator_count(buttons) > 0:
            self._found += 1
            self._log.info(f"Found a possible login button")

        # Iterate over each button
        for i in range(get_locator_count(buttons)):
            # Get button and validate
            button: Optional[Locator] = get_locator_nth(buttons, i)
            if button is None:
                continue

            # If button is an SSO login -> ignore
            if re.search(SSO, get_outer_html(button), flags=re.I) is not None:
                continue

            # Click button and wait for some time
            try:
                invoke_click(page, get_locator_nth(buttons, i), 5000)
                page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                page.wait_for_load_state(timeout=Config.LOAD_TIMEOUT,
                                         state=Config.WAIT_LOAD_UNTIL)
                page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
            except Error:
                # Ignored
                pass

            # TODO here I can search again for forms before adding

            # Add url to database
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

            return

        # If we already found entries -> finish
        if self._found > 0 or len(context_database) > 0:
            return

        # TODO no entries for login for Web site -> use search engine with login keywords

    @staticmethod
    def find_login_form(form: Locator) -> bool:
        """
        Check if given form is a login form.

        Args:
            form (Locator): form

        Returns:
            true if the form is a login form, otherwise false
        """

        # Get all relevant fields
        try:
            password_fields: int = get_locator_count(form.locator('input[type="password"]:visible'))
            text_fields: int = get_locator_count(
                form.locator('input[type="email"]:visible')) + get_locator_count(
                form.locator('input[type="text"]:visible')) + get_locator_count(
                form.locator('input[type="tel"]:visible')) + get_locator_count(
                form.locator('input:not([type]):visible'))
        except Error:
            return False

        # If there is more than one password field -> it's not a login form
        # If there are not one or two text fields -> it's not a login form
        if password_fields > 1 or text_fields == 0 or text_fields > 2:
            return False

        # If there is exactly one password field -> it's a login form
        if password_fields == 1:
            return True

        # Find if there are login buttons
        try:
            check1_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge|proceed|' \
                              r'fortfahren)/i'
            check1: Locator = form.locator(f"text={check1_str}")
            button: Locator = form.locator(CLICKABLES, has=check1)
            button = form.locator(
                f"{CLICKABLES} >> text={check1_str}") if get_locator_count(button) == 0 else button
            button = form.locator(f'input[type="submit"]:visible') if get_locator_count(
                button) == 0 else button
        except Error:
            return False

        # Return true if there is at least one login button in the form
        return get_locator_count(button) > 0 and re.search(
            r'search|news.?letter|subscribe|contact|feedback', get_outer_html(form),
            flags=re.I) is None

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        def filt(url: tld.utils.Result) -> bool:
            url_full: str = get_url_full(url)

            # Ignore URLs which possibly do not lead to HTML pages, because login forms can only be
            # found on HTML pages
            return re.match(
                r'(\.js|\.mp3|\.wav|\.aif|\.aiff|\.wma|\.csv|\.pdf|\.jpg|\.png|\.gif|\.tif|\.svg'
                r'|\.bmp|\.psd|\.tiff|\.ai|\.lsm|\.3gp|\.avi|\.flv|\.gvi|\.m2v|\.m4v|\.mkv|\.mov'
                r'|\.mp4|\.mpg|\.ogv|\.wmv|\.xml|\.otf|\.ttf|\.css|\.rss|\.ico|\.cfg|\.ogg|\.mpa'
                r'|\.jpeg|\.webm|\.mpeg|\.webp)$', url_full, flags=re.I) is not None

        filters.append(filt)
