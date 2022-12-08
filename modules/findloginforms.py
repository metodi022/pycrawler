import re
import urllib.parse
from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple, Callable, Dict, Any

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

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger,
                 state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, database, log, state)
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
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        self._found = self._state.get('FindLoginForms', self._found)
        self._state['FindLoginForms'] = self._found

        temp: Optional[tld.utils.Result] = get_tld_object(self.domainurl)
        if temp is None:
            return

        # Add common URLs with logins
        url_origin: str = get_url_origin(temp)
        context_database.add_url((url_origin + '/login/', Config.DEPTH, self.rank, []))
        context_database.add_url((url_origin + '/signin/', Config.DEPTH, self.rank, []))
        context_database.add_url((url_origin + '/account/', Config.DEPTH, self.rank, []))
        context_database.add_url((url_origin + '/profile/', Config.DEPTH, self.rank, []))

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url, final_url, start, modules, repetition)

        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Find login forms
        form: Optional[Locator] = FindLoginForms.find_login_form(page, interact=(self._found >= 3))
        if form is not None:
            self._found += 1
            self._state['FindLoginForms'] = self._found

            self._database.invoke_transaction(
                "INSERT INTO LOGINFORMS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                    self.rank, self.job_id, self.crawler_id, self.domainurl, self.currenturl,
                    page.url, self.depth, url[3][-1][0] if len(url[3]) > 0 else None,
                    url[3][-1][1] if len(url[3]) > 0 else None), False)

        # If we already found entries or there are still URLs left -> stop here
        if self._found > 0 or len(context_database) > 0:
            return

        # Finally, use search engine with login keyword
        context_database.add_url((urllib.parse.quote(f"https://www.google.com/search?q=\"login\" site:{self.domainurl}"), Config.DEPTH - 1, self.rank, []))

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        def filt(url: tld.utils.Result) -> bool:
            url_full: str = get_url_full(url)

            # Ignore URLs which possibly do not lead to HTML pages, because login forms should only be found on HTML pages
            return re.search(
                r'(\.js|\.mp3|\.wav|\.aif|\.aiff|\.wma|\.csv|\.pdf|\.jpg|\.png|\.gif|\.tif|\.svg'
                r'|\.bmp|\.psd|\.tiff|\.ai|\.lsm|\.3gp|\.avi|\.flv|\.gvi|\.m2v|\.m4v|\.mkv|\.mov'
                r'|\.mp4|\.mpg|\.ogv|\.wmv|\.xml|\.otf|\.ttf|\.css|\.rss|\.ico|\.cfg|\.ogg|\.mpa'
                r'|\.jpeg|\.webm|\.mpeg|\.webp)$', url_full, flags=re.I) is not None

        filters.append(filt)

    @staticmethod
    def verify_login_form(form: Locator) -> bool:
        """
        Check if given locator is a login form.

        Args:
            form (Locator): locator

        Returns:
            true if the form is a login form, otherwise false
        """

        # Get all relevant fields
        try:
            password_fields: int = get_locator_count(form.locator('input[type="password"]:visible'))
            text_fields: int = get_locator_count(
                form.locator('input[type="email"]:visible')) + get_locator_count(
                form.locator('input[type="text"]:visible')) + get_locator_count(
                form.locator('input:not([type]):visible'))
        except Error:
            return False

        # If there is more than one password field -> it's not a login form
        # If there are not one or two text fields -> it's not a login form
        if password_fields > 1 or text_fields < 1 or text_fields > 2:
            return False

        # Find if there are login buttons
        try:
            check_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge|proceed|' \
                              r'fortfahren|anmeldung|einmeldung|submit)/i'
            button: Locator = form.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return False

        # Return true if there is at least one login button in the form and avoid false positives
        return get_locator_count(button) > 0 and re.search(r'search|news.?letter|subscribe', get_outer_html(form) or '', flags=re.I) is None

    @staticmethod
    def _find_login_form(page: Page) -> Optional[Locator]:
        # Find all forms on a page
        try:
            forms: Locator = page.locator('form:visible,fieldset:visible')
        except Error:
            # Ignored
            return None

        # Check if each form is a login form
        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)
            if form is None or not FindLoginForms.verify_login_form(form):
                continue
            return form

        # If we did not find login forms, try to find password field
        try:
            form = page.locator('input[type="password"]:visible').locator('..')
        except Error:
            return None

        # Go up the node tree of the password field and search for login forms (w/o form tags)
        while form.count() == 1:
            # Get relevant fields
            passwords: int = get_locator_count(form.locator('input[type="password"]:visible'))
            text_fields: int = get_locator_count(
                form.locator('input[type="email"]:visible')) + get_locator_count(
                form.locator('input[type="text"]:visible')) + get_locator_count(
                form.locator('input:not([type]):visible'))

            # Stop earlier if it cannot be a login form
            if passwords != 1 or text_fields > 2:
                return None

            # Check if element tree is a login form
            if FindLoginForms.verify_login_form(form):
                return form

            # Go up the node tree
            try:
                form = form.locator('..')
            except Error:
                return None

        return None

    @staticmethod
    def find_login_form(page: Page, interact: bool = True) -> Optional[Locator]:
        # Get login form from page
        form: Optional[Locator] = FindLoginForms._find_login_form(page)
        if form is not None:
            return form

        # If you don't want to interact with the page
        # and click on potential login buttons, stop here
        if not interact:
            return None

        # Get all buttons with login keywords
        try:
            check_str: str = r'/log.?in|sign.?in|melde|logge|user.?name|e.?mail|nutzer|next|' \
                             r'continue|proceed|fortfahren|weiter|anmeldung|einmeldung/i'
            buttons: Locator = page.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return None

        # Click each button with login keyword
        for i in range(get_locator_count(buttons, page)):
            button: Optional[Locator] = get_locator_nth(buttons, i)
            if button is None:
                continue

            # Avoid clicking SSO login buttons
            if re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                continue

            invoke_click(page, button, 2000)

            form = FindLoginForms._find_login_form(page)
            if form is not None:
                break

        return form
