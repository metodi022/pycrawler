import re
import urllib.parse
from datetime import datetime
from logging import Logger
from typing import Dict, Any, Tuple, List, Callable, Optional

import tld
from peewee import IntegerField, CharField
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database import DequeDB, BaseModel, database
from modules.module import Module
from utils import get_url_full, get_locator_count, get_tld_object, get_url_origin, CLICKABLES, \
    get_outer_html, get_locator_nth, SSO, invoke_click


class RegistrationForm(BaseModel):
    rank = IntegerField()
    job = IntegerField()
    crawler = IntegerField()
    site = CharField()
    depth = IntegerField()
    formurl = CharField()
    formurlfinal = CharField()


class FindRegistrationForms(Module):
    """
        Module to automatically find registration forms.
    """

    def __init__(self, job_id: int, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, log, state)
        self._found: int = 0

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create registration form table')
        with database:
            database.create_tables([RegistrationForm])

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        self._found = self._state.get('FindRegistrationForms', self._found)
        self._state['FindRegistrationForms'] = self._found

        temp: Optional[tld.utils.Result] = get_tld_object(self.url)
        if temp is None:
            return

        # Add common URLs for registration
        url_origin: str = get_url_origin(temp)
        context_database.add_url((url_origin + '/register/', Config.DEPTH, self.rank, []))
        context_database.add_url((url_origin + '/registration/', Config.DEPTH, self.rank, []))
        context_database.add_url((url_origin + '/signup/', Config.DEPTH, self.rank, []))
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

        # Find registration forms
        form: Optional[Locator] = FindRegistrationForms.find_registration_form(page, interact=(
                self._found >= 3))
        if form is not None:
            self._found += 1
            self._state['FindRegistrationForms'] = self._found
            RegistrationForm.create(rank=self.rank, job=self.job_id, crawler=self.crawler_id,
                                    site=self.site, depth=self.depth, formurl=self.currenturl,
                                    formurlfinal=page.url)

        # If we already found entries or there are still URLs left -> stop here
        if self._found > 0 or len(context_database) > 0:
            return

        # Do not use search engine without recursive option
        if not Config.RECURSIVE or Config.DEPTH <= 0:
            return

        # Finally, use search engine with registration keyword
        context_database.add_url((urllib.parse.quote(f"https://www.google.com/search?q=\"register\" site:{self.site}"), Config.DEPTH - 1, self.rank, []))

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        def filt(url: tld.utils.Result) -> bool:
            url_full: str = get_url_full(url)

            # Ignore URLs which possibly do not lead to HTML pages, because registration forms should only be found on HTML pages
            return re.search(
                r'(\.js|\.mp3|\.wav|\.aif|\.aiff|\.wma|\.csv|\.pdf|\.jpg|\.png|\.gif|\.tif|\.svg'
                r'|\.bmp|\.psd|\.tiff|\.ai|\.lsm|\.3gp|\.avi|\.flv|\.gvi|\.m2v|\.m4v|\.mkv|\.mov'
                r'|\.mp4|\.mpg|\.ogv|\.wmv|\.xml|\.otf|\.ttf|\.css|\.rss|\.ico|\.cfg|\.ogg|\.mpa'
                r'|\.jpeg|\.webm|\.mpeg|\.webp)$', url_full, flags=re.I) is not None

        filters.append(filt)

    @staticmethod
    def verify_registration_form(form: Locator) -> bool:
        """
        Check if given locator is a registration form.

        Args:
            form (Locator): locator

        Returns:
            true if the form is a registration form, otherwise false
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

        # If there are two or more password fields -> it's a registration form
        if password_fields > 1:
            return True

        # If there are no text fields -> it's not a registration form
        if text_fields == 0:
            return False

        # Find if there are registration buttons
        try:
            check_str: str = r'/(regist|sign.?up|continue|next|weiter|melde|proceed|submit' \
                             r'fortfahren|anmeldung)/i'
            button1: Locator = form.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return False

        # Find if there is registration link
        button2: Optional[Locator] = None
        try:
            check_str = r'/regist|sing.?up/i'
            button2 = form.locator(f"a[href] >> text={check_str} >> visible=true")
        except Error:
            # Ignored
            pass

        # Forms that are not registration or login forms
        misc_form: bool = re.search(r'search|news.?letter|subscribe', get_outer_html(form) or '', flags=re.I) is not None

        # Return true if there is at least one registration button in the form and avoid false positives
        return get_locator_count(button1) > 0 and get_locator_count(button2) == 0 and not misc_form

    @staticmethod
    def _find_registration_form(page: Page) -> Optional[Locator]:
        # Find all forms on a page
        try:
            forms: Locator = page.locator('form:visible,fieldset:visible')
        except Error:
            # Ignored
            return None

        # Check if each form is a registration form
        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)
            if form is None or not FindRegistrationForms.verify_registration_form(form):
                continue
            return form

        # If we did not find registration forms, try to find password field
        try:
            form = page.locator('input[type="password"]:visible').locator('..')
        except Error:
            return None

        # Go up the node tree of the password field and search for registration forms (w/o form tags)
        while form.count() == 1:
            # Get relevant fields
            passwords: int = get_locator_count(form.locator('input[type="password"]:visible'))

            # Stop earlier if it cannot be a registration form
            if passwords > 2:
                return None

            # Check if element tree is a registration form
            if FindRegistrationForms.verify_registration_form(form):
                return form

            # Go up the node tree
            try:
                form = form.locator('..')
            except Error:
                break

        return None

    @staticmethod
    def find_registration_form(page: Page, interact: bool = True) -> Optional[Locator]:
        # Get registration form from page
        form: Optional[Locator] = FindRegistrationForms._find_registration_form(page)
        if form is not None:
            return form

        # If you don't want to interact with the page
        # and click on potential login buttons, stop here
        if not interact:
            return None

        # Get all buttons with registration keywords
        try:
            check_str: str = r'/regist|sign.?up|melde|user.?name|e.?mail|nutzer|next|' \
                             r'continue|proceed|fortfahren|weiter|anmeldung/i'
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

            form = FindRegistrationForms._find_registration_form(page)
            if form is not None:
                break

        return form
