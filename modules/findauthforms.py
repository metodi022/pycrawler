import re
import urllib.parse
from logging import Logger
from typing import Callable, List, Optional

import tld.utils
from peewee import ForeignKeyField, IntegerField, TextField
from playwright.sync_api import Error, Locator, Page, Response

from config import Config
from database import URL, BaseModel, database
from modules.module import Module
from utils import CLICKABLES, SSO, get_locator_count, get_locator_nth, get_locator_outer_html, get_tld_object, get_url_full, get_url_origin, invoke_click


class RegistrationForm(BaseModel):
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    depth = IntegerField()
    url = ForeignKeyField(URL)


class LoginForm(BaseModel):
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    depth = IntegerField()
    url = ForeignKeyField(URL)


class FindAuthForms(Module):
    """
        Module to automatically find login and registration forms.
    """

    def __init__(self, crawler) -> None:
        super().__init__(crawler)
        self._found: int = self.crawler.state.get('FindAuthForms', 0)
        self.crawler.state['FindLoginForms'] = self._found

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create login form table')
        with database:
            database.create_tables([RegistrationForm, LoginForm])

    def add_handlers(self, url: URL) -> None:
        super().add_handlers(url)

        # Common registration locations
        self.crawler.urldb.add_url(f"{self.crawler.origin}/register/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/register/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.origin}/registration/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/registration/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.origin}/signup/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/signup/", Config.DEPTH, None)

        # Common login locations
        self.crawler.urldb.add_url(f"{self.crawler.origin}/login/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/login/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.origin}/signin/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/signin/", Config.DEPTH, None)

        # Common account locations
        self.crawler.urldb.add_url(f"{self.crawler.origin}/account/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/account/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.origin}/profile/", Config.DEPTH, None)
        self.crawler.urldb.add_url(f"{self.crawler.scheme}://{self.crawler.site}/profile/", Config.DEPTH, None)

    def receive_response(self, responses: List[Optional[Response]], url: URL, final_url: str, repetition: int) -> None:
        super().receive_response(responses, url, final_url, repetition)

        # Parse current page URL
        parsed_url: Optional[tld.utils.Result] = get_tld_object(final_url)
        if parsed_url is None:
            return

        # Check for same origin
        if Config.SAME_ORIGIN and get_url_origin(parsed_url) != self.crawler.origin:
            return

        # Check for same site
        if Config.SAME_ETLDP1 and parsed_url.fld != self.crawler.site:
            return

        # TODO check for same entity

        # Find login forms
        form: Optional[Locator] = FindAuthForms.find_auth_form(self.crawler.page, interact=(self._found < 3))

        if form is not None:
            self._found += 1
            self.crawler.state['FindLoginForms'] = self._found

            if self.verify_login_form(form):
                self.crawler.log.info("Found a login form")
                LoginForm.create(job=self.crawler.job_id,
                                crawler=self.crawler.crawler_id,
                                site=self.crawler.site,
                                depth=self.crawler.depth,
                                formurl=url)
            else:
                self.crawler.log.info("Found a registration form")
                RegistrationForm.create(job=self.crawler.job_id,
                                        crawler=self.crawler.crawler_id,
                                        site=self.crawler.site,
                                        depth=self.crawler.depth,
                                        formurl=url)

        # If we are at the end of the crawl -> stop here
        if self.crawler.urldb.active != 1:
            return

        # If we haven't found authentication forms -> use search engine
        if self._found == 0:
            self.crawler.urldb.add_url('https://www.google.com/search?q=' + urllib.parse.quote(f"\"register\" site:{self.crawler.site}"), Config.DEPTH - 1, None)
            self.crawler.urldb.add_url('https://www.google.com/search?q=' + urllib.parse.quote(f"\"login\" site:{self.crawler.site}"), Config.DEPTH - 1, None)

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
            password_fields: int = get_locator_count(form.locator('input[type="password"]'))
            text_fields: int = get_locator_count(form.locator('input[type="email"]:visible')) + \
                               get_locator_count(form.locator('input[type="text"]:visible')) + \
                               get_locator_count(form.locator('input:not([type]):visible'))
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
            check_str: str = r'/regist|sign.?up|continue|next|weiter|melde|proceed|submit' \
                             r'fortfahren|anmeld/i'
            button: Locator = form.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return False

        # Return true if there is at least one registration button in the form and avoid false positives
        return get_locator_count(button) > 0

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
            password_fields: int = get_locator_count(form.locator('input[type="password"]'))
            text_fields: int = get_locator_count(form.locator('input[type="email"]:visible')) + \
                               get_locator_count(form.locator('input[type="text"]:visible')) + \
                               get_locator_count(form.locator('input:not([type]):visible'))
        except Error:
            return False

        # If there is more than one password field -> it's not a login form
        # If there are no text fields or more than two text fields -> it's not a login form
        if password_fields > 1 or text_fields == 0 or text_fields > 2:
            return False

        # Find if there are login buttons
        try:
            check_str: str = r'/log.?in|sign.?in|continue|next|weiter|anmeld|logge|proceed|' \
                             r'fortfahren|submit|melde/i'
            button: Locator = form.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return False

        # Return true if there is at least one login button in the form
        return get_locator_count(button) > 0

    @staticmethod
    def _find_auth_form(page: Page) -> Optional[Locator]:
        # Find all forms on a page
        try:
            forms: Locator = page.locator('form:visible,fieldset:visible')
        except Error:
            # Ignored
            return None

        # Check if each form is a login form
        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)

            if form is None:
                continue
            elif not FindAuthForms.verify_registration_form(form):
                continue
            elif not FindAuthForms.verify_login_form(form):
                continue

            return form

        # If we did not find auth forms, try to find password field
        try:
            forms = page.locator('input[type="password"]:visible')#.locator('..')
        except Error:
            return None

        # Go up the node tree of the password field and search for login forms (w/o form tags)
        for i in range(get_locator_count(forms)):
            form: Optional[Locator] = get_locator_nth(forms, i)
            limit: int = 5

            while (get_locator_count(form) >= 1) and (limit > 0) and form:
                # Get relevant fields
                passwords: int = get_locator_count(form.locator('input[type="password"]'))

                # Stop earlier if it cannot be an authentication form
                if passwords > 2:
                    break

                # Check if element tree is a login form
                if not FindAuthForms.verify_registration_form(form):
                    return form
                elif FindAuthForms.verify_login_form(form):
                    return form

                # Go up the node tree
                try:
                    form = form.locator('..')
                    limit -= 1
                except Error:
                    break

        return None

    @staticmethod
    def find_auth_form(page: Page, interact: bool = True) -> Optional[Locator]:
        # Get login form from page
        form: Optional[Locator] = FindAuthForms._find_auth_form(page)
        if form is not None:
            return form

        # If you don't want to interact with the page and click on potential buttons, stop here
        if not interact:
            return None

        # Get all buttons with auth keywords
        try:
            check_str : str = r'/log.?in|sign.?in|continue|next|weiter|anmeld|logge|proceed|' \
                              r'fortfahren|submit/i|user.?name|e.?mail|nutzer|melde|regist|' \
                              r'sign.?up/i'
            buttons: Locator = page.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return None

        # Click each button with auth keyword
        for i in range(get_locator_count(buttons)):
            button: Optional[Locator] = get_locator_nth(buttons, i)
            if button is None:
                continue

            # Avoid clicking SSO login buttons
            if re.search(SSO, get_locator_outer_html(button) or '', flags=re.I) is not None:
                continue

            if not invoke_click(page, button, 2000):
                continue

            form = FindAuthForms._find_auth_form(page)
            if form is not None:
                break

        return form
