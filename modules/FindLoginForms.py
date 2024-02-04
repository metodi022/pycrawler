import re
import urllib.parse
from logging import Logger
from typing import Callable, List, Optional

import tld.utils
from peewee import BooleanField, ForeignKeyField, IntegerField
from playwright.sync_api import Error, Locator, Page, Response

import utils
from config import Config
from database import URL, BaseModel, Site, Task, database
from modules.Module import Module


class LoginForm(BaseModel):
    task = ForeignKeyField(Task)
    site = ForeignKeyField(Site)
    url = ForeignKeyField(URL)
    depth = IntegerField()
    success = BooleanField(null=True)


class FindLoginForms(Module):
    """
        Module to automatically find login forms.
    """

    def __init__(self, crawler) -> None:
        super().__init__(crawler)

        self._found: int = self.crawler.state.get('FindLoginForms', 0)
        self.crawler.state['FindLoginForms'] = self._found

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create login form table')
        with database:
            database.create_tables([LoginForm])

    def add_handlers(self) -> None:
        super().add_handlers()

        # Add common URLs with logins
        self.crawler.urldb.add_url(self.crawler.landing.origin + '/login/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.origin + '/signin/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.origin + '/account/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.origin + '/profile/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.origin + '/auth/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.origin + '/authenticate/', Config.DEPTH, None)

        if Config.SAME_ETLDP1:
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/login/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/signin/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/account/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/profile/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/auth/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/authenticate/', Config.DEPTH, None)

    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        # Find login forms
        form: Optional[Locator] = FindLoginForms.find_login_form(self.crawler.page, interact=(self._found < 3))
        if form is not None:
            self.crawler.log.info("Found a login form")
            self._found += 1
            self.crawler.state['FindLoginForms'] = self._found
            LoginForm.create(task=self.crawler.task, site=self.crawler.task.site, url=self.crawler.url, depth=self.crawler.depth)
            utils.get_screenshot(self.crawler.page, (Config.LOG / f"screenshots/{self.crawler.site.site}-{self.crawler.job_id}-Login{self._found}.png"))

        # If we already found login forms, don't use search engine
        if self._found > 0:
            return

        # If we are not at the end of the crawl, stop
        if (self.crawler.repetition != Config.REPETITIONS) or (len(self.crawler.urldb.get_state('free')) > 0):
            return

        # Finally, use search engine with login keyword
        self.crawler.urldb.add_url(
            f'https://www.google.com/search?q="login"++site%3A{urllib.parse.quote(self.crawler.site.site)}',
            Config.DEPTH - 1,
            None
        )

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        def filt(url: tld.utils.Result) -> bool:
            url_full: str = utils.get_url_str(url)

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
            password_fields: int = utils.get_locator_count(form.locator('input[type="password"]'))
            text_fields: int = \
                utils.get_locator_count(form.locator('input[type="email"]:visible')) +\
                utils.get_locator_count(form.locator('input[type="text"]:visible')) +\
                utils.get_locator_count(form.locator('input:not([type]):visible'))
        except Error:
            return False

        # If there is one password field -> it's a login forms
        if password_fields == 1:
            return True

        # If there is more than one password field -> it's not a login form
        # If there are no text fields or more than two text fields -> it's not a login form
        if password_fields > 1 or text_fields == 0 or text_fields > 2:
            return False

        # Find if there are login buttons
        try:
            check_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge|proceed|' \
                              r'fortfahren|anmeldung|einmeldung|submit)/i'
            button1: Locator = form.locator(f"{utils.CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return False

        # Forms that are not registration or login forms
        misc_form: bool = re.search(r'search|news.?letter|subscribe', utils.get_locator_outer_html(form) or '', flags=re.I) is not None

        # Return true if there is at least one login button in the form and avoid false positives
        return (utils.get_locator_count(button1) > 0) and (not misc_form)

    @staticmethod
    def _find_login_form(page: Page) -> Optional[Locator]:
        # Find all forms on a page
        try:
            forms: Locator = page.locator('form:visible,fieldset:visible')
        except Error:
            # Ignored
            return None

        # Check if each form is a login form
        for i in range(utils.get_locator_count(forms)):
            form: Optional[Locator] = utils.get_locator_nth(forms, i)
            if form is None or not FindLoginForms.verify_login_form(form):
                continue
            return form

        # If we did not find login forms, try to find password field
        try:
            form = page.locator('input[type="password"]:visible').locator('..')
        except Error:
            return None

        # Go up the node tree of the password field and search for login forms (w/o form tags)
        try:
            while form.count() >= 1:
                # Get relevant fields
                passwords: int = utils.get_locator_count(form.locator('input[type="password"]'))
                text_fields: int = \
                    utils.get_locator_count(form.locator('input[type="email"]:visible')) +\
                    utils.get_locator_count(form.locator('input[type="text"]:visible')) +\
                    utils.get_locator_count(form.locator('input:not([type]):visible'))

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
        except Error:
            # Ignored
            pass

        return None

    @staticmethod
    def find_login_form(page: Page, interact: bool = True) -> Optional[Locator]:
        # Get login form from page
        form: Optional[Locator] = FindLoginForms._find_login_form(page)
        if form is not None:
            return form

        # If you don't want to interact with the page and click on potential buttons, stop here
        if not interact:
            return None

        # Get all buttons with login keywords
        try:
            check_str: str = r'/log.?in|sign.?in|melde|logge|user.?name|e.?mail|nutzer|next|' \
                             r'continue|proceed|fortfahren|weiter|anmeldung|einmeldung/i'
            buttons: Locator = page.locator(f"{utils.CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return None

        # Click each button with login keyword
        for i in range(utils.get_locator_count(buttons)):
            button: Optional[Locator] = utils.get_locator_nth(buttons, i)
            if button is None:
                continue

            # Avoid clicking SSO login buttons
            if re.search(utils.SSO, utils.get_locator_outer_html(button) or '', flags=re.I) is not None:
                continue

            utils.invoke_click(page, button, 2000)

            form = FindLoginForms._find_login_form(page)
            if form is not None:
                break

        return form
