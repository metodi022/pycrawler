import re
import urllib.parse
from logging import Logger
from typing import List, Optional

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
    KEYWORDS_1 = r'/(log.?in|sign.?in|logge|anmeldung|anmelde|auth|' \
                 r'user.?name|e.?mail|nutzer|passwor|account|konto|mitglied)/i'
    KEYWORDS_2 = r'/(continue|next|weiter|proceed|fortfahren|submit|access|enter|eintragen|zugang)/i'

    IGNORE = r'search|news.?letter|subscribe'

    def __init__(self, crawler) -> None:
        super().__init__(crawler)

        self._found: int = self.crawler.state.get('FindLoginForms', 0)
        self.crawler.state['FindLoginForms'] = self._found

        if not self.crawler.initial:
            return

        # Search engine with login keyword
        self.crawler.urldb.add_url(
            f'https://www.google.com/search?q="login"+site%3A{urllib.parse.quote(self.crawler.site.site)}',
            Config.DEPTH - 1,
            None
        )

        # Add common URLs with logins
        self.crawler.urldb.add_url(self.crawler.landing.url + '/login/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.url + '/signin/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.url + '/account/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.url + '/profile/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.url + '/auth/', Config.DEPTH, None)
        self.crawler.urldb.add_url(self.crawler.landing.url + '/authenticate/', Config.DEPTH, None)

        if Config.SAME_ETLDP1:
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/login/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/signin/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/account/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/profile/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/auth/', Config.DEPTH, None)
            self.crawler.urldb.add_url(self.crawler.landing.scheme + '://' + self.crawler.site.site + '/authenticate/', Config.DEPTH, None)

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create login form table')

        with database:
            database.create_tables([LoginForm])

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
        if (password_fields > 1) or (text_fields == 0) or (text_fields > 2):
            return False

        # Find if there are login buttons
        try:
            button1: Locator = form.locator(f"{utils.CLICKABLES} >> text={FindLoginForms.KEYWORDS_1} >> visible=true")
            button1 = button1 if utils.get_locator_count(button1) > 0 else form.locator(f"{utils.CLICKABLES} >> text={FindLoginForms.KEYWORDS_2} >> visible=true")
        except Error:
            return False

        # Forms that are not registration or login forms
        misc_form = re.search(FindLoginForms.IGNORE, utils.get_locator_outer_html(form) or '', flags=re.I)

        # Return true if there is at least one login button in the form and avoid false positives
        return (utils.get_locator_count(button1) > 0) and (misc_form is None)

    @staticmethod
    def _find_login_form(page: Page) -> Optional[Locator]:
        # Find all forms on a page
        try:
            forms: Locator = page.locator('form:visible,fieldset:visible')

            # Check if each form is a login form
            for i in range(utils.get_locator_count(forms)):
                form: Optional[Locator] = utils.get_locator_nth(forms, i)

                if (form is not None) and (FindLoginForms.verify_login_form(form)):
                    return form
        except Error:
            # Ignored
            return None

        # If we did not find login forms, try to find password field
        try:
            form = page.locator('input[type="password"]').locator('..')
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
            return None

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
            buttons: Locator = page.locator(f"{utils.CLICKABLES} >> text={FindLoginForms.KEYWORDS_1} >> visible=true")
            buttons = buttons if utils.get_locator_count(buttons) > 0 else page.locator(f"{utils.CLICKABLES} >> text={FindLoginForms.KEYWORDS_2} >> visible=true")
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
