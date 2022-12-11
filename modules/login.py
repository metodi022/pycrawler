import re
from datetime import datetime
from logging import Logger
from typing import List, Tuple, Callable, Optional, cast, Dict, Any

import tld
from peewee import CharField, DateField, DoesNotExist
from playwright.sync_api import Browser, BrowserContext, Page, Response, Error, Locator

from config import Config
from database import DequeDB, BaseModel, database
from modules.acceptcookies import AcceptCookies
from modules.findloginforms import FindLoginForms, LoginForm
from modules.module import Module
from utils import get_locator_count, get_locator_nth, CLICKABLES, \
    get_locator_attribute, get_outer_html, invoke_click, SSO, get_label_for, \
    get_url_full_with_query_fragment, get_visible_extra


class Account(BaseModel):
    site = CharField()
    email = CharField()
    username = CharField()
    password = CharField()
    firstname = CharField()
    lastname = CharField()
    birthday = DateField()
    gender = CharField()


class Login(Module):
    ERROR_MESSAGE: str = r"(\W|^)(incorrect|wrong|falsch|fehlerhaft|ungültig|ungueltig|" \
                         r"not match|stimmt nicht|existiert nicht|doesn't match|doesn't exist|" \
                         r"not exist|isn't right|not right|nicht richtig|fail|fehlgeschlagen|" \
                         r"wasn't right|not right)(\W|$)"

    LOGOUTKEYWORDS = r'log.?out|sign.?out|log.?off|sign.?off|exit|quit|invalidate|ab.?melden|' \
                     r'aus.?loggen|ab.?meldung|verlassen|aus.?treten|annullieren'

    def __init__(self, job_id: int, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, log, state)
        self.login = False
        self.loginurl: Optional[str] = None
        self._account: Optional[Account] = None
        self._cookies: Optional[AcceptCookies] = None

    @staticmethod
    def register_job(log: Logger) -> None:
        FindLoginForms.register_job(log)
        log.info('Create accounts table')
        with database:
            database.create_tables([Account])

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        if Config.ACCEPT_COOKIES:
            self._cookies = cast(AcceptCookies, modules[0])

        # Get account from database
        try:
            self._account = Account.get(site=self.site)
        except DoesNotExist:
            self._log.info(f"Found no credentials for {self.site}")
            return

        self._account = cast(Account, self._account)

        if self._state.get('Login', None) is not None:
            self.login = True
            self.loginurl = self._state['Login']
            return

        # Get URLs with login forms for given site
        formsurls = LoginForm.select().where(LoginForm.site == self.site).execute()
        if formsurls.count == 0:
            self._log.info(f"Found no login URLs for {self.site}")
            return

        account: Tuple[str, str, str, str, str] = cast(Tuple[str, str, str, str, str],
                                                       (self._account.email,
                                                        self._account.username,
                                                        self._account.password,
                                                        self._account.firstname,
                                                        self._account.lastname))

        # Iterate over login form URLs
        formurl: LoginForm
        for formurl in formsurls:
            self._log.info(f"Get login URL {formurl.formurl}")

            if not Login.login(browser, context, self.site, formurl.formurl, self._cookies, account):
                if formurl.success is None:
                    formurl.success = False
                    formurl.save()
                continue

            self._log.info(f"Login success {formurl.formurl}")
            self.login = True
            self.loginurl = formurl
            self._state['Login'] = self.loginurl
            formurl.success = True
            formurl.save()
            return

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url,
                                 final_url, start, modules, repetition)

        # Check if we are at the end of the crawl
        if len(context_database) > 0 or repetition < Config.REPETITIONS:
            return

        # TODO At the end of the crawl check if we are still logged-in
        # TODO screenshots + better feedback

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        # Ignore URLs which could lead to logout
        def filt(url: tld.utils.Result) -> bool:
            return re.search(Login.LOGOUTKEYWORDS, get_url_full_with_query_fragment(url),
                             flags=re.I) is not None

        filters.append(filt)

    @staticmethod
    def login(browser: Browser, context: BrowserContext, domainurl: str, loginurl: str,
              cookies: Optional[AcceptCookies], account: Tuple[str, str, str, str, str]) -> bool:
        # Navigate to login form URL
        page: Page = context.new_page()
        try:
            response: Optional[Response] = page.goto(loginurl, timeout=Config.LOAD_TIMEOUT,
                                                     wait_until=Config.WAIT_LOAD_UNTIL)
        except Error:
            page.close()
            return False

        # Check if response status is valid
        if response is None or response.status >= 400:
            page.close()
            return False

        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

        # Accept cookie banners, sometimes they block login forms
        if Config.ACCEPT_COOKIES and cookies is not None:
            cookies = cast(AcceptCookies, cookies)
            cookies.receive_response(browser, context, page, [response], DequeDB(),
                                     (loginurl, 0, 0, []), page.url, [], [], 1, force=True)

        # Find login form
        form: Optional[Locator] = FindLoginForms.find_login_form(page)
        if form is None:
            page.close()
            return False

        # If filling of login form fails, continue to next login form URL
        if not Login._fill_login_form(page, form, account):
            page.close()
            return False

        # If posting login form fails, continue to next login form URL
        if not Login._post_login_form(page, form):
            page.close()
            return False

        # Verify that login is successful
        result: bool = Login.verify_login_after_post(browser, context, page, form, domainurl, loginurl, cookies, account)
        page.close()
        return result

    @staticmethod
    def _fill_login_form(page: Page, form: Locator, account: Tuple[str, str, str, str, str]) -> bool:
        # Find relevant fields
        try:
            password_field: Locator = form.locator('input[type="password"]:visible')
            text_fields: Locator = form.locator(
                'input[type="email"],input[type="text"],input:not([type]):visible')
        except Error:
            return False

        # Iterate over all text fields and fill them
        for i in range(get_locator_count(text_fields)):
            text_field: Optional[Locator] = get_locator_nth(text_fields, i)
            text_type: Optional[str] = get_locator_attribute(text_field, 'type')
            label: Locator = get_label_for(form, get_locator_attribute(text_field, 'id') or '')
            placeholder: str = get_locator_attribute(text_field, 'placeholder') or ''

            # If not visible, skip
            if text_field is None or not get_visible_extra(text_field):
                continue

            # Decide if it is email or username
            try:
                if (text_type is not None and text_type == 'email') or \
                        re.search(r'e.?mail', get_outer_html(text_field) or '', flags=re.I):
                    text_field.type(account[0], delay=100)
                    break
                elif label.count() == 1 and re.search(r'e.?mail', get_outer_html(label) or '',
                                                      flags=re.I):
                    text_field.type(account[0], delay=100)
                    break
                elif re.search(r'e.?mail', placeholder, flags=re.I):
                    text_field.type(account[0], delay=100)
                    break
                else:
                    text_field.type(account[1], delay=100)
                    break
            except Error:
                # Ignored
                pass
        else:
            # If no text field was filled, fill all possible text fields with email
            for i in range(get_locator_count(text_fields)):
                text_field: Optional[Locator] = get_locator_nth(text_fields, i)
                if text_field is None or not get_visible_extra(text_field):
                    continue

                try:
                    text_field.type(account[0], delay=100)
                except Error:
                    continue

                break
            else:
                return False

        page.wait_for_timeout(500)

        # Check if password field is visible, if not try to click on a next/continue button
        # This is helpful for two-step logins
        if get_locator_count(password_field) != 1 or not get_visible_extra(password_field):
            # Get possible buttons similar to continue/next
            try:
                check_str: str = r'/log.?in|sign.?in|continue|next|weiter|melde|logge|e.?mail|' \
                                  r'user.?name|nutzer.?name|fortfahren|anmeldung|einmeldung|submit/i'
                buttons = form.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
            except Error:
                return False

            # Iterate over buttons and try to click them
            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)

                # Ignore certain buttons (SSO, help links, registration links)
                if button is None:
                    continue
                elif re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                    continue
                elif re.search(r'help|trouble|regist', get_outer_html(button) or '',
                               flags=re.I) is not None:
                    continue

                # Click on a button
                try:
                    invoke_click(page, button, 5000)
                    page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                    page.wait_for_load_state(timeout=Config.LOAD_TIMEOUT,
                                             state=Config.WAIT_LOAD_UNTIL)
                    page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                except Error:
                    # Ignored
                    pass

                break
            else:
                return False

            # Try to get password field again
            try:
                password_field: Locator = form.locator('input[type="password"]:visible')
            except Error:
                return False

            # If password field does not show again, return
            if get_locator_count(password_field) == 0 or not get_visible_extra(password_field):
                return False

        # Type password
        try:
            password_field.type(account[2], delay=100)
        except Error:
            return False

        page.wait_for_timeout(500)
        return True

    @staticmethod
    def _post_login_form(page: Page, form: Locator) -> bool:
        # Locate login button
        try:
            check_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge|fortfahren|' \
                             r'anmeldung|einmeldung|submit)/i'
            buttons = form.locator(f"{CLICKABLES} >> text={check_str} >> visible=true")
        except Error:
            return False

        # If no login button detected, return
        if get_locator_count(buttons) == 0:
            return False

        # Iterate over login buttons and find the correct one to click
        for i in range(get_locator_count(buttons)):
            button = get_locator_nth(buttons, i)

            # Ignore certain buttons for SSO, registration or help/trouble
            if button is None:
                continue
            elif re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                continue
            elif re.search(r'help|trouble|regist', get_outer_html(button) or '',
                           flags=re.I) is not None:
                continue

            # Click on button
            try:
                invoke_click(page, button, 5000)
                page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                page.wait_for_load_state(timeout=Config.LOAD_TIMEOUT, state=Config.WAIT_LOAD_UNTIL)
                page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
            except Error:
                # Ignored
                pass

            break
        else:
            return False

        return True

    @staticmethod
    def verify_login_after_post(browser: Browser, context: BrowserContext, page: Page,
                                form: Locator, domainurl: str, loginurl: Optional[str],
                                cookies: Optional[AcceptCookies],
                                account: Tuple[str, str, str, str, str]) -> bool:
        # Initialize variables
        error_message: bool = False
        captcha: bool = False
        verification: bool = False

        # Check for verification message
        inputs: Locator = page.locator('input:visible')
        # Iterate over inputs and detect if they are verification inputs
        for i in range(get_locator_count(inputs)):
            input_: Optional[Locator] = get_locator_nth(inputs, i)
            input_label: Locator = get_label_for(page, get_locator_attribute(input_, 'id') or '')
            if input_ is None:
                continue

            if re.search(r'(\W|^)(verify|verification)(\W|$)', get_outer_html(input_) or '',
                         flags=re.I) is not None:
                verification = True
                break

            if input_label.count() == 1 and re.search(r'(\W|^)(verify|verification)(\W|$)',
                                                      get_outer_html(input_label) or '',
                                                      flags=re.I) is not None:
                verification = True
                break

        # Search for captcha and error messages
        try:
            error_message = re.search(Login.ERROR_MESSAGE, form.inner_html(timeout=5000),
                                      flags=re.I) is not None
            captcha = re.search(r'captcha', form.inner_html(), flags=re.I) is not None
        except Error:
            redirected = True

        if captcha or error_message or verification:
            return False

        # If no error messages, captcha or verification exist, verify login successful
        return Login.verify_login(browser, context, domainurl, loginurl, cookies, account)

    @staticmethod
    def verify_login(browser: Browser, context: BrowserContext, domainurl: str,
                     loginurl: Optional[str], cookies: Optional[AcceptCookies],
                     account: Tuple[str, str, str, str, str]):
        # Create a fresh context
        context_alt: BrowserContext = browser.new_context()
        page_alt: Page = context_alt.new_page()
        page: Page = context.new_page()

        # Navigate to landing page
        try:
            response: Optional[Response] = page.goto(domainurl, timeout=Config.LOAD_TIMEOUT,
                                                     wait_until=Config.WAIT_LOAD_UNTIL)
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

            response_alt: Optional[Response] = page_alt.goto(domainurl, timeout=Config.LOAD_TIMEOUT,
                                                             wait_until=Config.WAIT_LOAD_UNTIL)
            page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error:
            page.close()
            page_alt.close()
            context_alt.close()
            return False

        # Verify response
        if response is None or response.status >= 400:
            page.close()
            page_alt.close()
            context_alt.close()
            return False

        # Verify response
        if response_alt is None or response_alt.status >= 400:
            page.close()
            page_alt.close()
            context_alt.close()
            return False

        # Accept cookies if needed
        if Config.ACCEPT_COOKIES and cookies is not None:
            cookies = cast(AcceptCookies, cookies)
            cookies.receive_response(browser, context, page, [response], DequeDB(), (page.url, 0, 0, []),
                                     page.url, [], [], 1, force=True)
            cookies.receive_response(browser, context_alt, page_alt, [response_alt], DequeDB(),
                                     (page_alt.url, 0, 0, []), page_alt.url, [], [], 1, force=True)

        # Search page HTML for account indicators (name, username, email)
        if Login._verify_account_indicator(page, account[0], account[1], account[3], account[4]) and not Login._verify_account_indicator(page_alt, account[0], account[1], account[3], account[4]):
            page.close()
            page_alt.close()
            context_alt.close()
            return True

        # Search page HTML for logout element
        if Login._verify_logout_element(page) and not Login._verify_logout_element(page_alt):
            page.close()
            page_alt.close()
            context_alt.close()
            return True

        if loginurl is None:
            page.close()
            page_alt.close()
            context_alt.close()
            return False

        # Check if login page is still accessible
        try:
            response = page.goto(loginurl, timeout=Config.LOAD_TIMEOUT,
                                 wait_until=Config.WAIT_LOAD_UNTIL)
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error:
            return True

        try:
            response_alt = page_alt.goto(loginurl, timeout=Config.LOAD_TIMEOUT,
                                         wait_until=Config.WAIT_LOAD_UNTIL)
            page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error:
            return False

        if response_alt is None or response_alt.status >= 400:
            return False

        if response is None or response.status >= 400:
            return True

        # Try to find login forms
        form = FindLoginForms.find_login_form(page)
        return form is None

    @staticmethod
    def _verify_account_indicator(page: Page, email: str, username: str, first: str, last: str) -> bool:
        # Get page HTML
        try:
            html: str = page.content()
        except Error:
            return False

        # Search page HTML for account indicators
        return re.search(f"(^|\\W)({email}|{username or email}|{first or email}|{last or email})($|\\W)", html, flags=re.I) is not None

    @staticmethod
    def _verify_logout_element(page: Page) -> bool:
        # Get clickable elements with logout keyword
        try:
            buttons: Locator = page.locator(
                f"{CLICKABLES} >> text=/{Login.LOGOUTKEYWORDS}/i >> visible=true")
            if get_locator_count(buttons, page) > 0:
                return True
        except Error:
            pass

        # Get URLs with logout keyword
        try:
            urls: Locator = page.locator(f"a[href] >> text=/{Login.LOGOUTKEYWORDS}/i >> visible=true")
        except Error:
            return False

        return urls.count() > 0
