import re
from datetime import datetime
from logging import Logger
from typing import List, Tuple, Callable, Optional

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Error, Locator

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.acceptcookies import AcceptCookies
from modules.findloginforms import FindLoginForms
from modules.module import Module
from utils import get_locator_count, get_locator_nth, CLICKABLES, \
    get_locator_attribute, get_outer_html, invoke_click, SSO, get_label_for, get_screenshot, \
    get_url_full_with_query_fragment, get_tld_object, get_url_full


class Login(Module):
    ERROR_MESSAGE: str = r"(\W|^)(incorrect|wrong|falsch|fehlerhaft|ungültig|ungueltig|" \
                         r"not match|stimmt nicht|existiert nicht|doesn't match|doesn't exist|" \
                         r"not exist|isn't right|not right|nicht richtig|fail|fehlgeschlagen|" \
                         r"wasn't right|not right)(\W|$)"

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self.success = False
        self._url: str = ''
        self._rank: int = 0
        self._account: List[Tuple[str, str, str, str, str]] = []

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINS (rank INT NOT NULL, job INT NOT NULL, "
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, loginform TEXT, "
            "loginformfinal TEXT, success BOOLEAN NOT NULL, captcha BOOLEAN NOT NULL, "
            "error BOOLEAN NOT NULL, verification BOOLEAN NOT NULL);", None, False)
        log.info('Create LOGINS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        self._url = url[0]
        self._rank = url[2]
        self.success = False

        # Get account details from database
        self._account = self._database.invoke_transaction(
            "SELECT email, username, password, first_name, last_name FROM accounts WHERE %s LIKE "
            "CONCAT(%s, site, %s)", (self._url, '%', '%'), True) or []

        # Check if we got credentials for the given site
        if self._account is None or len(self._account) == 0:
            self._log.info(f"Found no credentials for {self._url}")
            self._database.invoke_transaction(
                "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                    self._rank, self.job_id, self.crawler_id, self._url, None, None, False, False,
                    False, False), False)
            return

        # Get URLs with login forms for given site
        url_forms: Optional[List[Tuple[str]]] = self._database.invoke_transaction(
            "SELECT loginform FROM LOGINFORMS WHERE url=%s", (self._url,), True)

        # Check if we got URLs with login forms
        if url_forms is None or len(url_forms) == 0:
            self._log.info(f"Found no login URLs for {self._url}")
            self._database.invoke_transaction(
                "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                    self._rank, self.job_id, self.crawler_id, self._url, None, None, False, False,
                    False, False), False)
            return

        # Iterate over login form URLs
        for url_form in url_forms:
            self._log.info(f"Get login URL {url_form[0]}")

            # Navigate to log in form URL
            response: Optional[Response] = None
            try:
                response = page.goto(url_form[0], timeout=Config.LOAD_TIMEOUT,
                                     wait_until=Config.WAIT_LOAD_UNTIL)
            except Error as error:
                self._log.warning(error.message)

            # Check if response status is valid
            if response is None or response.status >= 400:
                self._database.invoke_transaction(
                    "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, url_form[0],
                        url_form[0], False, False, False, False), False)
                continue

            url_form_final: str = get_url_full(get_tld_object(page.url)) or page.url
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

            # Accept cookie banners, sometimes they block login forms
            if Config.ACCEPT_COOKIES:
                modules[0].receive_response(browser, context, page, [response], context_database,
                                            (url_form[0], 0, self._rank, []), page.url, [], modules)

            # Get all forms
            try:
                forms: Locator = page.locator('form:visible', has=page.locator('input:visible'))
            except Error:
                self._database.invoke_transaction(
                    "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, url_form[0], page.url,
                        False, False, False, False), False)
                continue

            # Iterate over all forms and get the login form
            form: Optional[Locator]
            for i in range(get_locator_count(forms)):
                form = get_locator_nth(forms, i)

                if form is not None and FindLoginForms.find_login_form(form):
                    break
            # If no login form is found, try clicking on a login button
            else:
                # Find login buttons
                try:
                    check_str: str = r'/log.?in|sign.?in|melde|logge|user.?name|e.?mail|nutzer|' \
                                     r'next|continue|fortfahren/i'
                    check: Locator = page.locator(f"text={check_str}")
                    buttons = page.locator(CLICKABLES, has=check)
                    buttons = page.locator(
                        f"{CLICKABLES} >> text={check_str}") if get_locator_count(
                        buttons) == 0 else buttons
                except Error:
                    self._database.invoke_transaction(
                        "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                            self._rank, self.job_id, self.crawler_id, self._url, url_form[0],
                            page.url, False, False, False, False), False)
                    continue

                # Iterate over buttons and click on correct login button
                for i in range(get_locator_count(buttons)):
                    button: Optional[Locator] = get_locator_nth(buttons, i)
                    if button is None or re.search(SSO, button.text_content(),
                                                   flags=re.I) is not None:
                        continue

                    # Click button and wait if redirect
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
                    self._database.invoke_transaction(
                        "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                            self._rank, self.job_id, self.crawler_id, self._url, url_form[0],
                            page.url, False, False, False, False), False)
                    continue

                # Get all forms again
                try:
                    forms = page.locator('form:visible',
                                         has=page.locator('input:visible'))
                except Error:
                    self._database.invoke_transaction(
                        "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                            self._rank, self.job_id, self.crawler_id, self._url, url_form[0],
                            page.url, False, False, False, False), False)
                    continue

                # Iterate over all forms again and get the correct login form
                for i in range(get_locator_count(forms)):
                    form = get_locator_nth(forms, i)

                    if form is not None and FindLoginForms.find_login_form(form):
                        break
                # If no login form is found this time, continue to next login form URL
                else:
                    self._database.invoke_transaction(
                        "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                            self._rank, self.job_id, self.crawler_id, self._url, url_form, page.url,
                            False, False, False, False), False)
                    continue

            get_screenshot(page,
                           Config.LOG / f"screenshots/job{self.job_id}rank{self._rank}login1.png",
                           True)

            # If filling of login form fails, continue to next login form URL
            if form is None or not self._fill_login_form(page, form):
                self._database.invoke_transaction(
                    "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, url_form[0], page.url,
                        False, False, False, False), False)
                continue

            get_screenshot(page,
                           Config.LOG / f"screenshots/job{self.job_id}rank{self._rank}login2.png",
                           True)

            # If posting login form fails, continue to next login form URL
            if not self._post_login_form(page, form):
                get_screenshot(page,
                               Config.LOG / f"screenshots/job{self.job_id}rank{self._rank}login3.png",
                               True)
                self._database.invoke_transaction(
                    "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, url_form, page.url,
                        False, False, False, False), False)
                continue

            get_screenshot(page,
                           Config.LOG / f"screenshots/job{self.job_id}rank{self._rank}login3.png",
                           True)

            # Accept cookie banners, sometimes they block login forms
            if Config.ACCEPT_COOKIES:
                module: AcceptCookies = modules[0]
                module.receive_response(browser, context, page, [response], context_database,
                                        (page.url, 0, self._rank, []), page.url, [], modules)

            # If login is successful, end
            if self._verify_login_after_post(page, form, url_form[0], url_form_final):
                self.success = True
                self._database.invoke_transaction(
                    "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, url_form[0], page.url,
                        True, False, False, False), False)
                break

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module]) -> None:
        pass

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        # TODO improve
        def filt(url: tld.utils.Result) -> bool:
            return re.match(r'log.?out|sign.?out|log.?off|sign.?off|exit|quit|invalidate',
                            get_url_full_with_query_fragment(url), flags=re.I) is not None

        filters.append(filt)

    def _fill_login_form(self, page: Page, form: Locator) -> bool:
        self._log.info('Fill login form')

        try:
            password_field: Locator = form.locator('input[type="password"]:visible')
            text_fields: Locator = form.locator(
                'input[type="email"],input[type="text"],input:not([type]):visible')
        except Error:
            return False

        for i in range(get_locator_count(text_fields)):
            text_field: Locator = get_locator_nth(text_fields, i) or text_fields
            text_type: Optional[str] = get_locator_attribute(text_field, 'type')
            label: Locator = get_label_for(form, get_locator_attribute(text_field, 'id') or '')
            placeholder: Locator = get_label_for(form, get_locator_attribute(text_field,
                                                                             'placeholder') or '')

            try:
                if (text_type is not None and text_type == 'email') or \
                        re.search(r'e.?mail', get_outer_html(text_field) or '', flags=re.I):
                    text_field.type(self._account[0][0], delay=100)
                    break
                elif label.count() == 1 and re.search(r'e.?mail', get_outer_html(label) or '',
                                                      flags=re.I):
                    text_field.type(self._account[0][0], delay=100)
                    break
                elif placeholder.count() == 1 and re.search(r'e.?mail',
                                                            get_outer_html(placeholder) or '',
                                                            flags=re.I):
                    text_field.type(self._account[0][0], delay=100)
                    break
                else:
                    text_field.type(self._account[0][1], delay=100)
            except Error:
                # Ignored
                pass
        else:
            for i in range(get_locator_count(text_fields)):
                try:
                    text_field: Locator = get_locator_nth(text_fields, i) or text_fields
                    text_field.type(self._account[0][0], delay=100)
                except Error:
                    return False

        page.wait_for_timeout(500)

        if get_locator_count(password_field) == 0:
            try:
                check2_str: str = r'/log.?in|sign.?in|continue|next|weiter|melde|logge|e.?mail|' \
                                  r'user.?name|nutzer.?name|fortfahren/i'
                check2: Locator = form.locator(f"text={check2_str}")
                buttons = form.locator(CLICKABLES, has=check2)
                buttons = form.locator(
                    f"{CLICKABLES} >> text={check2_str}") if get_locator_count(
                    buttons) == 0 else buttons
            except Error:
                return False

            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)

                if button is None:
                    continue
                elif re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                    continue
                elif re.search(r'help|trouble|regist', get_outer_html(button) or '',
                               flags=re.I) is not None:
                    continue

                try:
                    invoke_click(page, button)
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

            try:
                password_field: Locator = form.locator('input[type="password"]:visible')
            except Error:
                return False

            if get_locator_count(password_field) == 0:
                return False

        try:
            password_field.type(self._account[0][2], delay=100)
        except Error:
            return False

        page.wait_for_timeout(500)

        return True

    def _post_login_form(self, page: Page, form: Locator) -> bool:
        self._log.info('Post login form')

        try:
            check_str: str = r'/(log.?in|sign.?in|continue|next|weiter|melde|logge|fortfahren)/i'
            check: Locator = form.locator(f"text={check_str}")
            buttons = form.locator(CLICKABLES, has=check)
            buttons = form.locator(f"{CLICKABLES} >> text={check_str}") if get_locator_count(
                buttons) == 0 else buttons
            buttons = form.locator(f'input[type="submit"]:visible') if get_locator_count(
                buttons) == 0 else buttons
        except Error:
            return False

        if get_locator_count(buttons) == 0:
            return False

        for i in range(get_locator_count(buttons)):
            button = get_locator_nth(buttons, i)

            if button is None:
                continue
            elif re.search(SSO, get_outer_html(button) or '', flags=re.I) is not None:
                continue
            elif re.search(r'help|trouble|regist', get_outer_html(button) or '',
                           flags=re.I) is not None:
                continue

            try:
                invoke_click(page, button)
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

        return True

    def _verify_login_after_post(self, page: Page, form: Locator, url: str, final_url: str) -> bool:
        self._log.info('Verify login form')

        # Check if page is redirected or there are username/email indicators
        redirected: bool = get_url_full(get_tld_object(page.url)) != final_url
        indicators: bool = re.search(f"(^|\\W)({self._account[0][0]}|{self._account[0][1]}|"
                                     f"{self._account[0][3]}|{self._account[0][4]})($|\\W)",
                                     page.content()) is not None

        # Check if there are error messages, captcha or verifications
        error_message: bool = False
        captcha: bool = False
        verification: bool = False

        # Check for verification
        inputs: Locator = page.locator('input:visible')
        for i in range(get_locator_count(inputs)):
            input_: Optional[Locator] = get_locator_nth(inputs, i)
            input_label: Locator = get_label_for(page, get_locator_attribute(input_, 'id') or '')
            if input_ is None:
                continue

            if re.search(r'(\W|^)(verify|verification|code)(\W|$)', get_outer_html(input_) or '',
                         flags=re.I) is not None:
                verification = True
                break

            if input_label.count() == 1 and re.search(r'(\W|^)(verify|verification|code)(\W|$)',
                                                      get_outer_html(input_label) or '',
                                                      flags=re.I) is not None:
                verification = True
                break

        # Confirm redirection
        if not redirected:
            try:
                error_message = re.search(Login.ERROR_MESSAGE, form.inner_html(),
                                          flags=re.I) is not None
                captcha = re.search(r'captcha', form.inner_html(), flags=re.I) is not None
            except Error:
                redirected = True

        if indicators:
            try:
                page.goto(page.url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
                page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
                indicators = re.search(f"(^|\\W)({self._account[0][0]}|{self._account[0][1]}|"
                                       f"{self._account[0][3]}|{self._account[0][4]})($|\\W)",
                                       page.content()) is not None
                if indicators:
                    return True
            except Error:
                # Ignored
                pass

        if captcha:
            self._database.invoke_transaction(
                "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                    self._rank, self.job_id, self.crawler_id, self._url, url, page.url,
                    False, True, False, False), False)
            return False

        if error_message:
            self._database.invoke_transaction(
                "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                    self._rank, self.job_id, self.crawler_id, self._url, url, page.url,
                    False, False, True, False), False)
            return False

        if verification:
            self._database.invoke_transaction(
                "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                    self._rank, self.job_id, self.crawler_id, self._url, url, page.url,
                    False, False, False, True), False)
            return False

        # Check if page is still accessible
        try:
            response: Optional[Response] = page.goto(url, timeout=Config.LOAD_TIMEOUT,
                                                     wait_until=Config.WAIT_LOAD_UNTIL)
        except Error:
            return True

        if response is None or response.status >= 400:
            return True

        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)

        try:
            forms: Locator = page.locator('form:visible',
                                          has=page.locator('input:visible'))
        except Error:
            return True

        # Try to find login forms again
        for i in range(get_locator_count(forms)):
            form = get_locator_nth(forms, i)
            if form is not None and FindLoginForms.find_login_form(form):
                self._database.invoke_transaction(
                    "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                        self._rank, self.job_id, self.crawler_id, self._url, url, page.url,
                        False, False, False, False), False)
                return False
        else:
            try:
                check_str: str = r'/log.?in|sign.?in|[^sb]melde|[^sb]logge|user.?name|e.?mail|' \
                                 r'nutzer|next|continue|fortfahren/i'
                check: Locator = page.locator(f"text={check_str}")
                buttons = page.locator(CLICKABLES, has=check)
                buttons = page.locator(
                    f"{CLICKABLES} >> text={check_str}") if get_locator_count(
                    buttons) == 0 else buttons
            except Error:
                return True

            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)
                if button is None or re.search(SSO, button.text_content(),
                                               flags=re.I) is not None:
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
                return True

            # Get all forms again
            try:
                forms = page.locator('form:visible',
                                     has=page.locator('input:visible'))
            except Error:
                return True

            for i in range(get_locator_count(forms)):
                form = get_locator_nth(forms, i)
                if form is not None and FindLoginForms.find_login_form(form):
                    self._database.invoke_transaction(
                        "INSERT INTO LOGINS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)", (
                            self._rank, self.job_id, self.crawler_id, self._url, url, page.url,
                            False, False, False, False), False)
                    return False

        return True
