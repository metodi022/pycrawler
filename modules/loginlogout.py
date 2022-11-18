import sys
from logging import Logger
from typing import Dict, Any, Tuple, List, Optional

from playwright.sync_api import Browser, BrowserContext, Page, StorageState, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.findloginforms import FindLoginForms
from modules.findlogout import FindLogout
from modules.login import Login
from modules.module import Module
from utils import clear_cache, CLICKABLES, get_locator_count, get_locator_nth, invoke_click


class LoginLogout(Login):

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger,
                 state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, database, log, state, False)
        self._logout: bool = False

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        FindLoginForms.register_job(database, log)
        FindLogout.register_job(database, log)

        database.invoke_transaction(
            'CREATE TABLE IF NOT EXISTS LOGINLOGOUT (rank INT NOT NULL, job INT NOT NULL,'
            'crawler INT NOT NULL, url VARCHAR(255) NOT NULL, loginurl TEXT, logouturl TEXT,'
            'loginsuccess BOOLEAN NOT NULL, logoutsuccess BOOLEAN NOT NULL,'
            'relogsuccess BOOLEAN NOT NULL);', (None,), False)

        log.info('Create LOGINLOGOUT table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        # Restore old state
        self._logout = self._state.get('LoginLogout', self._logout)
        self._state['LoginLogout'] = self._logout

        if self._logout:
            self._log.info('Logout tested')
            self._log.info('Close Browser')
            page.close()
            context.close()
            browser.close()
            clear_cache(Config.RESTART,
                        Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
            sys.exit()

        # Log in
        super().add_handlers(browser, context, page, context_database, url, modules)

        # Check if login is successful
        if not self.login:
            self._log.info('Login failed')
            self._log.info('Close Browser')

            self._database.invoke_transaction(
                'INSERT INTO LOGINLOGOUT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (self.rank, self.job_id, self.crawler_id, self.domainurl, self.loginurl, None,
                 False, False, False), False)

            page.close()
            context.close()
            browser.close()
            clear_cache(Config.RESTART,
                        Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
            sys.exit()

        # Save state after login
        state: StorageState = context.storage_state()

        # Get logout URL
        logouturl: Optional[List[Tuple[str]]] = self._database.invoke_transaction(
            'SELECT fromurl FROM LOGOUTS WHERE url=%s;', (self.domainurl,), True)

        # If no logout URL, exit
        if logouturl is None:
            self._log.info('Logout failed')
            self._log.info('Close Browser')

            self._database.invoke_transaction(
                'INSERT INTO LOGINLOGOUT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (self.rank, self.job_id, self.crawler_id, self.domainurl, self.loginurl, None,
                 True, False, False), False)

            page.close()
            context.close()
            browser.close()
            clear_cache(Config.RESTART,
                        Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
            sys.exit()

        # Visit logout URL
        logouturl: str = logouturl[0][0]
        page_alt: Page = context.new_page()
        page_alt.goto(logouturl, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)

        # If logout unsuccessful, scan for logout clickable elements
        if self.verify_login(browser, context, page_alt, context_database, modules, self.loginurl):
            try:
                buttons: Locator = page.locator(
                    f"{CLICKABLES} >> text=/{FindLogout.LOGOUTKEYWORDS}/i")
            except Error:
                self._log.info('Logout failed')
                self._log.info('Close Browser')

                self._database.invoke_transaction(
                    'INSERT INTO LOGINLOGOUT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (self.rank, self.job_id, self.crawler_id, self.domainurl, self.loginurl,
                     logouturl, True, False, False), False)

                page.close()
                page_alt.close()
                context.close()
                browser.close()
                clear_cache(Config.RESTART,
                            Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
                sys.exit()

            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)

                if button is None:
                    continue

                try:
                    invoke_click(page, button, timeout=2000)
                except Error:
                    pass

                if not self.verify_login(browser, context, page_alt, context_database, modules,
                                         self.loginurl):
                    break
            else:
                self._log.info('Logout failed')
                self._log.info('Close Browser')

                self._database.invoke_transaction(
                    'INSERT INTO LOGINLOGOUT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (self.rank, self.job_id, self.crawler_id, self.domainurl, self.loginurl,
                     logouturl, True, False, False), False)

                page.close()
                page_alt.close()
                context.close()
                browser.close()
                clear_cache(Config.RESTART,
                            Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
                sys.exit()

        # Logout successful
        self._logout = True
        self._state['LoginLogout'] = self._logout
        page_alt.close()

        # Restore login state and check if login successful
        context_alt: BrowserContext = browser.new_context(storage_state=state)
        page_alt = context_alt.new_page()

        if super().verify_login(browser, context_alt, page_alt, context_database, modules,
                                self.loginurl):
            self._database.invoke_transaction(
                'INSERT INTO LOGINLOGOUT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (self.rank, self.job_id, self.crawler_id, self.domainurl, self.loginurl,
                 logouturl, True, True, True), False)
        else:
            self._database.invoke_transaction(
                'INSERT INTO LOGINLOGOUT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (self.rank, self.job_id, self.crawler_id, self.domainurl, self.loginurl,
                 logouturl, True, True, False), False)

        # Close resources
        self._log.info('Logout successful')
        self._log.info('Close Browser')
        page_alt.close()
        context_alt.close()
        page.close()
        context.close()
        browser.close()
        clear_cache(Config.RESTART,
                    Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")
        sys.exit()
