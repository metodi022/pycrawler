import json
import os
import sys
from datetime import datetime
from logging import Logger
from typing import List, Tuple, Optional, Dict, Any

import tld
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.login import Login
from modules.module import Module
from utils import get_locator_count, get_locator_nth, get_locator_attribute, CLICKABLES, \
    get_url_from_href, get_tld_object, get_url_full_with_query, invoke_click


class FindLogout(Login):
    LOGOUTKEYWORDS = r'log.?out|sign.?out|log.?off|sign.?off|exit|quit|invalidate|ab.?melden|' \
                     r'aus.?loggen|ab.?meldung|verlassen|aus.?treten|annullieren'

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger,
                 state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, database, log, state)
        self._logout = False

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        Login.register_job(database, log)

        database.invoke_transaction(
            'CREATE TABLE IF NOT EXISTS LOGOUTS (rank INT NOT NULL, job INT NOT NULL,'
            'crawler INT NOT NULL, url VARCHAR(255) NOT NULL, fromurl TEXT, fromurlfinal TEXT,'
            'code INT NOT NULL, headers TEXT)', (None,), False)

        log.info('Create LOGOUTS table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List['Module']) -> None:
        # Log in
        super().add_handlers(browser, context, page, context_database, url, modules)

        # Restore old state
        self._logout = self._state['FindLogout'] if 'FindLogout' in self._state else self._logout

        # Check if login is successful
        if not self.success or self._logout:
            self._log.info('Login failed')
            self._log.info('Close Browser')
            page.close()
            context.close()
            browser.close()

            if Config.RESTART and (
                    Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache").exists():
                os.remove(Config.LOG / f"job{self.job_id}crawler{self.crawler_id}.cache")

            sys.exit()

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url,
                                 final_url, start, modules, repetition)

        parsed_url_final: Optional[tld.utils.Result] = get_tld_object(final_url)
        if parsed_url_final is None:
            return

        # Get all <a> tags with a href with a logout keyword
        try:
            links: Locator = page.locator(f"a[href] >> text=/{FindLogout.LOGOUTKEYWORDS}/i")
        except Error:
            return

        # Prepare an alt page
        page_alt: Page = context.new_page()

        # Visit each link and verify until logout is found
        for i in range(get_locator_count(links)):
            link: Optional[str] = get_locator_attribute(get_locator_nth(links, i), 'href')

            if link is None:
                continue

            parsed_link: Optional[tld.utils.Result] = get_url_from_href(link.strip(),
                                                                        parsed_url_final)
            if parsed_link is None:
                continue

            parsed_link_full: str = get_url_full_with_query(parsed_link)

            # Visit parsed href
            try:
                response = page_alt.goto(parsed_link_full, timeout=Config.LOAD_TIMEOUT,
                                         wait_until=Config.WAIT_LOAD_UNTIL)
            except Error:
                continue

            page_alt.wait_for_timeout(Config.WAIT_AFTER_LOAD)

            if self.verify_login(browser, context, page_alt, context_database, modules,
                                 self._url_login):
                continue

            self._logout = True

            headers: Optional[str]
            try:
                headers = json.dumps(response.headers_array())
            except ValueError:
                headers = None

            self._database.invoke_transaction(
                'INSERT INTO LOGOUTS VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (
                    self._rank, self.job_id, self.crawler_id, self._url, parsed_link_full,
                    page_alt.url, response.status, headers), False)

            break
        # Find logout clickable elements, click each, and verify logout
        else:
            try:
                buttons: Locator = page.locator(
                    f"{CLICKABLES} >> text=/{FindLogout.LOGOUTKEYWORDS}/i")
            except Error:
                page_alt.close()
                return

            for i in range(get_locator_count(buttons)):
                button: Optional[Locator] = get_locator_nth(buttons, i)

                if button is None:
                    continue

                try:
                    invoke_click(page, button, timeout=2000)
                except Error:
                    continue

            if not self.verify_login(browser, context, page, context_database, modules,
                                     self._url_login):
                self._logout = True

                self._database.invoke_transaction(
                    'INSERT INTO LOGOUTS VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (
                        self._rank, self.job_id, self.crawler_id, self._url, page.url, page.url,
                        0, None), False)

        page_alt.close()
