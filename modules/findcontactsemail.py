import re
from datetime import datetime
from logging import Logger
from typing import List, Tuple, MutableSet, Optional

import nostril  # https://github.com/casics/nostril
import tld.utils
from playwright.sync_api import Browser, BrowserContext, Page, Response, Error

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module
from utils import get_tld_object, get_url_origin


class FindContactsEmail(Module):
    EMAILSRE: str = r'[a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+'

    def __init__(self, job_id: int, crawler_id: int, database: Postgres, log: Logger) -> None:
        super().__init__(job_id, crawler_id, database, log)
        self._url: str = ''
        self._rank: int = 0
        self._seen: MutableSet[str] = set()

    @staticmethod
    def register_job(database: Postgres, log: Logger) -> None:
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS CONTACTSEMAIL (rank INT NOT NULL, job INT NOT NULL,"
            "crawler INT NOT NULL, url VARCHAR(255) NOT NULL, emailurl TEXT NOT NULL, "
            "emailurlfinal TEXT NOT NULL, depth INT NOT NULL, email TEXT NOT NULL, "
            "gibberish BOOLEAN NOT NULL, fromurl TEXT, fromurlfinal TEXT)", None, False)
        log.info('Create CONTACTSEMAIL table IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        self._url = url[0]
        self._rank = url[2]
        self._seen.clear()

        temp: Optional[tld.utils.Result] = get_tld_object(url[0])
        if temp is None:
            return

        url_origin: str = get_url_origin(temp)
        context_database.add_url(
            (url_origin + '/.well-known/security.txt', Config.DEPTH, self._rank, []))

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module]) -> None:
        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        try:
            html: str = page.content()
        except Error:
            return

        email: str
        for email in re.findall(FindContactsEmail.EMAILSRE, html):
            if email in self._seen:
                continue

            self._seen.add(email)
            self._database.invoke_transaction(
                'INSERT INTO CONTACTSEMAIL VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (self._rank, self.job_id, self.crawler_id, self._url, url[0], final_url, url[1],
                 email, nostril.nonsense(email.split('@')[0]) == 'real',
                 url[3][-1][0] if len(url[3]) > 0 else None,
                 url[3][-1][1] if len(url[3]) > 0 else None), False)
