import re
from datetime import datetime
from logging import Logger
from typing import List, Tuple, MutableSet, Optional, Dict, Any

import nostril  # https://github.com/casics/nostril
import tld.utils
from peewee import IntegerField, CharField, BooleanField
from playwright.sync_api import Browser, BrowserContext, Page, Response, Error

from config import Config
from database import DequeDB, BaseModel, database
from modules.module import Module
from utils import get_tld_object, get_url_origin


class Email(BaseModel):
    rank = IntegerField()
    job = IntegerField()
    crawler = IntegerField()
    site = CharField()
    depth = IntegerField()
    email = CharField()
    nonsense = BooleanField()
    fromurl = CharField()
    fromurlfinal = CharField()


class FindEmails(Module):
    EMAILSRE: str = r'[a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+'

    def __init__(self, job_id: int, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, log, state)
        self._seen: MutableSet[str] = set()

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create email table')
        with database:
            database.create_tables([Email])

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        self._seen = self._state.get('FindContactsEmail', self._seen)
        self._state['FindContactsEmail'] = self._seen

        temp: Optional[tld.utils.Result] = get_tld_object(self.site)
        if temp is None:
            return

        url_origin: str = get_url_origin(temp)
        context_database.add_url(
            (url_origin + '/.well-known/security.txt', Config.DEPTH, self.rank, []))

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url, final_url, start, modules, repetition)

        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        try:
            html: str = page.content()
        except Error:
            return

        email: str
        for email in re.findall(FindEmails.EMAILSRE, html):
            if email in self._seen:
                continue
            self._seen.add(email)

            # Check for valid TLD
            if get_tld_object("https://" + email.split('@')[1]) is None:
                continue

            # Check if email makes sense (some emails are actually API endpoints)
            # If email is random -> mark it as nonsense
            nonsense: bool = False
            try:
                nonsense = nostril.nonsense(email)
            except Exception:
                # Ignored
                pass

            Email.create(rank=self.rank, job=self.job_id, crawler=self.crawler_id, site=self.site,
                         depth=self.depth, email=email, nonsense=nonsense, fromurl=self.currenturl,
                         finalurl=page.url)
