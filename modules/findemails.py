import re
from datetime import datetime
from logging import Logger
from typing import List, MutableSet, Optional, Tuple

import nostril  # https://github.com/casics/nostril
from peewee import BooleanField, IntegerField, TextField
from playwright.sync_api import Error, Response

from config import Config
from database import BaseModel, database
from modules.module import Module
from utils import get_tld_object


class Email(BaseModel):
    rank = IntegerField()
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    depth = IntegerField()
    email = TextField()
    nonsense = BooleanField()
    fromurl = TextField()
    fromurlfinal = TextField()


class FindEmails(Module):
    EMAILSRE: str = r'[a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+'

    def __init__(self, crawler) -> None:
        super().__init__(crawler)
        self._seen: MutableSet[str] = self.crawler.state.get('FindContactsEmail', set())

        self.crawler.state['FindContactsEmail'] = self._seen

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create email table')
        with database:
            database.create_tables([Email])

    def add_handlers(self, url: Tuple[str, int, int, List[Tuple[str, str]]]) -> None:
        super().add_handlers(url)

        self.crawler.context_database.add_url((self.crawler.origin + '/.well-known/security.txt', Config.DEPTH, self.crawler.rank, []))
        self.crawler.context_database.add_url((f"{self.crawler.scheme}://{self.crawler.site}" + '/.well-known/security.txt', Config.DEPTH, self.crawler.rank, []))

    def receive_response(self, responses: List[Optional[Response]],
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], repetition: int) -> None:
        super().receive_response(responses, url, final_url, start, repetition)

        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        try:
            html: str = self.crawler.page.content()
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

            Email.create(rank=self.crawler.rank, job=self.crawler.job_id,
                         crawler=self.crawler.crawler_id, site=self.crawler.site,
                         depth=self.crawler.depth, email=email, nonsense=nonsense,
                         fromurl=self.crawler.currenturl, finalurl=self.crawler.page.url)
