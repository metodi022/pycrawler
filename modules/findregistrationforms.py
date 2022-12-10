import re
import urllib.parse
from datetime import datetime
from logging import Logger
from typing import Dict, Any, Tuple, List, Callable, Optional

import tld
from peewee import IntegerField, CharField
from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator

from config import Config
from database import DequeDB, BaseModel, database
from modules.module import Module
from utils import get_url_full


class RegistrationForm(BaseModel):
    rank = IntegerField()
    job = IntegerField()
    crawler = IntegerField()
    site = CharField()
    depth = IntegerField()
    formurl = CharField()
    formurlfinal = CharField()


class FindRegistrationForms(Module):
    """
        Module to automatically find registration forms.
    """

    def __init__(self, job_id: int, crawler_id: int, log: Logger, state: Dict[str, Any]) -> None:
        super().__init__(job_id, crawler_id, log, state)
        self._found: int = 0

    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create registration form database')
        with database:
            database.create_tables([RegistrationForm])

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page,
                     context_database: DequeDB, url: Tuple[str, int, int, List[Tuple[str, str]]],
                     modules: List[Module]) -> None:
        super().add_handlers(browser, context, page, context_database, url, modules)

        if self.ready:
            return

        self._found = self._state.get('FindRegistrationForms', self._found)
        self._state['FindRegistrationForms'] = self._found

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url, final_url, start, modules, repetition)

        # Check if response is valid
        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        if response is None or response.status >= 400:
            return

        # Find registration forms
        form: Optional[Locator] = FindRegistrationForms.find_registration_form(page, interact=(
                self._found >= 3))
        if form is not None:
            self._found += 1
            self._state['FindRegistrationForms'] = self._found
            RegistrationForm(rank=self.rank, job=self.job_id, crawler=self.crawler_id,
                             site=self.site, depth=self.depth, formurl=self.currenturl,
                             formurlfinal=page.url)

        # If we already found entries or there are still URLs left -> stop here
        if self._found > 0 or len(context_database) > 0:
            return

        # Do not use search engine without recursive option
        if not Config.RECURSIVE or Config.DEPTH <= 0:
            return

        # Finally, use search engine with registration keyword
        context_database.add_url((urllib.parse.quote(f"https://www.google.com/search?q=\"register\" site:{self.site}"), Config.DEPTH - 1, self.rank, []))

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        def filt(url: tld.utils.Result) -> bool:
            url_full: str = get_url_full(url)

            # Ignore URLs which possibly do not lead to HTML pages, because registration forms should only be found on HTML pages
            return re.search(
                r'(\.js|\.mp3|\.wav|\.aif|\.aiff|\.wma|\.csv|\.pdf|\.jpg|\.png|\.gif|\.tif|\.svg'
                r'|\.bmp|\.psd|\.tiff|\.ai|\.lsm|\.3gp|\.avi|\.flv|\.gvi|\.m2v|\.m4v|\.mkv|\.mov'
                r'|\.mp4|\.mpg|\.ogv|\.wmv|\.xml|\.otf|\.ttf|\.css|\.rss|\.ico|\.cfg|\.ogg|\.mpa'
                r'|\.jpeg|\.webm|\.mpeg|\.webp)$', url_full, flags=re.I) is not None

        filters.append(filt)

    @staticmethod
    def find_registration_form(page: Page, interact: bool = True) -> Optional[Locator]:
        # TODO
        return None
