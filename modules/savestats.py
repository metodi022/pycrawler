from datetime import datetime
from logging import Logger
from typing import List, Tuple, Optional

from peewee import IntegerField, DateTimeField, TextField
from playwright.sync_api import Browser, BrowserContext, Page, Response

from config import Config
from database import DequeDB, BaseModel, database
from modules.module import Module


class URLFeedback(BaseModel):
    rank = IntegerField()
    job = TextField()
    crawler = IntegerField()
    site = TextField()
    url = TextField()
    urlfinal = TextField()
    depth = IntegerField()
    code = IntegerField()
    fromurl = TextField(null=True)
    fromurlfinal = TextField(null=True)
    start = DateTimeField()
    end = DateTimeField()
    repetition = IntegerField()


class SaveStats(Module):
    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create feedback stats table')
        with database:
            database.create_tables([URLFeedback])

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page,
                         responses: List[Optional[Response]], context_database: DequeDB,
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], modules: List[Module], repetition: int) -> None:
        super().receive_response(browser, context, page, responses, context_database, url,
                                 final_url, start, modules, repetition)

        with database.atomic():
            for i, response in enumerate((responses if len(responses) > 0 else [None])):
                code: int = response.status if response is not None else Config.ERROR_CODES['response_error']
                fromurl: Optional[str] = url[3][-1][0] if len(url[3]) > 0 else None
                fromurlfinal: Optional[str] = url[3][-1][1] if len(url[3]) > 0 else None
                URLFeedback.create(rank=self.rank, job=self.job_id, crawler=self.crawler_id,
                                   site=self.site, url=url[0], urlfinal=page.url, depth=self.depth,
                                   code=code, fromurl=fromurl, fromurlfinal=fromurlfinal,
                                   start=start[i], end=datetime.now(), repetition=repetition)
