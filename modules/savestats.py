from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple

from peewee import DateTimeField, IntegerField, TextField
from playwright.sync_api import Response

from config import Config
from database import BaseModel, database
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

    def receive_response(self, responses: List[Optional[Response]],
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], repetition: int) -> None:
        super().receive_response(responses, url, final_url, start, repetition)

        with database.atomic():
            for i, response in enumerate((responses if len(responses) > 0 else [None])):
                code: int = response.status if response is not None else Config.ERROR_CODES['response_error']
                fromurl: Optional[str] = url[3][-1][0] if len(url[3]) > 0 else None
                fromurlfinal: Optional[str] = url[3][-1][1] if len(url[3]) > 0 else None
                URLFeedback.create(rank=self.crawler.rank, job=self.crawler.job_id,
                                   crawler=self.crawler.crawler_id, site=self.crawler.site,
                                   url=url[0], urlfinal=self.crawler.page.url,
                                   depth=self.crawler.depth, code=code, fromurl=fromurl,
                                   fromurlfinal=fromurlfinal, start=start[i], end=datetime.now(),
                                   repetition=repetition)
