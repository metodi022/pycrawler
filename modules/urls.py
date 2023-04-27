from datetime import datetime
from logging import Logger
from typing import List, Optional, Tuple

from peewee import DateTimeField, ForeignKeyField, IntegerField, TextField
from playwright.sync_api import Response

from config import Config
from database import BaseModel, Task, database
from modules.module import Module


class URL(BaseModel):
    task = ForeignKeyField(Task)
    url = TextField()
    urlfinal = TextField()
    fromurl = TextField(default=None, null=True)
    fromurlfinal = TextField(default=None, null=True)
    depth = IntegerField()
    code = IntegerField()
    repetition = IntegerField()
    start = DateTimeField()
    end = DateTimeField()


class URLs(Module):
    @staticmethod
    def register_job(log: Logger) -> None:
        log.info('Create feedback stats table')
        with database:
            database.create_tables([URL])

    def receive_response(self, responses: List[Optional[Response]],
                         url: Tuple[str, int, int, List[Tuple[str, str]]], final_url: str,
                         start: List[datetime], repetition: int) -> None:
        super().receive_response(responses, url, final_url, start, repetition)

        with database.atomic():
            for i, response in enumerate((responses if len(responses) > 0 else [None])):
                code: int = response.status if response is not None else Config.ERROR_CODES['response_error']
                fromurl: Optional[str] = url[3][-1][0] if len(url[3]) > 0 else None
                fromurlfinal: Optional[str] = url[3][-1][1] if len(url[3]) > 0 else None

                URL.create(task=self.crawler.task, url=url[0], urlfinal=self.crawler.page.url,
                           depth=self.crawler.depth, code=code, fromurl=fromurl,
                           fromurlfinal=fromurlfinal, start=start[i], end=datetime.now(),
                           repetition=self.crawler.repetition)
