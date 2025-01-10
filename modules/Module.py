from logging import Logger
from typing import Callable, List, Optional

import tld
from playwright.sync_api import Response


class Module:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler

    @staticmethod
    def register_job(log: Logger) -> None:
        pass

    def add_handlers(self) -> None:
        pass

    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        pass

    def add_url_filter_out(self, filters: List[Callable[[tld.utils.Result], bool]]) -> None:
        pass
