from logging import Logger
from typing import List, Optional

from playwright.sync_api import Response

from config import Config
from database import URL, database
from modules.Module import Module


class FeedbackURL(Module):
    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        code: int = response.status if response is not None else Config.ERROR_CODES['response_error']

        self.crawler.url.urlfinal = final_url
        self.crawler.url.code = code
        self.crawler.url.state = 'complete'
        self.crawler.url.save()
