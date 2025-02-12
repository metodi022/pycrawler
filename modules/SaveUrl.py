from typing import List, Optional

from playwright.sync_api import Response

from config import Config
from modules.Module import Module


class SaveUrl(Module):
    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        response: Optional[Response] = responses[-1] if len(responses) > 0 else None
        code: int = response.status if response is not None else Config.ERROR_CODES['response_error']

        self.crawler.url.urlfinal = final_url
        self.crawler.url.code = code
        self.crawler.url.codetext = response.status_text if response is not None else None
        self.crawler.url.referer = (response.request.header_value('Referer') if response.request is not None else None) if response is not None else None
        self.crawler.url.state = 'complete'
        self.crawler.url.save()
