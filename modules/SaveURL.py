import json
import re
import traceback
from asyncio import CancelledError
from typing import List, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import Response

from config import Config
from database import URL
from modules.Module import Module


class SaveURL(Module):
    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        response: Optional[Response] = responses[-1] if len(responses) > 0 else None

        try:
            reqbody = response.request.post_data_buffer if response is not None else None
        except (Exception, CancelledError) as error:
            self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
            reqbody = None

        try:
            resbody = response.body() if response is not None else None
        except (Exception, CancelledError) as error:
            self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
            resbody = None

        if (response is not None) and (response.request.resource_type == 'document'):
            try:
                metaheaders = BeautifulSoup(response.text(), 'html.parser')
                metaheaders = metaheaders.find_all('meta', attrs={'http-equiv': re.compile('.*')})
                metaheaders = json.dumps([str(entry['content']) for entry in metaheaders])
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                metaheaders = None

        self.crawler.url.urlfinal = final_url
        self.crawler.url.method = response.request.method if response is not None else None
        self.crawler.url.code = response.status if response is not None else Config.ERROR_CODES['response_error']
        self.crawler.url.codetext = response.status_text if response is not None else None
        self.crawler.url.content = response.header_value('Content-Type') if response is not None else None
        self.crawler.url.resource = response.request.resource_type if response is not None else None
        self.crawler.url.referer = response.request.header_value('Referer') if response is not None else None
        self.crawler.url.location = response.header_value('Location') if response is not None else None
        self.crawler.url.reqheaders = json.dumps(response.request.headers_array()) if response is not None else None
        self.crawler.url.resheaders = json.dumps(response.headers_array()) if response is not None else None
        self.crawler.url.metaheaders = metaheaders
        self.crawler.url.reqbody = reqbody
        self.crawler.url.resbody = resbody
        self.crawler.url.state = 'complete'
        self.crawler.url.save()

        previous_response = self.crawler.url
        for response in responses[1:]:
            try:
                reqbody = response.request.post_data_buffer if response is not None else None
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                reqbody = None

            try:
                resbody = response.body() if response is not None else None
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                resbody = None

            if (response is not None) and (response.request.resource_type == 'document'):
                try:
                    metaheaders = BeautifulSoup(response.text(), 'html.parser')
                    metaheaders = json.dumps([str(entry) for entry in metaheaders.find_all('meta')])
                except (Exception, CancelledError) as error:
                    self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                    metaheaders = None

            previous_response = URL.create(
                task=self.crawler.task,
                site=self.crawler.site,
                url=response.url if response is not None else None,
                urlfinal=final_url,
                fromurl=self.crawler.url.fromurl,
                redirect=previous_response,
                depth=self.crawler.url.depth,
                method=response.request.method if response is not None else None,
                code=response.status if response is not None else Config.ERROR_CODES['response_error'],
                codetext=response.status_text if response is not None else None,
                content=response.header_value('Content-Type') if response is not None else None,
                resource=response.request.resource_type if response is not None else None,
                repetition=repetition,
                referer=response.request.header_value('Referer') if response is not None else None,
                location = response.header_value('Location') if response is not None else None,
                reqheaders=json.dumps(response.request.headers_array()) if response is not None else None,
                resheaders=json.dumps(response.headers_array()) if response is not None else None,
                metaheaders=metaheaders,
                reqbody=reqbody,
                resbody=resbody,
                state='complete'
            )
