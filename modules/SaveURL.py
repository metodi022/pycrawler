import json
import re
import traceback
from asyncio import CancelledError
from typing import List, Optional, cast

from bs4 import BeautifulSoup
from playwright.sync_api import Response

from config import Config
from database import URL
from modules.Module import Module


class SaveURL(Module):
    def receive_response(self, responses: List[Optional[Response]], final_url: str, repetition: int) -> None:
        super().receive_response(responses, final_url, repetition)

        with self.crawler.database.atomic():
            previous_response = None
            for response in reversed(responses):
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

                if response is not None:
                    try:
                        metaheaders = BeautifulSoup(response.text(), 'html.parser')
                        metaheaders = metaheaders.find_all('meta', attrs={'http-equiv': re.compile('.*')})
                        metaheaders = json.dumps([str(entry) for entry in metaheaders])
                    except (Exception, CancelledError) as error:
                        self.crawler.log.warning('SaveURL.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                        metaheaders = None

                if previous_response is None:
                    previous_response = self.crawler.database.execute_sql(
                        f"""
                        UPDATE URL
                        SET task_id={self.crawler.database.param},site_id={self.crawler.database.param},fromurl_id={self.crawler.database.param},redirect_id={self.crawler.database.param},url={self.crawler.database.param},urlfinal={self.crawler.database.param},depth={self.crawler.database.param},repetition={self.crawler.database.param},state={self.crawler.database.param},method={self.crawler.database.param},code={self.crawler.database.param},codetext={self.crawler.database.param},resource={self.crawler.database.param},content={self.crawler.database.param},referer={self.crawler.database.param},location={self.crawler.database.param},reqheaders={self.crawler.database.param},resheaders={self.crawler.database.param},metaheaders={self.crawler.database.param},reqbody={self.crawler.database.param},resbody={self.crawler.database.param}
                        WHERE id={self.crawler.database.param}
                        RETURNING id
                        """,
                        (
                            self.crawler.task.get_id(),
                            self.crawler.site.get_id(),
                            self.crawler.url.fromurl.get_id() if self.crawler.url.fromurl is not None else None,
                            previous_response.get_id() if previous_response is not None else None,
                            response.url if response is not None else None,
                            final_url,
                            self.crawler.url.depth,
                            repetition,
                            'complete',
                            response.request.method if response is not None else None,
                            response.status if response is not None else Config.ERROR_CODES['response_error'],
                            response.status_text if response is not None else None,
                            response.request.resource_type if response is not None else None,
                            response.header_value('Content-Type') if response is not None else None,
                            response.request.header_value('Referer') if response is not None else None,
                            response.header_value('Location') if response is not None else None,
                            json.dumps(response.request.headers_array()) if response is not None else None,
                            json.dumps(response.headers_array()) if response is not None else None,
                            metaheaders,
                            reqbody,
                            resbody,
                            self.crawler.url.get_id()
                        )
                    ).fetchone()[0]
                else:
                    previous_response = self.crawler.database.execute_sql(
                        f"""
                        INSERT INTO URL (task_id, site_id, fromurl_id, redirect_id, url, urlfinal, depth, repetition, state, method, code, codetext, resource, content, referer, location, reqheaders, resheaders, metaheaders, reqbody, resbody)
                        VALUES ({self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param},{self.crawler.database.param})
                        RETURNING id
                        """,
                        (
                            self.crawler.task.get_id(),
                            self.crawler.site.get_id(),
                            self.crawler.url.fromurl.get_id() if self.crawler.url.fromurl is not None else None,
                            previous_response,
                            response.url if response is not None else None,
                            final_url,
                            self.crawler.url.depth,
                            repetition,
                            'complete',
                            response.request.method if response is not None else None,
                            response.status if response is not None else Config.ERROR_CODES['response_error'],
                            response.status_text if response is not None else None,
                            response.request.resource_type if response is not None else None,
                            response.header_value('Content-Type') if response is not None else None,
                            response.request.header_value('Referer') if response is not None else None,
                            response.header_value('Location') if response is not None else None,
                            json.dumps(response.request.headers_array()) if response is not None else None,
                            json.dumps(response.headers_array()) if response is not None else None,
                            metaheaders,
                            reqbody,
                            resbody
                        )
                    ).fetchone()[0]

            self.crawler.url = cast(URL, URL.get_by_id(self.crawler.url.get_id()))
