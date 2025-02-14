import json
import re
import traceback
from asyncio import CancelledError
from logging import Logger

from bs4 import BeautifulSoup
from peewee import BlobField, BooleanField, CharField, ForeignKeyField, IntegerField, TextField
from playwright.sync_api import Response

from database import URL, BaseModel, Site, Task, database
from modules.Module import Module


class Request(BaseModel):
    task = ForeignKeyField(Task, index=True)
    site = ForeignKeyField(Site, index=True)
    fromurl = ForeignKeyField(URL, null=True, index=True)
    redirectfrom = TextField(null=True)
    tourl = TextField()
    navigation = BooleanField(index=True)
    mainframe = BooleanField(null=True, index=True)
    serviceworker = BooleanField(index=True)
    frame = TextField(null=True)
    depth = IntegerField(index=True)
    repetition = IntegerField(index=True)
    method = CharField(index=True)
    code = IntegerField(index=True)
    codetext = CharField(index=True)
    content = CharField(null=True, index=True)
    resource = CharField(index=True)
    referer = TextField(null=True)
    location = TextField(null=True)
    reqheaders = TextField(null=True)
    resheaders = TextField(null=True)
    metaheaders = TextField(null=True)
    reqbody = BlobField(null=True)
    resbody = BlobField(null=True)

class CollectRequests(Module):
    """
    Module to collect headers, specifically CSP
    """

    @staticmethod
    def register_job(log: Logger) -> None:
        with database:
            log.info("Create CSP database")
            database.create_tables([Request])

    def add_handlers(self) -> None:
        super().add_handlers()

        # Create page handler
        def handler(response: Response) -> None:
            # Collect headers in meta tags
            try:
                metaheaders = BeautifulSoup(response.text(), 'html.parser')
                metaheaders = metaheaders.find_all('meta', attrs={'http-equiv': re.compile('.*')})
                metaheaders = json.dumps([str(entry) for entry in metaheaders])
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('CollectRequests.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                metaheaders = None

            # Get body
            try:
                reqbody = response.request.post_data_buffer
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('CollectRequests.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                reqbody = None

            try:
                resbody = response.body()
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('CollectRequests.py:%s %s', traceback.extract_stack()[-1].lineno, error)
                resbody = None

            # Record header
            try:
                Request.create(
                    task=self.crawler.task,
                    site=self.crawler.site,
                    fromurl=self.crawler.url,
                    redirectfrom=response.request.redirected_from.url if response.request.redirected_from is not None else None,
                    tourl=response.request.url,
                    navigation=response.request.is_navigation_request,
                    mainframe=(response.frame.parent_frame is None) if response.frame is not None else True,
                    serviceworker=response.from_service_worker,
                    frame=response.frame.url if response.frame is not None else None,
                    depth=self.crawler.depth,
                    repetition=self.crawler.repetition,
                    method=response.request.method,
                    code=response.status,
                    codetext=response.status_text,
                    content=response.header_value('Content-Type'),
                    resource=response.request.resource_type,
                    referer = response.request.header_value('Referer'),
                    location=response.header_value('Location'),
                    reqheaders=json.dumps(response.request.headers_array()),
                    resheaders=json.dumps(response.headers_array()),
                    metaheaders=metaheaders,
                    reqbody=reqbody,
                    resbody=resbody
                )
            except (Exception, CancelledError) as error:
                self.crawler.log.warning('CollectRequests.py:%s %s', traceback.extract_stack()[-1].lineno, error)

        # Set page handler
        try:
            self.crawler.page.on('response', handler)
        except (Exception, CancelledError) as error:
            self.crawler.log.warning('CollectRequests.py:%s %s', traceback.extract_stack()[-1].lineno, error)
