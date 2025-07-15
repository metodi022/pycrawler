import traceback
from asyncio import CancelledError

from playwright.sync_api import Request, Route

from modules.Module import Module


class InstrumentMedia(Module):
    def add_handlers(self) -> None:
        # Create page handler
        def handler(route: Route, request: Request) -> None:
            if request.method != 'GET':
                route.fallback()
                return

            if request.resource_type not in {'image', 'media'}:
                route.fallback()
                return

            response = route.fetch(method='HEAD')
            response.method = 'GET'

            # TODO actual real fake images?
            route.fulfill(body='', response=response, status=response.status)

        # Set context handler
        try:
            self.crawler.context.route(
            '**/*',
            handler
        )
        except (Exception, CancelledError) as error:
            self.crawler.log.warning('InstrumentMedia.py:%s %s', traceback.extract_stack()[-1].lineno, error)

        # Set page handler
        try:
            self.crawler.page.route(
            '**/*',
            handler
        )
        except (Exception, CancelledError) as error:
            self.crawler.log.warning('InstrumentMedia.py:%s %s', traceback.extract_stack()[-1].lineno, error)
