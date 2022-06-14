import re
from logging import Logger
from typing import Type, Optional

from playwright.sync_api import Browser, BrowserContext, Page, Response, Locator

from config import Config
from database.dequedb import DequeDB
from database.postgres import Postgres
from modules.module import Module


class FindLogin(Module):
    def __init__(self, job_id: int, crawler_id: int, config: Type[Config], database: Postgres, log: Logger) -> None:
        self.job_id: int = job_id
        self.crawler_id: int = crawler_id
        self._config: Type[Config] = config
        self._database: Postgres = database
        self._log: Logger = log
        self._url: str = ''
        self._rank: int = 0

    @staticmethod
    def register_job(database: Postgres, log: Logger):
        database.invoke_transaction(
            "CREATE TABLE IF NOT EXISTS LOGINFORMS (rank INT NOT NULL, job INT NOT NULL, crawler INT NOT NULL, "
            "url VARCHAR(255) NOT NULL, loginform TEXT NOT NULL, depth INT NOT NULL);", None, False)
        log.info('Create LOGINFORMS database IF NOT EXISTS')

    def add_handlers(self, browser: Browser, context: BrowserContext, page: Page, context_database: DequeDB,
                     url: str, rank: int) -> None:
        self._rank = rank
        self._url = url

    def receive_response(self, browser: Browser, context: BrowserContext, page: Page, response: Response,
                         context_database: DequeDB, url: str, depth: int) -> Response:
        forms: Locator = page.locator('form')
        for i in range(forms.count()):
            if FindLogin._find_login_form(forms.nth(i), url, page.url):
                self._database.invoke_transaction(
                    "INSERT INTO LOGIN_FORMS (rank, job, crawler, url, loginform, depth) "
                    "VALUES (%i, %i, %i, %s, %s, %i)",
                    (self._rank, self.job_id, self.crawler_id, self._url, url, depth), False)
        return response

    @staticmethod
    def _find_login_form(form: Locator, url: str, final_url: str) -> bool:
        fields: Locator = form.locator('input')
        password_fields: int = 0
        text_fields: int = 0

        for i in range(fields.count()):
            field: Locator = fields.nth(i)
            field_type: Optional[str] = field.get_attribute('type')

            if not field_type:
                continue

            password_fields += (field_type == 'password')
            text_fields += (field_type in {'text', 'email', 'tel'} and field.is_visible())

        if password_fields == 1:
            return True

        if password_fields > 1:
            return False

        if text_fields == 1:
            check: re.Pattern = re.compile("log.?in|sign.?in", re.S | re.I)
            return form.filter(has_text=check).count() != 0 or re.search(check, url) or re.search(check, final_url)

        return False
