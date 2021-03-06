import pathlib
import re
from typing import Optional

import tld
from playwright.sync_api import Page, Locator, Error
from tld.exceptions import TldBadUrl, TldDomainNotFound


def get_tld_object(url: str) -> Optional[tld.utils.Result]:
    try:
        return tld.get_tld(url, as_object=True)  # type: ignore
    except (TldBadUrl, TldDomainNotFound):
        return None


def get_url_origin(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc


def get_url_etldp1(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.fld


def get_url_full(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc + url.parsed_url.path


def get_url_from_href(href: str, origin: tld.utils.Result) -> Optional[tld.utils.Result]:
    if re.match('^http', href) is not None:
        res: Optional[tld.utils.Result] = get_tld_object(href)
    elif re.match('^//', href) is not None:
        res: Optional[tld.utils.Result] = get_tld_object(origin.parsed_url.scheme + ":" + href)
    else:
        if href[0] == '/':
            path: str = origin.parsed_url.path[:-1] if origin.parsed_url.path and \
                                                       origin.parsed_url.path[
                                                           -1] == '/' else origin.parsed_url.path
        else:
            path: str = origin.parsed_url.path if origin.parsed_url.path and origin.parsed_url.path[
                -1] == '/' else origin.parsed_url.path + '/'

        res: Optional[tld.utils.Result] = get_tld_object(
            origin.parsed_url.scheme + "://" + origin.parsed_url.netloc + path + href)

    return res


def get_screenshot(page: Page, path: pathlib.Path) -> None:
    if not path.exists():
        try:
            page.screenshot(path=path)
        except Error:
            return


def get_locator_count(locator: Optional[Locator]) -> int:
    if locator is None:
        return 0

    try:
        return locator.count()
    except Error:
        return 0


def get_locator_nth(locator: Optional[Locator], nth: int) -> Optional[Locator]:
    if locator is None:
        return None

    if nth >= get_locator_count(locator):
        return None

    try:
        return locator.nth(nth)
    except Error:
        return None


def get_locator_attribute(locator: Optional[Locator], attribute: str) -> Optional[str]:
    if locator is None:
        return None

    try:
        return locator.get_attribute(attribute)
    except Error:
        return None
