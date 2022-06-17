import re
from typing import Optional

import tld
from playwright.sync_api import Page


def get_tld_object(url: str) -> Optional[tld.utils.Result]:
    return tld.get_tld(url, as_object=True)


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

    if res is not None and re.match('htm$|html$|^((?!\\.).)*$', res.parsed_url.path) is None:
        return None

    if res is not None and 'https://www.google.com/search?q' in res:
        check: re.Match = re.match('(?<=url=)[^&]+', res)
        res = check.groups(0) if check is not None else res

    return res


def wait_after_load(page: Page, amount: int) -> None:
    if amount > 0:
        page.evaluate(
            'window.wait_after_load = 0; setTimeout(() => { window.wait_after_load = 1 }, ' + str(
                amount) + ');')
        page.wait_for_function('() => window.wait_after_load > 0')
