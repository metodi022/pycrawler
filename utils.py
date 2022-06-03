import tld
import re
from typing import Optional


def get_tld_object(url: str) -> Optional[tld.utils.Result]:
    return tld.get_tld(url, as_object=True)  # type: ignore


def get_origin(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc


def get_url_full(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc + url.parsed_url.path


def get_url_from_href(href: str, origin: tld.utils.Result) -> Optional[tld.utils.Result]:
    res: Optional[tld.utils.Result] = None

    if re.match('^http', href) is not None:
        res = get_tld_object(href)
    elif re.match('^//', href) is not None:
        res = get_tld_object(origin.parsed_url.scheme + ":" + href)
    else:
        if href[0] == '/':
            path: str = origin.parsed_url.path[:-1] if origin.parsed_url.path and origin.parsed_url.path[-1] == '/' else origin.parsed_url.path
        else:
            path: str = origin.parsed_url.path if origin.parsed_url.path and origin.parsed_url.path[-1] == '/' else origin.parsed_url.path + '/'

        res = get_tld_object(origin.parsed_url.scheme + "://" + origin.parsed_url.netloc + path + href)

    if res is None:
        return None

    if re.match('htm$|html$|^((?!\\.).)*$', res.parsed_url.path) is None:
        return None

    return res
