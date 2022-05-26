import tld
from urllib.parse import SplitResult
import re
from typing import Optional


def get_tld_object(url: str) -> Optional[tld.utils.Result]:
    return tld.get_tld(url, as_object=True)  # type: ignore


def get_origin(link: SplitResult) -> str:
    return link.scheme + '://' + link.netloc


def href_to_url(href: str, origin: tld.utils.Result) -> Optional[tld.utils.Result]:
    if re.match('^http', href):
        return get_tld_object(href)

    if re.match('^//', href):
        return get_tld_object(origin.parsed_url.scheme + ":" + href)

    if href[0] == '/':
        path: str = origin.parsed_url.path[:-
                                           1] if origin.parsed_url.path and origin.parsed_url.path[-1] == '/' else origin.parsed_url.path
    else:
        path: str = origin.parsed_url.path if origin.parsed_url.path and origin.parsed_url.path[
            -1] == '/' else origin.parsed_url.path + '/'

    return get_tld_object(origin.parsed_url.scheme + "://" + origin.parsed_url.netloc + path + href)
