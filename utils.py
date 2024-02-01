import pathlib
import re
from typing import List, Optional

import tld
from config import Config
from playwright.sync_api import BrowserContext, Error, Frame, Locator, Page, Response
from tld.exceptions import TldBadUrl, TldDomainNotFound

CLICKABLES: str = r'button,*[role="button"],*[onclick],input[type="button"],input[type="submit"],' \
                  r'a[href="#"]'


SSO: str = r'facebook|twitter|google|yahoo|windows.?live|linked.?in|git.?hub|pay.?pal|amazon|' \
           r'v.?kontakte|yandex|37.?signals|salesforce|fitbit|baidu|ren.?ren|weibo|aol|shopify|' \
           r'word.?press|dwolla|miicard|yammer|sound.?cloud|instagram|the.?city|apple|slack|' \
           r'evernote'


def get_tld_object(url: str) -> Optional[tld.utils.Result]:
    try:
        return tld.get_tld(url, as_object=True)
    except (TldBadUrl, TldDomainNotFound):
        return None

def get_url_origin(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc

def get_url_scheme_site(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.fld

def get_url_str(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc + url.parsed_url.path

def get_url_str_with_query(url: tld.utils.Result) -> str:
    return get_url_str(url) + ('?' if url.parsed_url.query else '') + url.parsed_url.query

def get_url_str_with_query_fragment(url: tld.utils.Result) -> str:
    return get_url_str_with_query(url) + ('#' if url.parsed_url.fragment else '') + url.parsed_url.fragment

def get_url_from_href(href: str, origin: tld.utils.Result) -> Optional[tld.utils.Result]:
    if re.match('^' + origin.parsed_url.scheme, href) is not None:
        return get_tld_object(href)

    if re.match('^//', href) is not None:
        return get_tld_object(origin.parsed_url.scheme + ":" + href)

    if href[0] == '/':
        path: str = origin.parsed_url.path[:-1] if (origin.parsed_url.path and origin.parsed_url.path[-1] == '/') else origin.parsed_url.path
    else:
        path: str = origin.parsed_url.path if (origin.parsed_url.path and origin.parsed_url.path[-1] == '/') else origin.parsed_url.path + '/'

    return get_tld_object(origin.parsed_url.scheme + "://" + origin.parsed_url.netloc + path + href)


def get_screenshot(page: Page, path: pathlib.Path, force: bool = False, full_page: bool = False) -> bool:
    if path.exists() and (not force):
        return False

    try:
        page.screenshot(path=path, full_page=full_page)
        return True
    except Error:
        return False


def get_locator_count(locator: Locator) -> int:
    try:
        return locator.count()
    except Error:
        return 0

def get_locator_nth(locator: Locator, nth: int) -> Optional[Locator]:
    count: int = get_locator_count(locator)

    if (count < 1) or (nth >= count):
        return None

    try:
        return locator.nth(nth)
    except Error:
        return None

def get_locator_attribute(locator: Locator, attribute: str) -> Optional[str]:
    if get_locator_count(locator) != 1:
        return None

    try:
        return locator.get_attribute(attribute)
    except Error:
        return None

def get_locator_inner_html(locator: Locator) -> Optional[str]:
    if get_locator_count(locator) != 1:
        return None

    try:
        return locator.inner_html()
    except Error:
        return None

def get_locator_outer_html(locator: Locator) -> Optional[str]:
    if get_locator_count(locator) != 1:
        return None

    try:
        return locator.evaluate("node => node.outerHTML;")
    except Error:
        return None


def invoke_click(page: Page | Frame, clickable: Locator, timeout=30000, trial=False) -> bool:
    if get_locator_count(clickable) != 1:
        return False

    try:
        clickable.hover(timeout=timeout, trial=trial)
        page.wait_for_timeout(250)
        clickable.click(delay=350, timeout=timeout, trial=trial)
        page.wait_for_load_state(Config.WAIT_LOAD_UNTIL)
        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        return True
    except Error:
        return False


def get_visible_advanced(locator: Locator) -> bool:
    if get_locator_count(locator) != 1:
        return False

    try:
        locator.hover(trial=True)
        locator.click(trial=True)
    except Error:
        return False

    opacity: str = locator.evaluate("""
                                    node => {
                                      var resultOpacity = 1;
                                    
                                      while (node) {
                                        try {
                                          resultOpacity = Math.min(resultOpacity, window.getComputedStyle(node).getPropertyValue("opacity") || resultOpacity);
                                        }
                                        catch { }
                                        node = node.parentNode;
                                      }
                                    
                                      return resultOpacity;
                                    }
                                    """)

    return locator.is_visible() and (float(opacity) >= 0.005)


def goto(page: Page | Frame, url: str) -> Optional[Response]:
    try:
        response = page.goto(url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
    except Error:
        return None

    try:
        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
    except Error:
        # Ignored
        pass

    return response


def search_google(context: BrowserContext, query: str, page_number: int = 0, start_page: int = 0) -> List[str]:
    result: List[str] = []
    page: Page = context.new_page()

    url: str = f'https://www.google.com/search?q={query}&start={(page_number + start_page) * 10}'
    response: Optional[Response] = None

    try:
        response = page.goto(url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
    except Error:
        # Ignored
        pass

    try:
        body: str = response.text()
        result = re.findall(r'<a jsname=".+?" href=".+?" data-ved=".+?" ping=".+?">', body)
        result = re.findall(r'(?<=href=").+?(?=")', ''.join(result))
    except Error:
        # Ignored
        pass

    try:
        page.close()
    except Error:
        # Ignored
        pass

    return result
