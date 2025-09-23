import base64
import codecs
import hashlib
import html
import json
import pathlib
import re
import urllib.parse
from typing import Dict, List, Optional

import nltk
import tld
from autocorrect import Speller
from config import Config
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.stem import SnowballStemmer
from playwright.sync_api import BrowserContext, Error, Frame, Locator, Page, Response
from tld.exceptions import TldBadUrl, TldDomainNotFound


CLICKABLES: str = r'button,*[role="button"],*[onclick],*[type="button"],*[type="submit"],*[type="reset"],' \
                  r'a[href="#"]'

SSO: str = r'facebook|twitter|google|yahoo|windows.?live|linked.?in|git.?hub|pay.?pal|amazon|' \
           r'v.?kontakte|yandex|37.?signals|salesforce|fitbit|baidu|ren.?ren|weibo|aol|shopify|' \
           r'word.?press|dwolla|miicard|yammer|sound.?cloud|instagram|the.?city|apple|slack|' \
           r'evernote'


_speller = Speller(only_replacements=True)
_stop = stopwords.words('english')
_lem = WordNetLemmatizer()
_stem = SnowballStemmer('english')


def get_tld_object(url: str) -> Optional[tld.utils.Result]:
    try:
        return tld.get_tld(url, as_object=True)  # type: ignore
    except (TldBadUrl, TldDomainNotFound):
        return None

def normalize_url(url: str, query: bool = True, fragment: bool = False) -> str:
    url = url.strip().rstrip('/')

    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return url

    scheme = parsed.scheme.lower()
    netloc = parsed.hostname.lower() if parsed.hostname else ''

    if ((scheme == 'http') and (parsed.port == 80)) or ((scheme == 'https') and (parsed.port == 443)):
        pass
    elif parsed.port:
        netloc += f':{parsed.port}'

    path = parsed.path or '/'
    while '//' in path:
        path = path.replace('//', '/')

    if path != '/' and path.endswith('/'):
        path = path.rstrip('/')

    return urllib.parse.urlunparse((scheme, netloc, path, '', parsed.query if query else '', parsed.fragment if fragment else ''))

def get_url_scheme(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme

def get_url_origin(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc

def get_url_site(url: tld.utils.Result) -> str:
    return url.fld

def get_url_scheme_site(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.fld

def get_url_str(url: tld.utils.Result) -> str:
    return url.parsed_url.scheme + '://' + url.parsed_url.netloc + url.parsed_url.path

def get_url_str_with_query(url: tld.utils.Result) -> str:
    return get_url_str(url) + ('?' if url.parsed_url.query else '') + url.parsed_url.query

def get_url_str_with_query_fragment(url: tld.utils.Result) -> str:
    return get_url_str_with_query(url) + ('#' if url.parsed_url.fragment else '') + url.parsed_url.fragment

def get_url_from_href(href: str, page: tld.utils.Result) -> Optional[tld.utils.Result]:
    if (href is None) or (not href.strip()):
        return None

    href_final = urllib.parse.urljoin(get_url_str_with_query_fragment(page), href)
    return get_tld_object(href_final)

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
        page.wait_for_load_state(state=(Config.WAIT_LOAD_UNTIL if Config.WAIT_LOAD_UNTIL != 'commit' else 'load'))
        page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        return True
    except Error:
        return False

def get_visible(locator: Locator) -> bool:
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

    return locator.is_visible() and (float(opacity) > float(0))

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

    try:
        url: str = f'https://www.google.com/search?q={query}&start={(page_number + start_page) * 10}'
        response: Optional[Response] = None

        try:
            response = page.goto(url, timeout=Config.LOAD_TIMEOUT, wait_until=Config.WAIT_LOAD_UNTIL)
            page.wait_for_timeout(Config.WAIT_AFTER_LOAD)
        except Error:
            # Ignored
            pass

        if response is not None:
            body: str = response.text()
            result = re.findall(r'<a jsname=".+?" href=".+?" data-ved=".+?" ping=".+?">', body)
            result = re.findall(r'(?<=href=").+?(?=")', ''.join(result))
    finally:
        page.close()

    return result

def tokenize(data: str, autocorrect: bool = False) -> str:
    result = data.strip().lower()

    result = re.sub(r'\s+', ' ', result.replace('\n', ' ').replace('\t', ' ')).strip()
    result = re.sub(r'[^A-Za-z\s-]', '', result).strip()

    if autocorrect:
        result = _speller(result)

    result = [entry for entry in result.split() if entry.strip().replace('-', '').strip().isalpha()]
    result = nltk.word_tokenize(' '.join(result))
    result = [entry for entry in result if entry.strip().replace('-', '').strip().isalpha()]

    result = [_lem.lemmatize(entry) for entry in result]

    result = [entry for entry in result if entry not in _stop and len(entry) > 1]

    result = [entry for entry in result if entry.strip().replace('-', '').strip().isalpha()]

    result = [_stem.stem(entry) for entry in result]

    return ''.join(result)

def decode(data: str) -> Dict[str, bytes | str]:
    data = data.strip()
    result = {}

    result['text'] = data

    try:
        result['url_plus'] = urllib.parse.unquote_plus(data)
    except Exception:
        pass

    try:
        result['url'] = urllib.parse.unquote(data)
    except Exception:
        pass

    try:
        result['html'] = html.unescape(data)
    except Exception:
        pass

    try:
        result['rot13'] = codecs.decode(data, "rot_13")
    except Exception:
        pass

    try:
        result['unicode'] = bytes(data, "utf-8").decode("unicode_escape")
    except Exception:
        pass

    try:
        result['punycode'] = data.encode().decode('idna')
    except Exception:
        pass

    try:
        result['base64'] = base64.b64decode(data)
        try:
            result['base64'] = result['base64'].decode()
        except UnicodeDecodeError:
            pass
    except Exception:
        pass

    try:
        result['hex'] = bytes.fromhex(data)
        try:
            result['hex'] = result['hex'].decode()
        except UnicodeDecodeError:
            pass
    except Exception:
        pass

    try:
        result['json'] = json.dumps(json.loads(data))
    except Exception:
        pass

    # TODO: try other things, e.g., ciphey

    return result

def hashes(data: bytes) -> Dict[str, str]:
    result = {}

    _hash = hashlib.md5()
    _hash.update(data)
    result['md5'] = _hash.digest()

    _hash = hashlib.sha1()
    _hash.update(data)
    result['sha1'] = _hash.digest()

    _hash = hashlib.sha256()
    _hash.update(data)
    result['sha256'] = _hash.digest()

    _hash = hashlib.sha512()
    _hash.update(data)
    result['sha512'] = _hash.digest()

    return result
