import pathlib
from logging import DEBUG
from typing import Literal


class Config:
    DATABASE: str = 'temp'  # database name
    USER: str = 'postgres'  # database user
    PASSWORD: str = 'postgres'  # database password
    HOST: str = 'localhost'  # database host
    PORT: str = '5432'  # database port

    LOG: pathlib.Path = pathlib.Path('./.logs/')  # path for saving logs
    LOG_LEVEL = DEBUG  # DEBUG|INFO|WARNING|ERROR

    HEADLESS: bool = True  # Headless browser

    RECURSIVE: bool = True  # Discover additional URLs while crawling
    SAME_ORIGIN: bool = False  # URL discovery for same-origin only
    SAME_ETLDP1: bool = True  # URL discovery for same ETLD+1 only
    DEPTH: int = 0  # URL discovery limit; 0 (initial URL only), 1, 2, etc.
    SAME_CONTEXT: bool = True  # crawl additional URLs in the same context

    WAIT_LOAD_UNTIL: Literal['commit', 'domcontentloaded', 'load', 'networkidle'] = 'load'
    LOAD_TIMEOUT: int = 30000  # URL page loading timeout in ms (0 = disable timeout)
    WAIT_AFTER_LOAD: int = 5000  # let page execute after loading in ms

    ACCEPT_COOKIES: bool = True  # Attempt to find cookie banners and accept them (unreliable)

    # TODO more options
    # OBEY_ROBOTS: bool = False  # crawler should obey robots.txt
    # FOCUS_FILTER: bool = False  # crawler should visit "interesting" URLS (experimental)

    # ERROR_CODES: Dict[str, int] = {'module_error': -1, 'page_load_error': -2}
