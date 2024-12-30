import pathlib
from logging import DEBUG, ERROR, INFO, WARNING
from typing import Dict, Literal, Optional


class Config:
    DATABASE: str = 'pycrawler'  # database name
    USER: str = 'postgres'  # database user
    PASSWORD: str = 'postgres'  # database password
    HOST: str = 'localhost'  # database host
    PORT: str = '5432'  # database port

    LOG: pathlib.Path = pathlib.Path('./logs/')  # path for saving logs
    LOG_LEVEL = INFO  # DEBUG|INFO|WARNING|ERROR

    HAR: Optional[pathlib.Path] = None

    BROWSER: Literal['chromium', 'firefox', 'webkit'] = 'chromium'
    DEVICE: str = 'Desktop Chrome'  # A device supported by playwright (https://github.com/microsoft/playwright/blob/main/packages/playwright-core/src/server/deviceDescriptorsSource.json)
    LOCALE: str = 'de-DE'
    TIMEZONE: str = 'Europe/Berlin'
    HEADLESS: bool = False  # Headless browser

    SAVE_CONTEXT: bool = False  # Store saved cookies and localStorage while crawling

    MANUAL_SETUP: bool = False  # Setup the state manually at the start of the crawl

    RECURSIVE: bool = True  # Discover additional URLs while crawling
    BREADTHFIRST: bool = True  # Visit URLs in a breadth-first manner, otherwise depth-first
    SAME_SCHEME: bool = True  # URL discovery for same scheme (protocol) only
    SAME_ORIGIN: bool = False  # URL discovery for same-origin only
    SAME_ETLDP1: bool = True  # URL discovery for same ETLD+1 and scheme only (schemeful same-site)
    SAME_ENTITY: bool = False  # URL discovery for same entity only (ETLD+1 or company, owner, etc.)
    DEPTH: int = 2  # URL discovery limit; 0 (initial URL only), 1 (+ all URLs from initial page), etc.
    MAX_URLS: int = 1000  # limit number of URLs gathered for a domain

    REPETITIONS: int = 1  # how many times to crawl the same URL and invoke module response handlers

    WAIT_LOAD_UNTIL: Literal['commit', 'domcontentloaded', 'load', 'networkidle'] = 'load'
    LOAD_TIMEOUT: int = 30000  # URL page loading timeout in ms (0 = disable timeout)
    WAIT_AFTER_LOAD: int = 5000  # let page execute after loading in ms
    RESTART_TIMEOUT: int = 600  # restart crawler if it hasn't done anything for ... seconds

    ACCEPT_COOKIES: bool = False  # Attempt to find cookie banners and accept them (unreliable)

    # TODO more options
    # OBEY_ROBOTS: bool = False  # obey robots.txt
    FOCUS_FILTER: bool = False  # prioritize visiting "interesting" URLS (experimental)
    # ADULT_FILTER: bool = False  # avoid visiting adult sites

    # Usually the code of the response in DB will be the response status (200, 404, etc.); if an
    # error occurs, for example response is NULL or browser is stuck, use the error codes below
    ERROR_CODES: Dict[str, int] = {'response_error': -1, 'browser_error': -2}
