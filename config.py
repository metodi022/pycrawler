from logging import INFO
from typing import Literal


class Config:
    RECURSIVE: bool = True  # Discover additional URLs while crawling
    SAME_ORIGIN: bool = False  # URL discovery for same-origin only
    SAME_ETLDP1: bool = True  # URL discovery for same ETLD+1 only
    DEPTH: int = 1  # 0 (initial URL only), 1 (first outgoing links), etc.
    SAME_CONTEXT: bool = True  # crawl additional URLs in the same context

    WAIT_LOAD_UNTIL: Literal['commit', 'domcontentloaded', 'load', 'networkidle'] = 'load'
    LOAD_TIMEOUT: int = 30000  # URL page loading timeout in ms or 0 (disable timeout)
    WAIT_AFTER_LOAD: int = 5000  # let page execute after loading in ms

    # TODO error codes and log level
    LOG_LEVEL = INFO  # DEBUG|INFO|WARNING|ERROR
    # ERROR_CODES: Dict[str, int] = {'module_error': -1, 'page_load_error': -2}
