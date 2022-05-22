from typing import Dict


class Config:
    RECURSIVE: bool = False  # Discover additional URLs while crawling
    SAME_ORIGIN: bool = False  # URL discovery for same-origin only
    SAME_ETLDP1: bool = False  # URL discovery for same ETLD+1 only
    DEPTH: int = 0  # -1 (unlimited), 0 (initial URL only), 1 (first outgoing links), etc.
    SAME_CONTEXT: bool = True  # crawl aditional URLs in the same context

    WAIT_UNTIL: str = 'load'  # domcontentloaded | load | networkidle | commit
    LOAD_TIMEOUT: int = 30000  # URL page loading timeout in ms or 0 (disable timeout)
    AFTER_LOAD_WAIT: int = 5000  # let page execute after loading in ms

    # TODO error codes
    # ERROR_CODES: Dict[str, int] = {'module_error': -1, 'page_load_error': -2}
