from typing import Optional, Tuple


class Database:
    """Interface class for a database.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    def initialize_job(self, job_id: int, source: str) -> None:
        """Add URLs to the database.

        Args:
            job_id (int): job id associated with the URLs
            source (str): source for URLs
        """
        raise NotImplementedError

    def get_url(self, job_id: int, crawler_id: int) -> Optional[Tuple[str, int]]:
        """Get the next url to crawl.

        Args:
            job_id (int): job id associated with the URLs
            crawler_id (int): id of crawler which will crawl the given URL

        Returns:
            Optional[Tuple[str, int]]: URL, depth
        """
        raise NotImplementedError

    def add_url(self, job_id: int, url: str, depth: int) -> None:
        """Add a URL to the database.

        Args:
            job_id (int): job id associated with the URL
            url (str): URL
            depth (int): depth of URL
        """

    def update_url(self, job_id: int, crawler_id: int, url: str, code: int) -> None:
        """Update a crawled URL.

        Args:
            job_id (int): job id associated with the URLs
            crawler_id (int): id of crawler which finished crawling the given URL
            url (str): URL
            code (int): crawl code
        """
        raise NotImplementedError
