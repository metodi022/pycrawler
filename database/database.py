from typing import Any, Optional, List
from log.log import Log


class Database:
    """Interface class for a database.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    def initialize_job(self, job_id: int, source: str, *args, **kwargs) -> None:
        """Add URLs to the database.

        Args:
            job_id (int): job id associated with the URLs
            source (str): source for URLs
        """
        raise NotImplementedError

    def get_url(self, job_id: int, crawler_id: int, log: Log, *args, **kwargs) -> Optional[str]:
        """Get the next url to crawl.

        Args:
            job_id (int): job id associated with the URLs
            crawler_id (int): id of crawler which will crawl the given URL

        Returns:
            Optional[str]: URL
        """
        raise NotImplementedError

    def update_url(self, job_id: int, crawler_id: int, url: str, code: int, log: Log, *args, **kwargs) -> None:
        """Update a crawled url.

        Args:
            job_id (int): job id associated with the URLs
            crawler_id (int): id of crawler which finished crawling the given URL
            url (str): URL
            code (int): crawl code
        """
        raise NotImplementedError

    def invoke_transaction(self, statement: str, log: Log) -> Optional[List[Any]]:
        """Execute a statement in a transaction and fetch results.

        Args:
            statement (str): the statement

        Returns:
            Optional[List[Any]]: statement results
        """
        raise NotImplementedError
