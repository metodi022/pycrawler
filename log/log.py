class Log:
    """Interface class for a log.
    """

    def __init__(self, job_id: int, crawler_id: int) -> None:
        raise NotImplementedError

    def append(self, message: str) -> None:
        """Append a message to a log.

        Args:
            message (str): message to append
        """
        raise NotImplementedError
