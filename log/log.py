class Log:
    """Interface class for a log.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    def add_message(self, message: str) -> None:
        """Append a message to a log.

        Args:
            message (str): message to append
        """
        raise NotImplementedError
