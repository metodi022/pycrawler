class Log:
    """Interface class for a log.
    """

    def __init__(self, *args, **kwargs) -> None:
        raise NotImplementedError

    def add_message(self, message: str, *args, **kwargs) -> None:
        """Append a message to a log.

        Args:
            message (str): message to append
        """
        raise NotImplementedError
