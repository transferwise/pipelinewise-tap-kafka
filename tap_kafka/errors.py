class InvalidBookmarkException(Exception):
    """
    Exception to raise when bookmark is not valid
    """
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class DiscoveryException(Exception):
    """
    Exception to raise when discovery failed
    """
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
