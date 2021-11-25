class InvalidBookmarkException(Exception):
    """
    Exception to raise when bookmark is not valid
    """
    pass


class DiscoveryException(Exception):
    """
    Exception to raise when discovery failed
    """
    pass


class InvalidTimestampException(Exception):
    """
    Exception to raise when a kafka timestamp tuple is invalid
    """
    pass


class TimestampNotAvailableException(Exception):
    """
    Exception to raise when timestamp not available in a kafka message
    """
    pass
