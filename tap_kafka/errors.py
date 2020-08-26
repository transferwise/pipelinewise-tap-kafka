class InvalidBookmarkException(Exception):
    """
    Exception to raise when state file is not valid
    """
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
