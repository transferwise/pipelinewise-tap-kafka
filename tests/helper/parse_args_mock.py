class ParseArgsMock:
    """
    Mocked singer args class
    """
    def __init__(self, state={}):
        self.state = state

    def get_args(self):
        return self
