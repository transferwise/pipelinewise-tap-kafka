class LocalStoreMock:
    """
    Mocked singer args class
    """
    def __init__(self, state={}):
        self.state = state
        self.messages = []

    def insert(self, message: str) -> float:
        self.messages.append(message)
        insert_ts = 123456
        return insert_ts
