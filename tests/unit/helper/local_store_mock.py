class LocalStoreMock:
    """
    Mocked singer args class
    """
    def __init__(self, state={}):
        self.state = state
        self.messages_to_persist = []
        self.messages = []
        self.last_inserted_ts = 123456
        self.last_persisted_ts = 123456

    def persist_messages(self) -> float:
        for message in self.messages_to_persist:
            self.messages.append(message)

        self.messages_to_persist = []
        persist_ts = 123456
        return persist_ts

    def insert(self, message: str) -> float:
        self.messages_to_persist.append(message)

        insert_ts = 123456
        return insert_ts
