class KafkaConsumerRecordMock(object):
    def __init__(self, topic, value, timestamp=None, key=None, timestamp_type=0, partition=0, offset=1, headers=None,
                 checksum=None, serialized_key_size=None, serialized_value_size=None, serialized_header_size=None):
        if headers is None:
            headers = []
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.timestamp_type = timestamp_type
        self.key = key
        self.value = value
        self.headers = headers
        self.checksum = checksum
        self.serialized_key_size = serialized_key_size
        self.serialized_value_size = serialized_value_size
        self.serialized_header_size = serialized_header_size


def _create_fake_record_message(fake_message):
    return KafkaConsumerRecordMock(
        topic=fake_message.get('topic'),
        offset=fake_message.get('offset'),
        timestamp=fake_message.get('timestamp'),
        value=fake_message.get('value'),
        partition=fake_message.get('partition'),
        timestamp_type=fake_message.get('timestamp_type'),
        key=fake_message.get('key'),
        headers=fake_message.get('headers'),
        checksum=fake_message.get('checksum'),
        serialized_key_size=fake_message.get('serialized_key_size'),
        serialized_value_size=fake_message.get('serialized_value_size'),
        serialized_header_size=fake_message.get('serialized_header_size'),
    )


class KafkaConsumerMock(object):
    def __init__(self, fake_messages):
        self.fake_messages = fake_messages
        self.fake_messages_pos = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.fake_messages_pos > len(self.fake_messages) - 1:
            raise StopIteration
        else:
            self.fake_messages_pos += 1
            current_fake_message = self.fake_messages[self.fake_messages_pos - 1]
            return _create_fake_record_message(current_fake_message)

    def commit(self, offsets=None, callback=None):
        pass
