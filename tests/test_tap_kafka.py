import sys
import json
import unittest

from io import StringIO

from tap_kafka import common
from tap_kafka import sync

from tests.helper.kafkaconsumermock import KafkaConsumerMock


def _message_to_singer_record(message):
    return {
        'message': message.get('value'),
        'message_timestamp': message.get('timestamp')
    }


def _parse_stdout(stdout):
    stdout_messages = []

    # Process only json messages
    for s in stdout.split("\n"):
        try:
            stdout_messages.append(json.loads(s))
        except Exception as e:
            pass

    return stdout_messages


def _check_everything_delivered(stdout, topic, fake_messages):
    stdout_messages = _parse_stdout(stdout)

    singer_records = list(map(lambda x: _message_to_singer_record(x), fake_messages))
    for msg in stdout_messages:
        if msg['type'] != 'RECORD':
            continue

        assert msg['stream'] == topic
        record = msg['record']
        singer_records.remove(record)

    # All the fake kafka message that we generated in consumer have been observed as a part of the output
    assert len(singer_records) == 0


class TestSync(object):
    """
    Unit Tests
    """

    @classmethod
    def setup_class(self):
        self.config = {
            "topic": "dummy_topic"
        }

    def test_generate_schema_with_no_pk(self):
        """Should not add extra column when no PK defined"""
        assert common.generate_schema([]) == \
            {
                "type": "object",
                "properties": {
                    "message_timestamp": {"type": ["integer", "string", "null"]},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            }

    def test_generate_schema_with_pk(self):
        """Should add one extra column if PK defined"""
        assert common.generate_schema(["id"]) == \
            {
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "message_timestamp": {"type": ["integer", "string", "null"]},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            }

    def test_generate_schema_with_composite_pk(self):
        """Should add multiple extra columns if composite PK defined"""
        assert common.generate_schema(["id", "version"]) == \
            {
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "version": {"type": ["string", "null"]},
                    "message_timestamp": {"type": ["integer", "string", "null"]},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            }

    def test_generate_catalog_with_no_pk(self):
        """table-key-properties should be empty list when no PK defined"""
        assert common.generate_catalog({"topic": "dummy_topic"}) == \
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": []}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_generate_catalog_with_pk(self):
        """table-key-properties should be a list with single item when PK defined"""
        assert common.generate_catalog({"topic": "dummy_topic", "primary_keys": {"id": "^.dummyJson.id"}}) == \
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string", "null"]},
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_generate_catalog_with_composite_pk(self):
        """table-key-properties should be a list with two itemi when composite PK defined"""
        assert common.generate_catalog({"topic": "dummy_topic", "primary_keys": {"id": "dummyJson.id", "version": "dummyJson.version"}}) == \
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id", "version"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string", "null"]},
                                "version": {"type": ["string", "null"]},
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_consuming_records(self):
        """Every consumed kafka message should generated a valid singer RECORD message"""
        stream = {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string", "null"]},
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }

        fake_messages = [
            {
                'offset': 1,
                'timestamp': 1575895711187,
                'value': {'result': 'SUCCESS',
                          'details': {'id': '1001', 'type': 'TYPE_1', 'profileId': 1234}}
            },
            {
                'offset': 2,
                'timestamp': 1575895711187,
                'value': {'result': 'SUCCESS',
                          'details': {'id': '1002', 'type': 'TYPE_2', 'profileId': 1235}},
            }
        ]

        # Create fake iterable kafka consumer
        consumer = KafkaConsumerMock(fake_messages)

        # Capture stdout
        saved_stdout = sys.stdout
        string_io = StringIO()
        sys.stdout = string_io

        # Run sync_stream
        sync.sync_stream(self.config, stream, {}, consumer)
        sys.stdout = saved_stdout

        # Check if every fake consumer message processed
        assert len(list(consumer)) == 0

        # Checks that every message got delivered in the right format
        _check_everything_delivered(string_io.getvalue(), self.config['topic'], fake_messages)


if __name__ == '__main__':
    unittest.main()
