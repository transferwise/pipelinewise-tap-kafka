import os
import sys
import json
import unittest
from unittest.mock import patch

from io import StringIO

from tap_kafka import common
from tap_kafka import sync

from tests.helper.parse_args_mock import ParseArgsMock
from tests.helper.kafka_consumer_mock import KafkaConsumerMock



def _get_resource_from_json(filename):
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as json_resource:
        return json.load(json_resource)


def _message_to_singer_record(message):
    return {
        'message': message.get('value'),
        'message_timestamp': message.get('timestamp'),
        'message_offset': message.get('offset'),
        'message_partition': message.get('partition')
    }


def _message_to_singer_state(message):
    return {
        'bookmarks': message
    }


def _delete_version_from_state_message(state):
    if 'bookmarks' in state:
        for key in state['bookmarks'].keys():
            if 'version' in state['bookmarks'][key]:
                del state['bookmarks'][key]['version']

    return state


def _parse_stdout(stdout):
    stdout_messages = []

    # Process only json messages
    for s in stdout.split("\n"):
        try:
            stdout_messages.append(json.loads(s))
        except Exception as e:
            pass

    return stdout_messages


def _run_sync(config, state, stream, kafka_messages):
    # Mock ParseArgs and KafkaConsumer classes
    parse_arg_mock = ParseArgsMock(state)
    consumer = KafkaConsumerMock(kafka_messages)

    # Capture singer messages on stdout
    saved_stdout = sys.stdout
    string_io = StringIO()
    sys.stdout = string_io

    # Run sync_stream
    sync.sync_stream(config, stream, state, consumer, parse_arg_mock.get_args)
    sys.stdout = saved_stdout

    # Check if every kafka message processed
    assert len(list(consumer)) == 0

    # Return everything from stdout
    return string_io.getvalue()


def _assert_singer_messages_on_stdout_equal(stdout, topic, exp_records, exp_states):
    stdout_messages = _parse_stdout(stdout)

    exp_singer_records = list(map(lambda x: _message_to_singer_record(x), exp_records))
    exp_singer_states = list(map(lambda x: _message_to_singer_state(x), exp_states))
    for msg in stdout_messages:
        if msg['type'] == 'RECORD':
            assert msg['stream'] == topic
            record = msg['record']
            exp_singer_records.remove(record)

        if msg['type'] == 'STATE':
            state = _delete_version_from_state_message(msg['value'])
            exp_singer_states.remove(state)

    # All the fake kafka message that we generated in consumer have been observed as a part of the output
    assert len(exp_singer_records) == 0
    assert len(exp_singer_states) == 0


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
                    "message_offset": {"type": ["integer", "null"]},
                    "message_partition": {"type": ["integer", "null"]},
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
                    "message_offset": {"type": ["integer", "null"]},
                    "message_partition": {"type": ["integer", "null"]},
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
                    "message_offset": {"type": ["integer", "null"]},
                    "message_partition": {"type": ["integer", "null"]},
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
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
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
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_generate_catalog_with_composite_pk(self):
        """table-key-properties should be a list with two items when composite PK defined"""
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
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_update_bookmark__on_empty_state(self):
        """Updating empty state should generate a new bookmark"""
        input_state = {}

        assert sync.update_bookmark(input_state, 'test-stream-id-1', 'test-topic', 11, 22) == \
            {'bookmarks': {'test-stream-id-1': {'topic': 'test-topic', 'offset': 11, 'partition': 22}}}

    def test_update_bookmark__update_stream(self):
        """Updating existing bookmark in state should update at every property"""
        input_state = {'bookmarks': {'test-stream-id-0': {'topic': 'test-topic', 'offset': 1, 'partition': 2}}}

        assert sync.update_bookmark(input_state, 'test-stream-id-0', 'test-topic-updated', 99, 999) == \
            {'bookmarks': {'test-stream-id-0': {'topic': 'test-topic-updated', 'offset': 99, 'partition': 999}}}

    def test_update_bookmark__add_new_stream(self):
        """Updating a not existing stream id should be appended to the bookmarks dictionary"""
        input_state = {'bookmarks': {'test-stream-id-0': {'topic': 'test-topic', 'offset': 1, 'partition': 2}}}

        assert sync.update_bookmark(input_state, 'test-stream-id-1', 'test-topic', 11, 22) == \
            {'bookmarks': {
                'test-stream-id-0': {'topic': 'test-topic', 'offset':  1, 'partition':  2},
                'test-stream-id-1': {'topic': 'test-topic', 'offset': 11, 'partition': 22}}}

    @patch('tap_kafka.sync.commit_kafka_consumer')
    def test_consuming_records_with_no_state(self, commit_kafka_consumer_mock):
        """Every consumed kafka message should generate a valid singer RECORD and a STATE messages at the end

        - Kafka commit should not be called at startup because state is NOT provided
        - STATE should return the max offset and partition from the consumed messages"""
        # Set test inputs
        state = {}
        stream = _get_resource_from_json('catalog.json')
        kafka_messages = _get_resource_from_json('kafka-messages-from-multiple-partitions.json')

        # Set expected result on stdout
        exp_topic = self.config['topic']
        exp_record_messages = kafka_messages
        exp_state_messages = [{'dummy_topic': {'topic': 'dummy_topic', 'offset': 3, 'partition': 2}}]

        # Run test
        sync_stdout = _run_sync(self.config, state, stream, kafka_messages)

        # Compare actual to expected results
        _assert_singer_messages_on_stdout_equal(sync_stdout, exp_topic, exp_record_messages, exp_state_messages)

        # Kafka commit should not be called because state is NOT provided
        assert commit_kafka_consumer_mock.call_count == 0

    @patch('tap_kafka.sync.commit_kafka_consumer')
    def test_consuming_records_with_state(self, commit_kafka_consumer_mock):
        """Every consumed kafka message should generate a valid singer RECORD and a STATE messages at the end

        - Kafka commit should be called at startup because state is provided
        - STATE should return the max offset and partition from the consumed messages"""
        # Set test inputs
        state = _get_resource_from_json('state-with-bookmark.json')
        stream = _get_resource_from_json('catalog.json')
        kafka_messages = _get_resource_from_json('kafka-messages-from-multiple-partitions.json')

        # Set expected results on stdout
        exp_topic = self.config['topic']
        exp_record_messages = kafka_messages
        exp_state_messages = [{'dummy_topic': {'topic': 'dummy_topic', 'offset': 3, 'partition': 2}}]

        # Run test
        sync_stdout = _run_sync(self.config, state, stream, kafka_messages)

        # Compare actual to expected results
        _assert_singer_messages_on_stdout_equal(sync_stdout, exp_topic, exp_record_messages, exp_state_messages)

        # Kafka commit should be called once because state is provided
        assert commit_kafka_consumer_mock.call_count == 1


if __name__ == '__main__':
    unittest.main()
