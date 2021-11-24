import os
import sys
import json
import unittest
import pytest
from unittest.mock import patch


import tap_kafka
from tap_kafka import common
from tap_kafka import sync
from tap_kafka.errors import DiscoveryException, InvalidBookmarkException, InvalidTimestampException
import confluent_kafka

from tests.unit.helper.parse_args_mock import ParseArgsMock
from tests.unit.helper.local_store_mock import LocalStoreMock
from tests.unit.helper.kafka_consumer_mock import KafkaConsumerMock, KafkaConsumerMessageMock


def _get_resource_from_json(filename):
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as json_resource:
        return json.load(json_resource)


def _message_to_singer_record(message):
    return {
        'message': message.get('value'),
        'message_timestamp': sync.get_timestamp_from_timestamp_tuple(message.get('timestamp')),
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


def _dict_to_kafka_message(dict_m):
    return {
        **dict_m,
        **{
            'timestamp': tuple(dict_m.get('timestamp', []))
        }
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


def _read_kafka_topic(config, state, stream, kafka_messages):
    # Mock ParseArgs and KafkaConsumer classes
    local_store_mock = LocalStoreMock()
    parse_arg_mock = ParseArgsMock(state)
    consumer = KafkaConsumerMock(kafka_messages)

    # Run sync_stream
    sync.read_kafka_topic(consumer, local_store_mock, config, state, parse_arg_mock.get_args)

    # Check if every kafka message processed
    # assert len(list(consumer)) == 0

    # Return everything from stdout
    return local_store_mock


def _assert_singer_messages_in_local_store_equal(local_store, topic, exp_records, exp_states):
    exp_singer_records = list(map(lambda x: _message_to_singer_record(x), exp_records))
    exp_singer_states = list(map(lambda x: _message_to_singer_state(x), exp_states))
    for msg in map(json.loads, local_store.messages):
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
            'topic': 'dummy_topic',
            'primary_keys': {},
            'max_runtime_ms': tap_kafka.DEFAULT_MAX_RUNTIME_MS,
            'consumer_timeout_ms': tap_kafka.DEFAULT_CONSUMER_TIMEOUT_MS,
            'commit_interval_ms': tap_kafka.DEFAULT_COMMIT_INTERVAL_MS,
            'batch_size_rows': tap_kafka.DEFAULT_BATCH_SIZE_ROWS
        }

    def test_generate_config_with_defaults(self):
        """Should generate config dictionary with every required and optional parameter with defaults"""
        minimal_config = {
            'topic': 'my_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3'
        }
        assert tap_kafka.generate_config(minimal_config) == {
            'topic': 'my_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3',
            'primary_keys': {},
            'max_runtime_ms': tap_kafka.DEFAULT_MAX_RUNTIME_MS,
            'commit_interval_ms': tap_kafka.DEFAULT_COMMIT_INTERVAL_MS,
            'batch_size_rows': tap_kafka.DEFAULT_BATCH_SIZE_ROWS,
            'batch_flush_interval_ms': tap_kafka.DEFAULT_BATCH_FLUSH_INTERVAL_MS,
            'consumer_timeout_ms': tap_kafka.DEFAULT_CONSUMER_TIMEOUT_MS,
            'session_timeout_ms': tap_kafka.DEFAULT_SESSION_TIMEOUT_MS,
            'heartbeat_interval_ms': tap_kafka.DEFAULT_HEARTBEAT_INTERVAL_MS,
            'max_poll_records': tap_kafka.DEFAULT_MAX_POLL_RECORDS,
            'max_poll_interval_ms': tap_kafka.DEFAULT_MAX_POLL_INTERVAL_MS,
            'encoding': tap_kafka.DEFAULT_ENCODING,
            'local_store_dir': tap_kafka.DEFAULT_LOCAL_STORE_DIR,
            'local_store_batch_size_rows': tap_kafka.DEFAULT_LOCAL_STORE_BATCH_SIZE_ROWS
        }

    def test_generate_config_with_custom_parameters(self):
        """Should generate config dictionary with every required and optional parameter with custom values"""
        custom_config = {
            'topic': 'my_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3',
            'primary_keys': {
                'id': '$.jsonpath.to.primary_key'
            },
            'max_runtime_ms': 1111,
            'commit_interval_ms': 10000,
            'batch_size_rows': 2222,
            'batch_flush_interval_ms': 3333,
            'consumer_timeout_ms': 1111,
            'session_timeout_ms': 2222,
            'heartbeat_interval_ms': 3333,
            'max_poll_records': 4444,
            'max_poll_interval_ms': 5555,
            'encoding': 'iso-8859-1',
            'local_store_dir': '/tmp/local-store',
            'local_store_batch_size_rows': 500
        }
        assert tap_kafka.generate_config(custom_config) == {
            'topic': 'my_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3',
            'primary_keys': {
                'id': '$.jsonpath.to.primary_key'
            },
            'max_runtime_ms': 1111,
            'commit_interval_ms': 10000,
            'batch_size_rows': 2222,
            'batch_flush_interval_ms': 3333,
            'consumer_timeout_ms': 1111,
            'session_timeout_ms': 2222,
            'heartbeat_interval_ms': 3333,
            'max_poll_records': 4444,
            'max_poll_interval_ms': 5555,
            'encoding': 'iso-8859-1',
            'local_store_dir': '/tmp/local-store',
            'local_store_batch_size_rows': 500
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

        assert sync.update_bookmark(input_state, 'test-topic', 123456) == \
            {'bookmarks': {'test-topic': {'timestamp': 123456}}}

    def test_update_bookmark__update_stream(self):
        """Updating existing bookmark in state should update at every property"""
        input_state = {'bookmarks': {'test-topic-updated': {'timestamp': 111}}}

        assert sync.update_bookmark(input_state, 'test-topic-updated', 999) == \
            {'bookmarks': {'test-topic-updated': {'timestamp': 999}}}

    def test_update_bookmark__add_new_stream(self):
        """Updating a not existing stream id should be appended to the bookmarks dictionary"""
        input_state = {'bookmarks': {'test-topic-0': {'timestamp': 111}}}

        assert sync.update_bookmark(input_state, 'test-topic-1', 222) == \
            {'bookmarks': {
                'test-topic-0': {'timestamp': 111},
                'test-topic-1': {'timestamp': 222}}}

    def test_update_bookmark__not_numeric(self):
        """Timestamp in the bookmark should be auto-converted to
        float whenever it's possible"""
        input_state = {'bookmarks': {'test-topic-updated': {'timestamp': 111}}}

        # Timestamp should be converted from string to float
        assert sync.update_bookmark(input_state, 'test-topic-updated', '999.9999') == \
            {'bookmarks': {'test-topic-updated': {'timestamp': 999.9999}}}

        # Timestamp that cannot be converted to float should raise exception
        with pytest.raises(InvalidBookmarkException):
            sync.update_bookmark(input_state, 'test-topic-updated', 'this-is-not-numeric')

    @patch('tap_kafka.sync.commit_kafka_consumer')
    def test_consuming_records_with_no_state(self, commit_kafka_consumer_mock):
        """Every consumed kafka message should generate a valid singer RECORD and a STATE messages at the end

        - Kafka commit should be called once at the end
        - STATE should return insert timestamp from the local store"""
        # Set test inputs
        state = {}
        stream = _get_resource_from_json('catalog.json')
        messages = _get_resource_from_json('kafka-messages-from-multiple-partitions.json')
        kafka_messages = list(map(_dict_to_kafka_message, messages))

        # Set expected result on stdout
        exp_topic = self.config['topic']
        exp_record_messages = kafka_messages
        exp_state_messages = [{'dummy_topic': {'timestamp': 123456}}]

        # Run test
        local_store = _read_kafka_topic(self.config, state, stream, kafka_messages)

        # Compare actual to expected results
        _assert_singer_messages_in_local_store_equal(local_store,
                                                     exp_topic,
                                                     exp_record_messages,
                                                     exp_state_messages)

        # Kafka commit should be called once at the end, because
        # every message fits into one persisting batch size
        assert commit_kafka_consumer_mock.call_count == 1

    @patch('tap_kafka.sync.commit_kafka_consumer')
    def test_consuming_records_with_state(self, commit_kafka_consumer_mock):
        """Every consumed kafka message should generate a valid singer RECORD and a STATE messages at the end

        - Kafka commit should be called once at the end
        - STATE should return insert timestamp from the local store"""
        # Set test inputs
        state = _get_resource_from_json('state-with-bookmark.json')
        stream = _get_resource_from_json('catalog.json')
        messages = _get_resource_from_json('kafka-messages-from-multiple-partitions.json')
        kafka_messages = list(map(_dict_to_kafka_message, messages))

        # Set expected results on stdout
        exp_topic = self.config['topic']
        exp_record_messages = kafka_messages
        exp_state_messages = [{'dummy_topic': {'timestamp': 123456}}]

        # Run test
        local_store = _read_kafka_topic(self.config, state, stream, kafka_messages)

        # Compare actual to expected results
        _assert_singer_messages_in_local_store_equal(local_store,
                                                     exp_topic,
                                                     exp_record_messages,
                                                     exp_state_messages)

        # Kafka commit should be called once at the end, because
        # every message fits into one persisting batch size
        assert commit_kafka_consumer_mock.call_count == 1

    def test_kafka_message_to_singer_record(self):
        """Validate if kafka messages converted to singer messages correctly"""
        topic = 'test-topic'

        # Converting without primary key
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 123456789),
                                           offset=1234,
                                           partition=0)
        primary_keys = {}
        assert sync.kafka_message_to_singer_record(message, topic, primary_keys) == {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'message_timestamp': 123456789,
            'message_offset': 1234,
            'message_partition': 0
        }

        # Converting with primary key
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 123456789),
                                           offset=1234,
                                           partition=0)
        primary_keys = {'id': '/id'}
        assert sync.kafka_message_to_singer_record(message, topic, primary_keys) == {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'id': 1,
            'message_timestamp': 123456789,
            'message_offset': 1234,
            'message_partition': 0
        }

        # Converting with nested and multiple primary keys
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 123456789),
                                           offset=1234,
                                           partition=0)
        primary_keys = {'id': '/id', 'y': '/data/y'}
        assert sync.kafka_message_to_singer_record(message, topic, primary_keys) == {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'id': 1,
            'y': 'value-y',
            'message_timestamp': 123456789,
            'message_offset': 1234,
            'message_partition': 0
        }

        # Converting with not existing primary keys
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 123456789),
                                           offset=1234,
                                           partition=0)
        primary_keys = {'id': '/id', 'not-existing-key': '/path/not/exists'}
        assert sync.kafka_message_to_singer_record(message, topic, primary_keys) == {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'id': 1,
            'message_timestamp': 123456789,
            'message_offset': 1234,
            'message_partition': 0
        }

    def test_do_disovery_failure(self):
        """Validate if kafka messages converted to singer messages correctly"""
        minimal_config = {
            'topic': 'not_existing_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'not-existing-server1,not-existing-server2',
            'session_timeout_ms': 1000,
        }
        config = tap_kafka.generate_config(minimal_config)

        with pytest.raises(DiscoveryException):
            tap_kafka.do_discovery(config)

    def test_get_timestamp_from_timestamp_tuple(self):
        """Validate if the actual timestamp can be extracted from a kafka timestamp"""
        # Timestamps as tuples
        assert sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_NOT_AVAILABLE, 1234)) == 0
        assert sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_CREATE_TIME, 1234)) == 1234
        assert sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_LOG_APPEND_TIME, 1234)) == 1234

        # Invalid timestamp type
        with pytest.raises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple(([confluent_kafka.TIMESTAMP_CREATE_TIME, 1234], 1234))

        # Invalid timestamp type
        with pytest.raises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple((9999, 1234))

        # Invalid timestamp type
        with pytest.raises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple("not_a_tuple_or_list")


if __name__ == '__main__':
    unittest.main()
