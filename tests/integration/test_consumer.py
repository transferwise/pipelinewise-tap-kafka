import unittest
import time
from datetime import datetime
import singer

import tap_kafka.serialization

from tap_kafka import sync, get_args
from tap_kafka.errors import AllBrokersDownException
from tap_kafka.errors import DiscoveryException
from tap_kafka.serialization.json_with_no_schema import JSONSimpleSerializer
import tests.integration.utils as test_utils

SINGER_MESSAGES = []


def accumulate_singer_messages(message):
    singer_message = singer.parse_message(message)
    SINGER_MESSAGES.append(singer_message)


def message_types(messages):
    message_types_set = set()
    for message in messages:
        message_types_set.add(type(message))
    return message_types_set


class TestKafkaConsumer(unittest.TestCase):

    def _test_tap_kafka_discovery(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic_name = test_utils.create_topic(kafka_config['bootstrap_servers'],
                                             'test-topic-for-discovery',
                                             num_partitions=4)

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic_name,
            'group_id': 'tap_kafka_integration_test',
        })

        catalog_streams = []
        tap_kafka.dump_catalog = lambda c: catalog_streams.extend(c)
        tap_kafka.do_discovery(tap_kafka_config)

        self.assertEqual(catalog_streams, [
            {
                'tap_stream_id': catalog_streams[0]['tap_stream_id'],
                'metadata': [
                    {'breadcrumb': (), 'metadata': {'table-key-properties': []}}
                ],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']}
                    }
                }
            }
        ])

    def _test_tap_kafka_discovery_failure(self):
        kafka_config = test_utils.get_kafka_config()

        # Trying to discover topic
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': 'not-existing-topic',
            'group_id': 'tap_kafka_integration_test',
            'session_timeout_ms': 1000,
        })

        with self.assertRaises(DiscoveryException):
            tap_kafka.do_discovery(tap_kafka_config)

    def test_tap_kafka_consumer_brokers_down(self):
        # Consume test messages from not existing broker
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': 'localhost:12345',
            'topic': 'foo',
            'group_id': test_utils.generate_unique_consumer_group(),
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        with self.assertRaises(AllBrokersDownException):
            sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})

    def test_tap_kafka_consumer(self):
        kafka_config = test_utils.get_kafka_config()

        start_time = int(time.time() * 1000) - 60000

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-one', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'primary_keys': {
                'id': '/id'
            }
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should not receive any RECORD and STATE messages because we start consuming from latest
        sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})
        self.assertEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'id': {'type': ['string', 'null']}
                    }
                },
                'key_properties': ['id']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            }
        ])

        # Position to the time when the test started
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state={
            'bookmarks': {
                topic: {'partition_0': {'partition': 0,
                                        'offset': 0,
                                        'timestamp': start_time}}}})

        self.assertEqual(len(singer_messages), 8)
        self.assertEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'id': {'type': ['string', 'null']}
                    }
                },
                'key_properties': ['id']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 0,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {'id': 1, 'value': 'initial id 1'},
                    'id': 1
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 1,
                    'message_timestamp': singer_messages[3]['record']['message_timestamp'],
                    'message': {'id': 1, 'value': 'updated id 1'},
                    'id': 1
                },
                'time_extracted': singer_messages[3]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 2,
                    'message_timestamp': singer_messages[4]['record']['message_timestamp'],
                    'message': {'id': 2, 'value': 'initial id 2'},
                    'id': 2
                },
                'time_extracted': singer_messages[4]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 3,
                    'message_timestamp': singer_messages[5]['record']['message_timestamp'],
                    'message': {'id': 2, 'value': 'updated id 2'},
                    'id': 2
                },
                'time_extracted': singer_messages[5]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 4,
                    'message_timestamp': singer_messages[6]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'initial id 3'},
                    'id': 3
                },
                'time_extracted': singer_messages[6]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[7]['value']['bookmarks'][topic]
                    }
                }
            }
        ])

        # Save state with bookmarks
        state = singer_messages[7]['value']

        # Increase the bookmarked timestamp by one second to skip the already consumed messages.
        # Normally this is done only when tracking messages by offsets. Increasing timestamps
        # should not happen on a prod environment because we can loose data, but for this test
        # it's perfectly fine.
        last_timestamp = state['bookmarks'][topic]['partition_0']['timestamp']
        state['bookmarks'][topic]['partition_0']['timestamp'] = last_timestamp + 1000

        # Produce some new messages
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce_2.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Position to the time in the state
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state=state)

        self.assertEqual(len(singer_messages), 6)
        self.assertEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'id': {'type': ['string', 'null']}
                    }
                },
                'key_properties': ['id']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 5,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'updated id 3'},
                    'id': 3
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 6,
                    'message_timestamp': singer_messages[3]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'initial id 4'},
                    'id': 4
                },
                'time_extracted': singer_messages[3]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 7,
                    'message_timestamp': singer_messages[4]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'updated id 4'},
                    'id': 4
                },
                'time_extracted': singer_messages[4]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[5]['value']['bookmarks'][topic]
                    }
                }
            }
        ])

    def test_tap_kafka_consumer_initial_start_time_earliest(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-earliest', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'initial_start_time': 'earliest'
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should receive all RECORD and STATE messages because we start consuming from the earliest
        sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})
        self.assertEqual(len(singer_messages), 8)

        # Second run should not receive any RECORD messages even if no state provided:
        # Last message should be committed, no new one produced so nothing new expected to receive
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})
        self.assertEqual(len(singer_messages), 2)
        self.assertEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']}
                    }
                },
                'key_properties': []
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            }
        ])

    def test_tap_kafka_consumer_initial_start_time_latest(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-latest', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'initial_start_time': 'latest'
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should not receive any RECORD and STATE messages because we start consuming from latest
        sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})
        self.assertEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']}
                    }
                },
                'key_properties': []
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            }
        ])

    def test_tap_kafka_consumer_initial_start_time_timestamp(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-init-start-ts', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Wait a couple of seconds before producing some more test messages
        time.sleep(2)

        # We will start consuming message starting from this moment
        initial_start_time = datetime.now().isoformat()

        # Produce some more test messages
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce_2.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages from a given timestamp
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'initial_start_time': initial_start_time
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should receive RECORD and STATE messages only from json_messages_to_produce_2.json
        sync.do_sync(tap_kafka_config, catalog, state={})
        self.assertEqual(len(singer_messages), 6)
        self.assertEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']}
                    }
                },
                'key_properties': []
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 5,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'updated id 3'}
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 6,
                    'message_timestamp': singer_messages[3]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'initial id 4'}
                },
                'time_extracted': singer_messages[3]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 7,
                    'message_timestamp': singer_messages[4]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'updated id 4'}
                },
                'time_extracted': singer_messages[4]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[5]['value']['bookmarks'][topic]
                    }
                }
            }
        ])
