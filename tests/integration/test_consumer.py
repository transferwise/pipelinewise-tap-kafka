import unittest
import time
import singer

import tap_kafka

from tap_kafka import sync, get_args
from tap_kafka.errors import DiscoveryException
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

    def test_tap_kafka_discovery(self):
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

        assert catalog_streams == [
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
        ]

    def test_tap_kafka_discovery_failure(self):
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

    def test_tap_kafka_consumer(self):
        kafka_config = test_utils.get_kafka_config()

        start_time = int(time.time() * 1000) - 60000

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-one', num_partitions=1)
        test_utils.produce_messages(
            kafka_config['bootstrap_servers'],
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json')
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
        sync.do_sync(tap_kafka_config, catalog, state={})
        assert singer_messages == [
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
        ]

        # Position to the time when the test started
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state={
            'bookmarks': {
                topic: {'partition_0': {'partition': 0,
                                        'offset': 0,
                                        'timestamp': start_time}}}})

        assert len(singer_messages) == 8
        assert singer_messages == [
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
        ]

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
            kafka_config['bootstrap_servers'],
            topic,
            test_utils.get_file_lines('json_messages_to_produce_2.json')
        )

        # Position to the time in the state
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state=state)

        assert len(singer_messages) == 6
        assert singer_messages == [
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
        ]
