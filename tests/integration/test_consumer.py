import unittest
from unittest import mock
import singer

from tap_kafka import generate_config, get_args
from tap_kafka import common as tap_kafka_common
from tap_kafka.sync import do_sync

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils

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

    @mock.patch('tap_kafka.local_store.LocalStore._flush')
    def test_tap_kafka_consumer(self, mock_flush):
        mock_flush.side_effect = accumulate_singer_messages
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic_name = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-one', num_partitions=4)
        test_utils.produce_messages(
            kafka_config['bootstrap_servers'],
            topic_name,
            test_utils.get_file_lines('json_messages_to_produce.json')
        )

        # Consume test messages
        tap_kafka_config = generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic_name,
            'group_id': 'tap_kafka_integration_test',
        })
        catalog = {'streams': tap_kafka_common.generate_catalog(tap_kafka_config)}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        do_sync(tap_kafka_config, catalog, state={}, fn_get_args=get_args)

        # Tap output should have at least one of the following message types
        self.assertSetEqual({singer.RecordMessage, singer.StateMessage}, message_types(SINGER_MESSAGES))

        # Should have 5 record messages
        record_messages = list(filter(lambda m: (type(m) == singer.RecordMessage), SINGER_MESSAGES))
        self.assertEqual(len(record_messages), 5)

        # Should have 1 state message
        state_messages = list(filter(lambda m: (type(m) == singer.StateMessage), SINGER_MESSAGES))
        self.assertEqual(len(state_messages), 1)
