"""pipelinewise-tap-kafka"""
import os
import sys
import json
import singer

from singer import utils
from confluent_kafka import Consumer, KafkaException

import tap_kafka.sync as sync
import tap_kafka.common as common

LOGGER = singer.get_logger('tap_kafka')

REQUIRED_CONFIG_KEYS = [
    'bootstrap_servers',
    'group_id',
    'topic'
]

DEFAULT_MAX_RUNTIME_MS = 300000
DEFAULT_COMMIT_INTERVAL_MS = 5000
DEFAULT_BATCH_SIZE_ROWS = 1000
DEFAULT_BATCH_FLUSH_INTERVAL_MS = 60000
DEFAULT_CONSUMER_TIMEOUT_MS = 10000
DEFAULT_SESSION_TIMEOUT_MS = 30000
DEFAULT_HEARTBEAT_INTERVAL_MS = 10000
DEFAULT_MAX_POLL_INTERVAL_MS = 300000
DEFAULT_MAX_POLL_RECORDS = 500
DEFAULT_ENCODING = 'utf-8'
DEFAULT_LOCAL_STORE_DIR = os.path.join(os.getcwd(), 'tap-kafka-local-store')
DEFAULT_LOCAL_STORE_BATCH_SIZE_ROWS = 1000


def dump_catalog(all_streams):
    """Dump every stream catalog as JSON to STDOUT"""
    json.dump({'streams': all_streams}, sys.stdout, indent=2)


def do_discovery(config):
    """Discover kafka topic by trying to connect to the topic and generate singer schema
    according to the config"""
    consumer = Consumer({
        'bootstrap.servers': config['bootstrap_servers'],
        'group.id': config['group_id'],
        'auto.offset.reset': 'earliest',
    })

    try:
        topic = config['topic']

        LOGGER.info(f"Discovering {topic} topic...")
        cluster_md = consumer.list_topics(topic=topic, timeout=config['session_timeout_ms'] / 1000)
        topic_md = cluster_md.topics[topic]

        if topic_md.error:
            raise KafkaException(topic_md.error)

        consumer.close()

    except KafkaException as exc:
        LOGGER.warning("Unable to view topic %s. bootstrap_servers: %s, topic: %s, group_id: %s",
                       config['topic'],
                       config['bootstrap_servers'], config['topic'], config['group_id'])

        raise Exception('Unable to view topic {} - {}'.format(config['topic'], exc))

    dump_catalog(common.generate_catalog(config))


def get_args():
    return utils.parse_args(REQUIRED_CONFIG_KEYS)


def generate_config(args_config):
    return {
        # Add required parameters
        'topic': args_config['topic'],
        'group_id': args_config['group_id'],
        'bootstrap_servers': args_config['bootstrap_servers'],

        # Add optional parameters with defaults
        'primary_keys': args_config.get('primary_keys', {}),
        'max_runtime_ms': args_config.get('max_runtime_ms', DEFAULT_MAX_RUNTIME_MS),
        'commit_interval_ms': args_config.get('commit_interval_ms', DEFAULT_COMMIT_INTERVAL_MS),
        'batch_size_rows': args_config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS),
        'batch_flush_interval_ms': args_config.get('batch_flush_interval_ms', DEFAULT_BATCH_FLUSH_INTERVAL_MS),
        'consumer_timeout_ms': args_config.get('consumer_timeout_ms', DEFAULT_CONSUMER_TIMEOUT_MS),
        'session_timeout_ms': args_config.get('session_timeout_ms', DEFAULT_SESSION_TIMEOUT_MS),
        'heartbeat_interval_ms': args_config.get('heartbeat_interval_ms', DEFAULT_HEARTBEAT_INTERVAL_MS),
        'max_poll_records': args_config.get('max_poll_records', DEFAULT_MAX_POLL_RECORDS),
        'max_poll_interval_ms': args_config.get('max_poll_interval_ms', DEFAULT_MAX_POLL_INTERVAL_MS),
        'encoding': args_config.get('encoding', DEFAULT_ENCODING),
        'local_store_dir': args_config.get('local_store_dir', DEFAULT_LOCAL_STORE_DIR),
        'local_store_batch_size_rows': args_config.get('local_store_batch_size_rows',
                                                       DEFAULT_LOCAL_STORE_BATCH_SIZE_ROWS)
    }


def main_impl():
    """Main tap-kafka implementation"""
    args = get_args()
    kafka_config = generate_config(args.config)

    if args.discover:
        do_discovery(kafka_config)
    elif args.properties:
        state = args.state or {}
        sync.do_sync(kafka_config, args.properties, state, fn_get_args=get_args)
    else:
        LOGGER.info("No properties were selected")


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
