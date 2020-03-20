"""pipelinewise-tap-kafka"""
import sys
import json

import singer
from singer import utils
from kafka import KafkaConsumer

import tap_kafka.sync as sync
import tap_kafka.common as common

LOGGER = singer.get_logger('tap_kafka')

REQUIRED_CONFIG_KEYS = [
    'group_id',
    'bootstrap_servers',
    'topic'
    # 'primary_keys'
]


def dump_catalog(all_streams):
    """Dump every stream catalog as JSON to STDOUT"""
    json.dump({'streams': all_streams}, sys.stdout, indent=2)


def do_discovery(config):
    """Discover kafka topic by trying to connect to the topic and generate singer schema
    according to the config"""
    try:
        consumer = KafkaConsumer(config['topic'],
                                 group_id=config['group_id'],
                                 enable_auto_commit=False,
                                 consumer_timeout_ms=config.get('consumer_timeout_ms', 10000),
                                 # value_deserializer=lambda m: json.loads(m.decode('ascii'))
                                 bootstrap_servers=config['bootstrap_servers'].split(','))

    except Exception as ex:
        LOGGER.warning("Unable to connect to kafka. bootstrap_servers: %s, topic: %s, group_id: %s",
                       config['bootstrap_servers'].split(','), config['topic'], config['group_id'])
        LOGGER.warning(ex)
        raise ex

    if config['topic'] not in consumer.topics():
        LOGGER.warning("Unable to view topic %s. bootstrap_servers: %s, topic: %s, group_id: %s",
                       config['topic'],
                       config['bootstrap_servers'].split(','), config['topic'], config['group_id'])
        raise Exception('Unable to view topic {}'.format(config['topic']))

    dump_catalog(common.generate_catalog(config))


def get_args():
    return utils.parse_args(REQUIRED_CONFIG_KEYS)


def main_impl():
    """Main tap-kafka implementation"""
    args = get_args()

    kafka_config = {'topic': args.config['topic'],
                    'group_id': args.config['group_id'],
                    'bootstrap_servers': args.config['bootstrap_servers'].split(','),
                    'encoding': args.config.get('encoding', 'utf-8'),
                    'primary_keys': args.config.get('primary_keys', {})
                    }

    if args.discover:
        do_discovery(args.config)
    elif args.properties:
        state = args.state or {}
        # streams = args.properties or {'streams' : common.default_streams(kafka_config)}
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
