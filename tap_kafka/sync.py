"""Sync functions that consumes and transforms kafka messages to singer messages"""
import sys
import json
import time
import copy

import singer
from singer import utils, metadata
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from jsonpath_ng import parse


LOGGER = singer.get_logger('tap_kafka')
UPDATE_BOOKMARK_PERIOD = 10000


def write_schema_message(schema_message):
    """Write singer SCHEMA message to STDOUT"""
    sys.stdout.write(json.dumps(schema_message) + '\n')
    sys.stdout.flush()


def send_schema_message(stream):
    """Generate and send singer SCHEMA message for the stream"""
    md_map = metadata.to_map(stream['metadata'])
    pks = md_map.get((), {}).get('table-key-properties', [])

    schema_message = {'type': 'SCHEMA',
                      'stream': stream['tap_stream_id'],
                      'schema': stream['schema'],
                      'key_properties': pks,
                      'bookmark_properties': pks}
    write_schema_message(schema_message)


def do_sync(kafka_config, catalog, state):
    """Set up kafka consumer, start reading the topi"""
    consumer = KafkaConsumer(
        kafka_config['topic'],
        group_id=kafka_config['group_id'],
        enable_auto_commit=False,
        consumer_timeout_ms=kafka_config.get('consumer_timeout_ms', 10000),
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode(kafka_config['encoding'])),
        bootstrap_servers=kafka_config['bootstrap_servers'])

    for stream in catalog['streams']:
        sync_stream(kafka_config, stream, state, consumer)

# pylint: disable=too-many-locals
def sync_stream(kafka_config, stream, state, consumer):
    """Read kafka topic continuously and generate singer compatible messages to STDOUT"""
    send_schema_message(stream)
    stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    activate_version_message = singer.ActivateVersionMessage(
        stream=stream['tap_stream_id'],
        version=stream_version)

    singer.write_message(activate_version_message)

    time_extracted = utils.now()
    rows_saved = 0
    for message in consumer:
        LOGGER.debug("%s:%s:%s: key=%s value=%s" % (message.topic, message.partition,
                                                    message.offset, message.key,
                                                    message.value))

        # Create record message with columns
        rec = {
            "message": message.value,
            "message_timestamp": message.timestamp
        }

        # Add primary keys to the record message
        pks = kafka_config.get("primary_keys", [])
        for key in pks:
            pk_selector = pks[key]
            match = parse(pk_selector).find(message.value)
            if match:
                rec[key] = match[0].value

        record = singer.RecordMessage(
            stream=stream['tap_stream_id'],
            record=rec,
            time_extracted=time_extracted)
        rows_saved += 1

        singer.write_message(record)
        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'offset',
                                      message.offset)

        # commit offsets because we processed the message
        topic_partition = TopicPartition(message.topic, message.partition)
        consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})

        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
