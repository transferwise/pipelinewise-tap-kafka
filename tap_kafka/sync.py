"""Sync functions that consumes and transforms kafka messages to singer messages"""
import sys
import json
import time
import copy
import datetime

import singer
from singer import utils, metadata
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from jsonpath_ng import parse

LOGGER = singer.get_logger('tap_kafka')
LOG_MESSAGES_PERIOD = 1000
UPDATE_BOOKMARK_PERIOD = 1000
COMMIT_INTERVAL = 30


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


def update_bookmark(state, tap_stream_id, topic, offset, partition):
    state = singer.write_bookmark(state, tap_stream_id, 'topic', topic)
    state = singer.write_bookmark(state, tap_stream_id, 'offset', offset)
    state = singer.write_bookmark(state, tap_stream_id, 'partition', partition)

    return state


def commit_kafka_consumer(consumer, state, tap_stream_id):
    topic_to_commit = singer.get_bookmark(state, tap_stream_id, 'topic')
    offset_to_commit = singer.get_bookmark(state, tap_stream_id, 'offset')
    partition_to_commit = singer.get_bookmark(state, tap_stream_id, 'partition')

    if topic_to_commit and offset_to_commit is not None and partition_to_commit is not None:
        topic_partition = TopicPartition(topic_to_commit, partition_to_commit)
        LOGGER.info("Committing consumed offset: %d", offset_to_commit + 1)
        consumer.commit({topic_partition: OffsetAndMetadata(offset_to_commit + 1, None)})


def do_sync(kafka_config, catalog, state, fn_get_args):
    """Set up kafka consumer, start reading the topic"""
    consumer = KafkaConsumer(
        kafka_config['topic'],
        group_id=kafka_config['group_id'],
        enable_auto_commit=False,
        consumer_timeout_ms=kafka_config['consumer_timeout_ms'],
        session_timeout_ms=kafka_config['session_timeout_ms'],
        heartbeat_interval_ms=kafka_config['heartbeat_interval_ms'],
        max_poll_interval_ms=kafka_config['max_poll_interval_ms'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode(kafka_config['encoding'])),
        bootstrap_servers=kafka_config['bootstrap_servers'])

    for stream in catalog['streams']:
        sync_stream(kafka_config, stream, state, consumer, fn_get_args)


# pylint: disable=too-many-locals
def sync_stream(kafka_config, stream, state, consumer, fn_get_args):
    """Read kafka topic continuously and generate singer compatible messages to STDOUT"""
    send_schema_message(stream)
    stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    time_extracted = utils.now()
    received_messages = 0
    topic = None
    offset = None
    partition = None
    commit_timestamp = None

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    singer.write_message(singer.ActivateVersionMessage(
        stream=stream['tap_stream_id'],
        version=stream_version))

    for message in consumer:
        LOGGER.debug("%s:%s:%s: key=%s value=%s" % (message.topic, message.partition,
                                                    message.offset, message.key,
                                                    message.value))

        # Create record message with columns
        rec = {
            "message": message.value,
            "message_timestamp": message.timestamp,
            "message_offset": message.offset,
            "message_partition": message.partition
        }

        # Log message stats periodically
        received_messages += 1
        if received_messages % LOG_MESSAGES_PERIOD == 0:
            bookmark = state.get('bookmarks', {}).get(stream['tap_stream_id'])
            LOGGER.info("%d messages received... Last received offset: %d Partition: %d -- Uncommitted bookmark: %s",
                        received_messages, message.offset, message.partition, bookmark)

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

        # Send record message
        singer.write_message(record)

        # Update offset and partition after every message but not committing yet
        topic = message.topic
        offset = message.offset
        partition = message.partition

        # Every UPDATE_BOOKMARK_PERIOD, update the bookmark and send state message
        if received_messages % UPDATE_BOOKMARK_PERIOD == 0:
            state = update_bookmark(state,
                                    stream['tap_stream_id'],
                                    topic,
                                    offset,
                                    partition)
            LOGGER.info("Updating bookmark and sending to tap consumer: %s", state)
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        # Every COMMIT_INTERVAL, commit the latest offset from the state_file
        if not commit_timestamp or \
                datetime.datetime.utcnow() >= (commit_timestamp + datetime.timedelta(seconds=COMMIT_INTERVAL)):
            # Read the state from disk, maybe a target connector updated it in the meantime
            args = fn_get_args()
            state = args.state or {}

            # Commit the kafka offset
            if state:
                commit_kafka_consumer(consumer, state, stream['tap_stream_id'])

            # Update commit timestamp
            commit_timestamp = datetime.datetime.utcnow()

    # Update singer bookmark at the last time to point it the the last processed offset
    if topic and offset and partition:
        state = update_bookmark(state,
                                stream['tap_stream_id'],
                                topic,
                                offset,
                                partition)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
