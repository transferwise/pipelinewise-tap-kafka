"""Sync functions that consumes and transforms kafka messages to singer messages"""
import json
import time
import copy
import dpath.util

import singer
from singer import utils, metadata
from tap_kafka.local_store import LocalStore
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from .errors import InvalidBookmarkException

LOGGER = singer.get_logger('tap_kafka')

LOG_MESSAGES_PERIOD = 1000          # Print log messages to stderr after every nth messages
UPDATE_BOOKMARK_PERIOD = 1000       # Update and send bookmark to stdout after nth messages
CLEANUP_LOCAL_STORE_INTERVAL = 30   # Seconds between cleanup processes in the local store


def search_in_list_of_dict_by_key_value(d_list, key, value):
    """Search a specific value of a certain key in a list of dictionary.
    Returns the index of first matching index item in the list or -1 if not found"""
    for idx, dic in enumerate(d_list):
        if dic.get(key) == value:
            return idx
    return -1


def send_activate_version_message(state, tap_stream_id):
    """Generate and send singer ACTIVATE message"""
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)
    singer.write_message(singer.ActivateVersionMessage(
        stream=tap_stream_id,
        version=stream_version))


def send_schema_message(stream):
    """Generate and send singer SCHEMA message for the stream"""
    md_map = metadata.to_map(stream['metadata'])
    pks = md_map.get((), {}).get('table-key-properties', [])

    singer.write_message(singer.SchemaMessage(
        stream=stream['tap_stream_id'],
        schema=stream['schema'],
        key_properties=pks,
        bookmark_properties=pks))


def update_bookmark(state, topic, timestamp):
    """Update bookmark with a new timestamp"""
    try:
        timestamp = float(timestamp) if timestamp else 0
    except ValueError:
        raise InvalidBookmarkException(f'The timestamp in the bookmark for {topic} stream is not numeric')

    return singer.write_bookmark(state, topic, 'timestamp', timestamp)


def init_local_store(kafka_config):
    LOGGER.info('Initialising local store at %s', kafka_config['local_store_dir'])
    return LocalStore(
        directory=kafka_config['local_store_dir'],
        batch_size_rows=kafka_config['local_store_batch_size_rows'],
        topic=kafka_config['topic'])


def init_kafka_consumer(kafka_config):
    LOGGER.info('Initialising Kafka Consumer...')
    return KafkaConsumer(
        # Required parameters
        kafka_config['topic'],
        bootstrap_servers=kafka_config['bootstrap_servers'],
        group_id=kafka_config['group_id'],

        # Optional parameters
        consumer_timeout_ms=kafka_config['consumer_timeout_ms'],
        session_timeout_ms=kafka_config['session_timeout_ms'],
        heartbeat_interval_ms=kafka_config['heartbeat_interval_ms'],
        max_poll_records=kafka_config['max_poll_records'],
        max_poll_interval_ms=kafka_config['max_poll_interval_ms'],

        # Non-configurable parameters
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode(kafka_config['encoding'])))


def kafka_message_to_singer_record(message, topic, primary_keys):
    """Transforms kafka message to singer record message"""
    # Create dictionary with base attributes
    record = {
        "message": message.value,
        "message_timestamp": message.timestamp,
        "message_offset": message.offset,
        "message_partition": message.partition
    }

    # Add primary keys to the record message
    for key in primary_keys:
        pk_selector = primary_keys[key]
        try:
            record[key] = dpath.util.get(message.value, pk_selector)
        # Do not fail if PK not found in the message.
        # Continue without adding the extracted PK to the message
        except KeyError:
            pass

    return record


def consume_kafka_message(message, topic, primary_keys, local_store):
    """Insert single kafka message into the internal store"""
    singer_record = kafka_message_to_singer_record(message, topic, primary_keys)
    singer_record_message = singer.format_message(singer.RecordMessage(stream=topic,
                                                                       record=singer_record,
                                                                       time_extracted=utils.now()))
    insert_ts = local_store.insert(singer_record_message)
    return insert_ts


def commit_kafka_consumer(consumer, topic, partition, offset):
    """Commit consumed message to kafka"""
    topic_partition = TopicPartition(topic, partition)
    consumer.commit({topic_partition: OffsetAndMetadata(offset + 1, None)})


# pylint: disable=too-many-locals
def read_kafka_topic(consumer, local_store, kafka_config, state, fn_get_args):
    """Read kafka topic continuously, insert into local store and flush singer
    compatible messages in batches to STDOUT

    Returns the timestamp of the last batch flush event"""
    topic = kafka_config['topic']
    primary_keys = kafka_config['primary_keys']
    max_runtime_ms = kafka_config['max_runtime_ms']
    commit_interval_ms = kafka_config['commit_interval_ms']
    batch_size_rows = kafka_config['batch_size_rows']
    local_store_cleanup_ts = time.time()
    received_messages = 0
    last_consumed_ts = 0
    start_time = 0
    last_commit_time = 0

    # Send singer ACTIVATE message
    send_activate_version_message(state, topic)

    # Start consuming kafka messages
    last_flush_ts = float(singer.get_bookmark(state, topic, 'timestamp') or 0)
    for message in consumer:
        LOGGER.debug("%s:%s:%s: key=%s value=%s" % (message.topic, message.partition,
                                                    message.offset, message.key,
                                                    message.value))

        # Initialise the start time after the first message
        if not start_time:
            start_time = time.time()

        # Initialise the last_commit_time after the first message
        if not last_commit_time:
            last_commit_time = time.time()

        # Generate and insert singer message into local store
        last_consumed_ts = consume_kafka_message(message, topic, primary_keys, local_store)

        # Commit periodically
        if last_consumed_ts - last_commit_time > commit_interval_ms / 1000:
            # Persist everything in the local store to disk and send commit message to kafka
            local_store.persist_messages()
            commit_kafka_consumer(consumer, message.topic, message.partition, message.offset)
            last_commit_time = time.time()

        # Log message stats periodically
        received_messages += 1
        if received_messages % LOG_MESSAGES_PERIOD == 0:
            LOGGER.info("%d messages received... Last consumed timestamp: %f Partition: %d Offset: %d",
                        received_messages, last_consumed_ts, message.partition, message.offset)

        # Every UPDATE_BOOKMARK_PERIOD, update the bookmark and send state message
        if received_messages % UPDATE_BOOKMARK_PERIOD == 0:
            state = update_bookmark(state, topic, local_store.last_persisted_ts)
            LOGGER.debug("Updating bookmark and inserting to local store: %s", state)
            local_store.insert(singer.format_message(singer.StateMessage(value=copy.deepcopy(state))))

        # Flush local store periodically
        if received_messages % batch_size_rows == 0:
            LOGGER.debug('Sending %d unprocessed messages from local store...', batch_size_rows)
            local_store.flush_after(last_flush_ts)
            last_flush_ts = local_store.last_persisted_ts

        now = time.time()
        # Every CLEANUP_LOCAL_STORE_INTERVAL delete the processed items from the local store
        if now >= (local_store_cleanup_ts + CLEANUP_LOCAL_STORE_INTERVAL):
            # Read the state from disk, maybe a target connector updated it in the meantime
            args = fn_get_args()
            state = args.state or {}

            # Delete every processed item from the local store
            LOGGER.debug(f'Deleting processed items from local store before state: %s', state)
            local_store.delete_before_bookmark(state)

            # Update last cleanup timestamp
            local_store_cleanup_ts = now

        # Stop consuming more messages if max runtime exceeded
        max_runtime_s = max_runtime_ms / 1000
        if now >= (start_time + max_runtime_s):
            LOGGER.info(f'Max runtime {max_runtime_s} seconds exceeded. Stop consuming more messages.')
            break

    # Update singer bookmark at the last time to point it the the last processed offset
    if last_consumed_ts:
        state = update_bookmark(state, topic, last_consumed_ts)
        local_store.insert(singer.format_message(singer.StateMessage(value=copy.deepcopy(state))))
        local_store.persist_messages()
        commit_kafka_consumer(consumer, message.topic, message.partition, message.offset)

    return last_flush_ts


def do_sync(kafka_config, catalog, state, fn_get_args):
    """Set up kafka consumer, start reading the topic"""
    topic = kafka_config['topic']

    # Only one stream
    streams = catalog.get('streams', [])
    topic_pos = search_in_list_of_dict_by_key_value(streams, 'tap_stream_id', topic)
    if topic_pos != -1:
        stream = streams[topic_pos]

        # Init local store and delete every processed item
        local_store = init_local_store(kafka_config)
        LOGGER.debug(f'Deleting processed items from local store before state: %s', state)
        local_store.delete_before_bookmark(state)

        # Send the initial schema message
        send_schema_message(stream)

        # Send messages from local store first
        LOGGER.info('Sending %d unprocessed messages from local store...', local_store.count_all())
        flush_ts = local_store.flush_after_bookmark(state)

        # Send updated state message
        state = update_bookmark(state, topic, flush_ts)
        LOGGER.info("Sending updated bookmark to tap consumer: %s", state)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        # Start consuming new messages from kafka
        consumer = init_kafka_consumer(kafka_config)
        last_flush_ts = read_kafka_topic(consumer, local_store, kafka_config, state, fn_get_args)

        # Flush remaining items in local store
        LOGGER.info('Sending remaining messages from local store...')
        local_store.flush_after(last_flush_ts)
    else:
        raise Exception(f'Invalid catalog object. Cannot find {topic} in catalog')
