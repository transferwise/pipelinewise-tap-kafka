"""Sync functions that consumes and transforms kafka messages to singer messages"""
import time
import copy
import dpath.util
import dateutil

import singer
import confluent_kafka

from singer import utils, metadata
from tap_kafka.errors import InvalidBookmarkException
from tap_kafka.errors import InvalidConfigException
from tap_kafka.errors import InvalidTimestampException
from tap_kafka.errors import TimestampNotAvailableException
from tap_kafka.errors import InvalidAssignByKeyException
from tap_kafka.serialization.json_with_no_schema import JSONSimpleDeserializer
from tap_kafka.serialization.protobuf import ProtobufDictDeserializer
from tap_kafka.serialization.protobuf import proto_to_message_type

LOGGER = singer.get_logger('tap_kafka')

LOG_MESSAGES_PERIOD = 5000  # Print log messages to stderr after every nth messages
SEND_STATE_PERIOD = 5000    # Update and send bookmark to stdout after nth messages


def search_in_list_of_dict_by_key_value(d_list, key, value):
    """Search a specific value of a certain key in a list of dictionary.
    Returns the index of first matching index item in the list or -1 if not found"""
    for idx, dic in enumerate(d_list):
        if dic.get(key) == value:
            return idx
    return -1


def init_value_deserializer(kafka_config):
    """Initialise the value deserializer"""
    value_deserializer = None
    if kafka_config['message_format'] == 'json':
        value_deserializer = JSONSimpleDeserializer()

    elif kafka_config['message_format'] == 'protobuf':
        message_type = proto_to_message_type(kafka_config['proto_schema'],
                                             kafka_config['proto_classes_dir'])
        value_deserializer = ProtobufDictDeserializer(message_type, {
            'use.deprecated.format': False
        })

    if not value_deserializer:
        raise InvalidConfigException(f"Unknown message format: {kafka_config['message_format']}")

    return value_deserializer

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
        key_properties=pks))


def update_bookmark(state, topic, message):
    """Update bookmark with a new timestamp"""
    return singer.write_bookmark(state,
                                 topic,
                                 f'partition_{message.partition()}',
                                 {
                                     'partition': message.partition(),
                                     'offset': message.offset(),
                                     'timestamp': get_timestamp_from_timestamp_tuple(message.timestamp())
                                 })


def initial_start_time_to_offset_reset(initial_start_time: str) -> str:
    """Convert initial_start_time to corresponding kafka auto_offset_reset"""
    return 'earliest' if initial_start_time == 'earliest' else 'latest'


def iso_timestamp_to_epoch(iso_timestamp: str) -> int:
    """Convert an ISO 8601 formatted string to epoch in milliseconds"""
    try:
        return int(dateutil.parser.parse(iso_timestamp).timestamp() * 1000)
    except dateutil.parser.ParserError:
        raise InvalidTimestampException(f'{iso_timestamp} is not a valid ISO formatted string.')


def assign_consumer(consumer, topic: str, state: dict, initial_start_time: str) -> None:
    """Assign consumer to the right position where we need to start consuming data from

    * If state exists then assign each partition to the positions in the state
    * If state is empty and the initial_start_time is a timestamp and not a reserved word
      then assign each partition to the offsets at the provided timestamp.
    * Otherwise do not assign"""
    if state:
        assign_consumer_to_bookmarked_state(consumer, topic, state)
    elif initial_start_time is not None and initial_start_time not in ['latest', 'earliest']:
        assign_consumer_to_timestamp(consumer, topic, initial_start_time)


def init_kafka_consumer(kafka_config, state, value_deserializer):
    LOGGER.info('Initialising Kafka Consumer...')
    topic = kafka_config['topic']
    initial_start_time = kafka_config['initial_start_time']
    consumer = confluent_kafka.DeserializingConsumer({
        # Required parameters
        'bootstrap.servers': kafka_config['bootstrap_servers'],
        'group.id': kafka_config['group_id'],

        # Optional parameters
        'session.timeout.ms': kafka_config['session_timeout_ms'],
        'heartbeat.interval.ms': kafka_config['heartbeat_interval_ms'],
        'max.poll.interval.ms': kafka_config['max_poll_interval_ms'],

        # Non-configurable parameters
        'enable.auto.commit': False,
        'auto.offset.reset': initial_start_time_to_offset_reset(initial_start_time),
        'value.deserializer': value_deserializer,
    })

    consumer.subscribe([topic])
    assign_consumer(consumer, topic, state, initial_start_time)

    return consumer


def get_timestamp_from_timestamp_tuple(kafka_ts: tuple) -> float:
    """Get the actual timestamp value from a kafka timestamp tuple"""
    if isinstance(kafka_ts, tuple):
        try:
            ts_type = kafka_ts[0]

            if ts_type == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                raise TimestampNotAvailableException('Required timestamp not available in the kafka message.')

            if ts_type in [confluent_kafka.TIMESTAMP_CREATE_TIME, confluent_kafka.TIMESTAMP_LOG_APPEND_TIME]:
                try:
                    timestamp = int(kafka_ts[1])
                    if timestamp > 0:
                        return timestamp

                    raise InvalidTimestampException(f'Invalid timestamp tuple. '
                                                    f'Timestamp {timestamp} needs to be greater than zero.')
                except ValueError:
                    raise InvalidTimestampException(f'Invalid timestamp tuple. Timestamp {kafka_ts[1]} is not integer.')

            raise InvalidTimestampException(f'Invalid timestamp tuple. Timestamp type {ts_type} is not valid.')
        except IndexError:
            raise InvalidTimestampException(f'Invalid timestamp tuple. '
                                            f'Timestamp type {kafka_ts} should have two elements.')

    raise InvalidTimestampException(f'Invalid kafka timestamp. It needs to be a tuple but it is a {type(kafka_ts)}.')


def kafka_message_to_singer_record(message, primary_keys):
    """Transforms kafka message to singer record message"""
    # Create dictionary with base attributes
    record = {
        "message": message.value(),
        "message_partition": message.partition(),
        "message_offset": message.offset(),
        "message_timestamp": get_timestamp_from_timestamp_tuple(message.timestamp()),
    }

    # Add primary keys to the record message
    for key in primary_keys:
        pk_selector = primary_keys[key]
        try:
            record[key] = dpath.util.get(message.value(), pk_selector)
        # Do not fail if PK not found in the message.
        # Continue without adding the extracted PK to the message
        except KeyError:
            pass

    return record


def consume_kafka_message(message, topic, primary_keys):
    """Insert single kafka message into the internal store"""
    singer_record = kafka_message_to_singer_record(message, primary_keys)
    singer.write_message(singer.RecordMessage(stream=topic, record=singer_record, time_extracted=utils.now()))


def bookmarked_partition_to_next_position(topic: str,
                                          partition_bookmark: dict,
                                          assign_by: str = 'timestamp') -> confluent_kafka.TopicPartition:
    """Transform a bookmarked partition to a kafka TopicPartition object"""
    try:
        if assign_by == 'timestamp':
            assign_to = partition_bookmark['timestamp']
        elif assign_by == 'offset':
            assign_to = partition_bookmark['offset'] + 1
        else:
            raise InvalidAssignByKeyException(f"Cannot set the consumer assignment by '{assign_by}'")

        return confluent_kafka.TopicPartition(topic, partition_bookmark['partition'], assign_to)
    except KeyError:
        raise InvalidBookmarkException(f"Invalid bookmark. Bookmark does not include 'partition' or '{assign_by}' "
                                       f"key(s).")
    except TypeError:
        raise InvalidBookmarkException(f"Invalid bookmark. One or more bookmark entries using invalid type(s).")


def assign_consumer_to_timestamp(consumer, topic: str, timestamp: str, timeout: int = 30) -> None:
    """Assign consumer topic of all partitions to given timestamp"""
    # Get list of all partitions
    cluster_md = consumer.list_topics(topic=topic, timeout=timeout)
    topic_md = cluster_md.topics[topic]
    partitions = topic_md.partitions

    # Find the offset of partitions by timestamps
    epoch = iso_timestamp_to_epoch(timestamp)
    partitions_list = [confluent_kafka.TopicPartition(topic, p, epoch) for p in partitions]
    partitions_to_assign = consumer.offsets_for_times(partitions_list)

    # Assign to the correct position
    consumer.assign(partitions_to_assign)


def assign_consumer_to_bookmarked_state(consumer, topic: str, state, assign_by: str = 'timestamp') -> None:
    """Assign consumer to bookmarked positions"""
    bookmarked_partitions = state['bookmarks'][topic]
    partitions_to_assign = [bookmarked_partition_to_next_position(topic,
                                                                  bookmarked_partitions[p],
                                                                  assign_by=assign_by) for p in bookmarked_partitions]
    # Translate timestamps to offsets if located by timestamp
    if partitions_to_assign and assign_by == 'timestamp':
        partitions_to_assign = consumer.offsets_for_times(partitions_to_assign)

    consumer.assign(partitions_to_assign)


def commit_consumer_to_bookmarked_state(consumer, topic, state):
    """Commit every bookmarked offset to kafka"""
    offsets_to_commit = []
    bookmarked_partitions = state.get('bookmarks', {}).get(topic, {})
    for partition in bookmarked_partitions:
        bookmarked_partition = bookmarked_partitions[partition]
        topic_partition = confluent_kafka.TopicPartition(topic,
                                                         bookmarked_partition['partition'],
                                                         bookmarked_partition['offset'])
        offsets_to_commit.append(topic_partition)

    consumer.commit(offsets=offsets_to_commit)


# pylint: disable=too-many-locals,too-many-statements
def read_kafka_topic(consumer, kafka_config, state):
    """Read kafka topic continuously and writing transformed singer messages to STDOUT"""
    topic = kafka_config['topic']
    primary_keys = kafka_config['primary_keys']
    max_runtime_ms = kafka_config['max_runtime_ms']
    commit_interval_ms = kafka_config['commit_interval_ms']
    consumed_messages = 0
    last_consumed_ts = 0
    start_time = 0
    last_commit_time = 0
    message = None

    # Send singer ACTIVATE message
    send_activate_version_message(state, topic)

    while True:
        polled_message = consumer.poll(timeout=kafka_config['consumer_timeout_ms'] / 1000)

        # Stop consuming more messages if no new message and consumer_timeout_ms exceeded
        if polled_message is None:
            break

        message = polled_message
        LOGGER.debug("topic=%s partition=%s offset=%s timestamp=%s key=%s value=<HIDDEN>" % (message.topic(),
                                                                                             message.partition(),
                                                                                             message.offset(),
                                                                                             message.timestamp(),
                                                                                             message.key()))

        # Initialise the start time after the first message
        if not start_time:
            start_time = time.time()

        # Initialise the last_commit_time after the first message
        if not last_commit_time:
            last_commit_time = time.time()

        # Generate singer message
        consume_kafka_message(message, topic, primary_keys)

        # Update bookmark after every consumed message
        state = update_bookmark(state, topic, message)

        now = time.time()
        # Commit periodically
        if now - last_commit_time > commit_interval_ms / 1000:
            commit_consumer_to_bookmarked_state(consumer, topic, state)
            last_commit_time = time.time()

        # Log message stats periodically every LOG_MESSAGES_PERIOD
        consumed_messages += 1
        if consumed_messages % LOG_MESSAGES_PERIOD == 0:
            LOGGER.info("%d messages consumed... Last consumed timestamp: %f Partition: %d Offset: %d",
                        consumed_messages, last_consumed_ts, message.partition(), message.offset())

        # Send state message periodically every SEND_STATE_PERIOD
        if consumed_messages % SEND_STATE_PERIOD == 0:
            LOGGER.debug("%d messages consumed... Sending latest state: %s", consumed_messages, state)
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        # Stop consuming more messages if max runtime exceeded
        max_runtime_s = max_runtime_ms / 1000
        if now >= (start_time + max_runtime_s):
            LOGGER.info(f'Max runtime {max_runtime_s} seconds exceeded. Stop consuming more messages.')
            break

    # Update bookmark and send state at the last time
    if message:
        state = update_bookmark(state, topic, message)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
        commit_consumer_to_bookmarked_state(consumer, topic, state)


def do_sync(kafka_config, catalog, state):
    """Set up kafka consumer, start reading the topic"""
    topic = kafka_config['topic']

    # Only one stream
    streams = catalog.get('streams', [])
    topic_pos = search_in_list_of_dict_by_key_value(streams, 'tap_stream_id', topic)
    value_deserializer = init_value_deserializer(kafka_config)

    if topic_pos != -1:
        # Send the initial schema message
        send_schema_message(streams[topic_pos])

        # Start consuming new messages from kafka
        consumer = init_kafka_consumer(kafka_config, state, value_deserializer)
        read_kafka_topic(consumer, kafka_config, state)
    else:
        raise Exception(f'Invalid catalog object. Cannot find {topic} in catalog')
