import singer
from singer import utils, metadata
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
import sys
import json
from jsonpath_ng import jsonpath, parse
import time
import copy
import tap_kafka.common as common

LOGGER = singer.get_logger()
UPDATE_BOOKMARK_PERIOD = 10000


def write_schema_message(schema_message):
    sys.stdout.write(json.dumps(schema_message) + '\n')
    sys.stdout.flush()


def send_schema_message(stream):
    md_map = metadata.to_map(stream['metadata'])
    pks =  md_map.get((), {}).get('table-key-properties', [])

    schema_message = {'type' : 'SCHEMA',
                      'stream' : stream['tap_stream_id'],
                      'schema' : stream['schema'],
                      'key_properties' : pks,
                      'bookmark_properties': pks}
    write_schema_message(schema_message)


def do_sync(kafka_config, catalog, state):
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


def sync_stream(kafka_config, stream, state, consumer):
    send_schema_message(stream)
    stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    activate_version_message = singer.ActivateVersionMessage(stream=stream['tap_stream_id'], version=stream_version)

    singer.write_message(activate_version_message)

    time_extracted = utils.now()
    rows_saved = 0
    for message in consumer:
        LOGGER.debug("%s:%s:%s: key=%s value=%s" % (message.topic, message.partition,
                                                   message.offset, message.key,
                                                   message.value))

        # Extract message timestamp from the message
        message_timestamp = None
        message_timestamp_selector = kafka_config.get("message_timestamp")
        if message_timestamp_selector:
            match = parse(message_timestamp_selector).find(message.value)
            if isinstance(match, list):
                message_timestamp = match[0].value

        # Create record message with columns
        rec = {
            "message": message.value,
            "message_timestamp": message_timestamp
        }

        # Add primary keys to the record message
        pks =  kafka_config.get("primary_keys", [])
        for pk in pks:
            pk_selector = pks[pk]
            match = parse(pk_selector).find(message.value)
            if match:
                rec[pk] = match[0].value

        record = singer.RecordMessage(stream=stream['tap_stream_id'], record=rec, time_extracted=time_extracted)
        singer.write_message(record)

        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'offset',
                                      message.offset)

        #commit offsets because we processed the message
        tp = TopicPartition(message.topic, message.partition)
        consumer.commit({tp: OffsetAndMetadata(message.offset + 1, None)})

        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
