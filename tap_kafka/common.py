"""Common functions"""
from singer import metadata


def generate_schema(primary_keys) -> object:
    """Generate singer schema for kafka messages

    MESSAGE_TIMESTAMP: Automatically will extract from kafka metadata
    MESSAGE: The original kafka message
    DYNAMIC_COLUMNS_FOR_PK: optional
    """
    # Static items in every message
    schema = {
        "type": "object",
        "properties": {
            "message_timestamp": {"type": ["integer", "string", "null"]},
            "message": {"type": ["object", "array", "string", "null"]}
        }
    }

    # Primary keys are dynamic schema items
    for key in primary_keys:
        schema["properties"][key] = {"type": ["string", "null"]}

    return schema


def generate_catalog(kafka_config) -> list:
    """Generate singer compatible catalog for the kafka topic"""
    pks = []
    if 'primary_keys' in kafka_config and kafka_config['primary_keys']:
        pk_config = kafka_config.get('primary_keys', [])
        if isinstance(pk_config, object):
            pks = list(pk_config.keys())

    # Add primary keys to schema
    mdata = {}
    metadata.write(mdata, (), 'table-key-properties', pks)
    schema = generate_schema(pks)

    return [{
        'tap_stream_id': kafka_config['topic'],
        'metadata': metadata.to_list(mdata),
        'schema': schema
    }]
