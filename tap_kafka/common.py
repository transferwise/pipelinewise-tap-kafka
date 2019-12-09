from singer import metadata


def generate_schema(primary_keys) -> object:
    # Static items in every message
    schema = {
        "type": "object",
        "properties": {
            "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
            "message": {"type": ["object", "array", "string", "null"]}
        }
    }

    # Primary keys are dynamic schema items
    for pk in primary_keys:
        schema["properties"][pk] = {"type": ["string", "null"]}

    return schema


def generate_catalog(kafka_config) -> list:
    pks = []
    if 'primary_keys' in kafka_config and kafka_config['primary_keys']:
        pk_config = kafka_config.get('primary_keys', [])
        if isinstance(pk_config, object):
            pks = list(pk_config.keys())

    # Add primary keys to schema
    mdata = {}
    metadata.write(mdata, (), 'table-key-properties', pks)
    schema = generate_schema(pks)

    return [{'tap_stream_id': kafka_config['topic'], 'metadata' : metadata.to_list(mdata), 'schema' : schema}]
