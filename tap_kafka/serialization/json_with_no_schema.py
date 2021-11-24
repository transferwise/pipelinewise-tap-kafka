import orjson
from confluent_kafka.serialization import Deserializer
from confluent_kafka.serialization import SerializationError


class JSONSimpleDeserializer(Deserializer):
    """
    Deserializes a Python objects from JSON bytes.
    """  # noqa: E501

    def __init__(self):
        pass

    def __call__(self, value, ctx):
        """
        Serializes unicode to bytes per the configured codec. Defaults to ``utf_8``.
        Args:
            value (bytes): bytes to be deserialized
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation
        Raises:
            SerializerError if an error occurs during deserialization.
        Returns:
            Python object if data is not None, otherwise None
        """
        if value is None:
            return None

        try:
            return orjson.loads(value)
        except orjson.JSONDecodeError as e:
            raise SerializationError(str(e))
