import pytest

from confluent_kafka.serialization import SerializationError
from tap_kafka.serialization.json_with_no_schema import JSONSimpleDeserializer


class TestSerializers(object):
    def test_generate_config_with_defaults(self):
        deserializer = JSONSimpleDeserializer()

        assert deserializer(value=None, ctx=None) is None
        assert deserializer(value='{}', ctx=None) == {}
        assert deserializer(value='{"abc": 123}', ctx=None) == {'abc': 123}

        with pytest.raises(SerializationError):
            deserializer(value='invalid-json', ctx=None)
