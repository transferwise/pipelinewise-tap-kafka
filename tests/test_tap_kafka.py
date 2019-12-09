import pytest

from tap_kafka import common
from tap_kafka import sync


class TestSync(object):
    """
    Unit Tests
    """

    def setUp(self):
        self.config = {}

    def test_generate_schema_with_no_pk(self):
        """Should not add extra column when no PK defined"""
        assert common.generate_schema([]) == \
            {
                "type": "object",
                "properties": {
                    "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            }

    def test_generate_schema_with_pk(self):
        """Should add one extra column if PK defined"""
        assert common.generate_schema(["id"]) == \
            {
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            }

    def test_generate_schema_with_composite_pk(self):
        """Should add multiple extra columns if composite PK defined"""
        assert common.generate_schema(["id", "version"]) == \
            {
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "version": {"type": ["string", "null"]},
                    "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            }

    def test_generate_catalog_with_no_pk(self):
        """table-key-properties should be empty list when no PK defined"""
        assert common.generate_catalog({"topic": "dummy_topic"}) == \
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": []}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                               "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
                               "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_generate_catalog_with_pk(self):
        """table-key-properties should be a list with single item when PK defined"""
        assert common.generate_catalog({"topic": "dummy_topic", "primary_keys": {"id": "^.dummyJson.id"}}) == \
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string", "null"]},
                               "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
                               "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]

    def test_generate_catalog_with_composite_pk(self):
        """table-key-properties should be a list with two itemi when composite PK defined"""
        assert common.generate_catalog({"topic": "dummy_topic", "primary_keys": {"id": "dummyJson.id", "version": "dummyJson.version"}}) == \
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id", "version"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string", "null"]},
                                "version": {"type": ["string", "null"]},
                               "message_timestamp": {"type": ["string", "null"], "format": "date-time"},
                               "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ]
