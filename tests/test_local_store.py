import os
import time
import pytest

from tap_kafka.local_store import LocalStore
from tap_kafka.errors import InvalidBookmarkException


class TestLocalStore:
    """
    Unit Tests
    """

    @classmethod
    def setup_class(cls):
        cls.test_dir = './test_dir'

    def test_properties(self):
        """Validate if default class variables defined correctly"""
        local_store = LocalStore(self.test_dir, 'my_stream_name')
        assert local_store.directory == self.test_dir
        assert local_store.name == 'tap-kafka-local-store-my_stream_name.db'
        assert local_store.path == os.path.join(local_store.directory, local_store.name)

    def test_properties_with_custom_args(self):
        """Validate if customised class variables defined correctly"""
        local_store = LocalStore(self.test_dir, 'my_stream_name', prefix='prefix-', postfix='-postfix', extension='txt')
        assert local_store.directory == self.test_dir
        assert local_store.name == 'prefix-my_stream_name-postfix.txt'
        assert local_store.path == os.path.join(local_store.directory, local_store.name)

    def test_add_and_get_and_write_messages(self, capsys):
        """Validate if adding, reading and writing message to stdout work as expected"""
        local_store = LocalStore(self.test_dir, 'my_stream_name')
        local_store.purge()

        # Add one single message
        time_of_first_insert = time.time()
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_1"}}')
        assert local_store.count_all() == 1

        # Add couple of more messages
        time_of_second_insert = time.time()
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_2"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_3"}}')
        assert local_store.count_all() == 3

        # Flush everything after the first insert
        local_store.flush_after(time_of_first_insert)
        stdout, stderr = capsys.readouterr()
        assert stdout == """{"type":"RECORD","value":{"col_1":"value_1"}}
{"type":"RECORD","value":{"col_1":"value_2"}}
{"type":"RECORD","value":{"col_1":"value_3"}}
"""

        # Flush everything after the second insert
        local_store.flush_after(time_of_second_insert)
        stdout, stderr = capsys.readouterr()
        assert stdout == """{"type":"RECORD","value":{"col_1":"value_2"}}
{"type":"RECORD","value":{"col_1":"value_3"}}
"""

        # Flush everything to stdout
        local_store.flush_all()
        stdout, stderr = capsys.readouterr()
        assert stdout == """{"type":"RECORD","value":{"col_1":"value_1"}}
{"type":"RECORD","value":{"col_1":"value_2"}}
{"type":"RECORD","value":{"col_1":"value_3"}}
"""

        # Delete some old items
        local_store.delete_before(time_of_second_insert)

        # Flush everything again, first inserted line should not be there
        local_store.flush_all()
        stdout, stderr = capsys.readouterr()
        assert stdout == """{"type":"RECORD","value":{"col_1":"value_2"}}
{"type":"RECORD","value":{"col_1":"value_3"}}
"""

        # Clear and test if it's empty
        local_store.purge()
        assert local_store.count_all() == 0

    def test_message_counter(self):
        """Validate if adding, reading and writing message to stdout work as expected"""
        local_store = LocalStore(self.test_dir, 'my_stream_name')
        local_store.purge()

        # Add some lines
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_1"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_2"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_3"}}')
        assert local_store.count_all() == 3

        # Add some more lines
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_4"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_5"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_6"}}')
        assert local_store.count_all() == 6

        # Clear and test if it's empty
        local_store.purge()
        assert local_store.count_all() == 0

    def test_insert_with_custom_batch(self):
        """Validate if using custom batch size rows works as expected"""
        local_store = LocalStore(self.test_dir, 'my_stream_name', batch_size_rows=5)
        local_store.purge()

        # Inserting 12 lines, should be persisted in multiple batches
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_1"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_2"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_3"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_4"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_5"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_6"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_7"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_8"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_9"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_10"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_11"}}')
        local_store.insert('{"type":"RECORD","value":{"col_1":"value_12"}}')
        assert local_store.count_all() == 12

    def test_get_timestamp_from_state(self):
        """Timestamp in the state file should be auto-converted to
        float whenever it's possible"""
        local_store = LocalStore(self.test_dir, 'my_stream_name')
        local_store.purge()

        state = {
            'bookmarks': {
                'stream_ts_as_number': {
                    'timestamp': 1598434967.5782337
                },
                'stream_ts_as_string': {
                    'timestamp': '1598434967.5782337'
                },
                'stream_ts_as_invalid_string': {
                    'timestamp': 'this-is-not-numeric'
                }
            }
        }

        # Timestamp should be float
        ts = local_store._get_timestamp_from_state(state, 'stream_ts_as_number')
        assert isinstance(ts, float)
        assert ts == 1598434967.5782337

        # Timestamp should be converted from string to float
        ts = local_store._get_timestamp_from_state(state, 'stream_ts_as_string')
        assert isinstance(ts, float)
        assert ts == 1598434967.5782337

        # Timestamp that cannot be converted to float should raise exception
        with pytest.raises(InvalidBookmarkException):
            local_store._get_timestamp_from_state(state, 'stream_ts_as_invalid_string')
