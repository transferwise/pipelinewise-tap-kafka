import os
import time

from tap_kafka.local_store import LocalStore


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
