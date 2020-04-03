"""Internal store to keep consumed messages from Kafka on disk to keep short commit periods"""
import os
import sys
import time
import singer
import logging
import filelock
from filelock import FileLock

# Set filelock logging less werbose
filelock.logger().setLevel(logging.WARNING)


class LocalStore:
    # pylint: disable=too-many-arguments
    def __init__(self, directory, topic, prefix='tap-kafka-local-store-', postfix='', extension='db'):
        """Initialize local storage

        :param directory: directory path
        :param topic: name of the local store
        :param prefix: Optional file name prefix
        :param postfix: Optional file name postfix
        :param extension: Optional file name extension
        """
        self.directory = os.path.expanduser(directory)
        self.topic = topic
        self.name = f'{prefix}{topic}{postfix}.{extension}'
        self.path = os.path.join(directory, self.name)
        self.temp = f'{self.path}.temp'
        self.lock = f'{self.path}.lock'

        # Create the directory if not exists
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        # Create an empty file if not exists
        if not os.path.exists(self.path):
            open(self.path, 'w')

    @staticmethod
    def _blocks(file, size=65536):
        """Reads a file by block chunks"""
        while True:
            b = file.read(size)
            if not b:
                break
            yield b

    @staticmethod
    def _split_line(line: str) -> list:
        """Every message in the file is timestamped in the following format:
          1585817817.5626152:{"type":"RECORD","value":{"col_1":"value_1"}}

        This method split the original message to timestamp and the real message
        componets and returns as list of two items.
        """
        return line.split(':', 1)

    @staticmethod
    def _flush(message: str):
        """Flush anything to stdout"""
        sys.stdout.write(message)
        sys.stdout.flush()

    def _flush_line(self, line: str):
        """Takes a line from the local store and sends *only* the message part to stdout.

         Every item in the file is timestamped in the following format:
          1585817817.5626152:{"type":"RECORD","value":{"col_1":"value_1"}}

        This function is removing the timestamp part"""
        message = self._split_line(line)[1]
        self._flush(message)

    def count_all(self) -> int:
        """Returns the number of messages in the store

        This functions reads the entire file with low memory footprint
        and counts the number of newline characters"""
        with FileLock(self.lock):
            try:
                with open(self.path, 'r') as msf:
                    return sum(block.count('\n') for block in self._blocks(msf))

            # Return 0 if no local store file
            except FileNotFoundError:
                return 0

    def purge(self):
        """Delete every message from the store"""
        if os.path.exists(self.path):
            os.remove(self.path)

        if os.path.exists(self.lock):
            os.remove(self.lock)

    def insert(self, message: str) -> float:
        """Insert a new message with a generated timestamp
        Returns the insert timestamp"""
        with FileLock(self.lock):
            with open(self.path, 'a+') as msf:
                insert_ts = time.time()
                msf.write(f'{insert_ts}:{message}' + '\n')
        return insert_ts

    def delete_before(self, timestamp: float) -> float:
        """Delete messages before a certain timestamp

        This function creates a temporary file *only* with lines required to keep.
        Once every required line copied, it swaps the files.

        Returns the timestamp of last delete message"""
        last_timestamp = timestamp
        with FileLock(self.lock):
            with open(self.path, 'r') as msf:
                with open(self.temp, 'w') as msf_temp:
                    for line in msf:
                        msg_timestamp, message = self._split_line(line)
                        if float(msg_timestamp) > timestamp:
                            msf_temp.write(line)
                        else:
                            last_timestamp = msg_timestamp

        # Rename the temp file to be the new active
        if os.path.exists(self.temp):
            os.rename(self.temp, self.path)

        return last_timestamp

    def delete_before_bookmark(self, state: dict) -> int:
        """Delete message before bookmarked timestamp in state

        Returns the timestamp of last delete message"""
        timestamp = singer.get_bookmark(state, self.topic, 'timestamp')
        if timestamp is not None:
            return self.delete_before(timestamp)
        return 0

    def flush_after(self, timestamp: float) -> float:
        """Flush every message after a certain timestamp to stdout

        Returns the timestamp of last flushed message"""
        pos = timestamp
        with FileLock(self.lock):
            with open(self.path, 'r') as msf:
                for line in msf:
                    msg_timestamp, message = self._split_line(line)
                    if float(msg_timestamp) >= timestamp:
                        self._flush(message)
                        pos = msg_timestamp
        return pos

    def flush_after_bookmark(self, state: dict) -> float:
        """Flush every message after a singer bookmark

        Returns the timestamp of last flushed message"""
        timestamp = singer.get_bookmark(state, self.topic, 'timestamp')
        if timestamp is not None:
            return self.flush_after(timestamp)
        return 0

    def flush_all(self):
        """Flush every message to stdout"""
        with FileLock(self.lock):
            with open(self.path, 'r') as msf:
                for line in msf:
                    self._flush_line(line)
