import os
from datetime import datetime
from typing import Dict, List

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def get_file_lines(filename: str) -> List:
    with open(f'{os.path.dirname(__file__)}/resources/{filename}') as f_lines:
        return f_lines.readlines()

    return []


def get_kafka_config(extra_config: Dict = None) -> Dict:
    default_config = {
        'bootstrap_servers': os.environ['TAP_KAFKA_BOOTSTRAP_SERVERS']
    }

    if extra_config:
        return {**default_config, **extra_config}

    return default_config


def delete_topic(
        bootstrap_servers: str,
        topic_name: str
) -> None:
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    fs = admin_client.delete_topics([topic_name], operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print('Topic {} deleted'.format(topic))
        except Exception as exc:
            print('Failed to delete topic {}: {}'.format(topic, exc))
            raise exc


def create_topic(
        bootstrap_servers: str,
        topic_name: str,
        num_partitions: int = 1,
        replica_factor: int = 1
) -> str:
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    full_topic_name = f'{topic_name}_{now}'
    fs = admin_client.create_topics([NewTopic(full_topic_name, num_partitions, replica_factor)])

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f'Topic {topic} created')
            return topic
        except Exception as exc:
            print(f'Failed to create topic {topic}: {exc}')
            raise exc


def produce_messages(
        bootstrap_servers: str,
        topic_name: str,
        messages: List[str]
) -> None:
    p = Producer({'bootstrap.servers': bootstrap_servers})

    def delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    for message in messages:
        p.poll(0)
        p.produce(topic_name, message.encode('utf-8'), callback=delivery_report)

    p.flush()
