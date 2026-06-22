#!/usr/bin/env python
"""Kafka topic helpers (create the topic, wait for the broker)."""

import logging
from time import sleep
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def setup_topic(topic: str, bootstrap_servers: str = "localhost:29092", num_partitions: int = 1,
                replication_factor: int = 1, num_attempts: int = 10,
                recreate_on_mismatch: bool = False) -> bool:

    client = AdminClient({"bootstrap.servers": bootstrap_servers})

    last_ex = None
    for attempt in range(1, num_attempts + 1):
        # bounded lookup + visible retry; without the timeout, list_topics() blocks
        # forever on a dead broker and the retry loop never applies
        try:
            t = client.list_topics(timeout=10).topics.get(topic)
        except KafkaException as ex:
            last_ex = ex
            logging.warning(f"Kafka not reachable at {bootstrap_servers}, "
                            f"retrying ({attempt}/{num_attempts})...")
            sleep(5.0)
            continue

        if t is not None:
            np_ = len(t.partitions)
            rf = len(t.partitions[0].replicas) if np_ > 0 else 0
            if num_partitions == np_ and replication_factor == rf:
                logging.info(f"Found existing topic '{topic}' with {np_} partitions, replication factor {rf}")
                return False
            if not recreate_on_mismatch:
                # deleting the topic throws away everything already streamed —
                # never do that implicitly on a relaunch with different flags
                raise SystemExit(
                    f"Topic '{topic}' exists with {np_} partitions / rf {rf}, but "
                    f"{num_partitions}/{replication_factor} was requested. Refusing to "
                    f"delete it implicitly — pass --recreate-topic to recreate.")
            client.delete_topics([topic]).get(topic).result()
            logging.info(f"Deleted existing topic '{topic}' ({np_} partitions, rf {rf})")

        try:
            n = NewTopic(topic=topic, num_partitions=num_partitions,
                         replication_factor=replication_factor)
            client.create_topics(new_topics=[n]).get(topic).result()
            logging.info(f"Created topic '{topic}' ({num_partitions} partitions, rf {replication_factor})")
            return True
        except KafkaException as ex:
            last_ex = ex
            sleep(1.0)

    raise Exception(f"Could not create topic {topic} after {num_attempts} attempts: {last_ex}")


def produce_or_block(producer: Producer, **kwargs):
    while True:
        try:
            producer.produce(**kwargs)
            break
        except BufferError:
            producer.flush(1.0)
