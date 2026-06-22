#!/usr/bin/env python
"""Wait until Kafka topics exist."""

from time import sleep
from confluent_kafka.admin import AdminClient


def wait_for_topics(bootstrap_servers: str, *topics):
    client = AdminClient({"bootstrap.servers": bootstrap_servers})
    while True:
        existing = client.list_topics().topics.keys()
        if all(t in existing for t in topics):
            print(f"Found topics {topics}", flush=True)
            return
        print(f"Waiting for topics {topics}", flush=True)
        sleep(1.0)
