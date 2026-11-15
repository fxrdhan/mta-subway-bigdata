"""GTFS-RT producer (Extract of the streaming ETL path).

Every 30 seconds it fetches the eight MTA subway GTFS-RT feeds and publishes each feed as one Kafka message
(key = feed name, value = the protobuf bytes, compressed with zstd) to the topic mta-gtfs-stream.
Environment: KAFKA_BOOTSTRAP (host:port), KAFKA_TOPIC, POLL_SECONDS.
"""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

FEEDS = {
    "1234567S": "nyct%2Fgtfs", "ACE": "nyct%2Fgtfs-ace", "BDFM": "nyct%2Fgtfs-bdfm", "G": "nyct%2Fgtfs-g",
    "JZ": "nyct%2Fgtfs-jz", "NQRW": "nyct%2Fgtfs-nqrw", "L": "nyct%2Fgtfs-l", "SIR": "nyct%2Fgtfs-si",
}
BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "mta-kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "mta-gtfs-stream")
POLL = int(os.environ.get("POLL_SECONDS", "30"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TOPIC in admin.list_topics(timeout=20).topics:
        return
    topic = NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                     config={"retention.ms": str(24 * 3600 * 1000), "compression.type": "producer"})
    admin.create_topics([topic])[TOPIC].result()
    log.info("created topic %s (retention 24 h)", TOPIC)


def fetch(item: tuple[str, str]) -> tuple[str, bytes | None]:
    name, path = item
    try:
        r = requests.get(BASE + path, timeout=20)
        r.raise_for_status()
        return name, r.content
    except requests.RequestException as e:
        log.warning("feed %s failed: %s", name, e)
        return name, None


def main() -> None:
    ensure_topic()
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "compression.type": "zstd", "linger.ms": 200,
                         "message.max.bytes": 4_000_000, "acks": "all"})
    session_round = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        while True:
            started = time.time()
            fetched_at = str(int(started)).encode()
            sent = size = 0
            for name, body in pool.map(fetch, FEEDS.items()):
                if body:
                    producer.produce(TOPIC, key=name.encode(), value=body, headers={"fetched_at": fetched_at})
                    sent, size = sent + 1, size + len(body)
            producer.flush(20)
            session_round += 1
            if session_round % 20 == 1:
                log.info("round %d: %d/8 feeds sent, %.0f KB", session_round, sent, size / 1024)
            time.sleep(max(0.0, POLL - (time.time() - started)))


if __name__ == "__main__":
    main()
