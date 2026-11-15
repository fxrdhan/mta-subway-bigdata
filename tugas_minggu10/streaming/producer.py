"""Producer GTFS-Realtime: tahap Extract jalur streaming.

Setiap 30 detik, kedelapan feed GTFS-RT kereta MTA diambil lalu dikirim ke topik Kafka
`mta-gtfs-stream`, satu pesan per feed:
    key                nama feed, misalnya "ACE"
    value              isi feed apa adanya (bytes protobuf), dikompresi zstd
    header fetched_at  waktu pengambilan (detik Unix)

Variabel lingkungan: KAFKA_BOOTSTRAP (host:port), KAFKA_TOPIC, POLL_SECONDS.
"""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BASE_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/"
FEEDS = {  # nama feed -> alamat feed di BASE_URL
    "1234567S": "nyct%2Fgtfs",
    "ACE": "nyct%2Fgtfs-ace",
    "BDFM": "nyct%2Fgtfs-bdfm",
    "G": "nyct%2Fgtfs-g",
    "JZ": "nyct%2Fgtfs-jz",
    "NQRW": "nyct%2Fgtfs-nqrw",
    "L": "nyct%2Fgtfs-l",
    "SIR": "nyct%2Fgtfs-si",
}
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "mta-kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "mta-gtfs-stream")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))
RETENTION_MS = 24 * 3600 * 1000  # pesan mentah hanya ditahan Kafka selama 24 jam

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def ensure_topic() -> None:
    """Membuat topik Kafka bila belum ada: satu partisi, retensi 24 jam."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    if TOPIC in admin.list_topics(timeout=20).topics:
        return
    topic = NewTopic(
        TOPIC,
        num_partitions=1,
        replication_factor=1,
        config={"retention.ms": str(RETENTION_MS), "compression.type": "producer"},
    )
    admin.create_topics([topic])[TOPIC].result()
    log.info("created topic %s (retention 24 h)", TOPIC)


def fetch(feed: tuple[str, str]) -> tuple[str, bytes | None]:
    """Mengambil satu feed. Bila gagal, isinya None dan feed itu dilewati pada putaran ini."""
    name, path = feed
    try:
        response = requests.get(BASE_URL + path, timeout=20)
        response.raise_for_status()
        return name, response.content
    except requests.RequestException as error:
        log.warning("feed %s failed: %s", name, error)
        return name, None


def main() -> None:
    ensure_topic()
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "compression.type": "zstd",
        "linger.ms": 200,
        "message.max.bytes": 4_000_000,
        "acks": "all",
    })
    round_number = 0
    with ThreadPoolExecutor(max_workers=len(FEEDS)) as pool:
        while True:
            started = time.time()
            fetched_at = str(int(started)).encode()
            sent_feeds = sent_bytes = 0

            # Kedelapan feed diambil paralel, lalu dikirim sebagai satu pesan per feed.
            for name, body in pool.map(fetch, FEEDS.items()):
                if not body:
                    continue
                headers = {"fetched_at": fetched_at}
                producer.produce(TOPIC, key=name.encode(), value=body, headers=headers)
                sent_feeds += 1
                sent_bytes += len(body)
            producer.flush(20)

            round_number += 1
            if round_number % 20 == 1:  # catatan log sekali setiap ±10 menit
                log.info("round %d: %d/8 feeds sent, %.0f KB",
                         round_number, sent_feeds, sent_bytes / 1024)
            time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    main()
