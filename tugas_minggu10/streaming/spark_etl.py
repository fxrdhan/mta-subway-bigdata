"""ETL streaming GTFS-RT dengan Spark Structured Streaming: tahap Transform dan Load.

Setiap 30 detik Spark membaca pesan baru dari Kafka (satu micro-batch), lalu:
  1. men-decode feed protobuf (dengan ekstensi NYCT) dan mengisi nilai yang tidak dikirim feed:
     arah kosong = utara, status kendaraan kosong = IN_TRANSIT_TO;
  2. membuang prediksi kedatangan yang sudah lewat;
  3. menghitung keterlambatan tiap kereta di stasiun berikutnya terhadap jadwal statis GTFS
     (feed MTA tidak punya kolom keterlambatan);
  4. mengagregasi keterlambatan per menit × rute × arah dan per jam × stasiun × rute, lalu
     meng-upsert hasilnya ke PostgreSQL (skema etl);
  5. menulis kedatangan kereta di stasiun dan gangguan layanan ke MongoDB (TTL 7 hari).

Setiap micro-batch dicatat di etl.gtfs_stream_batch. Batch yang diputar ulang Spark setelah restart
dilewati, sehingga upsert aditif hanya diterapkan sekali (exactly-once). Snapshot feed yang belum
berubah sejak pesan sebelumnya juga dilewati. Pesan mentah tidak disimpan di mana pun (ETL).

Variabel lingkungan: KAFKA_BOOTSTRAP, KAFKA_TOPIC, PG* (libpq), MONGO_URI, CHECKPOINT.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import os
import re
import time
import zipfile
from collections import defaultdict
from dataclasses import astuple, dataclass
from typing import NamedTuple
from zoneinfo import ZoneInfo

import psycopg
import pymongo
import requests
from nyct_gtfs.compiled_gtfs import gtfs_realtime_pb2, nyct_subway_pb2

NY = ZoneInfo("America/New_York")
# dt.UTC baru ada sejak Python 3.11, sedangkan image Spark memakai Python yang lebih lama.
UTC = dt.timezone.utc  # noqa: UP017
STATIC_GTFS_URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
# Kunci perjalanan = waktu berangkat, rute, dan arah, misalnya "084600_A..S".
# Akhiran pola rute di belakangnya diabaikan agar trip realtime cocok dengan jadwal statis.
TRIP_KEY = re.compile(r"^(\d{6}_[^.]+\.\.[NS])")
LATE_SECONDS = 300            # terlambat = lebih dari 5 menit
MAX_DELAY_SECONDS = 3 * 3600  # selisih lebih besar berarti bukan perjalanan yang sama
SCHEMA = "etl"

STOPPED_AT = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
SOUTH = nyct_subway_pb2.NyctTripDescriptor.SOUTH
ADDED, REMOVED = 1, 2  # exception_type di calendar_dates.txt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark-etl")

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

-- Agregat per menit × rute × arah
CREATE TABLE IF NOT EXISTS {SCHEMA}.gtfs_delay_minute (
    minute             timestamptz NOT NULL,
    route_id           text NOT NULL,
    direction          text NOT NULL,
    observations       integer NOT NULL,
    delay_observations integer NOT NULL,
    late_observations  integer NOT NULL,
    sum_delay_s        double precision NOT NULL,
    max_delay_s        double precision,
    stopped_at         integer NOT NULL,
    PRIMARY KEY (minute, route_id, direction)
);

-- Agregat per jam × stasiun × rute
CREATE TABLE IF NOT EXISTS {SCHEMA}.gtfs_delay_hourly (
    hour               timestamptz NOT NULL,
    stop_id            text NOT NULL,
    route_id           text NOT NULL,
    observations       integer NOT NULL,
    delay_observations integer NOT NULL,
    sum_delay_s        double precision NOT NULL,
    PRIMARY KEY (hour, stop_id, route_id)
);

-- Log setiap micro-batch (dasar exactly-once)
CREATE TABLE IF NOT EXISTS {SCHEMA}.gtfs_stream_batch (
    batch_id          bigint PRIMARY KEY,
    processed_at      timestamptz NOT NULL DEFAULT now(),
    messages          integer NOT NULL,
    skipped_snapshots integer NOT NULL,
    offset_from       bigint,
    offset_to         bigint,
    feed_time_from    timestamptz,
    feed_time_to      timestamptz,
    minute_rows       integer NOT NULL,
    hour_rows         integer NOT NULL,
    arrivals          integer NOT NULL,
    alerts            integer NOT NULL,
    seconds           double precision NOT NULL
);

-- Rata-rata keterlambatan per menit, siap dibaca
CREATE OR REPLACE VIEW {SCHEMA}.gtfs_delay_minute_v AS
SELECT minute, route_id, direction, observations, late_observations,
       CASE WHEN delay_observations > 0 THEN sum_delay_s / delay_observations END AS avg_delay_s,
       max_delay_s, stopped_at
FROM {SCHEMA}.gtfs_delay_minute;
"""

INSERT_BATCH_LOG = f"""
    INSERT INTO {SCHEMA}.gtfs_stream_batch (
        batch_id, processed_at, messages, skipped_snapshots, offset_from, offset_to,
        feed_time_from, feed_time_to, minute_rows, hour_rows, arrivals, alerts, seconds)
    VALUES (
        %(batch_id)s, now(), %(messages)s, %(skipped_snapshots)s, %(offset_from)s, %(offset_to)s,
        %(feed_time_from)s, %(feed_time_to)s, %(minute_rows)s, %(hour_rows)s,
        %(arrivals)s, %(alerts)s, %(seconds)s)
    ON CONFLICT (batch_id) DO NOTHING
    RETURNING batch_id
"""

# Upsert aditif: satu menit/jam bisa tersebar di beberapa micro-batch, jadi nilainya dijumlahkan.
UPSERT_MINUTE = f"""
    INSERT INTO {SCHEMA}.gtfs_delay_minute AS old
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (minute, route_id, direction) DO UPDATE SET
        observations       = old.observations + EXCLUDED.observations,
        delay_observations = old.delay_observations + EXCLUDED.delay_observations,
        late_observations  = old.late_observations + EXCLUDED.late_observations,
        sum_delay_s        = old.sum_delay_s + EXCLUDED.sum_delay_s,
        max_delay_s        = greatest(old.max_delay_s, EXCLUDED.max_delay_s),
        stopped_at         = old.stopped_at + EXCLUDED.stopped_at
"""
UPSERT_HOUR = f"""
    INSERT INTO {SCHEMA}.gtfs_delay_hourly AS old
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (hour, stop_id, route_id) DO UPDATE SET
        observations       = old.observations + EXCLUDED.observations,
        delay_observations = old.delay_observations + EXCLUDED.delay_observations,
        sum_delay_s        = old.sum_delay_s + EXCLUDED.sum_delay_s
"""


def utc_time(seconds: float) -> dt.datetime:
    """Detik Unix → datetime UTC."""
    return dt.datetime.fromtimestamp(seconds, UTC)


def seconds_after_midnight(hhmmss: str) -> int:
    """'25:10:00' → 90600. Jadwal GTFS boleh lewat dari 24 jam (perjalanan lewat tengah malam)."""
    hours, minutes, seconds = map(int, hhmmss.split(":"))
    return hours * 3600 + minutes * 60 + seconds


# ---------------------------------------------------------------------------------------------
# Jadwal statis
# ---------------------------------------------------------------------------------------------

class Schedule:
    """Jadwal statis GTFS: jam kedatangan terjadwal per (layanan, kunci perjalanan, stasiun).

    Jam disimpan sebagai detik sejak tengah malam hari layanan. Jadwal diunduh ulang sekali sehari.
    """

    def __init__(self) -> None:
        self.loaded_on: dt.date | None = None
        self.arrivals: dict[tuple[str, str, str], int] = {}  # (service, trip key, stop) -> detik
        self.calendar: list[dict] = []                        # baris calendar.txt
        self.exceptions: dict[tuple[str, str], int] = {}      # (service, yyyymmdd) -> ADDED/REMOVED
        self.stop_names: dict[str, str] = {}

    def refresh(self) -> None:
        today = dt.datetime.now(NY).date()
        if self.loaded_on == today:
            return
        archive = zipfile.ZipFile(io.BytesIO(requests.get(STATIC_GTFS_URL, timeout=120).content))

        def read(name: str) -> csv.DictReader:
            return csv.DictReader(io.TextIOWrapper(archive.open(name), "utf-8-sig"))

        # trip_id statis -> (service_id, kunci perjalanan)
        trips = {}
        for row in read("trips.txt"):
            key = TRIP_KEY.match(row["trip_id"].split("_", 1)[-1])
            if key:
                trips[row["trip_id"]] = (row["service_id"], key.group(1))

        arrivals = {}
        for row in read("stop_times.txt"):
            trip = trips.get(row["trip_id"])
            if trip is None or not row["arrival_time"]:
                continue
            service_id, trip_key = trip
            seconds = seconds_after_midnight(row["arrival_time"])
            arrivals.setdefault((service_id, trip_key, row["stop_id"]), seconds)

        self.arrivals = arrivals
        self.calendar = list(read("calendar.txt"))
        self.exceptions = {(row["service_id"], row["date"]): int(row["exception_type"])
                           for row in read("calendar_dates.txt")}
        self.stop_names = {row["stop_id"]: row["stop_name"] for row in read("stops.txt")}
        self.loaded_on = today
        log.info("static GTFS loaded: %d trips, %d scheduled stop times", len(trips), len(arrivals))

    def services_on(self, day: dt.date) -> list[str]:
        """service_id yang berlaku pada satu hari: calendar.txt ditambah pengecualian."""
        ymd = day.strftime("%Y%m%d")
        weekday = day.strftime("%A").lower()  # nama kolom di calendar.txt, misalnya "monday"
        active = []
        for row in self.calendar:
            service_id = row["service_id"]
            runs_today = row[weekday] == "1" and row["start_date"] <= ymd <= row["end_date"]
            removed = self.exceptions.get((service_id, ymd)) == REMOVED
            if runs_today and not removed:
                active.append(service_id)
        for (service_id, date), kind in self.exceptions.items():
            if date == ymd and kind == ADDED and service_id not in active:
                active.append(service_id)
        return active

    def scheduled_time(self, trip_key: str, stop_id: str, service_day: dt.date) -> float | None:
        """Waktu kedatangan terjadwal (detik Unix), atau None bila tidak ada di jadwal."""
        midnight = dt.datetime.combine(service_day, dt.time(), NY)
        for service_id in self.services_on(service_day):
            seconds = self.arrivals.get((service_id, trip_key, stop_id))
            if seconds is not None:
                return (midnight + dt.timedelta(seconds=seconds)).timestamp()
        return None


schedule = Schedule()

# State yang dibawa dari satu micro-batch ke micro-batch berikutnya
trip_state: dict[str, dict] = {}    # trip_id -> stasiun berikutnya pada snapshot terakhir
last_snapshot: dict[str, int] = {}  # nama feed -> timestamp snapshot terbaru yang sudah diproses
last_prune = 0.0


# ---------------------------------------------------------------------------------------------
# Transform: decode satu snapshot feed
# ---------------------------------------------------------------------------------------------

class Observation(NamedTuple):
    """Satu kereta yang masih berjalan, diamati pada satu snapshot."""
    minute: dt.datetime     # menit snapshot (UTC)
    route_id: str
    direction: str          # "N" atau "S"
    stop: str               # kode stasiun berikutnya tanpa akhiran arah, misalnya "127"
    delay_s: float | None   # prediksi − jadwal; None bila tidak ada jadwal pembanding


class StoppedTrains(NamedTuple):
    """Jumlah kereta yang sedang berhenti di stasiun pada satu menit, per rute dan arah."""
    minute: dt.datetime
    route_id: str
    direction: str
    count: int


def decode(value: bytes, feed_name: str):
    """Men-decode satu snapshot feed menjadi catatan (jenis, isi):

        ("snapshot", (feed, waktu))   snapshot baru yang diproses
        ("obs", Observation)          satu per perjalanan yang masih punya prediksi
        ("arrival", dokumen)          kereta yang sudah melewati stasiun sejak snapshot sebelumnya
        ("alert", dokumen)            gangguan layanan
        ("stopped", StoppedTrains)    kereta yang sedang berhenti, per rute dan arah
        ("duplicate", (feed, waktu))  satu-satunya catatan bila feed belum berubah
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(bytes(value))
    feed_time = feed.header.timestamp or int(time.time())

    if feed_time <= last_snapshot.get(feed_name, 0):
        yield "duplicate", (feed_name, feed_time)
        return
    last_snapshot[feed_name] = feed_time
    yield "snapshot", (feed_name, feed_time)

    minute = utc_time(feed_time - feed_time % 60)
    stopped = defaultdict(int)  # (rute, arah) -> jumlah kereta yang berhenti
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            if is_stopped(vehicle):
                stopped[(vehicle.trip.route_id, direction_of(vehicle.trip))] += 1
        elif entity.HasField("trip_update"):
            yield from observe_trip(entity.trip_update, feed_name, feed_time, minute)
        elif entity.HasField("alert"):
            yield "alert", alert_document(entity, feed_name, feed_time)

    for (route_id, direction), count in stopped.items():
        yield "stopped", StoppedTrains(minute, route_id, direction, count)


def observe_trip(trip_update, feed_name: str, feed_time: int, minute: dt.datetime):
    """Observasi satu perjalanan pada satu snapshot.

    Menghasilkan satu Observation untuk stasiun berikutnya, dan satu dokumen kedatangan bila
    kereta sudah melewati stasiun yang tercatat sebagai stasiun berikutnya pada snapshot sebelumnya.
    """
    # Prediksi yang lebih dari 30 detik sebelum snapshot sudah lewat dan dibuang.
    upcoming = [stop for stop in trip_update.stop_time_update
                if predicted_time(stop) >= feed_time - 30]
    if not upcoming:
        return  # semua prediksi perjalanan ini sudah lewat
    next_stop = upcoming[0]
    predicted = predicted_time(next_stop)
    trip = trip_update.trip
    direction = direction_of(trip)
    delay = delay_seconds(trip, next_stop.stop_id, predicted, feed_time)
    yield "obs", Observation(minute, trip.route_id, direction, next_stop.stop_id[:3], delay)

    previous = trip_state.get(trip.trip_id)
    if previous is not None:
        moved_on = previous["stop"] != next_stop.stop_id
        arrived = previous["predicted"] <= feed_time + 60
        if moved_on and arrived:
            yield "arrival", arrival_document(trip, direction, previous, feed_name, feed_time)

    trip_state[trip.trip_id] = {
        "stop": next_stop.stop_id,
        "predicted": predicted,
        "delay": delay,
        "seen": feed_time,
    }


def predicted_time(stop_time_update) -> int:
    """Waktu prediksi tiba di stasiun (atau berangkat, bila waktu tiba kosong)."""
    return stop_time_update.arrival.time or stop_time_update.departure.time


def direction_of(trip) -> str:
    """Arah "N"/"S" dari ekstensi NYCT; bila kosong, dari trip_id ("..S"), selain itu "N"."""
    nyct = trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor]
    if nyct.HasField("direction"):
        return "S" if nyct.direction == SOUTH else "N"
    return "S" if "..S" in trip.trip_id else "N"


def is_stopped(vehicle) -> bool:
    """Status kosong berarti IN_TRANSIT_TO (nilai bawaan GTFS-RT), jadi bukan berhenti."""
    return vehicle.HasField("current_status") and vehicle.current_status == STOPPED_AT


def delay_seconds(trip, stop_id: str, predicted: int, feed_time: int) -> float | None:
    """Keterlambatan = prediksi − jadwal statis.

    None bila perjalanan tidak ada di jadwal statis, atau bila selisihnya lebih dari 3 jam
    (berarti bukan perjalanan yang sama, misalnya di batas hari layanan).
    """
    if trip.start_date:
        service_day = dt.datetime.strptime(trip.start_date, "%Y%m%d").date()
    else:
        service_day = dt.datetime.fromtimestamp(feed_time, NY).date()
    key = TRIP_KEY.match(trip.trip_id)
    if key is None:
        return None
    scheduled = schedule.scheduled_time(key.group(1), stop_id, service_day)
    if scheduled is None:
        return None
    delay = predicted - scheduled
    return delay if abs(delay) <= MAX_DELAY_SECONDS else None


def arrival_document(trip, direction: str, previous: dict, feed_name: str, feed_time: int) -> dict:
    """Dokumen MongoDB untuk kereta yang tiba di stasiun `previous["stop"]`."""
    stop_id = previous["stop"]
    return {
        "_id": f"{trip.start_date}|{trip.trip_id}|{stop_id}",
        "start_date": trip.start_date,
        "trip_id": trip.trip_id,
        "route_id": trip.route_id,
        "direction": direction,
        "stop_id": stop_id,
        "stop_name": schedule.stop_names.get(stop_id),
        "arrived_at": utc_time(previous["predicted"]),
        "delay_s": previous["delay"],
        "feed": feed_name,
        "observed_at": utc_time(feed_time),
    }


def alert_document(entity, feed_name: str, feed_time: int) -> dict:
    """Dokumen MongoDB untuk satu gangguan layanan."""
    alert = entity.alert
    english = (t.text for t in alert.header_text.translation if t.language in ("", "en"))
    # Alert NYCT menyebut perjalanan yang terdampak, tidak selalu rutenya.
    routes = {informed.route_id or informed.trip.route_id for informed in alert.informed_entity}
    trips = {informed.trip.trip_id for informed in alert.informed_entity}
    return {
        "_id": entity.id,
        "header": next(english, ""),
        "routes": sorted(routes - {""}),
        "trips": sorted(trips - {""}),
        "feed": feed_name,
        "last_seen": utc_time(feed_time),
    }


# ---------------------------------------------------------------------------------------------
# Transform: agregasi satu micro-batch
# ---------------------------------------------------------------------------------------------

@dataclass
class MinuteDelay:
    """Agregat satu menit × rute × arah. Urutan field = urutan kolom etl.gtfs_delay_minute."""
    observations: int = 0          # kereta yang teramati
    delay_observations: int = 0    # yang keterlambatannya bisa dihitung
    late_observations: int = 0     # yang terlambat lebih dari LATE_SECONDS
    sum_delay_s: float = 0.0
    max_delay_s: float | None = None
    stopped_at: int = 0            # kereta yang sedang berhenti di stasiun

    def add(self, delay: float | None) -> None:
        self.observations += 1
        if delay is None:
            return
        self.delay_observations += 1
        if delay > LATE_SECONDS:
            self.late_observations += 1
        self.sum_delay_s += delay
        self.max_delay_s = delay if self.max_delay_s is None else max(self.max_delay_s, delay)


@dataclass
class HourDelay:
    """Agregat satu jam × stasiun × rute. Urutan field = urutan kolom etl.gtfs_delay_hourly."""
    observations: int = 0
    delay_observations: int = 0
    sum_delay_s: float = 0.0

    def add(self, delay: float | None) -> None:
        self.observations += 1
        if delay is None:
            return
        self.delay_observations += 1
        self.sum_delay_s += delay


def aggregate(messages) -> dict:
    """Transform satu micro-batch: men-decode pesan Kafka (key, value) lalu mengagregasinya.

    Dipakai oleh Spark (fungsi write) dan oleh notebook.
    """
    per_minute = defaultdict(MinuteDelay)  # (menit, rute, arah) -> MinuteDelay
    per_hour = defaultdict(HourDelay)      # (jam, stasiun, rute) -> HourDelay
    arrivals, alerts = {}, {}              # _id -> dokumen terbaru
    feed_times, skipped = [], 0

    for key, value in messages:
        feed_name = (key or b"").decode()
        for kind, record in decode(value, feed_name):
            if kind == "obs":
                hour = record.minute.replace(minute=0)
                per_minute[(record.minute, record.route_id, record.direction)].add(record.delay_s)
                per_hour[(hour, record.stop, record.route_id)].add(record.delay_s)
            elif kind == "stopped":
                minute_key = (record.minute, record.route_id, record.direction)
                per_minute[minute_key].stopped_at += record.count
            elif kind == "arrival":
                arrivals[record["_id"]] = record
            elif kind == "alert":
                alerts[record["_id"]] = record
            elif kind == "snapshot":
                feed_times.append(record[1])
            else:  # "duplicate"
                skipped += 1

    return {
        "minute": per_minute,
        "hour": per_hour,
        "arrivals": list(arrivals.values()),
        "alerts": list(alerts.values()),
        "feed_times": feed_times,
        "skipped": skipped,
    }


def as_rows(aggregates: dict) -> list[tuple]:
    """{kunci: agregat} → baris tabel PostgreSQL (kolom kunci, lalu kolom agregat)."""
    return [(*key, *astuple(values)) for key, values in aggregates.items()]


# ---------------------------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------------------------

pg: psycopg.Connection        # dibuka di main()
mongo: pymongo.database.Database


def write(batch_df, batch_id: int) -> None:
    """Load satu micro-batch; dipanggil Spark lewat foreachBatch."""
    started = time.time()
    rows = batch_df.select("key", "value", "offset").collect()
    if not rows:
        return
    schedule.refresh()
    result = aggregate((row.key, row.value) for row in rows)

    # MongoDB lebih dahulu. Upsert per _id bersifat idempoten, jadi batch yang diputar ulang
    # hanya menulis ulang dokumen yang sama.
    if result["arrivals"]:
        replaces = [pymongo.ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                    for doc in result["arrivals"]]
        mongo.arrivals.bulk_write(replaces, ordered=False)
    for doc in result["alerts"]:
        mongo.alerts.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    feed_times = result["feed_times"]
    batch_log = {
        "batch_id": batch_id,
        "messages": len(rows),
        "skipped_snapshots": result["skipped"],
        "offset_from": min(row.offset for row in rows),
        "offset_to": max(row.offset for row in rows),
        "feed_time_from": utc_time(min(feed_times)) if feed_times else None,
        "feed_time_to": utc_time(max(feed_times)) if feed_times else None,
        "minute_rows": len(result["minute"]),
        "hour_rows": len(result["hour"]),
        "arrivals": len(result["arrivals"]),
        "alerts": len(result["alerts"]),
        "seconds": time.time() - started,
    }

    # Log batch dan agregat ditulis dalam satu transaksi. Bila batch_id sudah tercatat (Spark
    # memutar ulang batch setelah restart), batch dilewati agar penjumlahan tidak terulang.
    with pg.transaction(), pg.cursor() as cur:
        cur.execute(INSERT_BATCH_LOG, batch_log)
        if cur.fetchone() is None:
            log.info("batch %d was already loaded before a restart; skipped", batch_id)
            return
        cur.executemany(UPSERT_MINUTE, as_rows(result["minute"]))
        cur.executemany(UPSERT_HOUR, as_rows(result["hour"]))

    prune_old_data()
    log.info("batch %d: %d messages (%d unchanged), %d minute rows, %d hour rows, "
             "%d arrivals, %d alerts", batch_id, len(rows), result["skipped"],
             len(result["minute"]), len(result["hour"]),
             len(result["arrivals"]), len(result["alerts"]))


def prune_old_data() -> None:
    """Sekali per jam: agregat menit dan log batch disimpan 14 hari, state perjalanan 2 jam."""
    global last_prune
    now = time.time()
    if now - last_prune <= 3600:
        return
    pg.execute(f"DELETE FROM {SCHEMA}.gtfs_delay_minute WHERE minute < now() - interval '14 days'")
    pg.execute(f"DELETE FROM {SCHEMA}.gtfs_stream_batch "
               "WHERE processed_at < now() - interval '14 days'")
    stale_trips = [trip_id for trip_id, state in trip_state.items() if state["seen"] < now - 7200]
    for trip_id in stale_trips:
        del trip_state[trip_id]
    last_prune = now


def main() -> None:
    global pg, mongo
    pg = psycopg.connect(application_name="spark-etl", autocommit=True)
    pg.execute(DDL)

    week = 7 * 24 * 3600
    mongo = pymongo.MongoClient(os.environ["MONGO_URI"])["mta"]
    mongo.arrivals.create_index("observed_at", expireAfterSeconds=week)
    mongo.arrivals.create_index([("route_id", 1), ("stop_id", 1), ("arrived_at", -1)])
    mongo.alerts.create_index("last_seen", expireAfterSeconds=week)
    schedule.refresh()

    # Diimpor di sini agar notebook bisa memakai decode() dan aggregate() tanpa Spark.
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("mta-gtfs-etl")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "mta-kafka:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "mta-gtfs-stream"))
        .option("startingOffsets", "latest")  # setelah restart, Spark lanjut dari checkpoint
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "400")
        .load()
    )
    query = (
        stream.writeStream.foreachBatch(write)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", os.environ.get("CHECKPOINT", "/checkpoint/gtfs-etl"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
