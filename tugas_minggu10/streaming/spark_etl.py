"""Spark Structured Streaming ETL for GTFS-RT (Transform and Load of the streaming ETL path).

Reads the protobuf messages from Kafka every 30 seconds and, per micro-batch:
  1. decodes the feed with the NYCT extension and fills defaults the feed omits
     (empty direction = north, empty vehicle status = IN_TRANSIT_TO);
  2. drops arrival predictions that are already in the past;
  3. computes the delay of each train at its next stop against the static GTFS schedule
     (the MTA feed has no delay field);
  4. aggregates delays per minute per route and direction, and per hour per station and route,
     and upserts them into PostgreSQL (schema etl);
  5. writes train arrivals (a train passing a station) and service alerts to MongoDB (TTL 7 days).
Each micro-batch is logged in etl.gtfs_stream_batch; a batch that Spark replays after a restart is skipped,
so the additive upserts are applied exactly once. Repeated snapshots of a feed that has not changed are skipped.
Raw messages are not stored anywhere (ETL). Environment: KAFKA_BOOTSTRAP, KAFKA_TOPIC, PG* (libpq), MONGO_URI.
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
from zoneinfo import ZoneInfo

import psycopg
import pymongo
import requests
from nyct_gtfs.compiled_gtfs import gtfs_realtime_pb2, nyct_subway_pb2

NY = ZoneInfo("America/New_York")
STATIC_URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
KEY = re.compile(r"^(\d{6}_[^.]+\.\.[NS])")  # origin time, route, direction (shape suffix ignored)
LATE_SECONDS = 300
SCHEMA = "etl"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark-etl")

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
CREATE TABLE IF NOT EXISTS {SCHEMA}.gtfs_delay_minute (
    minute timestamptz NOT NULL, route_id text NOT NULL, direction text NOT NULL,
    observations integer NOT NULL, delay_observations integer NOT NULL, late_observations integer NOT NULL,
    sum_delay_s double precision NOT NULL, max_delay_s double precision, stopped_at integer NOT NULL,
    PRIMARY KEY (minute, route_id, direction));
CREATE TABLE IF NOT EXISTS {SCHEMA}.gtfs_delay_hourly (
    hour timestamptz NOT NULL, stop_id text NOT NULL, route_id text NOT NULL,
    observations integer NOT NULL, delay_observations integer NOT NULL, sum_delay_s double precision NOT NULL,
    PRIMARY KEY (hour, stop_id, route_id));
CREATE TABLE IF NOT EXISTS {SCHEMA}.gtfs_stream_batch (
    batch_id bigint PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT now(), messages integer NOT NULL,
    skipped_snapshots integer NOT NULL, offset_from bigint, offset_to bigint, feed_time_from timestamptz, feed_time_to timestamptz,
    minute_rows integer NOT NULL, hour_rows integer NOT NULL, arrivals integer NOT NULL, alerts integer NOT NULL,
    seconds double precision NOT NULL);
CREATE OR REPLACE VIEW {SCHEMA}.gtfs_delay_minute_v AS
    SELECT minute, route_id, direction, observations, late_observations,
           CASE WHEN delay_observations > 0 THEN sum_delay_s / delay_observations END AS avg_delay_s, max_delay_s, stopped_at
    FROM {SCHEMA}.gtfs_delay_minute;
"""


class Schedule:
    """Static GTFS: scheduled arrival (seconds after service-day midnight) per (service, trip key, stop)."""

    def __init__(self) -> None:
        self.loaded_on: dt.date | None = None
        self.arrivals: dict[tuple[str, str, str], int] = {}
        self.calendar: list[dict] = []
        self.exceptions: dict[tuple[str, str], int] = {}
        self.stop_names: dict[str, str] = {}

    def refresh(self) -> None:
        today = dt.datetime.now(NY).date()
        if self.loaded_on == today:
            return
        z = zipfile.ZipFile(io.BytesIO(requests.get(STATIC_URL, timeout=120).content))
        read = lambda name: csv.DictReader(io.TextIOWrapper(z.open(name), "utf-8-sig"))
        trips = {}
        for row in read("trips.txt"):
            m = KEY.match(row["trip_id"].split("_", 1)[-1])
            if m:
                trips[row["trip_id"]] = (row["service_id"], m.group(1))
        arrivals: dict[tuple[str, str, str], int] = {}
        for row in read("stop_times.txt"):
            trip = trips.get(row["trip_id"])
            if trip and row["arrival_time"]:
                h, mi, s = map(int, row["arrival_time"].split(":"))
                arrivals.setdefault((trip[0], trip[1], row["stop_id"]), h * 3600 + mi * 60 + s)
        self.arrivals = arrivals
        self.calendar = list(read("calendar.txt"))
        self.exceptions = {(r["service_id"], r["date"]): int(r["exception_type"]) for r in read("calendar_dates.txt")}
        self.stop_names = {r["stop_id"]: r["stop_name"] for r in read("stops.txt")}
        self.loaded_on = today
        log.info("static GTFS loaded: %d trips, %d scheduled stop times", len(trips), len(arrivals))

    def services(self, day: dt.date) -> list[str]:
        ymd, weekday = day.strftime("%Y%m%d"), day.strftime("%A").lower()
        active = [c["service_id"] for c in self.calendar
                  if c[weekday] == "1" and c["start_date"] <= ymd <= c["end_date"] and self.exceptions.get((c["service_id"], ymd)) != 2]
        active += [s for (s, d), kind in self.exceptions.items() if d == ymd and kind == 1 and s not in active]
        return active

    def scheduled(self, trip_key: str, stop_id: str, start: dt.date) -> float | None:
        for service in self.services(start):
            seconds = self.arrivals.get((service, trip_key, stop_id))
            if seconds is not None:
                return (dt.datetime.combine(start, dt.time(), NY) + dt.timedelta(seconds=seconds)).timestamp()
        return None


schedule = Schedule()
trip_state: dict[str, dict] = {}  # last next-stop per trip, to detect arrivals between snapshots
last_snapshot: dict[str, int] = {}  # newest feed header timestamp seen per feed
last_prune = 0.0


def decode(value: bytes, feed_name: str):
    """Yields ('snapshot', ...), ('obs', ...), ('stopped', ...), ('arrival', ...), ('alert', ...) records from one feed snapshot,
    or a single ('duplicate', ...) record when the feed has not changed since the previous message."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(bytes(value))
    fetched = feed.header.timestamp or int(time.time())
    if fetched <= last_snapshot.get(feed_name, 0):
        yield "duplicate", (feed_name, fetched)
        return
    last_snapshot[feed_name] = fetched
    yield "snapshot", (feed_name, fetched)
    minute = dt.datetime.fromtimestamp(fetched - fetched % 60, dt.timezone.utc)
    stopped = defaultdict(int)
    for e in feed.entity:
        if e.HasField("vehicle"):
            v = e.vehicle
            status = v.current_status if v.HasField("current_status") else gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
            direction = _direction(v.trip)
            if status == gtfs_realtime_pb2.VehiclePosition.STOPPED_AT:
                stopped[(v.trip.route_id, direction)] += 1
        elif e.HasField("trip_update"):
            tu = e.trip_update
            future = [s for s in tu.stop_time_update if (s.arrival.time or s.departure.time) >= fetched - 30]
            if not future:
                continue  # every prediction of this trip is already in the past
            nxt = future[0]
            predicted = nxt.arrival.time or nxt.departure.time
            trip = tu.trip
            direction = _direction(trip)
            start = dt.datetime.strptime(trip.start_date, "%Y%m%d").date() if trip.start_date else \
                dt.datetime.fromtimestamp(fetched, NY).date()
            key = KEY.match(trip.trip_id)
            scheduled = schedule.scheduled(key.group(1), nxt.stop_id, start) if key else None
            delay = predicted - scheduled if scheduled is not None else None
            if delay is not None and abs(delay) > 3 * 3600:
                delay = None  # not the same trip instance (e.g. service day boundary)
            yield "obs", (minute, trip.route_id, direction, nxt.stop_id[:3], delay)
            prev = trip_state.get(trip.trip_id)
            if prev and prev["stop"] != nxt.stop_id and prev["predicted"] <= fetched + 60:
                yield "arrival", {"_id": f"{trip.start_date}|{trip.trip_id}|{prev['stop']}", "start_date": trip.start_date,
                                  "trip_id": trip.trip_id, "route_id": trip.route_id, "direction": direction,
                                  "stop_id": prev["stop"], "stop_name": schedule.stop_names.get(prev["stop"]),
                                  "arrived_at": dt.datetime.fromtimestamp(prev["predicted"], dt.timezone.utc),
                                  "delay_s": prev["delay"], "feed": feed_name,
                                  "observed_at": dt.datetime.fromtimestamp(fetched, dt.timezone.utc)}
            trip_state[trip.trip_id] = {"stop": nxt.stop_id, "predicted": predicted, "delay": delay, "seen": fetched}
        elif e.HasField("alert"):
            a = e.alert
            text = next((t.text for t in a.header_text.translation if t.language in ("", "en")), "")
            yield "alert", {"_id": e.id, "header": text,  # NYCT alerts name the affected trips, not always the route
                            "routes": sorted({i.route_id or i.trip.route_id for i in a.informed_entity} - {""}),
                            "trips": sorted({i.trip.trip_id for i in a.informed_entity} - {""}),
                            "feed": feed_name, "last_seen": dt.datetime.fromtimestamp(fetched, dt.timezone.utc)}
    for (route, direction), n in stopped.items():
        yield "stopped", (minute, route, direction, n)


def _direction(trip) -> str:
    ext = trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor]
    if ext.HasField("direction"):
        return "S" if ext.direction == nyct_subway_pb2.NyctTripDescriptor.SOUTH else "N"
    return "S" if "..S" in trip.trip_id else "N"  # empty direction = north


def aggregate(messages) -> dict:
    """Transform: decodes (key, value) messages and aggregates them. Shared by Spark and the notebook."""
    minute_agg = defaultdict(lambda: [0, 0, 0, 0.0, None, 0])  # obs, delay obs, late obs, sum, max, stopped
    hour_agg = defaultdict(lambda: [0, 0, 0.0])
    arrivals, alerts, feed_times, skipped = {}, {}, [], 0
    for key, value in messages:
        for kind, rec in decode(value, (key or b"").decode()):
            if kind == "obs":
                minute, route, direction, stop, delay = rec
                m = minute_agg[(minute, route, direction)]
                h = hour_agg[(minute.replace(minute=0), stop, route)]
                m[0] += 1
                h[0] += 1
                if delay is not None:
                    m[1] += 1
                    m[2] += delay > LATE_SECONDS
                    m[3] += delay
                    m[4] = delay if m[4] is None else max(m[4], delay)
                    h[1] += 1
                    h[2] += delay
            elif kind == "stopped":
                minute, route, direction, n = rec
                minute_agg[(minute, route, direction)][5] += n
            elif kind == "arrival":
                arrivals[rec["_id"]] = rec
            elif kind == "alert":
                alerts[rec["_id"]] = rec
            elif kind == "snapshot":
                feed_times.append(rec[1])
            else:
                skipped += 1
    return {"minute": minute_agg, "hour": hour_agg, "arrivals": list(arrivals.values()), "alerts": list(alerts.values()),
            "feed_times": feed_times, "skipped": skipped}


def write(batch_df, batch_id: int) -> None:
    global last_prune
    t0 = time.time()
    rows = batch_df.select("key", "value", "offset").collect()
    if not rows:
        return
    schedule.refresh()
    agg = aggregate((r.key, r.value) for r in rows)
    minute_agg, hour_agg, arrivals, alerts = agg["minute"], agg["hour"], agg["arrivals"], agg["alerts"]
    # MongoDB first: upserts by _id are idempotent, so a replayed batch rewrites the same documents.
    if arrivals:
        mongo.arrivals.bulk_write([pymongo.ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in arrivals], ordered=False)
    for doc in alerts:
        mongo.alerts.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    times = agg["feed_times"]
    with pg.transaction(), pg.cursor() as cur:
        cur.execute(f"INSERT INTO {SCHEMA}.gtfs_stream_batch VALUES (%s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (batch_id) DO NOTHING RETURNING batch_id",
                    (batch_id, len(rows), agg["skipped"], min(r.offset for r in rows), max(r.offset for r in rows),
                     dt.datetime.fromtimestamp(min(times), dt.timezone.utc) if times else None,
                     dt.datetime.fromtimestamp(max(times), dt.timezone.utc) if times else None,
                     len(minute_agg), len(hour_agg), len(arrivals), len(alerts), time.time() - t0))
        if cur.fetchone() is None:
            log.info("batch %d was already loaded before a restart; skipped", batch_id)
            return
        cur.executemany(
            f"INSERT INTO {SCHEMA}.gtfs_delay_minute VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (minute, route_id, direction) DO UPDATE SET "
            "observations = gtfs_delay_minute.observations + EXCLUDED.observations, "
            "delay_observations = gtfs_delay_minute.delay_observations + EXCLUDED.delay_observations, "
            "late_observations = gtfs_delay_minute.late_observations + EXCLUDED.late_observations, "
            "sum_delay_s = gtfs_delay_minute.sum_delay_s + EXCLUDED.sum_delay_s, "
            "max_delay_s = greatest(gtfs_delay_minute.max_delay_s, EXCLUDED.max_delay_s), "
            "stopped_at = gtfs_delay_minute.stopped_at + EXCLUDED.stopped_at",
            [(k[0], k[1], k[2], *v) for k, v in minute_agg.items()])
        cur.executemany(
            f"INSERT INTO {SCHEMA}.gtfs_delay_hourly VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (hour, stop_id, route_id) DO UPDATE SET "
            "observations = gtfs_delay_hourly.observations + EXCLUDED.observations, "
            "delay_observations = gtfs_delay_hourly.delay_observations + EXCLUDED.delay_observations, "
            "sum_delay_s = gtfs_delay_hourly.sum_delay_s + EXCLUDED.sum_delay_s",
            [(k[0], k[1], k[2], *v) for k, v in hour_agg.items()])
    now = time.time()
    if now - last_prune > 3600:  # retention: minute table and batch log 14 days; trip state 2 hours
        pg.execute(f"DELETE FROM {SCHEMA}.gtfs_delay_minute WHERE minute < now() - interval '14 days'")
        pg.execute(f"DELETE FROM {SCHEMA}.gtfs_stream_batch WHERE processed_at < now() - interval '14 days'")
        for trip in [t for t, s in trip_state.items() if s["seen"] < now - 7200]:
            del trip_state[trip]
        last_prune = now
    log.info("batch %d: %d messages (%d unchanged), %d minute rows, %d hour rows, %d arrivals, %d alerts",
             batch_id, len(rows), agg["skipped"], len(minute_agg), len(hour_agg), len(arrivals), len(alerts))


if __name__ == "__main__":
    pg = psycopg.connect(application_name="spark-etl", autocommit=True)
    pg.execute(DDL)
    mongo = pymongo.MongoClient(os.environ["MONGO_URI"])["mta"]
    mongo.arrivals.create_index("observed_at", expireAfterSeconds=7 * 24 * 3600)
    mongo.arrivals.create_index([("route_id", 1), ("stop_id", 1), ("arrived_at", -1)])
    mongo.alerts.create_index("last_seen", expireAfterSeconds=7 * 24 * 3600)
    schedule.refresh()

    from pyspark.sql import SparkSession  # imported here so the notebook can reuse decode() and aggregate() without Spark
    spark = (SparkSession.builder.appName("mta-gtfs-etl").master("local[2]")
             .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "mta-kafka:9092"))
              .option("subscribe", os.environ.get("KAFKA_TOPIC", "mta-gtfs-stream"))
              .option("startingOffsets", "latest").option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", "400").load())
    query = (stream.writeStream.foreachBatch(write).trigger(processingTime="30 seconds")
             .option("checkpointLocation", os.environ.get("CHECKPOINT", "/checkpoint/gtfs-etl")).start())
    query.awaitTermination()
