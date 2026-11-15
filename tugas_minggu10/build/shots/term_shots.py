"""Runs the evidence commands against the cluster and renders their real output as terminal screenshots.

Usage: python term_shots.py [out_dir]       -> 10, 11, 14, 15 (needs the helper pod from etl-psql.yaml)
       python term_shots.py job [out_dir]   -> 09, taken while the etl-notebook Job is running
out_dir defaults to tugas_minggu10/screenshots."""
import subprocess
import sys
from pathlib import Path

import shots

args = sys.argv[1:]
JOB = bool(args) and args[0] == "job"
args = args[1:] if JOB else args
OUT = Path(args[0]) if args else Path(__file__).resolve().parents[2] / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)
NS = ["-n", "mta-pipeline"]


def run(display: str, argv: list[str]) -> tuple[str, str]:
    r = subprocess.run(argv, capture_output=True, text=True, timeout=120)
    return display, (r.stdout + r.stderr).rstrip()


def psql(sql: str) -> tuple[str, str]:
    return run(f'kubectl -n mta-pipeline exec etl-psql -- psql -c "{sql}"',
               ["kubectl", *NS, "exec", "etl-psql", "--", "psql", "-P", "pager=off", "-c", sql])


if JOB:
    APPS = "etl-notebook,mta-kafka,mta-mongo,mta-gtfs-producer,mta-spark-streaming"
    objects = ["job/etl-notebook", "deploy/mta-gtfs-producer", "deploy/mta-spark-streaming", "sts/mta-kafka", "sts/mta-mongo"]
    nb_logs = subprocess.run(["kubectl", *NS, "logs", "job/etl-notebook"], capture_output=True, text=True).stdout
    shots.terminal(OUT / "09_kubernetes_komponen.png", "Komponen ETL di namespace mta-pipeline", [
        run("kubectl -n mta-pipeline get " + " ".join(objects), ["kubectl", *NS, "get", *objects]),
        run(f"kubectl -n mta-pipeline get pods -l 'app in ({APPS})'", ["kubectl", *NS, "get", "pods", "-l", f"app in ({APPS})"]),
        ("kubectl -n mta-pipeline logs job/etl-notebook | grep -v WARNING",
         "\n".join(l for l in nb_logs.splitlines() if "WARNING" not in l)),
    ])
    shots.close()
    sys.exit()

KAFKA_BIN = "/opt/kafka/bin"
MONGO = ('mongosh --quiet -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" '
         '--authenticationDatabase admin mta --eval')
MONGO_JS = ("print('arrivals:', db.arrivals.countDocuments(), '| alerts:', db.alerts.countDocuments()); "
            "db.arrivals.getIndexes().concat(db.alerts.getIndexes()).forEach(i => print(i.ns, EJSON.stringify(i.key), 'TTL detik:', i.expireAfterSeconds ?? '-')); "
            "printjson(db.arrivals.find({delay_s: {$ne: null}}, {_id: 0}).sort({arrived_at: -1}).limit(1).toArray()); "
            "printjson(db.alerts.find({}, {_id: 0}).sort({last_seen: -1}).limit(1).toArray())")

shots.terminal(OUT / "10_postgres.png", "PostgreSQL · skema etl (jalur batch)", [
    psql(r"\dt+ etl.*"),
    psql("SELECT 'fact_ridership_hourly' AS tabel, count(*) AS baris, sum(ridership) AS total_ridership FROM etl.fact_ridership_hourly "
         "UNION ALL SELECT 'fact_ridership_daily_station', count(*), sum(ridership) FROM etl.fact_ridership_daily_station "
         "UNION ALL SELECT 'fact_ridership_daily_fare_class', count(*), sum(ridership) FROM etl.fact_ridership_daily_fare_class"),
    psql("SELECT f.day, s.station_complex, s.borough, f.ridership, f.omny_ridership FROM etl.fact_ridership_daily_station f "
         "JOIN etl.dim_station s USING (station_complex_id) ORDER BY f.day DESC, f.ridership DESC LIMIT 5"),
])

shots.terminal(OUT / "11_kafka.png", "Apache Kafka · topik mta-gtfs-stream", [
    run(f"kubectl -n mta-pipeline exec mta-kafka-0 -- {KAFKA_BIN}/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic mta-gtfs-stream",
        ["kubectl", *NS, "exec", "mta-kafka-0", "--", f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
         "--describe", "--topic", "mta-gtfs-stream"]),
    run(f"kubectl -n mta-pipeline exec mta-kafka-0 -- {KAFKA_BIN}/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mta-gtfs-stream",
        ["kubectl", *NS, "exec", "mta-kafka-0", "--", f"{KAFKA_BIN}/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
         "--topic", "mta-gtfs-stream"]),
    run("kubectl -n mta-pipeline logs deploy/mta-gtfs-producer --tail=4",
        ["kubectl", *NS, "logs", "deploy/mta-gtfs-producer", "--tail=4"]),
])

shots.terminal(OUT / "14_mongodb.png", "MongoDB · basis data mta", [
    run(f"kubectl -n mta-pipeline exec mta-mongo-0 -- sh -c '{MONGO} \"$1\"' _ \"\n    " + MONGO_JS.replace("; ", ";\n    ") + "\"",
        ["kubectl", *NS, "exec", "mta-mongo-0", "--", "sh", "-c", f'{MONGO} "$1"', "_", MONGO_JS]),
])

logs = subprocess.run(["kubectl", *NS, "logs", "deploy/mta-spark-streaming", "--tail=400"], capture_output=True, text=True).stdout
batch_lines = "\n".join([l for l in logs.splitlines() if " batch " in l][-8:])
shots.terminal(OUT / "15_spark_logs.png", "Spark Structured Streaming · log micro-batch dan hasil di PostgreSQL", [
    ("kubectl -n mta-pipeline logs deploy/mta-spark-streaming | grep ' batch ' | tail -8", batch_lines),
    psql("SELECT batch_id, processed_at::timestamp(0), messages, offset_from, offset_to, minute_rows, arrivals, round(seconds::numeric, 2) AS detik "
         "FROM etl.gtfs_stream_batch ORDER BY batch_id DESC LIMIT 5"),
    psql("SELECT minute, route_id, direction, observations, late_observations, round(avg_delay_s) AS avg_delay_s, stopped_at "
         "FROM etl.gtfs_delay_minute_v WHERE minute = (SELECT max(minute) - interval '1 minute' FROM etl.gtfs_delay_minute) "
         "ORDER BY observations DESC LIMIT 6"),
], maxw=1700)
shots.close()
