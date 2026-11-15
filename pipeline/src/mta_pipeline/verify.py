"""Checks the serving tables against the server fingerprints stored at fetch time.

Usage: python -m mta_pipeline.verify  (exit code 1 when a check fails)
"""
from __future__ import annotations

import sys

from . import db

CHECKS = {
    # Ridership is never changed by the cleaning rules, so daily totals must match the server exactly.
    "days whose hourly ridership differs from the server":
        "SELECT count(*) FROM ingest_day i LEFT JOIN (SELECT hour::date AS d, sum(ridership) AS r "
        "FROM fact_ridership_hourly GROUP BY 1) f ON f.d = i.day WHERE f.r IS DISTINCT FROM i.ridership",
    "days whose station ridership differs from the server":
        "SELECT count(*) FROM ingest_day i LEFT JOIN (SELECT day AS d, sum(ridership) AS r "
        "FROM fact_ridership_daily_station GROUP BY 1) f ON f.d = i.day WHERE f.r IS DISTINCT FROM i.ridership",
    "months whose silver row count differs from bronze":
        "SELECT count(*) FROM (SELECT date_trunc('month', day)::date AS m, sum(rows) AS n FROM ingest_day GROUP BY 1) b "
        "LEFT JOIN silver_month s ON s.month = b.m WHERE s.rows IS DISTINCT FROM b.n",
}
INFO = {
    "days stored (count, first, last)": "SELECT count(*), min(day), max(day) FROM ingest_day",
    "rows and ridership stored": "SELECT sum(rows), sum(ridership) FROM ingest_day",
    "serving rows (hourly, daily station, fare class, stations, profile)":
        "SELECT (SELECT count(*) FROM fact_ridership_hourly), (SELECT count(*) FROM fact_ridership_daily_station), "
        "(SELECT count(*) FROM fact_ridership_daily_fare_class), (SELECT count(*) FROM dim_station), "
        "(SELECT count(*) FROM mart_station_hour_profile)",
    "violations found (tag, rows)": "SELECT tag, sum(rows) FROM quality_month WHERE rows > 0 GROUP BY tag ORDER BY tag",
    "last runs": "SELECT status, started_at::timestamp(0), finished_at::timestamp(0), days_fetched, rows_fetched, "
                 "left(coalesce(message, months_built, ''), 60) FROM pipeline_run ORDER BY started_at DESC LIMIT 5",
}


def main() -> int:
    pg = db.connect()
    for title, sql in INFO.items():
        print(f"{title}:")
        for row in pg.execute(sql).fetchall():
            print("   ", *row)
    failed = 0
    for title, sql in CHECKS.items():
        n = pg.execute(sql).fetchone()[0]
        failed += bool(n)
        print(f"{'FAIL' if n else 'ok  '} {title}: {n}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
