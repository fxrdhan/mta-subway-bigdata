"""Load gold marts into PostgreSQL: monthly facts are replaced per month in one transaction (idempotent)."""
from __future__ import annotations

import datetime as dt
import logging

import duckdb
import psycopg

from . import lake, quality
from .config import Settings
from .transform import BuiltMonth

log = logging.getLogger(__name__)

# gold mart -> (table, date column used to delete one month, columns)
MONTHLY = {
    "ridership_hourly": ("fact_ridership_hourly", "hour", ["hour", "payment_method", "ridership", "transfers"]),
    "ridership_daily_station": ("fact_ridership_daily_station", "day",
                                ["day", "station_complex_id", "ridership", "transfers", "omny_ridership"]),
    "ridership_daily_fare_class": ("fact_ridership_daily_fare_class", "day", ["day", "fare_class_category", "ridership"]),
}
FULL = {
    "dim_station": ("dim_station", ["station_complex_id", "station_complex", "borough", "transit_mode", "latitude", "longitude"]),
    "dim_date": ("dim_date", ["date", "year", "month", "day", "day_of_week", "is_weekend"]),
    "station_hour_profile": ("mart_station_hour_profile", ["station_complex_id", "day_type", "hour_of_day", "mean_ridership"]),
}


def _next_month(month: dt.date) -> dt.date:
    return (month + dt.timedelta(days=32)).replace(day=1)


def _copy(pg: psycopg.Connection, con: duckdb.DuckDBPyConnection, table: str, columns: list[str], path: str) -> int:
    cols = ", ".join(columns)
    rows = con.execute(f"SELECT {cols} FROM read_parquet('{path}')").fetchall()
    with pg.cursor() as cur, cur.copy(f"COPY {table} ({cols}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    return len(rows)


def run(settings: Settings, con: duckdb.DuckDBPyConnection, pg: psycopg.Connection, built: list[BuiltMonth]) -> None:
    for b in built:
        start, end = b.month, _next_month(b.month)
        with pg.transaction():
            for mart, (table, date_col, columns) in MONTHLY.items():
                pg.execute(f"DELETE FROM {table} WHERE {date_col} >= %s AND {date_col} < %s", (start, end))
                _copy(pg, con, table, columns, lake.gold_month(settings, mart, b.month))
            pg.execute("DELETE FROM quality_month WHERE month = %s", (start,))
            for tag, severity in quality.SEVERITY.items():
                pg.execute("INSERT INTO quality_month VALUES (%s, %s, %s, %s)", (start, tag, severity, b.tags.get(tag, 0)))
            pg.execute(
                "INSERT INTO silver_month VALUES (%s, %s, %s, now()) ON CONFLICT (month) DO UPDATE SET "
                "rows = EXCLUDED.rows, source_updated_at = EXCLUDED.source_updated_at, built_at = EXCLUDED.built_at",
                (start, b.rows, b.source_updated_at),
            )
        log.info("loaded %s into PostgreSQL", f"{b.month:%Y-%m}")
    if built:
        with pg.transaction():
            for mart, (table, columns) in FULL.items():
                pg.execute(f"TRUNCATE {table}")
                n = _copy(pg, con, table, columns, lake.gold_file(settings, mart))
                log.info("loaded %s (%s rows)", table, f"{n:,}")
