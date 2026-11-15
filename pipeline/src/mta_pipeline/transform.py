"""Transform inside the lake: bronze days -> silver months -> gold marts (DuckDB SQL over Parquet)."""
from __future__ import annotations

import datetime as dt
import logging
from collections import defaultdict
from dataclasses import dataclass

import duckdb

from . import lake, quality
from .config import Settings
from .soda import SORT_KEYS

log = logging.getLogger(__name__)

# Sums are cast to BIGINT: DuckDB writes HUGEINT (the type of sum over INTEGER) to Parquet as DOUBLE.
MONTHLY_MARTS = {
    "ridership_hourly": (
        "SELECT transit_timestamp AS hour, payment_method, sum(ridership)::BIGINT AS ridership, sum(transfers)::BIGINT AS transfers "
        "FROM {src} GROUP BY ALL ORDER BY ALL"),
    "ridership_daily_station": (
        "SELECT CAST(transit_timestamp AS DATE) AS day, station_complex_id, sum(ridership)::BIGINT AS ridership, "
        "sum(transfers)::BIGINT AS transfers, coalesce(sum(ridership) FILTER (payment_method = 'omny'), 0)::BIGINT AS omny_ridership "
        "FROM {src} GROUP BY ALL ORDER BY ALL"),
    "ridership_daily_fare_class": (
        "SELECT CAST(transit_timestamp AS DATE) AS day, fare_class_category, sum(ridership)::BIGINT AS ridership "
        "FROM {src} GROUP BY ALL ORDER BY ALL"),
}

FULL_MARTS = {
    "dim_station": (
        "SELECT station_complex_id, arg_max(station_complex, transit_timestamp) AS station_complex, "
        "arg_max(borough, transit_timestamp) AS borough, arg_max(transit_mode, transit_timestamp) AS transit_mode, "
        "arg_max(latitude, transit_timestamp) AS latitude, arg_max(longitude, transit_timestamp) AS longitude "
        "FROM {src} GROUP BY ALL ORDER BY ALL"),
    "dim_date": (
        "WITH span AS (SELECT min(CAST(transit_timestamp AS DATE)) AS a, max(CAST(transit_timestamp AS DATE)) AS b FROM {src}) "
        "SELECT CAST(d AS DATE) AS date, year(d) AS year, month(d) AS month, day(d) AS day, "
        "isodow(d) AS day_of_week, isodow(d) >= 6 AS is_weekend "
        "FROM span, generate_series(span.a, span.b, INTERVAL 1 DAY) AS g(d) ORDER BY 1"),
    # Hours without any record count as zero: divide by the number of days of each day type.
    "station_hour_profile": (
        "WITH h AS (SELECT station_complex_id, transit_timestamp AS ts, sum(ridership) AS r FROM {src} GROUP BY ALL), "
        "typed AS (SELECT *, CASE WHEN isodow(ts) >= 6 THEN 'weekend' ELSE 'weekday' END AS day_type FROM h), "
        "days AS (SELECT day_type, count(DISTINCT CAST(ts AS DATE)) AS n FROM typed GROUP BY ALL) "
        "SELECT station_complex_id, day_type, hour(ts) AS hour_of_day, sum(r) / any_value(n) AS mean_ridership "
        "FROM typed JOIN days USING (day_type) GROUP BY station_complex_id, day_type, hour_of_day ORDER BY ALL"),
}


class QualityError(RuntimeError):
    pass


@dataclass
class BuiltMonth:
    month: dt.date
    rows: int
    source_updated_at: dt.datetime
    tags: dict[str, int]


def _station_modes(con: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    """Dominant transit_mode of every station complex over all bronze data."""
    con.execute(
        "CREATE OR REPLACE TABLE station_mode AS SELECT station_complex_id, arg_max(transit_mode, n) AS station_mode "
        f"FROM (SELECT station_complex_id, transit_mode, count(*) AS n FROM read_parquet({lake.sql_list(files)}) "
        "GROUP BY ALL) GROUP BY station_complex_id"
    )


def _bronze_view(con: duckdb.DuckDBPyConnection, files: list[str]) -> list[dt.datetime]:
    con.execute(f"CREATE OR REPLACE VIEW bronze_m AS SELECT * FROM read_parquet({lake.sql_list(files)})")
    first, last = con.execute("SELECT min(transit_timestamp), max(transit_timestamp) FROM bronze_m").fetchone()
    return quality.dst_transition_hours(first, last)


def run(settings: Settings, con: duckdb.DuckDBPyConnection, days: list[dt.date], months: set[dt.date]) -> list[BuiltMonth]:
    """Rebuild silver and gold for `months` from the bronze `days`; full marts are rebuilt afterwards."""
    by_month: dict[dt.date, list[dt.date]] = defaultdict(list)
    for day in days:
        by_month[day.replace(day=1)].append(day)
    todo = sorted(m for m in months if m in by_month)
    if not todo:
        return []
    _station_modes(con, [lake.bronze_day(settings, d) for d in days])

    # Pass 1: check every month before writing anything, so a bad month leaves silver untouched.
    tags_by_month = {}
    for month in todo:
        dst = _bronze_view(con, [lake.bronze_day(settings, d) for d in by_month[month]])
        tags = dict(con.execute(quality.tag_counts_sql("bronze_m", dst)).fetchall())
        blocking = {t: n for t, n in tags.items() if t in quality.BLOCKING and n}
        if blocking:
            raise QualityError(f"{month:%Y-%m}: blocking rule violations {blocking}")
        tags_by_month[month] = tags
        log.info("quality %s: %s", f"{month:%Y-%m}", tags or "no violations")

    # Pass 2: silver and monthly gold.
    built = []
    for month in todo:
        dst = _bronze_view(con, [lake.bronze_day(settings, d) for d in by_month[month]])
        silver = lake.silver_month(settings, month)
        lake.write_parquet(con, settings, f"SELECT * FROM ({quality.clean_sql('bronze_m', dst)}) ORDER BY {', '.join(SORT_KEYS)}", silver)
        rows, updated = con.execute("SELECT count(*), max(source_updated_at) FROM bronze_m").fetchone()
        src = f"read_parquet('{silver}')"
        for mart, sql in MONTHLY_MARTS.items():
            lake.write_parquet(con, settings, sql.format(src=src), lake.gold_month(settings, mart, month))
        built.append(BuiltMonth(month, rows, updated, tags_by_month[month]))
        log.info("silver and gold %s: %s rows", f"{month:%Y-%m}", f"{rows:,}")

    src = f"read_parquet('{lake.silver_glob(settings)}')"
    for mart, sql in FULL_MARTS.items():
        lake.write_parquet(con, settings, sql.format(src=src), lake.gold_file(settings, mart))
    log.info("full marts rebuilt from all silver months")
    return built
