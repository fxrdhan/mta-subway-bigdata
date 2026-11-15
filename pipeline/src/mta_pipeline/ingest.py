"""Extract and load: copy every changed day from the SODA API to the bronze layer as it is."""
from __future__ import annotations

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import psycopg

from . import db, lake, soda
from .config import Settings

log = logging.getLogger(__name__)


def month_of(day: dt.date) -> dt.date:
    return day.replace(day=1)


def plan(server: dict[dt.date, soda.DayStats], stored: dict[dt.date, soda.DayStats]) -> tuple[list[dt.date], list[dt.date]]:
    """Days whose server fingerprint differs from the stored one, and stored days the server no longer has."""
    changed = sorted(day for day, stats in server.items() if stored.get(day) != stats)
    removed = sorted(stored.keys() - server.keys())
    return changed, removed


def _write_day(con: duckdb.DuckDBPyConnection, settings: Settings, day: dt.date, stats: soda.DayStats) -> tuple[int, dt.datetime]:
    table = soda.fetch_day(day, stats.rows)
    fetched_at = dt.datetime.now(dt.timezone.utc)
    cur = con.cursor()  # one DuckDB cursor per thread
    cur.register("day_rows", table)
    keys = ", ".join(soda.SORT_KEYS)
    lake.write_parquet(
        cur, settings,
        f"SELECT *, TIMESTAMPTZ '{stats.updated_at.isoformat()}' AS source_updated_at, "
        f"TIMESTAMPTZ '{fetched_at.isoformat()}' AS fetched_at FROM day_rows ORDER BY {keys}",
        lake.bronze_day(settings, day),
    )
    cur.close()
    return table.num_rows, fetched_at


def run(settings: Settings, con: duckdb.DuckDBPyConnection, pg: psycopg.Connection,
        limit_days: int | None = None) -> tuple[int, int, set[dt.date]]:
    """Fetch changed days; returns (days fetched, rows fetched, months whose bronze changed)."""
    server = soda.day_stats()
    changed, removed = plan(server, db.stored_days(pg))
    log.info("server has %d days; %d changed, %d removed", len(server), len(changed), len(removed))
    if limit_days is not None:
        changed = changed[-limit_days:] if limit_days > 0 else []  # the most recent days, for tests
        log.info("limited to %d days", len(changed))
    months = {month_of(d) for d in changed} | {month_of(d) for d in removed}
    if removed:
        db.forget_days(pg, removed)

    rows = 0
    with ThreadPoolExecutor(max_workers=settings.workers) as pool:
        futures = {pool.submit(_write_day, con, settings, day, server[day]): day for day in changed}
        for done, future in enumerate(as_completed(futures), 1):
            day = futures[future]
            n, fetched_at = future.result()
            db.save_day(pg, day, server[day], fetched_at)
            rows += n
            if done % 25 == 0 or done == len(futures):
                log.info("bronze: %d/%d days written (%s rows so far)", done, len(futures), f"{rows:,}")
    return len(changed), rows, months
