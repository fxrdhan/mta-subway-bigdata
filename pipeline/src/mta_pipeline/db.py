"""PostgreSQL bookkeeping: schema, ingest fingerprints, silver versions, and run log."""
from __future__ import annotations

import datetime as dt
import uuid
from importlib import resources

import psycopg

from .soda import DayStats


def connect() -> psycopg.Connection:
    """Connection configured by the libpq environment variables (PGHOST, PGUSER, ...).

    Autocommit, so every `with pg.transaction()` block is a real BEGIN ... COMMIT (not a savepoint
    inside an implicit transaction that is never committed).
    """
    return psycopg.connect(application_name="mta-pipeline", autocommit=True)


def apply_schema(pg: psycopg.Connection) -> None:
    sql = resources.files(__package__).joinpath("schema.sql").read_text()
    with pg.transaction():
        pg.execute(sql)


def stored_days(pg: psycopg.Connection) -> dict[dt.date, DayStats]:
    rows = pg.execute("SELECT day, rows, ridership, transfers, source_updated_at FROM ingest_day").fetchall()
    return {day: DayStats(rows=r, ridership=rd, transfers=t, updated_at=u) for day, r, rd, t, u in rows}


def save_day(pg: psycopg.Connection, day: dt.date, stats: DayStats, fetched_at: dt.datetime) -> None:
    pg.execute(
        "INSERT INTO ingest_day VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (day) DO UPDATE SET "
        "rows = EXCLUDED.rows, ridership = EXCLUDED.ridership, transfers = EXCLUDED.transfers, "
        "source_updated_at = EXCLUDED.source_updated_at, fetched_at = EXCLUDED.fetched_at",
        (day, stats.rows, stats.ridership, stats.transfers, stats.updated_at, fetched_at),
    )


def forget_days(pg: psycopg.Connection, days: list[dt.date]) -> None:
    with pg.transaction():
        pg.execute("DELETE FROM ingest_day WHERE day = ANY(%s)", (days,))


def start_run(pg: psycopg.Connection, job: str) -> uuid.UUID:
    run_id = uuid.uuid4()
    with pg.transaction():
        pg.execute("INSERT INTO pipeline_run (run_id, job, started_at, status) VALUES (%s, %s, now(), 'running')",
                   (run_id, job))
    return run_id


def finish_run(pg: psycopg.Connection, run_id: uuid.UUID, status: str, days: int = 0, rows: int = 0,
               months: list[dt.date] | None = None, message: str | None = None) -> None:
    with pg.transaction():
        pg.execute(
            "UPDATE pipeline_run SET finished_at = now(), status = %s, days_fetched = %s, rows_fetched = %s, "
            "months_built = %s, message = %s WHERE run_id = %s",
            (status, days, rows, ",".join(f"{m:%Y-%m}" for m in months or []) or None, message, run_id),
        )
