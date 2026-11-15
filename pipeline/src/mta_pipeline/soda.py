"""Minimal SODA client for the MTA Subway Hourly Ridership dataset on data.ny.gov."""
from __future__ import annotations

import datetime as dt
import io
import logging
import os
import random
import threading
import time
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.csv as pacsv
import requests

RESOURCE = "https://data.ny.gov/resource/5wq4-mkjj"

SCHEMA = pa.schema([
    ("transit_timestamp", pa.timestamp("us")),
    ("transit_mode", pa.string()),
    ("station_complex_id", pa.string()),
    ("station_complex", pa.string()),
    ("borough", pa.string()),
    ("payment_method", pa.string()),
    ("fare_class_category", pa.string()),
    ("ridership", pa.float64()),
    ("transfers", pa.float64()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("georeference", pa.string()),
])
COLUMNS = SCHEMA.names
SORT_KEYS = ["transit_timestamp", "station_complex_id", "payment_method", "fare_class_category"]

log = logging.getLogger(__name__)
_local = threading.local()


@dataclass(frozen=True)
class DayStats:
    """Server-side fingerprint of one day: row count, totals, and last revision time (UTC)."""

    rows: int
    ridership: int
    transfers: int
    updated_at: dt.datetime


def _session() -> requests.Session:
    session = getattr(_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "mta-subway-bigdata-pipeline/0.1"
        if token := os.environ.get("SODA_APP_TOKEN"):
            session.headers["X-App-Token"] = token
        _local.session = session
    return session


def get(url: str, params: dict | None = None, retries: int = 8) -> requests.Response:
    """GET with exponential backoff for 429, 5xx, and network errors; other 4xx fail at once."""
    for attempt in range(retries + 1):
        try:
            response = _session().get(url, params=params, timeout=(15, 600))
            if response.status_code == 200:
                _ = response.content  # read the body inside the try block
                return response
            if response.status_code != 429 and response.status_code < 500:
                raise ValueError(f"HTTP {response.status_code}: {response.text[:300]}")
            reason = f"HTTP {response.status_code}"
        except requests.RequestException as error:
            reason = type(error).__name__
        if attempt == retries:
            raise RuntimeError(f"{url} failed after {retries + 1} attempts: {reason}")
        delay = min(2**attempt, 120) + random.uniform(0, 1)
        log.warning("attempt %d failed (%s); retrying in %.0f s", attempt + 1, reason, delay)
        time.sleep(delay)
    raise AssertionError("unreachable")


def day_stats() -> dict[dt.date, DayStats]:
    """Row count, totals, and latest :updated_at of every day on the server."""
    params = {
        "$select": "date_trunc_ymd(transit_timestamp) AS ymd, count(*) AS n, sum(ridership) AS r, "
                   "sum(transfers) AS t, max(:updated_at) AS u",
        "$group": "ymd",
        "$order": "ymd",
        "$limit": 10000,
    }
    return {
        dt.date.fromisoformat(row["ymd"][:10]): DayStats(
            rows=int(row["n"]),
            ridership=round(float(row["r"])),
            transfers=round(float(row["t"])),
            updated_at=_utc(row["u"]),
        )
        for row in get(RESOURCE + ".json", params).json()
    }


def _utc(text: str) -> dt.datetime:
    """Parse a SODA timestamp; :updated_at is UTC even when the text carries no offset."""
    value = dt.datetime.fromisoformat(text)
    return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)


def _csv_to_table(content: bytes) -> pa.Table:
    if not content.strip():
        return SCHEMA.empty_table()
    options = pacsv.ConvertOptions(column_types=SCHEMA, include_columns=COLUMNS, strings_can_be_null=True)
    return pacsv.read_csv(io.BytesIO(content), convert_options=options).select(COLUMNS).cast(SCHEMA)


def fetch_day(day: dt.date, expected_rows: int, page_size: int = 500_000) -> pa.Table:
    """All rows of one day, paged in :id order; retried until the count matches the server."""
    where = f"transit_timestamp >= '{day}T00:00:00' AND transit_timestamp < '{day + dt.timedelta(days=1)}T00:00:00'"
    for attempt in range(3):
        pages, offset = [], 0
        while True:
            params = {"$where": where, "$order": ":id", "$limit": page_size, "$offset": offset}
            pages.append(_csv_to_table(get(RESOURCE + ".csv", params).content))
            if pages[-1].num_rows < page_size:
                break
            offset += page_size
        table = pa.concat_tables(pages)
        if table.num_rows == expected_rows:
            return table
        log.warning("%s: got %d rows, server has %d (attempt %d)", day, table.num_rows, expected_rows, attempt + 1)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{day}: row count still differs from the server")
