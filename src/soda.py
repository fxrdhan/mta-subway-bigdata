"""Klien SODA minimal untuk dataset MTA Subway Hourly Ridership (data.ny.gov).

- Retry + exponential backoff (dengan jitter) untuk 429/5xx/timeout/koneksi putus.
- Token opsional lewat env var SODA_APP_TOKEN (tidak pernah di-hardcode).
- Satu requests.Session per thread (aman untuk ThreadPoolExecutor).
- Skema Arrow tunggal yang dipakai oleh snapshot batch dan pengambilan inkremental,
  sehingga tipe kolom Parquet snapshot == Parquet inkremental.
"""
from __future__ import annotations

import io
import logging
import os
import random
import threading
import time

import pyarrow as pa
import pyarrow.csv as pacsv
import requests

DOMAIN = "https://data.ny.gov"
DATASET_ID = "5wq4-mkjj"
RESOURCE = f"{DOMAIN}/resource/{DATASET_ID}"
VIEW_META = f"{DOMAIN}/api/views/{DATASET_ID}.json"

# Urutan & tipe sesuai metadata Socrata (calendar_date -> timestamp tanpa zona,
# text -> string, number -> float64, point -> WKT string dari endpoint CSV).
SCHEMA = pa.schema(
    [
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
    ]
)
COLUMNS = SCHEMA.names
SORT_KEYS = ["transit_timestamp", "station_complex_id", "payment_method", "fare_class_category"]

log = logging.getLogger("soda")
_local = threading.local()


class NonRetryableError(RuntimeError):
    pass


def _session() -> requests.Session:
    s = getattr(_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers["Accept-Encoding"] = "gzip"
        s.headers["User-Agent"] = "mta-ridership-coursework/1.0"
        token = os.environ.get("SODA_APP_TOKEN")
        if token:
            s.headers["X-App-Token"] = token
        _local.session = s
    return s


def get(url: str, params: dict | None = None, *, max_retries: int = 8, timeout=(15, 600)) -> requests.Response:
    """GET dengan retry. 4xx selain 429 dianggap fatal (query salah)."""
    for attempt in range(max_retries + 1):
        wait = None
        try:
            r = _session().get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                _ = r.content  # paksa baca body di dalam blok try (ChunkedEncodingError)
                return r
            if r.status_code == 429 or r.status_code >= 500:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else None
                err = f"HTTP {r.status_code}: {r.text[:200]!r}"
            else:
                raise NonRetryableError(f"HTTP {r.status_code} for {r.url}: {r.text[:500]}")
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
            err = f"{type(e).__name__}: {e}"
        if attempt == max_retries:
            raise RuntimeError(f"gagal setelah {max_retries + 1} percobaan: {err}")
        backoff = wait if wait is not None else min(300.0, 2.0 ** attempt) + random.uniform(0, 1.5)
        log.warning("retry %d/%d dalam %.1fs (%s)", attempt + 1, max_retries, backoff, err)
        time.sleep(backoff)
    raise AssertionError("unreachable")


def query_json(params: dict) -> list[dict]:
    return get(RESOURCE + ".json", params).json()


def csv_to_table(content: bytes) -> pa.Table:
    """Parse CSV SODA ke Tabel Arrow dengan skema tetap (validasi header ikut)."""
    if not content.strip():
        return SCHEMA.empty_table()
    tbl = pacsv.read_csv(
        io.BytesIO(content),
        read_options=pacsv.ReadOptions(block_size=1 << 24),
        convert_options=pacsv.ConvertOptions(
            column_types=SCHEMA,
            include_columns=COLUMNS,
            strings_can_be_null=True,
        ),
    )
    extra = set(tbl.column_names) ^ set(COLUMNS)
    if extra:
        raise ValueError(f"kolom CSV tidak sesuai skema: {extra}")
    return tbl.select(COLUMNS).cast(SCHEMA)


def fetch_csv_table(where: str, *, order: str = ":id", page_size: int = 500_000) -> pa.Table:
    """Ambil semua baris yang cocok dengan `where`, dipaging dengan urutan deterministik."""
    parts, offset = [], 0
    while True:
        params = {"$where": where, "$order": order, "$limit": page_size, "$offset": offset}
        tbl = csv_to_table(get(RESOURCE + ".csv", params).content)
        parts.append(tbl)
        if tbl.num_rows < page_size:
            break
        offset += page_size
    return pa.concat_tables(parts) if len(parts) > 1 else parts[0]


def soql_ts(ts: str) -> str:
    """'2026-09-16T23:00:00' -> literal SoQL floating_timestamp."""
    return f"'{ts}'"
