"""Data lake layout and DuckDB access (MinIO through httpfs, or a local directory in tests)."""
from __future__ import annotations

import datetime as dt
import os

import duckdb

from .config import Settings


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
    con.execute(f"SET threads = {settings.duckdb_threads}")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{os.environ.get('DUCKDB_TEMP', '/tmp/duckdb')}'")
    if settings.on_s3:
        con.execute("INSTALL httpfs; LOAD httpfs")
        quote = lambda v: "'" + v.replace("'", "''") + "'"  # CREATE SECRET takes literals, not parameters
        con.execute(
            f"CREATE SECRET lake (TYPE s3, KEY_ID {quote(settings.s3_access_key)}, "
            f"SECRET {quote(settings.s3_secret_key)}, ENDPOINT {quote(settings.s3_endpoint)}, "
            f"URL_STYLE 'path', USE_SSL {str(settings.s3_use_ssl).lower()}, REGION 'us-east-1')"
        )
    return con


def bronze_day(settings: Settings, day: dt.date) -> str:
    return f"{settings.lake}/bronze/ridership/{day:%Y-%m}/{day:%Y-%m-%d}.parquet"


def silver_month(settings: Settings, month: dt.date) -> str:
    return f"{settings.lake}/silver/ridership/year={month:%Y}/month={month:%m}/part-0.parquet"


def silver_glob(settings: Settings) -> str:
    return f"{settings.lake}/silver/ridership/*/*/*.parquet"


def gold_month(settings: Settings, mart: str, month: dt.date) -> str:
    return f"{settings.lake}/gold/{mart}/year={month:%Y}/month={month:%m}/part-0.parquet"


def gold_file(settings: Settings, mart: str) -> str:
    return f"{settings.lake}/gold/{mart}/part-0.parquet"


def write_parquet(con: duckdb.DuckDBPyConnection, settings: Settings, query: str, path: str) -> None:
    """COPY a query result to one Parquet file (zstd), creating local folders when needed."""
    if not settings.on_s3:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT parquet, COMPRESSION zstd)")


def sql_list(paths: list[str]) -> str:
    return "[" + ", ".join(f"'{p}'" for p in paths) + "]"
