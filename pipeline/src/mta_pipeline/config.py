"""Runtime settings, read once from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class Settings:
    lake: str  # "s3://<bucket>" for MinIO, or a local directory for tests
    s3_endpoint: str  # host:port, without scheme
    s3_access_key: str
    s3_secret_key: str
    s3_use_ssl: bool
    duckdb_memory_limit: str
    duckdb_threads: int
    workers: int

    @property
    def on_s3(self) -> bool:
        return self.lake.startswith("s3://")

    @classmethod
    def from_env(cls) -> "Settings":
        lake = os.environ.get("LAKE", "s3://mta").rstrip("/")
        on_s3 = lake.startswith("s3://")
        return cls(
            lake=lake,
            s3_endpoint=_require("S3_ENDPOINT") if on_s3 else "",
            s3_access_key=_require("S3_ACCESS_KEY") if on_s3 else "",
            s3_secret_key=_require("S3_SECRET_KEY") if on_s3 else "",
            s3_use_ssl=os.environ.get("S3_USE_SSL", "false").lower() == "true",
            duckdb_memory_limit=os.environ.get("DUCKDB_MEMORY_LIMIT", "1GB"),
            duckdb_threads=int(os.environ.get("DUCKDB_THREADS", "2")),
            workers=int(os.environ.get("DOWNLOAD_WORKERS", "4")),
        )

# PostgreSQL is configured through the standard libpq variables
# (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD), so no setting is needed here.
