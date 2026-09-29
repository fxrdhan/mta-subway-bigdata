"""Unduh snapshot batch MTA Subway Hourly Ridership (5wq4-mkjj) ke Parquet.

Alur:
  1. cutoff_ts = MAX(transit_timestamp) di server saat pertama kali dijalankan
     -> data/state/cutoff.json (dipakai ulang di run berikutnya).
  2. Count server per hari (<= cutoff) -> data/state/server_counts_daily.json.
  3. Tiap hari diunduh via SODA CSV (gzip, $order=:id), divalidasi terhadap count
     server hari itu, ditulis ke staging Parquet data/raw/_staging/YYYY-MM/.
  4. Staging satu bulan digabung (DuckDB, ORDER BY kunci) ke
     data/parquet/year=YYYY/month=MM/part-0.parquet (zstd), divalidasi terhadap
     count server bulan itu, manifest ditulis, staging dihapus.
  5. Validasi akhir: total baris Parquet vs count(*) server sampai cutoff.

Idempotent: bulan dengan manifest + jumlah baris cocok dilewati; hari yang sudah
ada di staging dengan jumlah baris cocok juga dilewati.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import shutil
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

import soda

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "data" / "state"
STAGING = ROOT / "data" / "raw" / "_staging"
PARQUET = ROOT / "data" / "parquet"
MANIFESTS = STATE / "months"

log = logging.getLogger("download")


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False))
    tmp.replace(path)


def load_or_create_cutoff() -> dict:
    path = STATE / "cutoff.json"
    if path.exists():
        c = json.loads(path.read_text())
        log.info("cutoff dipakai ulang dari %s: %s", path, c["cutoff_ts"])
        return c
    meta = soda.get(soda.VIEW_META).json()
    row = soda.query_json({"$select": "max(transit_timestamp) AS max_ts, count(*) AS n"})[0]
    cutoff = row["max_ts"][:19]  # '2026-09-16T23:00:00.000' -> tanpa milidetik
    c = {
        "cutoff_ts": cutoff,
        "server_count_total_at_cutoff_time": int(row["n"]),
        "determined_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "rowsUpdatedAt_utc": dt.datetime.fromtimestamp(meta["rowsUpdatedAt"], dt.timezone.utc).isoformat(),
        "rule": "snapshot = transit_timestamp <= cutoff_ts; streaming = transit_timestamp > watermark (awal = cutoff_ts)",
        "timestamp_semantics": "floating_timestamp (tanpa zona); metadata kolom: 'local time' (NYC)",
    }
    write_json(path, c)
    log.info("cutoff baru: %s (count server %s)", cutoff, c["server_count_total_at_cutoff_time"])
    return c


def load_or_fetch_day_counts(cutoff: str) -> dict[str, int]:
    path = STATE / "server_counts_daily.json"
    if path.exists():
        d = json.loads(path.read_text())
        if d["cutoff_ts"] == cutoff:
            return d["counts"]
    rows = soda.query_json(
        {
            "$select": "date_trunc_ymd(transit_timestamp) AS d, count(*) AS n",
            "$where": f"transit_timestamp <= {soda.soql_ts(cutoff)}",
            "$group": "d",
            "$order": "d",
            "$limit": 10000,
        }
    )
    counts = {r["d"][:10]: int(r["n"]) for r in rows}
    write_json(
        path,
        {
            "cutoff_ts": cutoff,
            "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "total": sum(counts.values()),
            "counts": counts,
        },
    )
    log.info("count server harian: %d hari, total %d", len(counts), sum(counts.values()))
    return counts


def parquet_rows(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def month_file(month: str) -> Path:
    y, m = month.split("-")
    return PARQUET / f"year={y}" / f"month={m}" / "part-0.parquet"


def month_done(month: str, expected: int) -> bool:
    mf, pf = MANIFESTS / f"{month}.json", month_file(month)
    if not (mf.exists() and pf.exists()):
        return False
    man = json.loads(mf.read_text())
    return man.get("status") == "complete" and man.get("rows") == expected == parquet_rows(pf)


def fetch_day(day: str, expected: int, cutoff: str) -> tuple[str, int, float]:
    out = STAGING / day[:7] / f"{day}.parquet"
    if out.exists() and parquet_rows(out) == expected:
        return day, expected, 0.0
    start = dt.date.fromisoformat(day)
    end = start + dt.timedelta(days=1)
    where = (
        f"transit_timestamp >= '{start.isoformat()}T00:00:00' AND "
        f"transit_timestamp < '{end.isoformat()}T00:00:00' AND "
        f"transit_timestamp <= {soda.soql_ts(cutoff)}"
    )
    t0 = time.time()
    for attempt in range(3):
        tbl = soda.fetch_csv_table(where)
        if tbl.num_rows == expected:
            break
        log.warning("hari %s: dapat %d baris, server %d (percobaan %d)", day, tbl.num_rows, expected, attempt + 1)
        time.sleep(5 * (attempt + 1))
    else:
        raise RuntimeError(f"hari {day}: jumlah baris tetap tidak cocok ({tbl.num_rows} vs {expected})")
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    pq.write_table(tbl, tmp, compression="zstd")
    tmp.replace(out)
    return day, tbl.num_rows, time.time() - t0


def finalize_month(month: str, expected: int, cutoff: str, t_month: float) -> None:
    src = STAGING / month
    dst = month_file(month)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".parquet.tmp")
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET threads=4; SET preserve_insertion_order=false")
    order = ", ".join(soda.SORT_KEYS)
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src}/*.parquet') ORDER BY {order}) "
        f"TO '{tmp}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 245760)"
    )
    con.close()
    n = parquet_rows(tmp)
    if n != expected:
        tmp.unlink()
        raise RuntimeError(f"bulan {month}: Parquet {n} baris != server {expected}")
    tmp.replace(dst)
    write_json(
        MANIFESTS / f"{month}.json",
        {
            "month": month,
            "status": "complete",
            "rows": n,
            "server_rows": expected,
            "cutoff_ts": cutoff,
            "file": str(dst.relative_to(ROOT)),
            "bytes": dst.stat().st_size,
            "download_seconds": round(time.time() - t_month, 1),
            "finished_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        },
    )
    shutil.rmtree(src)  # hapus data mentah sementara setelah verifikasi
    log.info("BULAN %s selesai: %d baris (== server), %.1f MB, staging dihapus", month, n, dst.stat().st_size / 1e6)


def final_check(cutoff: str) -> dict:
    con = duckdb.connect()
    local = con.execute(
        f"SELECT count(*), min(transit_timestamp), max(transit_timestamp) "
        f"FROM read_parquet('{PARQUET}/*/*/*.parquet', hive_partitioning=false)"
    ).fetchone()
    con.close()
    srv = soda.query_json(
        {"$select": "count(*) AS n", "$where": f"transit_timestamp <= {soda.soql_ts(cutoff)}"}
    )[0]
    res = {
        "checked_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "cutoff_ts": cutoff,
        "parquet_rows": local[0],
        "parquet_min_ts": str(local[1]),
        "parquet_max_ts": str(local[2]),
        "server_rows_le_cutoff_now": int(srv["n"]),
        "match": local[0] == int(srv["n"]),
    }
    write_json(STATE / "final_check.json", res)
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--months", nargs="*", help="batasi ke bulan tertentu, mis. 2025-01 2025-02")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    t_all = time.time()
    cutoff = load_or_create_cutoff()["cutoff_ts"]
    day_counts = load_or_fetch_day_counts(cutoff)
    by_month: dict[str, dict[str, int]] = defaultdict(dict)
    for d, n in day_counts.items():
        by_month[d[:7]][d] = n
    months = sorted(by_month) if not args.months else [m for m in sorted(by_month) if m in args.months]

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for month in months:
            expected = sum(by_month[month].values())
            if month_done(month, expected):
                log.info("BULAN %s sudah lengkap (%d baris) - dilewati", month, expected)
                continue
            t_month = time.time()
            futs = {pool.submit(fetch_day, d, n, cutoff): d for d, n in sorted(by_month[month].items())}
            ok = True
            for f in as_completed(futs):
                try:
                    day, n, secs = f.result()
                    if secs:
                        log.info("hari %s: %d baris, %.1fs", day, n, secs)
                except Exception as e:  # noqa: BLE001 - dicatat, bulan ditandai gagal
                    ok = False
                    failures.append((futs[f], str(e)))
                    log.error("hari %s GAGAL: %s", futs[f], e)
            if ok:
                try:
                    finalize_month(month, expected, cutoff, t_month)
                except Exception as e:  # noqa: BLE001
                    failures.append((month, str(e)))
                    log.error("finalisasi %s GAGAL: %s", month, e)

    log.info("durasi total run: %.1f s", time.time() - t_all)
    if failures:
        log.error("%d kegagalan: %s", len(failures), failures)
        return 1
    if not args.months:
        res = final_check(cutoff)
        log.info("CEK AKHIR: %s", res)
        return 0 if res["match"] else 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
