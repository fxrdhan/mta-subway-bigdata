"""Poller "streaming" (micro-batch) untuk baris baru MTA Subway Hourly Ridership.

Satu siklus:
  1. Baca watermark dari file state (awal = cutoff_ts snapshot di data/state/cutoff.json,
     atau --init-watermark).
  2. Cek murah: count(*) dan max(transit_timestamp) di server untuk ts > watermark.
  3. Jika ada: ambil semua baris ts > watermark via SODA CSV (gzip), urutan deterministik
     (transit_timestamp, :id), dipaging.
  4. Dedup: buang duplikat baris penuh di dalam batch, lalu buang baris yang sudah ada di
     folder output (anti-join semua kolom) -> aman bila siklus sebelumnya crash setelah
     menulis file tetapi sebelum menyimpan watermark.
  5. Tulis Parquet (skema sama dengan snapshot) ke <out>/ingest_date=YYYY-MM-DD/
     batch_<minTs>_<maxTs>.parquet, lalu majukan watermark = max ts yang ditulis.
  6. Catat siklus ke <state>.log.jsonl.

Contoh:
  python src/poll_new_rows.py                          # satu siklus nyata -> data/stream/
  python src/poll_new_rows.py --interval 3600          # loop tiap jam
  python src/poll_new_rows.py --out-dir /tmp/x --state-file /tmp/x/wm.json \
      --init-watermark 2026-09-13T23:00:00             # dry-run ke folder lain

Batasan: baris yang direvisi/terlambat dengan ts <= watermark tidak tertangkap
(perlu rekonsiliasi berkala per hari terhadap count server).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import soda

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "data" / "stream"
DEFAULT_STATE = ROOT / "data" / "state" / "stream_watermark.json"
CUTOFF_FILE = ROOT / "data" / "state" / "cutoff.json"

log = logging.getLogger("poll")


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def load_watermark(state_file: Path, init: str | None) -> str:
    if state_file.exists():
        return json.loads(state_file.read_text())["watermark"]
    if init:
        return init
    return json.loads(CUTOFF_FILE.read_text())["cutoff_ts"]


def save_watermark(state_file: Path, wm: str, info: dict) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(".tmp")
    tmp.write_text(json.dumps({"watermark": wm, "updated_at_utc": now_utc(), "last_cycle": info}, indent=2))
    tmp.replace(state_file)


def dedup(tbl: pa.Table, out_dir: Path) -> tuple[pa.Table, int, int]:
    """Buang duplikat penuh di dalam batch dan baris yang sudah ada di out_dir."""
    con = duckdb.connect()
    con.register("batch", tbl)
    cols = ", ".join(soda.COLUMNS)
    uniq = con.execute(f"SELECT DISTINCT {cols} FROM batch").to_arrow_table()
    n_in_batch_dups = tbl.num_rows - uniq.num_rows
    existing = list(out_dir.glob("ingest_date=*/*.parquet"))
    n_existing_dups = 0
    if existing and uniq.num_rows:
        con.register("uniq", uniq)
        lo, hi = con.execute("SELECT min(transit_timestamp), max(transit_timestamp) FROM uniq").fetchone()
        files = ", ".join(f"'{p}'" for p in existing)
        con.execute(
            f"CREATE TEMP TABLE old AS SELECT {cols} FROM read_parquet([{files}], hive_partitioning=false) "
            "WHERE transit_timestamp BETWEEN ? AND ?",
            [lo, hi],
        )
        fresh = con.execute(f"SELECT * FROM uniq EXCEPT ALL SELECT * FROM old").to_arrow_table()
        n_existing_dups = uniq.num_rows - fresh.num_rows
        uniq = fresh
    order = ", ".join(soda.SORT_KEYS)
    con.register("final", uniq)
    out = con.execute(f"SELECT * FROM final ORDER BY {order}").to_arrow_table().cast(soda.SCHEMA)
    con.close()
    return out, n_in_batch_dups, n_existing_dups


def cycle(out_dir: Path, state_file: Path, init: str | None) -> dict:
    t0 = time.time()
    wm = load_watermark(state_file, init)
    meta = soda.get(soda.VIEW_META).json()
    probe = soda.query_json(
        {"$select": "count(*) AS n, max(transit_timestamp) AS max_ts", "$where": f"transit_timestamp > {soda.soql_ts(wm)}"}
    )[0]
    info = {
        "started_at_utc": now_utc(),
        "watermark_before": wm,
        "server_rowsUpdatedAt_utc": dt.datetime.fromtimestamp(meta["rowsUpdatedAt"], dt.timezone.utc).isoformat(),
        "server_new_rows": int(probe["n"]),
        "server_max_ts_after_wm": probe.get("max_ts"),
    }
    if info["server_new_rows"] == 0:
        info.update(rows_fetched=0, rows_written=0, watermark_after=wm, file=None)
    else:
        tbl = soda.fetch_csv_table(f"transit_timestamp > {soda.soql_ts(wm)}", order="transit_timestamp, :id")
        fetched = tbl.num_rows
        # Semua baris yang diambil kini tersimpan (baru ditulis atau sudah ada), jadi
        # watermark boleh maju ke max ts yang diambil walau semuanya duplikat.
        fetched_max = pc.max(tbl.column("transit_timestamp")).as_py() if fetched else None
        tbl, d_batch, d_exist = dedup(tbl, out_dir)
        info.update(rows_fetched=fetched, dup_in_batch=d_batch, dup_already_stored=d_exist)
        if fetched != info["server_new_rows"]:
            log.warning("fetched %d != count server %d (dataset berubah di tengah siklus?)", fetched, info["server_new_rows"])
        if tbl.num_rows:
            lo = tbl.column("transit_timestamp")[0].as_py()
            hi = pc.max(tbl.column("transit_timestamp")).as_py()
            part = out_dir / f"ingest_date={dt.date.today().isoformat()}"
            part.mkdir(parents=True, exist_ok=True)
            f = part / f"batch_{lo:%Y%m%dT%H}_{hi:%Y%m%dT%H}.parquet"
            tmp = f.with_suffix(".tmp")
            pq.write_table(tbl, tmp, compression="zstd")
            tmp.replace(f)
            info.update(rows_written=tbl.num_rows, file=str(f), batch_min_ts=str(lo), batch_max_ts=str(hi))
        else:
            info.update(rows_written=0, file=None)
        info["watermark_after"] = fetched_max.strftime("%Y-%m-%dT%H:%M:%S") if fetched_max else wm
    info["seconds"] = round(time.time() - t0, 1)
    save_watermark(state_file, info["watermark_after"], info)
    with open(state_file.with_suffix(".log.jsonl"), "a") as fh:
        fh.write(json.dumps(info) + "\n")
    return info


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--state-file", type=Path, default=DEFAULT_STATE)
    ap.add_argument("--init-watermark", help="dipakai hanya jika file state belum ada (format 2026-09-16T23:00:00)")
    ap.add_argument("--interval", type=int, default=0, help="detik antar siklus; 0 = satu siklus saja")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    while True:
        info = cycle(args.out_dir, args.state_file, args.init_watermark)
        log.info("siklus: %s", json.dumps(info))
        if not args.interval:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
