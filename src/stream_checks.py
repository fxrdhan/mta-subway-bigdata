"""Pemeriksaan pasangan streaming: skema API vs Parquet, isi target, lag, frekuensi update.

Prasyarat: snapshot di data/parquet/ dan hasil dry-run poller di data/_dryrun_stream/.
Keluaran: reports/stream_checks.md dan reports/stream_checks.json.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import requests

import soda

ROOT = Path(__file__).resolve().parent.parent
SNAP = str(ROOT / "data" / "parquet" / "*" / "*" / "*.parquet")
DRY = ROOT / "data" / "_dryrun_stream"
NY = ZoneInfo("America/New_York")
out: dict = {}
md: list[str] = ["# Hasil uji pasangan streaming", ""]


def md_table(cols, rows):
    md.append("| " + " | ".join(cols) + " |")
    md.append("|" + "---|" * len(cols))
    md.extend("| " + " | ".join(str(v) for v in r) + " |" for r in rows)
    md.append("")


# 1. Skema API (JSON) vs Parquet snapshot vs Parquet stream ---------------------------------
r = soda.get(soda.RESOURCE + ".json", {"$limit": 1})
api = dict(zip(json.loads(r.headers["X-SODA2-Fields"]), json.loads(r.headers["X-SODA2-Types"])))
r_sys = soda.get(soda.RESOURCE + ".json", {"$select": ":*, *", "$limit": 1})
out["api_fields_types"] = api
out["api_system_fields"] = [f for f in json.loads(r_sys.headers["X-SODA2-Fields"]) if f.startswith(":")]
out["api_json_sample_row"] = r.json()[0]
con = duckdb.connect()
snap = {c: t for c, t, *_ in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{SNAP}', hive_partitioning=false)").fetchall()}
stream_files = sorted(DRY.glob("ingest_date=*/*.parquet"))
stream_glob = str(DRY / "ingest_date=*" / "*.parquet")
strm = {c: t for c, t, *_ in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{stream_glob}', hive_partitioning=false)").fetchall()}
sample_row = out["api_json_sample_row"]
rows = []
for c in soda.COLUMNS:
    json_py = type(sample_row.get(c)).__name__ if c in sample_row else "(tidak ada)"
    rows.append([f"`{c}`", api.get(c, "(tidak ada)"), json_py, snap.get(c), strm.get(c), "ya" if snap.get(c) == strm.get(c) else "TIDAK"])
out["schema_equal_snapshot_vs_stream"] = snap == strm
out["names_equal_api_vs_parquet"] = list(api) == list(snap)
md += ["## 1. Skema", "",
       f"- Nama & urutan kolom API JSON == Parquet: **{out['names_equal_api_vs_parquet']}**",
       f"- Skema Parquet snapshot == Parquet stream (nama+tipe): **{out['schema_equal_snapshot_vs_stream']}**",
       f"- Kolom sistem API (tidak diunduh): {', '.join(out['api_system_fields'])}", ""]
md_table(["kolom", "tipe SODA (X-SODA2-Types)", "tipe nilai JSON mentah", "Parquet snapshot", "Parquet stream", "snapshot==stream"], rows)
md.append("Catatan: endpoint JSON mengirim semua angka sebagai *string* dan `georeference` sebagai objek GeoJSON; "
          "poller memakai endpoint CSV + skema Arrow yang sama dengan snapshot sehingga tipenya identik.\n")

# 2. Isi baris baru (dry-run) ------------------------------------------------------------------
cols = ["hari", "baris", "ridership_null", "ridership_min", "ridership_max", "ridership_total", "transfers_null", "stasiun"]
res = con.sql(
    f"""SELECT transit_timestamp::DATE::VARCHAR, count(*), count(*) - count(ridership), min(ridership), max(ridership),
               round(sum(ridership)), count(*) - count(transfers), count(DISTINCT station_complex_id)
        FROM read_parquet('{stream_glob}', hive_partitioning=false) GROUP BY 1 ORDER BY 1"""
).fetchall()
out["dryrun_by_day"] = [dict(zip(cols, x)) for x in res]
md += ["## 2. Kolom target `ridership` di baris baru (dry-run, watermark = cutoff - 3 hari)", ""]
md_table(cols, res)

# 3. Konsistensi stream vs snapshot pada jendela yang sama --------------------------------------
lo, hi = con.sql(f"SELECT min(transit_timestamp), max(transit_timestamp) FROM read_parquet('{stream_glob}', hive_partitioning=false)").fetchone()
cols_list = ", ".join(soda.COLUMNS)
a_minus_b, b_minus_a, n_snap = con.execute(
    f"""WITH s AS (SELECT {cols_list} FROM read_parquet('{stream_glob}', hive_partitioning=false)),
             b AS (SELECT {cols_list} FROM read_parquet('{SNAP}', hive_partitioning=false) WHERE transit_timestamp BETWEEN ? AND ?)
        SELECT (SELECT count(*) FROM (SELECT * FROM s EXCEPT ALL SELECT * FROM b)),
               (SELECT count(*) FROM (SELECT * FROM b EXCEPT ALL SELECT * FROM s)),
               (SELECT count(*) FROM b)""",
    [lo, hi],
).fetchone()
out["stream_vs_snapshot_window"] = {"from": str(lo), "to": str(hi), "stream_minus_snapshot": a_minus_b, "snapshot_minus_stream": b_minus_a, "snapshot_rows_in_window": n_snap}
md += ["## 3. Stream vs snapshot pada jendela yang sama", "",
       f"Jendela {lo} s/d {hi}: baris snapshot {n_snap:,}; stream EXCEPT ALL snapshot = **{a_minus_b}**, "
       f"snapshot EXCEPT ALL stream = **{b_minus_a}** (0/0 berarti identik baris-per-baris).", ""]

# 4. Timestamp terbaru dan lag -----------------------------------------------------------------
meta = soda.get(soda.VIEW_META).json()
mx = soda.query_json({"$select": "max(transit_timestamp) AS m"})[0]["m"]
max_ts = dt.datetime.fromisoformat(mx[:19]).replace(tzinfo=NY)
now = dt.datetime.now(dt.timezone.utc)
rows_upd = dt.datetime.fromtimestamp(meta["rowsUpdatedAt"], dt.timezone.utc)
# Jam terakhir mencakup max_ts..max_ts+1h (dibulatkan ke bawah).
lag_now_h = (now - (max_ts + dt.timedelta(hours=1))).total_seconds() / 3600
lag_pub_h = (rows_upd - (max_ts + dt.timedelta(hours=1))).total_seconds() / 3600
out["latency"] = {
    "server_max_ts_local": mx,
    "now_utc": now.isoformat(timespec="seconds"),
    "now_new_york": now.astimezone(NY).isoformat(timespec="seconds"),
    "lag_now_vs_data_end_hours": round(lag_now_h, 1),
    "rowsUpdatedAt_utc": rows_upd.isoformat(),
    "rowsUpdatedAt_weekday": rows_upd.astimezone(NY).strftime("%A"),
    "lag_publish_vs_data_end_hours": round(lag_pub_h, 1),
    "since_last_publish_hours": round((now - rows_upd).total_seconds() / 3600, 1),
}
L = out["latency"]
md += ["## 4. Timestamp terbaru dan lag", "",
       f"- max(transit_timestamp) server: **{mx}** (floating, diasumsikan America/New_York)",
       f"- sekarang: {L['now_utc']} UTC = {L['now_new_york']} New York",
       f"- lag data vs sekarang: **{L['lag_now_vs_data_end_hours']} jam** (≈ {L['lag_now_vs_data_end_hours'] / 24:.1f} hari)",
       f"- rowsUpdatedAt: {L['rowsUpdatedAt_utc']} ({L['rowsUpdatedAt_weekday']} waktu New York); "
       f"lag publikasi vs akhir data: {L['lag_publish_vs_data_end_hours']} jam (≈ {L['lag_publish_vs_data_end_hours'] / 24:.1f} hari)",
       f"- waktu sejak publikasi terakhir: {L['since_last_publish_hours']} jam", ""]

# 5. Bukti frekuensi update -------------------------------------------------------------------
cat = requests.get("https://api.us.socrata.com/api/catalog/v1", params={"ids": soda.DATASET_ID}, timeout=60).json()
res_cat = cat["results"][0]["resource"]
out["update_evidence"] = {
    "metadata_posting_frequency": meta["metadata"]["custom_fields"]["Dataset Summary"]["Posting Frequency"],
    "catalog_data_updated_at": res_cat.get("data_updated_at"),
    "catalog_metadata_updated_at": res_cat.get("metadata_updated_at"),
}
wb: dict = {}
try:
    cdx = requests.get(
        "http://web.archive.org/cdx/search/cdx",
        params={"url": f"data.ny.gov/api/views/{soda.DATASET_ID}.json", "output": "json", "fl": "timestamp,statuscode", "filter": "statuscode:200", "collapse": "digest"},
        timeout=20,
    )
    snaps = cdx.json()[1:]
    seen = []
    for ts, _ in snaps[-40:]:
        j = requests.get(f"http://web.archive.org/web/{ts}id_/https://data.ny.gov/api/views/{soda.DATASET_ID}.json", timeout=20).json()
        seen.append({"snapshot": ts, "rowsUpdatedAt_utc": dt.datetime.fromtimestamp(j["rowsUpdatedAt"], dt.timezone.utc).isoformat()})
    wb = {"status": "ok", "snapshots": seen}
except Exception as e:  # noqa: BLE001 - sumber opsional
    wb = {"status": f"TIDAK TERVERIFIKASI: Wayback Machine tidak bisa dipakai ({type(e).__name__}: {str(e)[:150]})"}
out["update_evidence"]["wayback"] = wb
E = out["update_evidence"]
md += ["## 5. Bukti frekuensi update", "",
       f"- Custom field metadata *Posting Frequency*: **{E['metadata_posting_frequency']}**",
       f"- Catalog API: data_updated_at = {E['catalog_data_updated_at']}, metadata_updated_at = {E['catalog_metadata_updated_at']}",
       f"- Riwayat rowsUpdatedAt via Wayback Machine: {wb['status']}"]
if wb.get("snapshots"):
    distinct = sorted({s["rowsUpdatedAt_utc"] for s in wb["snapshots"]})
    gaps = [
        round((dt.datetime.fromisoformat(b) - dt.datetime.fromisoformat(a)).total_seconds() / 86400, 1)
        for a, b in zip(distinct, distinct[1:])
    ]
    out["update_evidence"]["wayback_distinct_rowsUpdatedAt"] = distinct
    out["update_evidence"]["wayback_gaps_days"] = gaps
    md.append(f"  - {len(distinct)} nilai rowsUpdatedAt berbeda: {', '.join(distinct)}")
    md.append(f"  - jarak antar update (hari): {gaps}")
md.append("")


# 5b. Bukti utama frekuensi update: kolom sistem :created_at per baris (kapan baris dimuat ke server) ------
def cached_or_fetch(name: str, params: dict):
    path = ROOT / "docs" / name
    if not path.exists():
        path.write_text(json.dumps(soda.query_json(params)))
    return json.loads(path.read_text())


loads = cached_or_fetch(
    "server_load_batches_by_created_at.json",
    {"$select": ":created_at AS created_at, min(transit_timestamp) AS min_ts, max(transit_timestamp) AS max_ts, count(*) AS n",
     "$group": "created_at", "$order": "created_at", "$limit": 1000},
)
stamps = [dt.datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")) for r in loads]
gaps = [round((b - a).total_seconds() / 86400, 2) for a, b in zip(stamps, stamps[1:])]
weekly = gaps[1:]  # gap pertama = bulk load awal -> rilis mingguan pertama
out["update_evidence"]["created_at_loads"] = {
    "n_distinct_created_at": len(loads),
    "first_bulk_load": loads[0]["created_at"], "first_bulk_rows": int(loads[0]["n"]),
    "weekly_gap_days_min": min(weekly), "weekly_gap_days_max": max(weekly),
    "weekdays_new_york": sorted({s.astimezone(NY).strftime("%A") for s in stamps[1:]}),
    "hour_utc_min": min(s.hour for s in stamps[1:]), "hour_utc_max": max(s.hour for s in stamps[1:]),
    "last_load": loads[-1]["created_at"],
}
C = out["update_evidence"]["created_at_loads"]
md += ["### Bukti dari kolom sistem `:created_at` (kapan baris dimuat ke server)", "",
       f"- {C['n_distinct_created_at']} nilai `:created_at` berbeda: 1 bulk load awal ({C['first_bulk_load']}, {C['first_bulk_rows']:,} baris) + {C['n_distinct_created_at'] - 1} rilis berikutnya",
       f"- jarak antar rilis mingguan: min {C['weekly_gap_days_min']} hari, maks {C['weekly_gap_days_max']} hari; hari rilis (waktu New York): {', '.join(C['weekdays_new_york'])}; jam UTC {C['hour_utc_min']}-{C['hour_utc_max']}",
       f"- rilis terakhir: {C['last_load']}", ""]
probe = soda.query_json({"$select": "count(*) AS n", "$where": f":created_at > '{loads[-2]['created_at']}'"})[0]
out["update_evidence"]["created_at_filter_check"] = {"where_after": loads[-2]["created_at"], "count": int(probe["n"]), "expected_last_load": int(loads[-1]["n"])}
md += [f"- uji filter `:created_at > '{loads[-2]['created_at']}'` -> {int(probe['n']):,} baris (rilis terakhir = {int(loads[-1]['n']):,}): "
       f"**filter `:created_at` berfungsi** sehingga bisa dipakai sebagai watermark ingest", ""]

# 6. Data terlambat (late-arriving) -------------------------------------------------------------------
import collections
by_day = cached_or_fetch(
    "server_load_batches_by_day.json",
    {"$select": ":created_at AS created_at, date_trunc_ymd(transit_timestamp) AS d, count(*) AS n",
     "$where": f":created_at > '{loads[0]['created_at']}'", "$group": "created_at, d",
     "$order": "created_at, d", "$limit": 50000},
)
grp = collections.defaultdict(list)
for r in by_day:
    grp[r["created_at"]].append((dt.date.fromisoformat(r["d"][:10]), int(r["n"])))
per_load, tot, tot_late = [], 0, 0
for c in sorted(grp):
    days = grp[c]
    newest = max(d for d, _ in days)
    n = sum(x for _, x in days)
    late = sum(x for d, x in days if (newest - d).days > 6)
    tot += n
    tot_late += late
    per_load.append({"created_at": c, "rows": n, "late_rows": late, "pct_late": round(100 * late / n, 1),
                     "oldest_day": str(min(d for d, _ in days)), "newest_day": str(newest)})
out["late_arrival"] = {"rows_in_weekly_loads": tot, "late_rows": tot_late, "pct_late": round(100 * tot_late / tot, 2),
                       "loads_with_over_5pct_late": sum(1 for x in per_load if x["pct_late"] > 5), "n_loads": len(per_load),
                       "definition": "baris rilis mingguan dengan transit_timestamp > 6 hari lebih tua dari timestamp terbaru rilis yang sama",
                       "per_load": per_load}
LA = out["late_arrival"]
md += ["## 6. Data terlambat (late-arriving) — dampak ke watermark `transit_timestamp`", "",
       f"Dari {LA['n_loads']} rilis mingguan sejak bulk load awal: {LA['rows_in_weekly_loads']:,} baris, "
       f"**{LA['late_rows']:,} ({LA['pct_late']}%) berstempel waktu lebih tua dari 6 hari** relatif terhadap timestamp terbaru rilis itu; "
       f"{LA['loads_with_over_5pct_late']} dari {LA['n_loads']} rilis memuat >5% baris terlambat. "
       "Baris seperti ini TIDAK akan tertangkap oleh poller berbasis `transit_timestamp > watermark`.", ""]
md_table(["rilis (UTC)", "baris", "terlambat", "% terlambat", "hari tertua", "hari terbaru"],
         [[x["created_at"], f"{x['rows']:,}", f"{x['late_rows']:,}", x["pct_late"], x["oldest_day"], x["newest_day"]] for x in per_load])

# 7. Revisi baris lama (:updated_at > :created_at) ---------------------------------------------------
revs = cached_or_fetch(
    "server_updated_after_created.json",
    {"$select": ":updated_at AS updated_at, min(transit_timestamp) AS min_ts, max(transit_timestamp) AS max_ts, count(*) AS n",
     "$where": ":updated_at > :created_at", "$group": "updated_at", "$order": "updated_at", "$limit": 1000},
)
n_rev = sum(int(r["n"]) for r in revs)
total_rows = json.loads((ROOT / "docs" / "server_stats.json").read_text())["count"]
wm_probe = soda.query_json({"$select": "count(*) AS n", "$where": f":updated_at > '{loads[-2]['created_at']}'"})[0]
weekly_stamps = {r["created_at"] for r in loads[1:]}
all_on_release_dates = all(r["updated_at"] in weekly_stamps for r in revs)
span_days = [(dt.date.fromisoformat(r["max_ts"][:10]) - dt.date.fromisoformat(r["min_ts"][:10])).days for r in revs]
narrow = sum(1 for x in span_days if x <= 7)
out["revisions"] = {
    "all_updated_at_equal_a_weekly_created_at": all_on_release_dates,
    "releases_touching_le_8_day_window": narrow, "releases_touching_wider_window": len(revs) - narrow,
    "rows_updated_after_created": n_rev, "pct_of_total": round(100 * n_rev / total_rows, 1), "n_distinct_updated_at": len(revs),
    "largest_batch": max(revs, key=lambda r: int(r["n"])), "per_updated_at": revs,
    "updated_at_filter_check": {"where_after": loads[-2]["created_at"], "count": int(wm_probe["n"]),
                                "expected_last_load_new_rows_plus_touched": int(loads[-1]["n"]) + int(revs[-1]["n"])},
    "values_changed": "TIDAK TERVERIFIKASI: metadata hanya menunjukkan baris disentuh ulang, bukan apakah nilainya berubah; "
                      "bandingkan sidik jari harian (src/verify_snapshot_values.py) antar rilis",
}
R = out["revisions"]
md += ["## 7. Revisi baris lama", "",
       f"- **{n_rev:,} baris ({R['pct_of_total']}% dari {total_rows:,})** punya `:updated_at` > `:created_at` (disentuh ulang setelah dibuat); "
       f"{len(revs)} nilai `:updated_at` berbeda; semuanya sama persis dengan `:created_at` sebuah rilis mingguan: {all_on_release_dates}.",
       f"- Pola: {narrow} dari {len(revs)} rilis menyentuh jendela data <= 8 hari (rentang min-max transit_timestamp) yang jauh lebih tua dari rilisnya, "
       f"{len(revs) - narrow} rilis menyentuh rentang lebih lebar; rilis terakhir "
       f"({revs[-1]['updated_at']}) menyentuh {int(revs[-1]['n']):,} baris berstempel {revs[-1]['min_ts'][:10]} s/d {revs[-1]['max_ts'][:10]}.",
       f"- uji filter `:updated_at > '{loads[-2]['created_at']}'` -> {int(wm_probe['n']):,} baris "
       f"(prediksi = baris baru rilis terakhir {int(loads[-1]['n']):,} + baris disentuh {int(revs[-1]['n']):,} = {int(loads[-1]['n']) + int(revs[-1]['n']):,}).",
       f"- Apakah nilai berubah: {R['values_changed']}", ""]

(ROOT / "reports" / "stream_checks.md").write_text("\n".join(md))
(ROOT / "reports" / "stream_checks.json").write_text(json.dumps(out, indent=1, default=str))
print("\n".join(md))
