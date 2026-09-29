"""Bandingkan dataset terkait 'MTA Subway Hourly Ridership: 2020-2024' (wujg-7c2s) dengan 5wq4-mkjj.

HANYA metadata + kueri agregat server-side (hasil <= beberapa ratus baris). Tidak mengunduh data baris.
Prasyarat: `python src/fetch_metadata.py` (menulis docs/related_wujg-7c2s_*.json) dan snapshot Parquet.
Keluaran: reports/related_dataset.md, reports/related_dataset.json
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import duckdb

import soda

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
RID = "wujg-7c2s"
RES = f"{soda.DOMAIN}/resource/{RID}.json"
SNAP = str(ROOT / "data" / "parquet" / "*" / "*" / "*.parquet")

new = json.loads((DOCS / "metadata_raw.json").read_text())
old = json.loads((DOCS / f"related_{RID}_metadata.json").read_text())
count = json.loads((DOCS / f"related_{RID}_count.json").read_text())[0]
fares_old = json.loads((DOCS / f"related_{RID}_fare_classes.json").read_text())

out: dict = {"related_id": RID, "name": old["name"]}
md = [f"# Dataset terkait — {old['name']} (`{RID}`)", "",
      "Hanya metadata dan kueri agregat server-side; tidak ada data baris yang diunduh.", ""]

# 1. Metadata ------------------------------------------------------------------------------
cf = old["metadata"]["custom_fields"]["Dataset Summary"]
ts = lambda e: dt.datetime.fromtimestamp(e, dt.timezone.utc).isoformat()  # noqa: E731
out["meta"] = {"rowsUpdatedAt": ts(old["rowsUpdatedAt"]), "viewLastModified": ts(old["viewLastModified"]),
               "createdAt": ts(old["createdAt"]), "posting_frequency": cf.get("Posting Frequency"), "time_period": cf.get("Time Period")}
out["count"] = {"rows": int(count["n"]), "min_ts": count["min_ts"], "max_ts": count["max_ts"]}
md += ["## Metadata dan ukuran", "",
       f"- Jumlah baris (count(*) server): **{int(count['n']):,}**",
       f"- Rentang transit_timestamp: {count['min_ts']} s/d {count['max_ts']}",
       f"- Posting Frequency (custom field): **{cf.get('Posting Frequency')}**; Time Period: {cf.get('Time Period')}",
       f"- rowsUpdatedAt: {out['meta']['rowsUpdatedAt']}; viewLastModified: {out['meta']['viewLastModified']}", ""]

# 2. Skema ---------------------------------------------------------------------------------
def cols(m):
    return {c["fieldName"]: c["dataTypeName"] for c in m["columns"]}


cn, co = cols(new), cols(old)
regular_old = {k: v for k, v in co.items() if not k.startswith(":")}
hidden_old = [k for k in co if k.startswith(":")]
same = regular_old == cn and list(regular_old) == list(cn)
out["schema"] = {"regular_columns_identical_names_types_order": same, "hidden_columns_only_in_old": hidden_old}
md += ["## Skema", "",
       f"- 12 kolom reguler (nama, tipe, urutan) identik dengan 5wq4-mkjj: **{same}**",
       f"- Kolom tersembunyi hanya di dataset lama (bukan bagian output default API): {', '.join(hidden_old) or '-'}", ""]

# 3. Kompatibilitas nilai (agregat server-side) -----------------------------------------------
st = soda.query_json({"$select": "station_complex_id, transit_mode, count(*) AS n", "$group": "station_complex_id, transit_mode", "$limit": 5000})
old_ids = {r["station_complex_id"] for r in st}
con = duckdb.connect()
new_ids = {r[0] for r in con.sql(f"SELECT DISTINCT station_complex_id FROM read_parquet('{SNAP}', hive_partitioning=false)").fetchall()}
new_fares = {r[0] for r in con.sql(f"SELECT DISTINCT fare_class_category FROM read_parquet('{SNAP}', hive_partitioning=false)").fetchall()}
old_fares = {r["fare_class_category"] for r in fares_old}
out["values"] = {
    "stations_old": len(old_ids), "stations_new": len(new_ids), "stations_common": len(old_ids & new_ids),
    "only_in_old": sorted(old_ids - new_ids), "only_in_new": sorted(new_ids - old_ids),
    "fare_classes_old": len(old_fares), "fare_classes_new": len(new_fares),
    "fare_only_in_old": sorted(old_fares - new_fares), "fare_only_in_new": sorted(new_fares - old_fares),
}
V = out["values"]
md += ["## Kompatibilitas nilai kunci", "",
       f"- station_complex_id: lama {V['stations_old']}, baru {V['stations_new']}, irisan {V['stations_common']}; "
       f"hanya di lama: {len(V['only_in_old'])}, hanya di baru: {len(V['only_in_new'])}",
       f"- fare_class_category: lama {V['fare_classes_old']}, baru {V['fare_classes_new']}; "
       f"hanya di lama: {V['fare_only_in_old'] or '-'}; hanya di baru: {V['fare_only_in_new'] or '-'}", ""]

# 4. Sambungan waktu ---------------------------------------------------------------------------
first_new = con.sql(f"SELECT min(transit_timestamp) FROM read_parquet('{SNAP}', hive_partitioning=false)").fetchone()[0]
last_old = dt.datetime.fromisoformat(count["max_ts"][:19])
gap_h = (first_new - last_old).total_seconds() / 3600
out["join"] = {"last_old": str(last_old), "first_new": str(first_new), "gap_hours_between_last_and_first_stamp": gap_h,
               "total_rows_if_combined": int(count["n"]) + con.sql(f"SELECT count(*) FROM read_parquet('{SNAP}', hive_partitioning=false)").fetchone()[0]}
J = out["join"]
md += ["## Sambungan histori", "",
       f"- Timestamp terakhir dataset lama: {J['last_old']}; timestamp pertama 5wq4-mkjj: {J['first_new']} "
       f"(selisih {gap_h:g} jam antar-stempel; 1 jam = berurutan tanpa celah)",
       f"- Total baris jika digabung (lama + snapshot): {J['total_rows_if_combined']:,}", ""]
(ROOT / "reports" / "related_dataset.md").write_text("\n".join(md) + "\n")
(ROOT / "reports" / "related_dataset.json").write_text(json.dumps(out, indent=1, default=str))
print("\n".join(md))
