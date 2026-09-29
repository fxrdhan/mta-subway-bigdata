"""Ambil metadata Socrata + statistik server, lalu tulis data dictionary.

Keluaran:
  docs/metadata_raw.json          metadata mentah /api/views/5wq4-mkjj.json
  docs/server_stats.json          count(*), min/max transit_timestamp, header SODA
  docs/server_counts_by_month.json count per bulan (server)
  docs/data_dictionary.md         kamus data (dari metadata + statistik server)
  docs/related_wujg-7c2s_metadata.json  metadata dataset 2020-2024 (TANPA unduh data)
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import soda

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
RELATED_ID = "wujg-7c2s"


def ts(epoch: int | None) -> str:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat() if epoch else "-"


def main() -> None:
    DOCS.mkdir(exist_ok=True)
    meta_resp = soda.get(soda.VIEW_META)
    (DOCS / "metadata_raw.json").write_bytes(meta_resp.content)
    meta = meta_resp.json()

    r = soda.get(
        soda.RESOURCE + ".json",
        {"$select": "count(*) AS n, min(transit_timestamp) AS min_ts, max(transit_timestamp) AS max_ts"},
    )
    row = r.json()[0]
    stats = {
        "queried_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "count": int(row["n"]),
        "min_transit_timestamp": row["min_ts"],
        "max_transit_timestamp": row["max_ts"],
        "soda_headers": {k: v for k, v in r.headers.items() if k.lower().startswith(("x-soda2", "last-modified"))},
        "rowsUpdatedAt_utc": ts(meta.get("rowsUpdatedAt")),
        "viewLastModified_utc": ts(meta.get("viewLastModified")),
        "createdAt_utc": ts(meta.get("createdAt")),
    }
    (DOCS / "server_stats.json").write_text(json.dumps(stats, indent=2))

    months = soda.query_json(
        {
            "$select": "date_trunc_ym(transit_timestamp) AS m, count(*) AS n",
            "$group": "m",
            "$order": "m",
            "$limit": 1000,
        }
    )
    (DOCS / "server_counts_by_month.json").write_text(json.dumps(months, indent=1))

    # Dataset terkait: hanya metadata + kueri agregat (satu baris hasil), TIDAK mengunduh data.
    rel = soda.get(f"{soda.DOMAIN}/api/views/{RELATED_ID}.json")
    (DOCS / f"related_{RELATED_ID}_metadata.json").write_bytes(rel.content)
    rel_res = f"{soda.DOMAIN}/resource/{RELATED_ID}.json"
    rel_count = soda.get(rel_res, {"$select": "count(*) AS n, min(transit_timestamp) AS min_ts, max(transit_timestamp) AS max_ts"})
    (DOCS / f"related_{RELATED_ID}_count.json").write_bytes(rel_count.content)
    rel_fares = soda.get(
        rel_res,
        {
            "$select": "payment_method, fare_class_category, count(*) AS n",
            "$group": "payment_method, fare_class_category",
            "$order": "payment_method, fare_class_category",
        },
    )
    (DOCS / f"related_{RELATED_ID}_fare_classes.json").write_bytes(rel_fares.content)

    write_dictionary(meta, stats)
    print(json.dumps(stats, indent=2))


def write_dictionary(meta: dict, stats: dict) -> None:
    cf = meta.get("metadata", {}).get("custom_fields", {})
    summ = cf.get("Dataset Summary", {})
    lic = meta.get("license") or {}
    L = [
        f"# Data Dictionary — {meta['name']} (`{meta['id']}`)",
        "",
        f"Dibuat otomatis oleh `src/fetch_metadata.py` pada {stats['queried_at_utc']} dari "
        f"`{soda.VIEW_META}` dan SODA `{soda.RESOURCE}.json`.",
        "",
        "## Ringkasan dataset",
        "",
        f"- **Deskripsi**: {meta.get('description', '').strip()}",
        f"- **Pemilik / atribusi**: {meta.get('attribution')} ({meta.get('attributionLink')})",
        f"- **Organisasi**: {summ.get('Organization', '-')}",
        f"- **Kategori / provenance**: {meta.get('category')} / {meta.get('provenance')}",
        f"- **Frekuensi posting (custom field)**: {summ.get('Posting Frequency', '-')}",
        f"- **Granularitas**: {summ.get('Granularity', '-')}",
        f"- **Periode**: {summ.get('Time Period', '-')}; cakupan: {summ.get('Coverage', '-')}",
        f"- **Lisensi**: {lic.get('name') or meta.get('licenseId') or 'TIDAK ADA di field `license`/`licenseId` metadata (null)'}",
        f"- **Dibuat**: {stats['createdAt_utc']}; **rowsUpdatedAt**: {stats['rowsUpdatedAt_utc']}; "
        f"**viewLastModified**: {stats['viewLastModified_utc']}",
        f"- **count(*) server**: {stats['count']:,}; **min/max transit_timestamp**: "
        f"{stats['min_transit_timestamp']} / {stats['max_transit_timestamp']}",
        f"- **Zona waktu timestamp**: tipe SODA `floating_timestamp` (tanpa offset/zona). "
        "Deskripsi kolom menyatakan *local time* dan dibulatkan ke bawah ke jam terdekat; "
        "zona tidak disebut eksplisit, diasumsikan waktu lokal New York (America/New_York) — "
        "konsekuensi: ada jam 'hilang'/'ganda' saat pergantian DST.",
        f"- **Lampiran PDF resmi**: "
        + ", ".join(a["filename"] for a in meta.get("metadata", {}).get("attachments", [])),
        "",
        "## Kolom",
        "",
        "Statistik null/min/max/top berasal dari `cachedContents` metadata Socrata "
        "(dihitung Socrata, bukan oleh kita; field `cardinality` di cache Socrata selalu = count "
        "sehingga tidak informatif dan tidak ditampilkan).",
        "",
        "| # | Kolom (fieldName) | Tipe Socrata | Tipe Parquet | Null | Min | Max | Deskripsi |",
        "|---|---|---|---|---|---|---|---|",
    ]
    arrow_types = {f.name: str(f.type) for f in soda.SCHEMA}
    for c in meta["columns"]:
        cc = c.get("cachedContents", {})
        desc = (c.get("description") or "").replace("\n", " ").replace("|", "\\|")
        L.append(
            f"| {c.get('position')} | `{c['fieldName']}` | {c['dataTypeName']} | "
            f"{arrow_types.get(c['fieldName'], '-')} | {cc.get('null', '-')} | "
            f"{cc.get('smallest', '-')} | {cc.get('largest', '-')} | {desc} |"
        )
    L += ["", "## Top values (cache Socrata)", ""]
    for c in meta["columns"]:
        top = c.get("cachedContents", {}).get("top") or []
        if not top or c["dataTypeName"] == "point":
            continue
        vals = "; ".join(f"`{t['item']}` ({int(t['count']):,})" for t in top[:10])
        L.append(f"- **{c['fieldName']}**: {vals}")
    L += [
        "",
        "## Catatan",
        "",
        "- `transfers` adalah subset dari `ridership` (sudah termasuk di dalamnya).",
        "- `georeference` = titik WKT `POINT (lon lat)` di endpoint CSV / GeoJSON di endpoint JSON; "
        "redundan dengan `latitude`/`longitude`.",
        "- Nilai `fare_class_category` di data memakai ejaan `Metrocard - ...`/`OMNY - ...` dan "
        "memuat kategori (mis. `OMNY - Students`, `OMNY - Fair Fare`) yang tidak ada di daftar "
        "deskripsi kolom — lihat profil untuk daftar aktual.",
    ]
    (DOCS / "data_dictionary.md").write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
