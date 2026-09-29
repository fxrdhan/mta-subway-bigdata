"""Profil snapshot Parquet dengan DuckDB (langsung di file, tanpa pandas).

Keluaran: reports/profile.md (tabel) dan reports/profile.json (angka mentah).
"""
from __future__ import annotations

import datetime as dt
import json
import resource
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
GLOB = str(ROOT / "data" / "parquet" / "*" / "*" / "*.parquet")
OUT_MD = ROOT / "reports" / "profile.md"
OUT_JSON = ROOT / "reports" / "profile.json"
KEY = ["transit_timestamp", "station_complex_id", "payment_method", "fare_class_category"]
CATEGORICAL = ["transit_mode", "station_complex_id", "station_complex", "borough", "payment_method", "fare_class_category", "georeference"]
# Kotak batas NYC (lima borough) yang longgar.
NYC_BBOX = {"lat_min": 40.49, "lat_max": 40.92, "lon_min": -74.26, "lon_max": -73.68}

con = duckdb.connect()
con.execute(f"SET memory_limit='8GB'; SET threads=8; SET temp_directory='{ROOT / 'data' / 'tmp_duckdb'}'")
con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{GLOB}', hive_partitioning=false)")

md: list[str] = []
js: dict = {}


def q(sql: str):
    rel = con.sql(sql)
    return rel.columns, rel.fetchall()


def fmt(v):
    if isinstance(v, float):
        return f"{v:,.4f}".rstrip("0").rstrip(".") if abs(v) < 1e15 else str(v)
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def table(title: str, sql: str, key: str | None = None, note: str | None = None):
    cols, rows = q(sql)
    md.append(f"### {title}\n")
    if note:
        md.append(note + "\n")
    md.append("| " + " | ".join(cols) + " |")
    md.append("|" + "---|" * len(cols))
    for r in rows:
        md.append("| " + " | ".join(fmt(v).replace("|", "\\|") for v in r) + " |")
    md.append("")
    if key:
        js[key] = [dict(zip(cols, [v if isinstance(v, (int, float, str)) or v is None else str(v) for v in r])) for r in rows]
    return cols, rows


def h2(s: str):
    md.extend([f"## {s}", ""])


t0 = time.time()
md += [
    "# Profil data — MTA Subway Hourly Ridership (snapshot Parquet)",
    "",
    f"Dibuat oleh `src/profile_parquet.py` pada {dt.datetime.now().isoformat(timespec='seconds')} "
    f"(waktu lokal mesin). Semua angka dari kueri DuckDB {duckdb.__version__} di `data/parquet/`.",
    "",
]

# 1. Bentuk & tipe --------------------------------------------------------------
h2("1. Bentuk dan tipe")
n_rows = con.sql("SELECT count(*) FROM t").fetchone()[0]
cols, desc = q("DESCRIBE t")
js["n_rows"], js["n_cols"] = n_rows, len(desc)
js["schema"] = {r[0]: r[1] for r in desc}
md.append(f"- Jumlah baris: **{n_rows:,}**; jumlah kolom: **{len(desc)}**")
files = sorted((ROOT / "data" / "parquet").glob("*/*/*.parquet"))
js["n_files"], js["parquet_bytes"] = len(files), sum(f.stat().st_size for f in files)
md.append(f"- File Parquet: {len(files)}, total {js['parquet_bytes'] / 1e6:,.1f} MB (zstd)\n")
md.append("| kolom | tipe DuckDB |\n|---|---|")
md += [f"| {r[0]} | {r[1]} |" for r in desc]
md.append("")

# 2. Null ---------------------------------------------------------------------
h2("2. Null dan string kosong per kolom")
null_sql = ", ".join(f"count(*) - count({c}) AS {c}" for c, *_ in desc)
cols, rows = q(f"SELECT {null_sql} FROM t")
js["nulls"] = dict(zip(cols, rows[0]))
str_cols = [r[0] for r in desc if r[1] == "VARCHAR"]
cols2, rows2 = q("SELECT " + ", ".join(f"count(*) FILTER (WHERE trim({c}) = '') AS {c}" for c in str_cols) + " FROM t")
js["empty_strings"] = dict(zip(cols2, rows2[0]))
md.append("| kolom | null | string kosong |\n|---|---|---|")
md += [f"| {c} | {js['nulls'][c]:,} | {js['empty_strings'].get(c, '-')} |" for c in cols]
md.append("")

# 3. Keunikan kunci -------------------------------------------------------------
h2("3. Keunikan kunci")
k = ", ".join(KEY)
cols, rows = q(
    f"""WITH g AS (SELECT {k}, count(*) AS c FROM t GROUP BY ALL)
    SELECT count(*) AS n_key_distinct, count(*) FILTER (WHERE c > 1) AS n_key_duplikat,
           coalesce(sum(c - 1) FILTER (WHERE c > 1), 0) AS baris_berlebih, max(c) AS max_per_key FROM g"""
)
js["key_uniqueness"] = dict(zip(cols, rows[0]))
cols_f, rows_f = q("SELECT count(*) AS n_distinct_full_row FROM (SELECT DISTINCT * FROM t)")
js["full_row_distinct"] = rows_f[0][0]
md.append(f"Kunci = ({k}).\n")
md.append("| n_key_distinct | key dengan >1 baris | baris berlebih | max baris per key | baris distinct (semua kolom) | baris duplikat penuh |")
md.append("|---|---|---|---|---|---|")
ku = js["key_uniqueness"]
md.append(
    f"| {ku['n_key_distinct']:,} | {ku['n_key_duplikat']:,} | {ku['baris_berlebih']:,} | {ku['max_per_key']:,} | "
    f"{js['full_row_distinct']:,} | {n_rows - js['full_row_distinct']:,} |"
)
md.append("")
if ku["n_key_duplikat"]:
    table(
        "Contoh key duplikat (10)",
        f"""SELECT {k}, count(*) AS n, list(ridership ORDER BY ridership) AS ridership_values
            FROM t GROUP BY ALL HAVING count(*) > 1 ORDER BY n DESC, transit_timestamp LIMIT 10""",
        "dup_key_examples",
    )
    table(
        "Key duplikat per bulan",
        f"""SELECT strftime(transit_timestamp, '%Y-%m') AS bulan, count(*) AS n_key_duplikat
            FROM (SELECT {k} FROM t GROUP BY ALL HAVING count(*) > 1) GROUP BY 1 ORDER BY 1""",
        "dup_key_by_month",
    )

# 4. Rentang waktu & kelengkapan -----------------------------------------------------
h2("4. Rentang waktu, baris per bulan, jam bolong")
table("Rentang", "SELECT min(transit_timestamp) AS min_ts, max(transit_timestamp) AS max_ts, "
      "count(DISTINCT transit_timestamp) AS n_jam_distinct, count(DISTINCT transit_timestamp::DATE) AS n_hari_distinct FROM t", "time_range")
server_months = {r["m"][:7]: int(r["n"]) for r in json.loads((ROOT / "docs" / "server_counts_by_month.json").read_text())}
cols, rows = q(
    """SELECT strftime(transit_timestamp, '%Y-%m') AS bulan, count(*) AS baris,
              count(DISTINCT transit_timestamp) AS jam_ada,
              count(DISTINCT station_complex_id) AS stasiun,
              count(*) FILTER (WHERE payment_method = 'omny') AS baris_omny,
              count(*) FILTER (WHERE payment_method = 'metrocard') AS baris_metrocard,
              round(sum(ridership)) AS total_ridership
       FROM t GROUP BY 1 ORDER BY 1"""
)
md.append("### Baris per bulan (vs count server dari docs/server_counts_by_month.json)\n")
md.append("| bulan | baris Parquet | baris server | selisih | jam ada | jam seharusnya | stasiun | baris OMNY | baris MetroCard | total ridership |")
md.append("|---|---|---|---|---|---|---|---|---|---|")
js["by_month"] = []
for b, n, hrs, st, om, mc, tot in rows:
    y, m = map(int, b.split("-"))
    start = dt.datetime(y, m, 1)
    end = dt.datetime(y + (m == 12), m % 12 + 1, 1)
    exp_h = int((end - start).total_seconds() // 3600)
    srv = server_months.get(b)
    md.append(f"| {b} | {n:,} | {srv:,} | {n - srv:,} | {hrs:,} | {exp_h:,} | {st} | {om:,} | {mc:,} | {tot:,.0f} |")
    js["by_month"].append(dict(bulan=b, rows=n, server=srv, hours=hrs, hours_expected=exp_h, stations=st, omny=om, metrocard=mc, ridership=tot))
md.append("\n*Jam seharusnya = jam kalender penuh; bulan terakhir terpotong oleh cutoff.*\n")
table(
    "Jam yang hilang di dalam rentang (tidak ada satu baris pun)",
    """WITH r AS (SELECT min(transit_timestamp) a, max(transit_timestamp) b FROM t),
         s AS (SELECT unnest(generate_series(a, b, INTERVAL 1 HOUR)) AS ts FROM r),
         h AS (SELECT DISTINCT transit_timestamp AS ts FROM t)
       SELECT s.ts AS jam_hilang FROM s ANTI JOIN h USING (ts) ORDER BY 1""",
    "missing_hours",
    note="Catatan DST: 2025-03-09 02:00 dan 2026-03-08 02:00 tidak ada di waktu lokal New York.",
)
table(
    "Hari dengan jumlah stasiun < 95% median (indikasi data parsial)",
    """WITH d AS (SELECT transit_timestamp::DATE AS hari, count(DISTINCT station_complex_id) AS stasiun, count(*) AS baris FROM t GROUP BY 1)
       SELECT * FROM d WHERE stasiun < 0.95 * (SELECT median(stasiun) FROM d) ORDER BY hari""",
    "sparse_days",
)
table(
    "Cek DST: total ridership jam 00-04 pada hari Minggu pergantian DST vs Minggu sebelum/sesudahnya",
    """WITH d(tag, hari) AS (VALUES
            ('spring 2025 -7h', DATE '2025-03-02'), ('SPRING 2025 (DST)', DATE '2025-03-09'), ('spring 2025 +7h', DATE '2025-03-16'),
            ('fall 2025 -7h', DATE '2025-10-26'), ('FALL 2025 (DST)', DATE '2025-11-02'), ('fall 2025 +7h', DATE '2025-11-09'),
            ('spring 2026 -7h', DATE '2026-03-01'), ('SPRING 2026 (DST)', DATE '2026-03-08'), ('spring 2026 +7h', DATE '2026-03-15'))
       SELECT d.tag, d.hari AS hari,
              round(sum(ridership) FILTER (WHERE hour(transit_timestamp) = 0)) AS j00,
              round(sum(ridership) FILTER (WHERE hour(transit_timestamp) = 1)) AS j01,
              round(sum(ridership) FILTER (WHERE hour(transit_timestamp) = 2)) AS j02,
              round(sum(ridership) FILTER (WHERE hour(transit_timestamp) = 3)) AS j03,
              round(sum(ridership) FILTER (WHERE hour(transit_timestamp) = 4)) AS j04,
              count(DISTINCT transit_timestamp) AS jam_ada_hari_itu
       FROM d JOIN t ON t.transit_timestamp::DATE = d.hari
       GROUP BY ALL ORDER BY d.hari""",
    "dst_check",
    note="Jika timestamp = jam dinding New York, jam 02 pada hari spring-forward seharusnya tidak ada "
    "dan jam 01 pada hari fall-back seharusnya ~2x lipat (dua jam 01:00 digabung).",
)

# 5. Kategorikal -----------------------------------------------------------------------
h2("5. Kardinalitas dan top values kolom kategorikal")
cols, rows = q("SELECT " + ", ".join(f"count(DISTINCT {c}) AS {c}" for c in CATEGORICAL) + " FROM t")
js["cardinality"] = dict(zip(cols, rows[0]))
md.append("| kolom | kardinalitas |\n|---|---|")
md += [f"| {c} | {v:,} |" for c, v in js["cardinality"].items()]
md.append("")
for c in ["transit_mode", "borough", "payment_method", "fare_class_category"]:
    table(f"Top values `{c}`", f"SELECT {c}, count(*) AS baris, round(100.0 * count(*) / {n_rows}, 3) AS pct, round(sum(ridership)) AS ridership FROM t GROUP BY 1 ORDER BY 2 DESC", f"top_{c}")
table(
    "`fare_class_category` per tahun (baris / ridership)",
    """SELECT fare_class_category,
              count(*) FILTER (WHERE year(transit_timestamp) = 2025) AS baris_2025,
              count(*) FILTER (WHERE year(transit_timestamp) = 2026) AS baris_2026,
              round(sum(ridership) FILTER (WHERE year(transit_timestamp) = 2025)) AS ridership_2025,
              round(sum(ridership) FILTER (WHERE year(transit_timestamp) = 2026)) AS ridership_2026,
              max(transit_timestamp)::VARCHAR AS terakhir_muncul
       FROM t GROUP BY 1 ORDER BY 1""",
    "fare_class_by_year",
)
table("Top 10 `station_complex_id` menurut jumlah baris", "SELECT station_complex_id, any_value(station_complex) AS station_complex, count(*) AS baris FROM t GROUP BY 1 ORDER BY 3 DESC LIMIT 10", "top_station_rows")
table(
    "Konsistensi atribut stasiun (per station_complex_id)",
    """SELECT count(*) AS n_id,
              count(*) FILTER (WHERE n_name > 1) AS id_nama_ganda,
              count(*) FILTER (WHERE n_boro > 1) AS id_borough_ganda,
              count(*) FILTER (WHERE n_ll > 1) AS id_koordinat_ganda,
              count(*) FILTER (WHERE n_mode > 1) AS id_mode_ganda
       FROM (SELECT station_complex_id, count(DISTINCT station_complex) n_name, count(DISTINCT borough) n_boro,
                    count(DISTINCT (latitude, longitude)) n_ll, count(DISTINCT transit_mode) n_mode
             FROM t GROUP BY 1)""",
    "station_consistency",
)
table(
    "station_complex_id dengan >1 transit_mode",
    """SELECT station_complex_id, station_complex, borough, transit_mode, count(*) AS baris,
              min(transit_timestamp)::VARCHAR AS pertama, max(transit_timestamp)::VARCHAR AS terakhir
       FROM t WHERE station_complex_id IN (SELECT station_complex_id FROM t GROUP BY 1 HAVING count(DISTINCT transit_mode) > 1)
       GROUP BY ALL ORDER BY 1, 4""",
    "multi_mode_station",
)
table(
    "Stasiun yang mulai terlambat / berhenti lebih awal (>1 hari dari tepi rentang)",
    """WITH g AS (SELECT min(transit_timestamp) a, max(transit_timestamp) b FROM t)
       SELECT station_complex_id, any_value(station_complex) AS station_complex, min(transit_timestamp)::VARCHAR AS pertama,
              max(transit_timestamp)::VARCHAR AS terakhir, count(DISTINCT transit_timestamp) AS jam_ada, count(*) AS baris
       FROM t, g GROUP BY station_complex_id, g.a, g.b HAVING min(transit_timestamp) > g.a + INTERVAL 1 DAY OR max(transit_timestamp) < g.b - INTERVAL 1 DAY
       ORDER BY 3""",
    "stations_partial_range",
)
table(
    "Kepadatan grid (baris ada vs kombinasi jam x stasiun x fare_class_category yang mungkin)",
    """SELECT count(*) AS baris_ada,
              (SELECT count(DISTINCT transit_timestamp) FROM t) * (SELECT count(DISTINCT station_complex_id) FROM t)
                * (SELECT count(DISTINCT fare_class_category) FROM t) AS kombinasi_teoritis,
              round(100.0 * count(*) / ((SELECT count(DISTINCT transit_timestamp) FROM t) * (SELECT count(DISTINCT station_complex_id) FROM t)
                * (SELECT count(DISTINCT fare_class_category) FROM t)), 2) AS pct_terisi
       FROM t""",
    "grid_density",
    note="ridership minimum = 1 (tidak ada baris bernilai 0): kombinasi tanpa penumpang tidak punya baris (nol implisit).",
)
table(
    "Nama stasiun yang dipakai >1 station_complex_id",
    """SELECT station_complex, count(DISTINCT station_complex_id) AS n_id, list(DISTINCT station_complex_id) AS ids
       FROM t GROUP BY 1 HAVING count(DISTINCT station_complex_id) > 1 ORDER BY 2 DESC LIMIT 20""",
    "names_multi_id",
)
table(
    "Koordinat yang dipakai >1 station_complex_id",
    """SELECT latitude, longitude, count(DISTINCT station_complex_id) AS n_id, list(DISTINCT station_complex) AS stasiun
       FROM t GROUP BY 1, 2 HAVING count(DISTINCT station_complex_id) > 1 ORDER BY 3 DESC LIMIT 20""",
    "coords_multi_id",
)
table(
    "georeference vs latitude/longitude",
    "SELECT count(*) FILTER (WHERE georeference <> printf('POINT (%s %s)', longitude::VARCHAR, latitude::VARCHAR)) AS tidak_cocok_string FROM t",
    "georef_mismatch",
    note="Perbandingan string WKT vs lat/lon; selisih bisa karena format angka, bukan beda titik.",
)

# 6. Numerik -------------------------------------------------------------------------
h2("6. Statistik numerik")
num_rows = []
for c in ["ridership", "transfers"]:
    cols, rows = q(
        f"""SELECT '{c}' AS kolom, min({c}) AS min, max({c}) AS max, avg({c}) AS mean, stddev_samp({c}) AS std,
                  quantile_cont({c}, 0.5) AS median, quantile_cont({c}, 0.95) AS p95, quantile_cont({c}, 0.99) AS p99,
                  count(*) FILTER (WHERE {c} = 0) AS n_nol, count(*) FILTER (WHERE {c} < 0) AS n_negatif,
                  count(*) FILTER (WHERE {c} <> floor({c})) AS n_non_integer, sum({c}) AS total FROM t"""
    )
    num_rows.append(rows[0])
    js[f"stats_{c}"] = dict(zip(cols, rows[0]))
md.append("| " + " | ".join(cols) + " |")
md.append("|" + "---|" * len(cols))
md += ["| " + " | ".join(fmt(v) for v in r) + " |" for r in num_rows]
md.append("")
table("transfers > ridership (seharusnya subset)", "SELECT count(*) AS n FROM t WHERE transfers > ridership", "transfers_gt_ridership")
table("Distribusi transfers negatif", "SELECT transfers, count(*) AS baris FROM t WHERE transfers < 0 GROUP BY 1 ORDER BY 1", "neg_transfers")
table("Contoh baris transfers negatif (10)", "SELECT * EXCLUDE (georeference, latitude, longitude) FROM t WHERE transfers < 0 ORDER BY transfers, transit_timestamp LIMIT 10", "neg_transfer_examples")
table(
    "Rentang koordinat",
    f"""SELECT min(latitude) AS lat_min, max(latitude) AS lat_max, min(longitude) AS lon_min, max(longitude) AS lon_max,
               count(*) FILTER (WHERE NOT (latitude BETWEEN {NYC_BBOX['lat_min']} AND {NYC_BBOX['lat_max']}
                                  AND longitude BETWEEN {NYC_BBOX['lon_min']} AND {NYC_BBOX['lon_max']})) AS baris_di_luar_bbox_nyc
        FROM t""",
    "coord_range",
    note=f"BBox NYC yang dipakai: {NYC_BBOX}.",
)

# 7. Pola ---------------------------------------------------------------------------
h2("7. Pola ridership")
table(
    "Rata-rata total ridership sistem per jam (0-23)",
    """WITH h AS (SELECT transit_timestamp, sum(ridership) AS tot FROM t GROUP BY 1)
       SELECT hour(transit_timestamp) AS jam, round(avg(tot)) AS rata2_total_per_jam, count(*) AS n_jam
       FROM h GROUP BY 1 ORDER BY 1""",
    "hourly_pattern",
    note="Total ridership seluruh sistem per timestamp, lalu dirata-ratakan per jam-of-day.",
)
table(
    "Rata-rata total ridership harian per hari dalam seminggu",
    """WITH d AS (SELECT transit_timestamp::DATE AS hari, sum(ridership) AS tot FROM t GROUP BY 1)
       SELECT isodow(hari) AS isodow, dayname(hari) AS hari_nama, round(avg(tot)) AS rata2_total_harian, count(*) AS n_hari
       FROM d GROUP BY 1, 2 ORDER BY 1""",
    "dow_pattern",
    note="Hari parsial di ujung rentang ikut terhitung (lihat bagian 4).",
)
table(
    "Top 10 stasiun menurut total ridership",
    """SELECT station_complex_id, any_value(station_complex) AS station_complex, any_value(borough) AS borough,
              round(sum(ridership)) AS total_ridership, round(100.0 * sum(ridership) / (SELECT sum(ridership) FROM t), 2) AS pct
       FROM t GROUP BY 1 ORDER BY 4 DESC LIMIT 10""",
    "top10_stations",
)

js["profile_seconds"] = round(time.time() - t0, 1)
js["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # macOS: byte
md.append(f"---\nDurasi profil: {js['profile_seconds']} s; RSS puncak proses: {js['peak_rss_bytes'] / 1e9:.2f} GB.\n")
OUT_MD.parent.mkdir(exist_ok=True)
OUT_MD.write_text("\n".join(md))
OUT_JSON.write_text(json.dumps(js, indent=1, default=str))
print(f"OK {n_rows:,} baris, {js['profile_seconds']} s, peak RSS {js['peak_rss_bytes'] / 1e9:.2f} GB -> {OUT_MD}")
