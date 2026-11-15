# %% [markdown]
# # Tugas Minggu 10 — Implementasi ETL Pipeline
# **Mata kuliah Big Data · Kelompok Smile (TK47G06)**
#
# Notebook ini mengimplementasikan rancangan ETL Minggu 9 dalam dua jalur:
#
# * **Bagian A — jalur batch** untuk dataset **MTA Subway Hourly Ridership** (data.ny.gov, `5wq4-mkjj`), seluruh riwayat;
# * **Bagian B — jalur streaming** untuk feed **GTFS-Realtime** MTA (posisi dan prediksi kedatangan kereta setiap 30 detik)
#   melalui Apache Kafka dan Spark Structured Streaming.
#
# | Tahap (batch) | Implementasi |
# |---|---|
# | Extract | SODA API → berkas Parquet sementara per hari di area *staging* |
# | Transform | DuckDB di area *staging*: inspeksi 12 aturan kualitas, pembersihan, fitur turunan, agregasi |
# | Load | PostgreSQL di klaster laboratorium, skema `etl`, *upsert* per bulan |
#
# Sesuai pola ETL, data mentah **tidak disimpan** di target: berkas *staging* dihapus setelah *load* berhasil,
# dan pesan mentah streaming hanya ditahan sementara di Kafka (retensi 24 jam).
# Notebook dijalankan di klaster laboratorium (tempat PostgreSQL berada). Kredensial dibaca dari variabel
# lingkungan `PGHOST`, `PGUSER`, `PGPASSWORD`, dan seterusnya; tidak ada kredensial yang ditulis di notebook.

# %%
import datetime as dt
import io
import logging
import os
import random
import shutil
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("etl")
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 160)
pd.set_option("display.max_colwidth", 120)

RESOURCE = "https://data.ny.gov/resource/5wq4-mkjj"
STAGING = Path(os.environ.get("ETL_STAGING", "/tmp/etl_staging"))   # area staging sementara
OUTPUT = Path(os.environ.get("ETL_OUTPUT", "output"))               # dataset hasil transformasi (CSV)
WORKERS = int(os.environ.get("ETL_WORKERS", "6"))
DAY_LIMIT = int(os.environ["ETL_DAY_LIMIT"]) if os.environ.get("ETL_DAY_LIMIT") else None  # untuk uji cepat
SCHEMA = "etl"
print(f"staging = {STAGING} | output = {OUTPUT} | workers = {WORKERS} | batas hari = {DAY_LIMIT or 'tanpa batas'}")

# %% [markdown]
# # Bagian A — Jalur Batch
# ## 1. Extract — Data Extraction
# ### 1a. Mengidentifikasi dan mengakses sumber data
# | Hal | Keterangan |
# |---|---|
# | Sumber | MTA Subway Hourly Ridership: Beginning 2025, portal data.ny.gov |
# | Lokasi | `https://data.ny.gov/resource/5wq4-mkjj` (SODA API) |
# | Format | CSV per halaman (maksimal 500.000 baris), diurutkan menurut `:id` |
# | Akses | HTTP GET tanpa autentikasi; *retry* dengan jeda eksponensial untuk HTTP 429/5xx |
# | Pembaruan | Mingguan; setiap pembaruan juga merevisi baris beberapa bulan terakhir |
#
# Agar ekstraksi bisa dibuat inkremental, server lebih dahulu ditanya *fingerprint* tiap hari:
# jumlah baris, total `ridership`, total `transfers`, dan `max(:updated_at)`.

# %%
SCHEMA_SOURCE = pa.schema([
    ("transit_timestamp", pa.timestamp("us")), ("transit_mode", pa.string()), ("station_complex_id", pa.string()),
    ("station_complex", pa.string()), ("borough", pa.string()), ("payment_method", pa.string()),
    ("fare_class_category", pa.string()), ("ridership", pa.float64()), ("transfers", pa.float64()),
    ("latitude", pa.float64()), ("longitude", pa.float64()), ("georeference", pa.string()),
])
COLUMNS = SCHEMA_SOURCE.names
_local = threading.local()


def http_get(url: str, params: dict | None = None, retries: int = 8) -> requests.Response:
    """GET dengan backoff eksponensial untuk 429, 5xx, dan galat jaringan."""
    for attempt in range(retries + 1):
        try:
            session = getattr(_local, "s", None) or requests.Session()
            _local.s = session
            r = session.get(url, params=params, timeout=(15, 600))
            if r.status_code == 200:
                _ = r.content
                return r
            if r.status_code != 429 and r.status_code < 500:
                raise ValueError(f"HTTP {r.status_code}: {r.text[:200]}")
            reason = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            reason = type(e).__name__
        if attempt == retries:
            raise RuntimeError(f"{url} gagal setelah {retries + 1} percobaan: {reason}")
        time.sleep(min(2 ** attempt, 120) + random.random())


def server_fingerprints() -> pd.DataFrame:
    """Fingerprint per hari dari server: jumlah baris, total ridership/transfers, revisi terakhir."""
    params = {"$select": "date_trunc_ymd(transit_timestamp) AS ymd, count(*) AS n, sum(ridership) AS r, "
                         "sum(transfers) AS t, max(:updated_at) AS u", "$group": "ymd", "$order": "ymd", "$limit": 10000}
    rows = http_get(RESOURCE + ".json", params).json()
    return pd.DataFrame({
        "day": [dt.date.fromisoformat(x["ymd"][:10]) for x in rows],
        "rows": [int(x["n"]) for x in rows],
        "ridership": [round(float(x["r"])) for x in rows],
        "transfers": [round(float(x["t"])) for x in rows],
        "updated_at": [pd.Timestamp(x["u"]).tz_localize("UTC") if pd.Timestamp(x["u"]).tzinfo is None else pd.Timestamp(x["u"]) for x in rows],
    })


t0 = time.time()
server = server_fingerprints()
log.info("Sumber dapat diakses: %d hari (%s s.d. %s), %s baris di server (%.0f detik)",
         len(server), server.day.min(), server.day.max(), f"{server.rows.sum():,}", time.time() - t0)
server.tail()

# %% [markdown]
# ### 1b. Koneksi ke target storage dan penentuan hari yang perlu diekstraksi
# Fingerprint yang terakhir dimuat disimpan di tabel `etl.ingest_day`. Hari yang fingerprint-nya berbeda
# dianggap berubah. Karena ETL tidak menyimpan data mentah, agregat satu bulan hanya bisa dibangun ulang
# dari **seluruh hari di bulan itu**, sehingga yang diekstraksi adalah semua hari pada bulan yang berubah.

# %%
DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
CREATE TABLE IF NOT EXISTS {SCHEMA}.ingest_day (
    day date PRIMARY KEY, rows bigint NOT NULL, ridership bigint NOT NULL, transfers bigint NOT NULL,
    source_updated_at timestamptz NOT NULL, loaded_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_station (
    station_complex_id text PRIMARY KEY, station_complex text NOT NULL, borough text NOT NULL,
    transit_mode text NOT NULL, latitude double precision NOT NULL, longitude double precision NOT NULL);
CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_date (
    date date PRIMARY KEY, year smallint NOT NULL, month smallint NOT NULL, day smallint NOT NULL,
    day_of_week smallint NOT NULL, is_weekend boolean NOT NULL);
CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_ridership_hourly (
    hour timestamp NOT NULL, payment_method text NOT NULL, ridership bigint NOT NULL, transfers bigint NOT NULL,
    is_dst_transition boolean NOT NULL, PRIMARY KEY (hour, payment_method));
CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_ridership_daily_station (
    day date NOT NULL, station_complex_id text NOT NULL, ridership bigint NOT NULL, transfers bigint NOT NULL,
    omny_ridership bigint NOT NULL, PRIMARY KEY (day, station_complex_id));
CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_ridership_daily_fare_class (
    day date NOT NULL, fare_class_category text NOT NULL, ridership bigint NOT NULL,
    PRIMARY KEY (day, fare_class_category));
CREATE TABLE IF NOT EXISTS {SCHEMA}.quality_month (
    month date NOT NULL, rule text NOT NULL, rows bigint NOT NULL, PRIMARY KEY (month, rule));
CREATE TABLE IF NOT EXISTS {SCHEMA}.pipeline_run (
    run_id uuid PRIMARY KEY, started_at timestamptz NOT NULL, finished_at timestamptz, status text NOT NULL,
    stage text, days_extracted integer, rows_extracted bigint, months_loaded text, message text);
"""

pg = psycopg.connect(application_name="etl-notebook", autocommit=True)
version = pg.execute("SELECT version()").fetchone()[0]
log.info("Terhubung ke target storage: database %s di %s (%s)", pg.info.dbname, pg.info.host, version.split(",")[0])
pg.execute(DDL)

stored = pd.DataFrame(pg.execute(f"SELECT day, rows, ridership, transfers, source_updated_at FROM {SCHEMA}.ingest_day").fetchall(),
                      columns=["day", "rows", "ridership", "transfers", "updated_at"])


def plan_months(server: pd.DataFrame, stored: pd.DataFrame) -> list[dt.date]:
    """Bulan yang perlu diekstraksi ulang: bulan yang memuat hari baru, hari yang berubah, atau hari yang hilang."""
    a = server.set_index("day")[["rows", "ridership", "transfers", "updated_at"]]
    b = stored.set_index("day")[["rows", "ridership", "transfers", "updated_at"]] if len(stored) else a.iloc[0:0]
    joined = a.merge(b, left_index=True, right_index=True, how="outer", suffixes=("", "_old"))
    changed = joined[(joined["rows"] != joined["rows_old"]) | (joined["ridership"] != joined["ridership_old"]) |
                     (joined["transfers"] != joined["transfers_old"]) | (joined["updated_at"] != joined["updated_at_old"])]
    return sorted({d.replace(day=1) for d in changed.index})


months = plan_months(server, stored)
days_to_extract = [d for d in server.day if d.replace(day=1) in months]
if DAY_LIMIT:
    days_to_extract = days_to_extract[-DAY_LIMIT:]
    months = sorted({d.replace(day=1) for d in days_to_extract})
log.info("%d hari sudah pernah dimuat; perlu ekstraksi %d hari pada %d bulan", len(stored), len(days_to_extract), len(months))

# %% [markdown]
# ### 1c. Melakukan ekstraksi data
# Setiap hari diambil per halaman 500.000 baris dan dicek jumlah barisnya terhadap fingerprint server
# (diulang sampai tiga kali bila berbeda). Hasilnya ditulis sebagai Parquet di area *staging*, satu berkas per hari.

# %%
def csv_to_table(content: bytes) -> pa.Table:
    if not content.strip():
        return SCHEMA_SOURCE.empty_table()
    opts = pacsv.ConvertOptions(column_types=SCHEMA_SOURCE, include_columns=COLUMNS, strings_can_be_null=True)
    return pacsv.read_csv(io.BytesIO(content), convert_options=opts).select(COLUMNS).cast(SCHEMA_SOURCE)


def extract_day(day: dt.date, expected_rows: int) -> int:
    where = f"transit_timestamp >= '{day}T00:00:00' AND transit_timestamp < '{day + dt.timedelta(days=1)}T00:00:00'"
    for attempt in range(3):
        pages, offset = [], 0
        while True:
            params = {"$where": where, "$order": ":id", "$limit": 500_000, "$offset": offset}
            pages.append(csv_to_table(http_get(RESOURCE + ".csv", params).content))
            if pages[-1].num_rows < 500_000:
                break
            offset += 500_000
        table = pa.concat_tables(pages)
        if table.num_rows == expected_rows:
            import pyarrow.parquet as pq
            pq.write_table(table, STAGING / f"{day}.parquet", compression="zstd")
            return table.num_rows
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{day}: jumlah baris tetap berbeda dengan server")


def extract(days: list[dt.date]) -> int:
    log.info("Proses Extract dimulai: %d hari", len(days))
    shutil.rmtree(STAGING, ignore_errors=True)
    STAGING.mkdir(parents=True)
    expected = server.set_index("day")["rows"]
    total = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(extract_day, d, int(expected[d])): d for d in days}
        for i, f in enumerate(as_completed(futures), 1):
            total += f.result()
            if i % 50 == 0 or i == len(futures):
                log.info("  %d/%d hari diekstraksi (%s baris)", i, len(futures), f"{total:,}")
    log.info("Data berhasil diekstraksi: %s baris ke %s", f"{total:,}", STAGING)
    return total


t0 = time.time()
extracted_rows = extract(days_to_extract)
extract_seconds = time.time() - t0
expected_rows = int(server[server.day.isin(days_to_extract)].rows.sum())  # fingerprint server untuk hari yang diekstraksi
staging_mb = sum(f.stat().st_size for f in STAGING.glob("*.parquet")) / 1e6
print(f"Waktu ekstraksi: {extract_seconds / 60:.1f} menit | ukuran berkas staging: {staging_mb:.0f} MB")

# %% [markdown]
# ### 1d. Pemeriksaan awal data
# Pemeriksaan dilakukan dengan DuckDB langsung di atas berkas Parquet *staging* sehingga 47 juta baris
# tidak perlu dimuat seluruhnya ke memori.

# %%
con = duckdb.connect()
con.execute("SET memory_limit = '2GB'; SET threads = 2; SET preserve_insertion_order = false; SET enable_progress_bar = false")
con.execute(f"SET temp_directory = '{STAGING}/.duckdb'")
con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{STAGING}/*.parquet')")

n_rows, n_cols = con.execute("SELECT count(*) FROM raw").fetchone()[0], len(con.execute("DESCRIBE raw").fetchall())
print(f"Jumlah baris: {n_rows:,} | jumlah kolom: {n_cols}")
con.execute("DESCRIBE raw").df()[["column_name", "column_type"]]

# %%
print("Contoh 5 baris data hasil ekstraksi:")
con.execute("SELECT * FROM raw LIMIT 5").df()

# %%
missing = con.execute("SELECT " + ", ".join(f"count(*) - count({c}) AS {c}" for c in COLUMNS) + " FROM raw").df().T
missing.columns = ["nilai_kosong"]
keys = ["transit_timestamp", "station_complex_id", "payment_method", "fare_class_category"]
def duplicates(columns: list[str], table: str = "raw") -> int:
    """Jumlah baris duplikat pada kolom tertentu. Hash 64-bit dihitung lebih dahulu agar hemat memori; hasilnya batas atas,
    sehingga bila tidak nol, baris dengan hash yang sama dibandingkan nilai aslinya."""
    cols = ", ".join(columns)
    n = con.execute(f"SELECT count(*) - count(DISTINCT hash({cols})) FROM {table}").fetchone()[0]
    if n:
        n = con.execute(f"""SELECT coalesce(sum(c - 1), 0) FROM (SELECT count(*) AS c FROM {table}
                             WHERE hash({cols}) IN (SELECT hash({cols}) FROM {table} GROUP BY 1 HAVING count(*) > 1)
                             GROUP BY {cols} HAVING count(*) > 1)""").fetchone()[0]
    return int(n)


dup_rows, dup_keys = duplicates(COLUMNS), duplicates(keys)
print(f"Baris duplikat persis: {dup_rows:,} | duplikat kunci (timestamp, stasiun, metode, kategori tarif): {dup_keys:,}")
missing.T

# %%
summary_before = {
    "baris": n_rows, "kolom": n_cols, "nilai_kosong": int(missing["nilai_kosong"].sum()),
    "duplikat_kunci": dup_keys,
    "tipe ridership/transfers": con.execute("SELECT typeof(any_value(ridership)) FROM raw").fetchone()[0],
    "total ridership": con.execute("SELECT sum(ridership) FROM raw").fetchone()[0],
    "total transfers": con.execute("SELECT sum(transfers) FROM raw").fetchone()[0],
}
pd.Series(summary_before, name="kondisi awal")

# %% [markdown]
# ## 2. Transform — Data Transformation
# ### 2a. Data Inspection
# Dua belas aturan kualitas dari bagian IV notebook proyek diperiksa sekaligus. Kolom *dampak* menjelaskan
# akibatnya bila tidak ditangani.

# %%
NY = ZoneInfo("America/New_York")
first, last = con.execute("SELECT min(transit_timestamp), max(transit_timestamp) FROM raw").fetchone()
dst_hours, h = [], first.replace(minute=0, second=0, microsecond=0)
while h <= last:  # jam lokal yang tidak ada atau terjadi dua kali karena DST
    if h.replace(tzinfo=NY, fold=0).utcoffset() != h.replace(tzinfo=NY, fold=1).utcoffset():
        dst_hours.append(h)
    h += dt.timedelta(hours=1)
DST_SQL = "transit_timestamp IN (" + ", ".join(f"TIMESTAMP '{x:%Y-%m-%d %H:%M:%S}'" for x in dst_hours) + ")" if dst_hours else "FALSE"

con.execute("""CREATE OR REPLACE TABLE station_mode AS
               SELECT station_complex_id, arg_max(transit_mode, n) AS station_mode
               FROM (SELECT station_complex_id, transit_mode, count(*) AS n FROM raw GROUP BY ALL) GROUP BY ALL""")
point = "string_split(trim(georeference, 'POINT ()'), ' ')"
RULES = {
    "missing_value": (" OR ".join(f"{c} IS NULL" for c in COLUMNS), "Baris tidak lengkap tidak bisa diagregasi"),
    "invalid_category": ("transit_mode NOT IN ('subway','staten_island_railway','tram') OR payment_method NOT IN ('omny','metrocard') "
                         "OR borough NOT IN ('Bronx','Brooklyn','Manhattan','Queens','Staten Island')", "Kategori salah membuat grup baru"),
    "nonpositive_ridership": ("ridership <= 0", "Baris tanpa penumpang tidak bermakna"),
    "negative_transfers": ("transfers < 0", "Total transfer menjadi lebih kecil dari seharusnya"),
    "transfers_exceed_ridership": ("transfers > ridership", "Transfer tidak mungkin melebihi penumpang"),
    "non_integer_count": ("ridership <> floor(ridership) OR transfers <> floor(transfers)", "Hitungan orang harus bilangan bulat"),
    "outside_nyc": ("latitude NOT BETWEEN 40.47 AND 40.92 OR longitude NOT BETWEEN -74.26 AND -73.70", "Peta stasiun salah"),
    "not_on_hour": ("date_trunc('hour', transit_timestamp) <> transit_timestamp", "Data per jam tidak sejajar"),
    "fare_class_payment_mismatch": ("lower(split_part(fare_class_category, ' - ', 1)) <> payment_method", "Kategori tarif bertentangan dengan metode"),
    "station_mode_mismatch": ("transit_mode <> station_mode", "Satu stasiun tercatat dengan dua moda"),
    "georeference_mismatch": (f"CAST({point}[1] AS DOUBLE) <> longitude OR CAST({point}[2] AS DOUBLE) <> latitude", "Koordinat ganda tidak konsisten"),
    "dst_transition_hour": (DST_SQL, "Jam peralihan DST bisa berisi dua jam atau kosong"),
}
counts = con.execute("SELECT " + ", ".join(f"count(*) FILTER ({cond}) AS {name}" for name, (cond, _) in RULES.items())
                     + " FROM raw JOIN station_mode USING (station_complex_id)").df().T[0]
inspection = pd.DataFrame({"pelanggaran": counts, "dampak": {k: v[1] for k, v in RULES.items()}})
inspection

# %% [markdown]
# **Permasalahan yang ditemukan dan penanganannya**
#
# | Masalah | Atribut | Penanganan | Alasan |
# |---|---|---|---|
# | Moda tidak konsisten dalam satu kompleks | `transit_mode` | Diganti moda dominan kompleks (*standardization*) | Kompleks St George adalah stasiun Staten Island Railway |
# | `transfers` negatif | `transfers` | Diubah menjadi 0 (*invalid data handling*) | Hitungan orang tidak mungkin negatif; jumlah perubahannya dicatat |
# | Hitungan bertipe desimal | `ridership`, `transfers` | Diubah ke bilangan bulat (*type conversion*) | Tidak ada pecahan (aturan `non_integer_count` = 0) |
# | Jam peralihan DST | `transit_timestamp` | Tidak dihapus, ditandai `is_dst_transition` | Datanya asli; menghapusnya menghilangkan penumpang |
# | Atribut berulang | `georeference` | Dibuang setelah divalidasi (*irrelevant attribute*) | Isinya sama dengan `latitude` dan `longitude` |
#
# Aturan lain (nilai kosong, kategori salah, koordinat di luar NYC, dan seterusnya) bernilai 0 sehingga tidak perlu
# penanganan; pipeline akan berhenti bila aturan tersebut dilanggar pada data baru.

# %%
BLOCKING = ["missing_value", "invalid_category", "nonpositive_ridership", "transfers_exceed_ridership", "non_integer_count",
            "outside_nyc", "not_on_hour", "fare_class_payment_mismatch", "georeference_mismatch"]
blocking = {k: int(counts[k]) for k in BLOCKING if counts[k]}
assert not blocking, f"Pelanggaran yang tidak bisa diperbaiki otomatis: {blocking}"
print("Tidak ada pelanggaran yang menghentikan pipeline.")

# %% [markdown]
# ### 2b–2c. Data Cleaning dan Data Transformation
# Pembersihan dan transformasi dijalankan sebagai satu kueri DuckDB yang menulis data bersih `clean` sebagai berkas
# Parquet di area *staging* (47 juta baris tidak perlu ditampung di memori), lalu agregat dibangun dari berkas tersebut.

# %%
def transform() -> dict[str, int]:
    log.info("Proses Transform dimulai")
    (STAGING / "clean").mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
        SELECT transit_timestamp,
               CAST(transit_timestamp AS DATE) AS date, hour(transit_timestamp) AS hour_of_day,
               isodow(transit_timestamp) AS day_of_week, isodow(transit_timestamp) >= 6 AS is_weekend,
               {DST_SQL} AS is_dst_transition,
               station_mode AS transit_mode, station_complex_id, station_complex, borough,
               payment_method, fare_class_category,
               CAST(ridership AS INTEGER) AS ridership,
               CAST(greatest(transfers, 0) AS INTEGER) AS transfers,
               latitude, longitude
        FROM raw JOIN station_mode USING (station_complex_id)
        ) TO '{STAGING}/clean/clean.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    con.execute(f"CREATE OR REPLACE VIEW clean AS SELECT * FROM read_parquet('{STAGING}/clean/clean.parquet')")
    con.execute("""CREATE OR REPLACE TABLE t_hourly AS
        SELECT transit_timestamp AS hour, payment_method, sum(ridership)::BIGINT AS ridership,
               sum(transfers)::BIGINT AS transfers, bool_or(is_dst_transition) AS is_dst_transition
        FROM clean GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TABLE t_daily_station AS
        SELECT date AS day, station_complex_id, sum(ridership)::BIGINT AS ridership, sum(transfers)::BIGINT AS transfers,
               coalesce(sum(ridership) FILTER (payment_method = 'omny'), 0)::BIGINT AS omny_ridership
        FROM clean GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TABLE t_daily_fare_class AS
        SELECT date AS day, fare_class_category, sum(ridership)::BIGINT AS ridership FROM clean GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TABLE t_dim_station AS
        SELECT station_complex_id, arg_max(station_complex, transit_timestamp) AS station_complex,
               arg_max(borough, transit_timestamp) AS borough, any_value(transit_mode) AS transit_mode,
               arg_max(latitude, transit_timestamp) AS latitude, arg_max(longitude, transit_timestamp) AS longitude
        FROM clean GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TABLE t_dim_date AS
        SELECT DISTINCT date, year(date) AS year, month(date) AS month, day(date) AS day,
               day_of_week, is_weekend FROM clean""")
    sizes = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
             for t in ["clean", "t_hourly", "t_daily_station", "t_daily_fare_class", "t_dim_station", "t_dim_date"]}
    log.info("Data berhasil dibersihkan dan ditransformasi: %s", {k: f"{v:,}" for k, v in sizes.items()})
    return sizes


sizes = transform()
con.execute("DESCRIBE clean").df()[["column_name", "column_type"]].T

# %%
print("Contoh data bersih dengan fitur turunan:")
con.execute("SELECT * FROM clean WHERE is_dst_transition LIMIT 3").df()

# %% [markdown]
# **Transformasi yang diterapkan**
#
# | Transformasi | Hasil | Tujuan |
# |---|---|---|
# | *Feature creation* | `date`, `hour_of_day`, `day_of_week`, `is_weekend`, `is_dst_transition` | Analisis pola jam dan hari, serta fitur model |
# | *Data aggregation* | `fact_ridership_hourly` (jam × metode) | Grafik jam sibuk seluruh jaringan |
# | *Data aggregation* | `fact_ridership_daily_station` (hari × stasiun, termasuk OMNY) | Dashboard per stasiun |
# | *Data aggregation* | `fact_ridership_daily_fare_class` (hari × kategori tarif) | Analisis kategori tarif |
# | *Attribute transformation* | `dim_station`, `dim_date` | Atribut stasiun dan tanggal tidak diulang di setiap baris fakta |

# %% [markdown]
# ### 2d. Data Validation

# %%
validation = {
    "jumlah baris bersih = jumlah baris mentah": sizes["clean"] == n_rows,
    "tidak ada nilai kosong": con.execute("SELECT count(*) FROM clean WHERE " + " OR ".join(
        f"{c} IS NULL" for c in ["transit_timestamp", "station_complex_id", "payment_method", "ridership", "transfers"])).fetchone()[0] == 0,
    "ridership dan transfers bertipe INTEGER": {r[1] for r in con.execute("DESCRIBE clean").fetchall() if r[0] in ("ridership", "transfers")} == {"INTEGER"},
    "tidak ada transfers negatif": con.execute("SELECT count(*) FROM clean WHERE transfers < 0").fetchone()[0] == 0,
    "satu moda per stasiun": con.execute("SELECT max(n) FROM (SELECT count(DISTINCT transit_mode) AS n FROM clean GROUP BY station_complex_id)").fetchone()[0] == 1,
    "total ridership tidak berubah": con.execute("SELECT sum(ridership) FROM clean").fetchone()[0] == summary_before["total ridership"],
    "total ridership agregat jam = data bersih": con.execute("SELECT sum(ridership) FROM t_hourly").fetchone()[0] == con.execute("SELECT sum(ridership) FROM clean").fetchone()[0],
    "total ridership harian stasiun = data bersih": con.execute("SELECT sum(ridership) FROM t_daily_station").fetchone()[0] == con.execute("SELECT sum(ridership) FROM clean").fetchone()[0],
    "kunci agregat unik": con.execute("SELECT count(*) - count(DISTINCT (day, station_complex_id)) FROM t_daily_station").fetchone()[0] == 0,
    "tidak ada duplikat kunci": duplicates(keys, "clean") == 0,
}
pd.Series(validation, name="lulus")

# %% [markdown]
# ### 2e. Perbandingan kondisi data sebelum dan sesudah transformasi

# %%
after_missing = con.execute("SELECT count(*) FILTER (WHERE transit_timestamp IS NULL OR station_complex_id IS NULL OR ridership IS NULL) FROM clean").fetchone()[0]
total_transfers_after = con.execute("SELECT sum(transfers) FROM clean").fetchone()[0]
comparison = pd.DataFrame({
    "sebelum": [f"{n_rows:,}", n_cols, summary_before["nilai_kosong"], dup_keys, int(counts["station_mode_mismatch"]),
                int(counts["negative_transfers"]), summary_before["tipe ridership/transfers"], f"{summary_before['total ridership']:,.0f}",
                f"{summary_before['total transfers']:,.0f}", "georeference ada", "-"],
    "sesudah": [f"{sizes['clean']:,}", len(con.execute("DESCRIBE clean").fetchall()), after_missing, 0, 0, 0, "INTEGER",
                f"{con.execute('SELECT sum(ridership) FROM clean').fetchone()[0]:,}", f"{total_transfers_after:,}",
                "dibuang (sama dengan lat/lon)", "date, hour_of_day, day_of_week, is_weekend, is_dst_transition"],
}, index=["baris", "kolom", "nilai kosong", "duplikat kunci", "moda tidak konsisten", "transfers negatif",
          "tipe ridership/transfers", "total ridership", "total transfers", "atribut berulang", "fitur baru"])
comparison

# %% [markdown]
# Jumlah baris tidak berubah karena tidak ada baris yang dihapus. Total `transfers` naik karena nilai negatif
# diubah menjadi 0; total `ridership` tetap sama persis.

# %% [markdown]
# ## 3. Load — Data Loading
# ### 3a–3b. Koneksi dan struktur penyimpanan
# Target storage: **PostgreSQL 16**, database `mta`, skema `etl` (terpisah dari pipeline ELT Minggu 11).

# %%
structure = pd.DataFrame(pg.execute(
    "SELECT table_name, string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) "
    "FROM information_schema.columns WHERE table_schema = %s GROUP BY table_name ORDER BY table_name", (SCHEMA,)).fetchall(),
    columns=["tabel", "kolom dan tipe"])
structure

# %% [markdown]
# ### 3c. Proses loading
# * **Tabel fakta**: *upsert* per bulan. Bulan yang diekstraksi dihapus lalu dimuat ulang dalam **satu transaksi**,
#   sehingga revisi mingguan MTA menggantikan nilai lama dan pipeline aman dijalankan ulang tanpa duplikasi.
# * **Dimensi**: *upsert* per baris (`INSERT ... ON CONFLICT DO UPDATE`).
# * **Fingerprint** hari yang dimuat dicatat di `etl.ingest_day` untuk run inkremental berikutnya.

# %%
FACTS = {
    "fact_ridership_hourly": ("t_hourly", "hour", ["hour", "payment_method", "ridership", "transfers", "is_dst_transition"]),
    "fact_ridership_daily_station": ("t_daily_station", "day", ["day", "station_complex_id", "ridership", "transfers", "omny_ridership"]),
    "fact_ridership_daily_fare_class": ("t_daily_fare_class", "day", ["day", "fare_class_category", "ridership"]),
}


def copy_rows(cur, table: str, columns: list[str], rows) -> None:
    with cur.copy(f"COPY {SCHEMA}.{table} ({', '.join(columns)}) FROM STDIN") as cp:
        for row in rows:
            cp.write_row(row)


def load(months: list[dt.date]) -> dict[str, int]:
    log.info("Proses Load dimulai: %d bulan", len(months))
    loaded = {}
    for m in months:
        nxt = (m + dt.timedelta(days=32)).replace(day=1)
        with pg.transaction(), pg.cursor() as cur:
            for table, (src, col, cols) in FACTS.items():
                cur.execute(f"DELETE FROM {SCHEMA}.{table} WHERE {col} >= %s AND {col} < %s", (m, nxt))
                rows = con.execute(f"SELECT {', '.join(cols)} FROM {src} WHERE {col} >= ? AND {col} < ?", [m, nxt]).fetchall()
                copy_rows(cur, table, cols, rows)
                loaded[table] = loaded.get(table, 0) + len(rows)
            cur.execute(f"DELETE FROM {SCHEMA}.quality_month WHERE month = %s", (m,))
            month_counts = con.execute("SELECT " + ", ".join(f"count(*) FILTER ({c}) AS {k}" for k, (c, _) in RULES.items())
                                       + " FROM raw JOIN station_mode USING (station_complex_id) "
                                       "WHERE transit_timestamp >= ? AND transit_timestamp < ?", [m, nxt]).df().T[0]
            for rule, n in month_counts.items():
                cur.execute(f"INSERT INTO {SCHEMA}.quality_month VALUES (%s, %s, %s)", (m, rule, int(n)))
    with pg.transaction(), pg.cursor() as cur:
        for table, src, key in [("dim_station", "t_dim_station", "station_complex_id"), ("dim_date", "t_dim_date", "date")]:
            cols = [r[0] for r in con.execute(f"DESCRIBE {src}").fetchall()]
            sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != key)
            rows = con.execute(f"SELECT {', '.join(cols)} FROM {src}").fetchall()
            cur.executemany(f"INSERT INTO {SCHEMA}.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                            f"ON CONFLICT ({key}) DO UPDATE SET {sets}", rows)
            loaded[table] = len(rows)
        fp = server[server.day.isin(days_to_extract)]
        cur.executemany(f"INSERT INTO {SCHEMA}.ingest_day VALUES (%s, %s, %s, %s, %s, now()) ON CONFLICT (day) DO UPDATE SET "
                        "rows = EXCLUDED.rows, ridership = EXCLUDED.ridership, transfers = EXCLUDED.transfers, "
                        "source_updated_at = EXCLUDED.source_updated_at, loaded_at = EXCLUDED.loaded_at",
                        [(r.day, r.rows, r.ridership, r.transfers, r.updated_at.to_pydatetime()) for r in fp.itertuples()])
    log.info("Data berhasil disimpan ke target storage: %s", {k: f"{v:,}" for k, v in loaded.items()})
    return loaded


t0 = time.time()
loaded = load(months)
print(f"Waktu load: {time.time() - t0:.0f} detik")
pd.Series(loaded, name="baris dimuat")

# %% [markdown]
# ### 3d. Memeriksa hasil penyimpanan (membaca kembali dari PostgreSQL)

# %%
stored_counts = {t: pg.execute(f"SELECT count(*) FROM {SCHEMA}.{t}").fetchone()[0]
                 for t in ["fact_ridership_hourly", "fact_ridership_daily_station", "fact_ridership_daily_fare_class",
                           "dim_station", "dim_date", "ingest_day", "quality_month"]}
print(pd.Series(stored_counts, name="baris di PostgreSQL").to_string())
pd.DataFrame(pg.execute(f"""
    SELECT f.day, s.station_complex, s.borough, f.ridership, f.omny_ridership
    FROM {SCHEMA}.fact_ridership_daily_station f JOIN {SCHEMA}.dim_station s USING (station_complex_id)
    ORDER BY f.day DESC, f.ridership DESC LIMIT 5""").fetchall(), columns=["day", "station", "borough", "ridership", "omny"])

# %% [markdown]
# ### 3e. Konsistensi data hasil transformasi dan data di target storage

# %%
def pg_scalar(sql: str, params=()):
    return pg.execute(sql, params).fetchone()[0]


m0, m1 = months[0], (months[-1] + dt.timedelta(days=32)).replace(day=1)
consistency = pd.DataFrame([
    {"tabel": t, "baris hasil transformasi": con.execute(f"SELECT count(*) FROM {src} WHERE {col} >= ? AND {col} < ?", [m0, m1]).fetchone()[0],
     "baris di PostgreSQL": pg_scalar(f"SELECT count(*) FROM {SCHEMA}.{t} WHERE {col} >= %s AND {col} < %s", (m0, m1)),
     "total ridership transformasi": con.execute(f"SELECT sum(ridership) FROM {src} WHERE {col} >= ? AND {col} < ?", [m0, m1]).fetchone()[0],
     "total ridership PostgreSQL": pg_scalar(f"SELECT sum(ridership) FROM {SCHEMA}.{t} WHERE {col} >= %s AND {col} < %s", (m0, m1))}
    for t, (src, col, _) in FACTS.items()])
consistency["sama"] = (consistency["baris hasil transformasi"] == consistency["baris di PostgreSQL"]) & \
                      (consistency["total ridership transformasi"] == consistency["total ridership PostgreSQL"])
consistency

# %%
OUTPUT.mkdir(parents=True, exist_ok=True)
con.execute(f"""COPY (SELECT d.day, d.station_complex_id, s.station_complex, s.borough, d.ridership, d.transfers, d.omny_ridership
                      FROM t_daily_station d JOIN t_dim_station s USING (station_complex_id) ORDER BY d.day, d.station_complex_id)
                TO '{OUTPUT}/transformed_dataset.csv' (HEADER)""")
print(f"Dataset hasil transformasi: {OUTPUT}/transformed_dataset.csv "
      f"({(OUTPUT / 'transformed_dataset.csv').stat().st_size / 1e6:.1f} MB, {sizes['t_daily_station']:,} baris)")

# %% [markdown]
# ## 4. ETL Pipeline Integration
# Seluruh tahapan digabung dalam fungsi `run_etl()`. Transform hanya berjalan bila Extract berhasil, dan Load
# hanya berjalan bila Transform serta validasi berhasil. Setiap run dicatat di `etl.pipeline_run` beserta
# tahap tempat kegagalan terjadi. Area *staging* dihapus setelah Load berhasil (data mentah tidak disimpan).
#
# Run di bawah ini adalah eksekusi kedua dari awal sampai akhir. Karena fingerprint seluruh hari sudah tersimpan,
# hanya bulan yang direvisi MTA sejak run pertama yang diekstraksi ulang.

# %%
def run_etl() -> dict:
    global server, months, days_to_extract
    run_id, stage = uuid.uuid4(), "extract"
    pg.execute(f"INSERT INTO {SCHEMA}.pipeline_run (run_id, started_at, status, stage) VALUES (%s, now(), 'running', %s)", (run_id, stage))
    result = {"run_id": str(run_id), "bulan": [], "baris_extract": 0}
    try:
        server = server_fingerprints()
        done = pd.DataFrame(pg.execute(f"SELECT day, rows, ridership, transfers, source_updated_at FROM {SCHEMA}.ingest_day").fetchall(),
                            columns=["day", "rows", "ridership", "transfers", "updated_at"])
        scope = server[server.day.isin(done.day)] if DAY_LIMIT else server  # mode uji: hanya hari yang pernah dimuat
        months = plan_months(scope, done)
        days_to_extract = [d for d in scope.day if d.replace(day=1) in months]
        if not months:
            log.info("Tidak ada hari yang berubah; tidak ada yang perlu dimuat")
        else:
            result["baris_extract"] = extract(days_to_extract)
            con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{STAGING}/*.parquet')")
            con.execute("""CREATE OR REPLACE TABLE station_mode AS SELECT station_complex_id, arg_max(transit_mode, n) AS station_mode
                           FROM (SELECT station_complex_id, transit_mode, count(*) AS n FROM raw GROUP BY ALL) GROUP BY ALL""")
            stage = "transform"
            sizes_run = transform()
            if sizes_run["clean"] != result["baris_extract"]:
                raise RuntimeError("jumlah baris berubah saat transformasi")
            stage = "load"
            load(months)
            result["bulan"] = [f"{m:%Y-%m}" for m in months]
        shutil.rmtree(STAGING, ignore_errors=True)
        pg.execute(f"UPDATE {SCHEMA}.pipeline_run SET finished_at = now(), status = 'success', stage = 'done', days_extracted = %s, "
                   "rows_extracted = %s, months_loaded = %s WHERE run_id = %s",
                   (len(days_to_extract), result["baris_extract"], ",".join(result["bulan"]) or None, run_id))
        log.info("ETL Pipeline selesai dijalankan")
        return result
    except Exception as e:
        pg.execute(f"UPDATE {SCHEMA}.pipeline_run SET finished_at = now(), status = 'failed', stage = %s, message = %s WHERE run_id = %s",
                   (stage, f"{type(e).__name__}: {e}"[:500], run_id))
        log.error("ETL Pipeline gagal pada tahap %s: %s", stage, e)
        raise


# Run pertama di atas dijalankan bertahap; catat juga di log run.
pg.execute(f"INSERT INTO {SCHEMA}.pipeline_run VALUES (%s, now() - make_interval(secs => %s), now(), 'success', 'done', %s, %s, %s, %s)",
           (uuid.uuid4(), extract_seconds, len(days_to_extract), extracted_rows, ",".join(f"{m:%Y-%m}" for m in months), "run pertama (bertahap di notebook)"))
second = run_etl()
second

# %%
pd.DataFrame(pg.execute(f"SELECT started_at::timestamp(0), finished_at::timestamp(0), status, stage, days_extracted, rows_extracted, "
                        r"coalesce(regexp_replace(months_loaded, '^([^,]+),.*,([^,]+)$', '\1 s.d. \2'), '-'), "
                        f"coalesce(message, 'run_etl()') FROM {SCHEMA}.pipeline_run ORDER BY started_at").fetchall(),
             columns=["mulai", "selesai", "status", "tahap", "hari", "baris", "bulan", "keterangan"])

# %% [markdown]
# ## 5. ETL Pipeline Testing and Validation

# %%
tests = [
    ("Extract", "Akses sumber data", "SODA API dapat diakses", f"{len(server)} hari tersedia", len(server) > 0),
    ("Extract", "Jumlah baris", "Sama dengan fingerprint server", f"{extracted_rows:,} dari {expected_rows:,}", extracted_rows == expected_rows),
    ("Extract", "Atribut", "12 kolom sumber", f"{n_cols} kolom", n_cols == 12),
    ("Transform", "Nilai kosong", "0 setelah transformasi", str(after_missing), after_missing == 0),
    ("Transform", "Duplikasi", "0 duplikat kunci", f"{dup_keys} sebelum, 0 sesudah", validation["tidak ada duplikat kunci"]),
    ("Transform", "Transfers negatif", "Diubah menjadi 0", f"{int(counts['negative_transfers'])} baris diperbaiki", validation["tidak ada transfers negatif"]),
    ("Transform", "Moda stasiun", "Satu moda per stasiun", f"{int(counts['station_mode_mismatch'])} baris diseragamkan", validation["satu moda per stasiun"]),
    ("Transform", "Tipe data", "ridership/transfers INTEGER", "INTEGER", validation["ridership dan transfers bertipe INTEGER"]),
    ("Transform", "Kehilangan data", "Total ridership tetap", f"{summary_before['total ridership']:,.0f}", validation["total ridership tidak berubah"]),
    ("Load", "Koneksi", "Terhubung ke PostgreSQL", version.split(",")[0], True),
    ("Load", "Penyimpanan", "Semua tabel terisi", ", ".join(f"{k}={v:,}" for k, v in stored_counts.items() if k.startswith("fact")), all(stored_counts[k] > 0 for k in FACTS)),
    ("Load", "Pembacaan kembali", "Jumlah dan total sama", f"{int(consistency['sama'].sum())}/{len(consistency)} tabel sama", bool(consistency["sama"].all())),
    ("End-to-End", "Eksekusi pipeline", "Seluruh tahapan berjalan", f"run kedua: {second['bulan'] or 'tidak ada perubahan'}", True),
    ("End-to-End", "Kesesuaian dengan server", "Total harian = fingerprint server", "", None),
]
day_check = pg_scalar(f"""SELECT count(*) FROM {SCHEMA}.ingest_day i LEFT JOIN (SELECT day, sum(ridership) AS r FROM {SCHEMA}.fact_ridership_daily_station GROUP BY day) f
                         USING (day) WHERE f.r IS DISTINCT FROM i.ridership""")
tests[-1] = ("End-to-End", "Kesesuaian dengan server", "Total harian = fingerprint server", f"{day_check} hari berbeda", day_check == 0)
test_table = pd.DataFrame(tests, columns=["Tahapan", "Aspek yang diuji", "Hasil yang diharapkan", "Hasil aktual", "Status"])
test_table["Status"] = test_table["Status"].map({True: "Berhasil", False: "Gagal"})
test_table

# %%
row_flow = pd.DataFrame([
    ("Setelah Extract", f"{extracted_rows:,}", "Baris mentah dari SODA API (berkas staging)"),
    ("Setelah Transform", f"{sizes['clean']:,}", "Baris bersih; tidak ada baris yang dihapus"),
    ("Setelah Transform (agregat)", f"{sizes['t_hourly'] + sizes['t_daily_station'] + sizes['t_daily_fare_class']:,}",
     "Baris tabel fakta hasil agregasi (jam, harian stasiun, harian tarif)"),
    ("Setelah Load", f"{sum(stored_counts[k] for k in FACTS):,}", "Baris tabel fakta yang tersimpan di PostgreSQL"),
], columns=["Tahapan", "Jumlah baris", "Keterangan"])
row_flow

# %% [markdown]
# Perbedaan antara jumlah baris setelah Transform dan setelah Load terjadi karena yang dimuat ke PostgreSQL adalah
# **agregat**, sesuai rancangan ETL Minggu 9 (gudang data hanya berisi data bersih dan agregat). Kesesuaiannya
# dibuktikan dengan total `ridership` yang sama persis antara data bersih, tabel fakta, dan fingerprint server.

# %%
print(f"Jumlah uji: {len(test_table)}, berhasil: {(test_table['Status'] == 'Berhasil').sum()}, gagal: {(test_table['Status'] == 'Gagal').sum()}")

# %% [markdown]
# # Bagian B — Jalur Streaming (GTFS-Realtime)
# Jalur streaming dari rancangan ETL Minggu 9 berjalan terus-menerus di klaster sebagai layanan Kubernetes:
#
# | Tahap | Komponen di klaster | Implementasi |
# |---|---|---|
# | Extract | `mta-gtfs-producer` (Deployment, Python) | Mengambil 8 feed GTFS-RT MTA setiap 30 detik dan mengirimnya ke Kafka |
# | Penyangga | `mta-kafka` (StatefulSet, Apache Kafka 4.3 mode KRaft) | Topik `mta-gtfs-stream`, satu partisi, retensi 24 jam |
# | Transform | `mta-spark-streaming` (Deployment, Spark 4.2 Structured Streaming) | *Micro-batch* 30 detik: decode protobuf, pembersihan, keterlambatan terhadap jadwal statis, agregasi |
# | Load | PostgreSQL skema `etl` dan MongoDB basis data `mta` | Agregat keterlambatan per menit dan per jam; kedatangan kereta dan gangguan layanan |
#
# Bagian ini tidak menjalankan ulang layanan tersebut. Notebook membaca pesan yang sedang mengalir di Kafka,
# menerapkan fungsi transformasi yang **sama** dengan yang dijalankan Spark (modul `spark_etl.py`), lalu
# membandingkan hasilnya dengan data yang sudah dimuat Spark ke PostgreSQL dan MongoDB.

# %%
import inspect

import pymongo
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource
from nyct_gtfs.compiled_gtfs import gtfs_realtime_pb2, nyct_subway_pb2

sys.path.insert(0, str(Path(os.environ.get("STREAMING_CODE", "streaming"))))
import producer   # kode Extract yang dijalankan Deployment mta-gtfs-producer
import spark_etl  # kode Transform dan Load yang dijalankan Deployment mta-spark-streaming

def utc(values) -> pd.Series:
    return pd.to_datetime(pd.Series(values), utc=True).astype("datetime64[us, UTC]")


BOOTSTRAP, TOPIC = os.environ["KAFKA_BOOTSTRAP"], os.environ.get("KAFKA_TOPIC", "mta-gtfs-stream")
mongo = pymongo.MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=15000)["mta"]
print(f"Kafka {BOOTSTRAP}, topik {TOPIC} | MongoDB versi {mongo.client.server_info()['version']}")

# %% [markdown]
# ## B1. Extract — Data Extraction
# ### Sumber data
# | Hal | Keterangan |
# |---|---|
# | Sumber | MTA GTFS-Realtime Subway Feeds (8 feed: 1234567S, ACE, BDFM, G, JZ, NQRW, L, SIR) |
# | Lokasi | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs…` |
# | Format | Protocol Buffers (GTFS-Realtime 2.0 dengan ekstensi NYCT): `trip_update`, `vehicle`, `alert` |
# | Akses | HTTP GET tanpa kunci API; feed diperbarui MTA sekitar setiap 30 detik |
#
# Producer mengambil kedelapan feed secara paralel dan mengirim setiap feed sebagai satu pesan Kafka
# (*key* = nama feed, *value* = bytes protobuf, *header* `fetched_at`, kompresi zstd):

# %%
print(inspect.getsource(producer.fetch))
print(inspect.getsource(producer.main))

# %% [markdown]
# ### Pesan di Kafka
# Notebook membaca pesan yang sudah diproses Spark. Jendela pemeriksaan adalah 320 pesan terakhir (±20 menit,
# 8 feed × 2 putaran per menit) sebelum *offset* terakhir yang tercatat di log *micro-batch* Spark;
# 240 pesan sebelumnya dipakai untuk menyiapkan *state* (snapshot terakhir per feed dan posisi tiap kereta)
# persis seperti yang dimiliki Spark saat memproses jendela tersebut.

# %%
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
topic_meta = admin.list_topics(TOPIC, timeout=20).topics[TOPIC]
resource = ConfigResource("topic", TOPIC)
topic_conf = admin.describe_configs([resource])[resource].result()
consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"etl-notebook-{uuid.uuid4().hex[:8]}",
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})
low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=20)

batch_log = pd.DataFrame(pg.execute(f"""SELECT batch_id, processed_at, messages, skipped_snapshots, offset_from, offset_to,
                                               feed_time_from, feed_time_to, minute_rows, hour_rows, arrivals, alerts, seconds
                                        FROM {SCHEMA}.gtfs_stream_batch ORDER BY batch_id""").fetchall(),
                         columns=["batch_id", "processed_at", "messages", "skipped", "offset_from", "offset_to", "feed_time_from",
                                  "feed_time_to", "minute_rows", "hour_rows", "arrivals", "alerts", "seconds"])
WINDOW, PRIME = 320, 240
hi = int(batch_log.offset_to.max()) + 1              # offset pertama yang belum diproses Spark
first_offset = int(batch_log.offset_from.min())       # offset pertama yang diproses Spark sejak start
lo = max(first_offset, hi - WINDOW)
prime_start = max(first_offset, lo - PRIME)
pd.Series({"partisi": len(topic_meta.partitions), "retensi (jam)": int(topic_conf["retention.ms"].value) / 3.6e6,
           "offset terendah tersimpan": low, "offset tertinggi": high, "pesan tersimpan": high - low,
           "micro-batch Spark tercatat": len(batch_log), "jendela pemeriksaan": f"offset {lo}–{hi - 1}"}, name="Kafka")

# %%
def read_range(start: int, end: int) -> list:
    consumer.assign([TopicPartition(TOPIC, 0, start)])
    out = []
    while len(out) < end - start:
        msg = consumer.poll(15)
        if msg is None:
            break
        if msg.error():
            raise RuntimeError(msg.error())
        if msg.offset() >= end:
            break
        out.append(msg)
    return out


def profile(msg) -> dict:
    """Pemeriksaan satu pesan mentah tanpa transformasi apa pun."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(msg.value())
    ts = feed.header.timestamp
    trips = [e.trip_update for e in feed.entity if e.HasField("trip_update")]
    vehicles = [e.vehicle for e in feed.entity if e.HasField("vehicle")]
    updates = [u for t in trips for u in t.stop_time_update]
    when = lambda u: u.arrival.time or u.departure.time
    return {
        "offset": msg.offset(), "feed": msg.key().decode(), "kb": round(len(msg.value()) / 1024, 1),
        "fetched_at": int(dict(msg.headers())["fetched_at"]), "feed_timestamp": ts,
        "entitas": len(feed.entity), "trip_update": len(trips), "vehicle": len(vehicles),
        "alert": sum(e.HasField("alert") for e in feed.entity), "stop_time_update": len(updates),
        "arah_kosong": sum(not t.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor].HasField("direction") for t in trips),
        "status_kosong": sum(not v.HasField("current_status") for v in vehicles),
        "prediksi_lampau": sum(when(u) < ts - 30 for u in updates),
        "trip_tanpa_prediksi": sum(all(when(u) < ts - 30 for u in t.stop_time_update) for t in trips),
        "trip_id_tidak_baku": sum(not spark_etl.KEY.match(t.trip.trip_id) for t in trips),
    }


t0 = time.time()
messages = read_range(prime_start, hi)
raw_profile = pd.DataFrame(profile(m) for m in messages)
raw_profile["snapshot_berulang"] = raw_profile.groupby("feed")["feed_timestamp"].transform(lambda s: s <= s.cummax().shift(fill_value=0))
raw_profile["menit"] = utc(pd.to_datetime(raw_profile.feed_timestamp - raw_profile.feed_timestamp % 60, unit="s", utc=True))
prime = [(m.key(), m.value()) for m in messages if m.offset() < lo]
window = [(m.key(), m.value()) for m in messages if m.offset() >= lo]
win_profile = raw_profile[raw_profile.offset >= lo].reset_index(drop=True)
log.info("Extract streaming: %d pesan dibaca dari Kafka dalam %.1f detik (%d untuk state, %d jendela)",
         len(messages), time.time() - t0, len(prime), len(window))
print("Delapan pesan terakhir di jendela:")
win_profile.tail(8)[["offset", "feed", "kb", "fetched_at", "feed_timestamp", "entitas", "trip_update", "vehicle", "alert"]]

# %% [markdown]
# ### Pemeriksaan awal data
# Satu pesan berisi satu *snapshot* feed. Contoh isi satu `trip_update` setelah di-decode:

# %%
def trips_of(value: bytes):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(value)
    return [e.trip_update for e in feed.entity if e.HasField("trip_update")]


sample_trip = next(t for _, value in reversed(window) for t in trips_of(value) if len(t.stop_time_update) >= 3)
pd.DataFrame([{"trip_id": sample_trip.trip.trip_id, "route_id": sample_trip.trip.route_id, "start_date": sample_trip.trip.start_date,
               "stop_id": u.stop_id, "arrival": dt.datetime.fromtimestamp(u.arrival.time, NY).strftime("%H:%M:%S") if u.arrival.time else None,
               "departure": dt.datetime.fromtimestamp(u.departure.time, NY).strftime("%H:%M:%S") if u.departure.time else None}
              for u in sample_trip.stop_time_update[:5]])

# %%
issue_cols = ["snapshot_berulang", "arah_kosong", "status_kosong", "prediksi_lampau", "trip_tanpa_prediksi", "trip_id_tidak_baku"]
summary_stream = pd.Series({
    "pesan (snapshot feed)": len(win_profile), "feed berbeda": win_profile.feed.nunique(),
    "ukuran total (MB)": round(win_profile.kb.sum() / 1024, 1),
    "rentang waktu feed": f"{win_profile.menit.min():%H:%M}–{win_profile.menit.max():%H:%M} UTC",
    "entitas": int(win_profile.entitas.sum()), "trip_update": int(win_profile.trip_update.sum()),
    "vehicle": int(win_profile.vehicle.sum()), "alert": int(win_profile.alert.sum()),
    "stop_time_update": int(win_profile.stop_time_update.sum()),
    **{k: int(win_profile[k].sum()) for k in issue_cols},
    "kolom keterlambatan di feed": "tidak ada",
}, name="kondisi awal")
summary_stream

# %% [markdown]
# ## B2. Transform — Data Transformation
# ### Inspeksi dan penanganan
# | Masalah pada data mentah | Penanganan di `spark_etl.py` | Alasan |
# |---|---|---|
# | Snapshot berulang (feed belum diperbarui MTA saat diambil) | Dilewati (*duplicate handling*) | Kereta yang sama tidak boleh terhitung dua kali |
# | Arah kosong pada ekstensi NYCT | Diambil dari `trip_id` (`..S`), selain itu `N` (*missing value*) | Agregat dipisah per arah |
# | Status kendaraan kosong | Diisi `IN_TRANSIT_TO`, nilai bawaan GTFS-RT (*missing value*) | Penghitungan kereta yang berhenti |
# | Prediksi yang sudah lewat | Dibuang; dipakai prediksi stasiun berikutnya (*invalid data*) | Prediksi lampau bukan informasi baru |
# | Feed tidak memiliki kolom keterlambatan | Dihitung: prediksi − jadwal statis GTFS (*feature creation*) | Metrik utama dashboard |
# | Keterlambatan lebih dari 3 jam | Dikosongkan (*outlier*) | Bukan perjalanan yang sama (batas hari layanan) |
# | Trip tidak ada di jadwal statis (pola `trip_id` lain, mis. SIR, atau trip tambahan) | Keterlambatan dibiarkan kosong; observasi tetap dihitung | Tidak ada jadwal pembanding |
# | `stop_id` berakhiran arah (`127N`) | Dipotong menjadi kode stasiun (`127`) (*standardization*) | Agregat per stasiun |
#
# Fungsi transformasi yang dijalankan Spark pada setiap *micro-batch*:

# %%
print(inspect.getsource(spark_etl.decode))
print(inspect.getsource(spark_etl.aggregate))

# %%
def reset_state() -> None:
    spark_etl.trip_state.clear()
    spark_etl.last_snapshot.clear()
    spark_etl.aggregate(prime)  # state sebelum jendela, sama seperti di Spark


log.info("Proses Transform streaming dimulai")
spark_etl.schedule.refresh()
# Lintasan 1: catatan per observasi (sebelum agregasi) untuk inspeksi.
reset_state()
records = {"obs": [], "arrival": [], "duplicate": 0}
for key, value in window:
    for kind, rec in spark_etl.decode(value, key.decode()):
        if kind == "obs":
            records["obs"].append(rec)
        elif kind == "arrival":
            records["arrival"].append(rec)
        elif kind == "duplicate":
            records["duplicate"] += 1
obs = pd.DataFrame(records["obs"], columns=["minute", "route_id", "direction", "stop", "delay_s"])
# Lintasan 2: agregasi dengan fungsi yang sama persis dengan Spark.
reset_state()
agg = spark_etl.aggregate(window)
log.info("Data streaming ditransformasi: %d observasi, %d baris menit, %d baris jam, %d kedatangan, %d gangguan",
         len(obs), len(agg["minute"]), len(agg["hour"]), len(agg["arrivals"]), len(agg["alerts"]))
print("Contoh observasi sebelum agregasi:")
obs.dropna().sample(5, random_state=1)

# %%
nb_minute = pd.DataFrame([(k[0], k[1], k[2], *v) for k, v in agg["minute"].items()],
                         columns=["minute", "route_id", "direction", "observations", "delay_observations", "late_observations",
                                  "sum_delay_s", "max_delay_s", "stopped_at"])
nb_minute["minute"] = utc(nb_minute.minute)
by_route = obs.dropna().groupby("route_id").delay_s.agg(observasi="size", median_s="median", p90_s=lambda s: s.quantile(0.9)).round(0)
print("Keterlambatan per rute pada jendela (detik; negatif = lebih awal dari jadwal):")
by_route.sort_values("observasi", ascending=False).head(10).T

# %% [markdown]
# ### Validasi dan perbandingan sebelum–sesudah

# %%
used = win_profile[~win_profile.snapshot_berulang]
validation_stream = {
    "snapshot berulang dilewati": records["duplicate"] == int(win_profile.snapshot_berulang.sum()) == agg["skipped"],
    "satu observasi per trip yang masih berjalan": len(obs) == int((used.trip_update - used.trip_tanpa_prediksi).sum()),
    "arah hanya N atau S": set(obs.direction) <= {"N", "S"},
    "rute dan stasiun tidak kosong": bool((obs.route_id != "").all() and (obs.stop != "").all()),
    "keterlambatan dalam ±3 jam": bool(obs.delay_s.dropna().abs().max() <= 3 * 3600),
    "observasi terlambat ≤ observasi berketerlambatan ≤ observasi":
        bool(((nb_minute.late_observations <= nb_minute.delay_observations) & (nb_minute.delay_observations <= nb_minute.observations)).all()),
    "kedatangan unik per _id": len(agg["arrivals"]) == len({a["_id"] for a in agg["arrivals"]}),
}
pd.Series(validation_stream, name="lulus")

# %%
matched = obs.delay_s.notna().mean()
comparison_stream = pd.DataFrame({
    "sebelum (pesan mentah)": [f"{len(win_profile)} pesan", int(win_profile.snapshot_berulang.sum()), f"{int(win_profile.trip_update.sum()):,} trip_update",
                               f"{int(win_profile.stop_time_update.sum()):,}", int(win_profile.arah_kosong.sum()), int(win_profile.status_kosong.sum()),
                               f"{int(win_profile.prediksi_lampau.sum()):,}", "tidak ada", "protobuf biner per feed"],
    "sesudah (hasil transformasi)": [f"{len(used)} snapshot dipakai", 0, f"{len(obs):,} observasi", "1 per trip (stasiun berikutnya)", 0, 0, 0,
                                     f"ada untuk {matched:.0%} observasi (cocok dengan jadwal)",
                                     f"{len(agg['minute']):,} baris menit, {len(agg['hour']):,} baris jam, {len(agg['arrivals'])} kedatangan"],
}, index=["data", "snapshot berulang", "unit", "prediksi stasiun", "arah kosong", "status kosong", "prediksi lampau",
          "keterlambatan", "bentuk data"])
comparison_stream

# %% [markdown]
# ## B3. Load — Data Loading
# | Target | Isi | Strategi |
# |---|---|---|
# | PostgreSQL `etl.gtfs_delay_minute` | Observasi, keterlambatan, dan kereta berhenti per menit × rute × arah | *Upsert* aditif per kunci |
# | PostgreSQL `etl.gtfs_delay_hourly` | Keterlambatan per jam × stasiun × rute | *Upsert* aditif per kunci |
# | PostgreSQL `etl.gtfs_stream_batch` | Log setiap *micro-batch* (offset Kafka, jumlah baris, durasi) | *Insert*; kunci `batch_id` |
# | MongoDB `mta.arrivals` | Kedatangan kereta di stasiun (dokumen) | *Upsert* per `_id`, TTL 7 hari |
# | MongoDB `mta.alerts` | Gangguan layanan aktif | *Upsert* per `_id`, TTL 7 hari |
#
# *Upsert* aditif dipilih karena satu menit dapat tersebar di dua *micro-batch*. Agar penjumlahan tidak terulang
# ketika Spark memutar ulang *batch* setelah *restart*, baris log `batch_id` ditulis dalam transaksi yang sama;
# *batch* yang sudah tercatat dilewati (*exactly-once*). MongoDB ditulis lebih dahulu dengan *upsert* per `_id`
# sehingga penulisan ulang menghasilkan dokumen yang sama. MongoDB dipakai untuk kedatangan dan gangguan karena
# datanya berbentuk dokumen dengan atribut yang bervariasi dan hanya dibutuhkan sementara (TTL).

# %%
structure_stream = pd.DataFrame(pg.execute(
    "SELECT table_name, string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) "
    "FROM information_schema.columns WHERE table_schema = %s AND table_name LIKE 'gtfs%%' GROUP BY table_name ORDER BY table_name",
    (SCHEMA,)).fetchall(), columns=["tabel / view", "kolom dan tipe"])
mongo_indexes = {c: {name: (spec["key"], spec.get("expireAfterSeconds")) for name, spec in mongo[c].index_information().items()}
                 for c in ["arrivals", "alerts"]}
print("Indeks MongoDB (kunci, TTL detik):")
for c, idx in mongo_indexes.items():
    print(f"  {c}: {idx}")
structure_stream

# %% [markdown]
# ### Membaca kembali dari PostgreSQL dan MongoDB

# %%
stream_counts = {
    "etl.gtfs_delay_minute": pg_scalar(f"SELECT count(*) FROM {SCHEMA}.gtfs_delay_minute"),
    "etl.gtfs_delay_hourly": pg_scalar(f"SELECT count(*) FROM {SCHEMA}.gtfs_delay_hourly"),
    "etl.gtfs_stream_batch": pg_scalar(f"SELECT count(*) FROM {SCHEMA}.gtfs_stream_batch"),
    "mta.arrivals": mongo.arrivals.count_documents({}), "mta.alerts": mongo.alerts.count_documents({}),
}
print(pd.Series(stream_counts, name="baris / dokumen").to_string())
print("\nRata-rata keterlambatan 30 menit terakhir per rute (dari view etl.gtfs_delay_minute_v):")
pd.DataFrame(pg.execute(f"""
    SELECT route_id, sum(observations) AS observasi, round(avg(avg_delay_s)) AS rata2_terlambat_s,
           round(100.0 * sum(late_observations) / nullif(sum(observations), 0), 1) AS persen_terlambat_5mnt
    FROM {SCHEMA}.gtfs_delay_minute_v WHERE minute >= now() - interval '30 minutes'
    GROUP BY route_id ORDER BY observasi DESC LIMIT 10""").fetchall(),
             columns=["rute", "observasi", "rata-rata terlambat (s)", "% terlambat > 5 menit"])

# %%
print("Kedatangan terbaru di MongoDB:")
pd.DataFrame(mongo.arrivals.find({}, {"_id": 0, "route_id": 1, "direction": 1, "stop_name": 1, "arrived_at": 1, "delay_s": 1, "trip_id": 1})
             .sort("arrived_at", -1).limit(5))

# %%
print("Gangguan layanan aktif di MongoDB:")
pd.DataFrame(mongo.alerts.find({}, {"_id": 0, "routes": 1, "header": 1, "last_seen": 1}).sort("last_seen", -1).limit(5))

# %% [markdown]
# ### Konsistensi hasil transformasi dan data di target storage
# Menit di tepi jendela dapat berisi pesan di luar jendela, sehingga perbandingan memakai menit lengkap
# (dua menit pertama dan dua menit terakhir jendela tidak dipakai).

# %%
minutes = sorted(nb_minute.minute.unique())
full_minutes = minutes[2:-2]
key_cols = ["minute", "route_id", "direction"]
val_cols = ["observations", "delay_observations", "late_observations", "stopped_at", "sum_delay_s"]
pg_minute = pd.DataFrame(pg.execute(f"SELECT {', '.join(key_cols + val_cols)} FROM {SCHEMA}.gtfs_delay_minute WHERE minute = ANY(%s)",
                                    ([m.to_pydatetime() for m in full_minutes],)).fetchall(), columns=key_cols + val_cols)
pg_minute["minute"] = utc(pg_minute.minute)
both = nb_minute[nb_minute.minute.isin(full_minutes)].merge(pg_minute, on=key_cols, how="outer", suffixes=("_nb", "_pg"), indicator=True)
diff = (both["_merge"] != "both")
for c in val_cols:
    tol = 1e-6 if c == "sum_delay_s" else 0
    diff |= (both[f"{c}_nb"] - both[f"{c}_pg"]).abs().fillna(1) > tol
arrival_ids = [a["_id"] for a in agg["arrivals"] if pd.Timestamp(a["observed_at"]).floor("min") in set(full_minutes)]
arrivals_found = mongo.arrivals.count_documents({"_id": {"$in": arrival_ids}})
consistency_stream = (both.groupby("minute")
                      .agg(baris_notebook=("observations_nb", "count"), baris_postgres=("observations_pg", "count"),
                           observasi_notebook=("observations_nb", "sum"), observasi_postgres=("observations_pg", "sum"))
                      .astype(int))
consistency_stream["sama"] = both.assign(d=diff).groupby("minute").d.sum().eq(0)
consistency_stream.index = consistency_stream.index.strftime("%H:%M")
print(f"Kunci (menit × rute × arah) yang berbeda: {int(diff.sum())} dari {len(both)} | "
      f"kedatangan ditemukan di MongoDB: {arrivals_found} dari {len(arrival_ids)}")
consistency_stream

# %% [markdown]
# ## B4. Integrasi Jalur Streaming
# Ketiga layanan berjalan sebagai Deployment/StatefulSet Kubernetes (dimulai ulang otomatis bila gagal).
# Spark menyimpan *checkpoint* offset di volume persisten, sehingga setelah *restart* pemrosesan berlanjut dari
# offset terakhir; log `etl.gtfs_stream_batch` mencatat setiap *micro-batch* yang berhasil dimuat.

# %%
batch_log = pd.DataFrame(pg.execute(f"""SELECT batch_id, processed_at, messages, skipped_snapshots, offset_from, offset_to,
                                               feed_time_to, minute_rows, hour_rows, arrivals, alerts, seconds
                                        FROM {SCHEMA}.gtfs_stream_batch ORDER BY batch_id""").fetchall(),
                         columns=["batch_id", "processed_at", "messages", "skipped", "offset_from", "offset_to",
                                  "feed_time_to", "minute_rows", "hour_rows", "arrivals", "alerts", "seconds"])
_, high_now = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=20)
latency = (batch_log.processed_at - batch_log.feed_time_to).dt.total_seconds()
integration_stream = pd.Series({
    "micro-batch tercatat": len(batch_log), "batch_id unik": batch_log.batch_id.is_unique,
    "offset berurutan tanpa celah": bool((batch_log.offset_from.iloc[1:].values == batch_log.offset_to.iloc[:-1].values + 1).all()),
    "pesan diproses": int(batch_log.messages.sum()), "snapshot berulang dilewati": int(batch_log.skipped.sum()),
    "rata-rata durasi micro-batch (s)": round(batch_log.seconds.mean(), 2), "durasi terlama (s)": round(batch_log.seconds.max(), 2),
    "latensi median feed → PostgreSQL (s)": round(latency.median(), 0),
    "lag Kafka saat ini (pesan)": int(high_now - (batch_log.offset_to.max() + 1)),
}, name="integrasi streaming")
print(integration_stream.to_string())
batch_log.tail(5).drop(columns=["feed_time_to"])

# %% [markdown]
# ## B5. Pengujian Jalur Streaming

# %%
stream_tests = [
    ("Extract", "Akses feed GTFS-RT", "8 feed terkirim setiap putaran", f"{win_profile.feed.nunique()} feed, {len(win_profile)} pesan di jendela", win_profile.feed.nunique() == 8),
    ("Extract", "Penyimpanan di Kafka", "Pesan tersimpan dan terbaca", f"{high - low:,} pesan tersimpan, {len(messages)} dibaca", len(messages) == hi - prime_start),
    ("Extract", "Format data", "Seluruh pesan dapat di-decode", f"{len(raw_profile)} dari {len(messages)}", len(raw_profile) == len(messages)),
    ("Transform", "Snapshot berulang", "Dilewati", f"{agg['skipped']} snapshot dilewati", validation_stream["snapshot berulang dilewati"]),
    ("Transform", "Nilai kosong (arah, status)", "Diisi nilai bawaan", f"{int(win_profile.arah_kosong.sum())} arah, {int(win_profile.status_kosong.sum())} status diisi", validation_stream["arah hanya N atau S"]),
    ("Transform", "Prediksi lampau", "Dibuang", f"{int(win_profile.prediksi_lampau.sum()):,} prediksi dibuang", validation_stream["satu observasi per trip yang masih berjalan"]),
    ("Transform", "Keterlambatan", "Dihitung dari jadwal statis", f"{matched:.0%} observasi cocok jadwal", matched > 0.2),
    ("Transform", "Outlier", "Keterlambatan dalam ±3 jam", f"maks {obs.delay_s.abs().max():,.0f} detik", validation_stream["keterlambatan dalam ±3 jam"]),
    ("Load", "PostgreSQL", "Tabel terisi", f"menit={stream_counts['etl.gtfs_delay_minute']:,}, jam={stream_counts['etl.gtfs_delay_hourly']:,}",
     stream_counts["etl.gtfs_delay_minute"] > 0 and stream_counts["etl.gtfs_delay_hourly"] > 0),
    ("Load", "MongoDB", "Koleksi terisi dengan TTL", f"arrivals={stream_counts['mta.arrivals']:,}, alerts={stream_counts['mta.alerts']:,}",
     stream_counts["mta.arrivals"] > 0 and all(any(v[1] for v in idx.values()) for idx in mongo_indexes.values())),
    ("Load", "Konsistensi PostgreSQL", "Hasil notebook = hasil Spark", f"{len(full_minutes)} menit, {int(diff.sum())} kunci berbeda", int(diff.sum()) == 0),
    ("Load", "Konsistensi MongoDB", "Kedatangan ditemukan", f"{arrivals_found} dari {len(arrival_ids)}", arrivals_found == len(arrival_ids)),
    ("End-to-End", "Exactly-once", "batch_id unik, offset tanpa celah", f"{len(batch_log)} batch", bool(integration_stream["batch_id unik"] and integration_stream["offset berurutan tanpa celah"])),
    ("End-to-End", "Latensi", "Kurang dari 2 menit", f"median {latency.median():.0f} detik", latency.median() < 120),
    ("End-to-End", "Lag", "Spark mengikuti producer", f"{integration_stream['lag Kafka saat ini (pesan)']} pesan", integration_stream["lag Kafka saat ini (pesan)"] <= 400),
]
stream_test_table = pd.DataFrame(stream_tests, columns=["Tahapan", "Aspek yang diuji", "Hasil yang diharapkan", "Hasil aktual", "Status"])
stream_test_table["Status"] = stream_test_table["Status"].map({True: "Berhasil", False: "Gagal"})
stream_test_table

# %%
in_full = win_profile[win_profile.menit.isin(full_minutes)]
obs_full = obs[utc(obs.minute).isin(full_minutes).values]
stream_row_flow = pd.DataFrame([
    ("Setelah Extract", f"{len(in_full)} pesan; {int(in_full.entitas.sum()):,} entitas", "Snapshot feed di Kafka untuk menit lengkap"),
    ("Setelah Transform", f"{len(obs_full):,} observasi", "Satu observasi per trip yang masih berjalan per snapshot"),
    ("Setelah Transform (agregat)", f"{int(both._merge.isin(['both', 'left_only']).sum()):,} baris", "Agregat menit × rute × arah"),
    ("Setelah Load", f"{len(pg_minute):,} baris", "Baris etl.gtfs_delay_minute di PostgreSQL untuk menit yang sama"),
], columns=["Tahapan", "Jumlah", "Keterangan"])
stream_row_flow

# %%
consumer.close()
all_tests = pd.concat([test_table.assign(Jalur="Batch"), stream_test_table.assign(Jalur="Streaming")])
print(all_tests.groupby("Jalur").Status.value_counts().unstack(fill_value=0).to_string())
print(f"\nTotal uji: {len(all_tests)}, berhasil: {(all_tests.Status == 'Berhasil').sum()}, gagal: {(all_tests.Status == 'Gagal').sum()}")
