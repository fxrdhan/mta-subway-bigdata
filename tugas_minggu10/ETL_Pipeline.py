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
from typing import NamedTuple
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    stream=sys.stdout, force=True)
log = logging.getLogger("etl")
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 160)
pd.set_option("display.max_colwidth", 120)

SOURCE_URL = "https://data.ny.gov/resource/5wq4-mkjj"
STAGING = Path(os.environ.get("ETL_STAGING", "/tmp/etl_staging"))  # area staging sementara
OUTPUT = Path(os.environ.get("ETL_OUTPUT", "output"))              # dataset hasil transformasi
WORKERS = int(os.environ.get("ETL_WORKERS", "6"))                  # unduhan paralel
# Uji cepat: isi ETL_DAY_LIMIT (misalnya 3) agar hanya N hari terakhir yang diproses.
# Bila kosong, seluruh hari diproses.
DAY_LIMIT = int(os.environ["ETL_DAY_LIMIT"]) if os.environ.get("ETL_DAY_LIMIT") else None
SCHEMA = "etl"
print(f"staging = {STAGING} | output = {OUTPUT} | workers = {WORKERS} | "
      f"batas hari = {DAY_LIMIT or 'tanpa batas'}")

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
SOURCE_SCHEMA = pa.schema([
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
])
COLUMNS = SOURCE_SCHEMA.names
_thread_local = threading.local()  # satu requests.Session untuk setiap thread unduhan


def http_get(url: str, params: dict | None = None, retries: int = 8) -> requests.Response:
    """GET dengan jeda eksponensial bila server sibuk (HTTP 429/5xx) atau jaringan bermasalah."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    for attempt in range(retries + 1):
        try:
            response = _thread_local.session.get(url, params=params, timeout=(15, 600))
            if response.status_code == 200:
                _ = response.content  # unduh isinya sekarang agar galat jaringan ikut diulang
                return response
            if response.status_code != 429 and response.status_code < 500:
                raise ValueError(f"HTTP {response.status_code}: {response.text[:200]}")
            reason = f"HTTP {response.status_code}"
        except requests.RequestException as error:
            reason = type(error).__name__
        if attempt == retries:
            raise RuntimeError(f"{url} gagal setelah {retries + 1} percobaan: {reason}")
        time.sleep(min(2 ** attempt, 120) + random.random())


def as_utc(text: str) -> pd.Timestamp:
    """Timestamp dari server; yang tidak berzona waktu dianggap UTC."""
    timestamp = pd.Timestamp(text)
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp


FINGERPRINT_QUERY = {
    "$select": "date_trunc_ymd(transit_timestamp) AS day_start, count(*) AS row_count, "
               "sum(ridership) AS ridership_sum, sum(transfers) AS transfers_sum, "
               "max(:updated_at) AS last_updated",
    "$group": "day_start",
    "$order": "day_start",
    "$limit": 10000,
}


def server_fingerprints() -> pd.DataFrame:
    """Fingerprint tiap hari menurut server: jumlah baris, total ridership dan transfers,
    serta waktu revisi terakhir. Hari yang fingerprint-nya berubah perlu diekstraksi ulang."""
    rows = http_get(SOURCE_URL + ".json", FINGERPRINT_QUERY).json()
    return pd.DataFrame({
        "day": [dt.date.fromisoformat(row["day_start"][:10]) for row in rows],
        "rows": [int(row["row_count"]) for row in rows],
        "ridership": [round(float(row["ridership_sum"])) for row in rows],
        "transfers": [round(float(row["transfers_sum"])) for row in rows],
        "updated_at": [as_utc(row["last_updated"]) for row in rows],
    })


started = time.time()
server = server_fingerprints()
log.info("Sumber dapat diakses: %d hari (%s s.d. %s), %s baris di server (%.0f detik)",
         len(server), server.day.min(), server.day.max(), f"{server.rows.sum():,}",
         time.time() - started)
server.tail()

# %% [markdown]
# ### 1b. Koneksi ke target storage dan penentuan hari yang perlu diekstraksi
# Fingerprint yang terakhir dimuat disimpan di tabel `etl.ingest_day`. Hari yang fingerprint-nya berbeda
# dianggap berubah. Karena ETL tidak menyimpan data mentah, agregat satu bulan hanya bisa dibangun ulang
# dari **seluruh hari di bulan itu**, sehingga yang diekstraksi adalah semua hari pada bulan yang berubah.

# %%
DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

-- Fingerprint hari yang sudah dimuat (dasar ekstraksi inkremental)
CREATE TABLE IF NOT EXISTS {SCHEMA}.ingest_day (
    day               date PRIMARY KEY,
    rows              bigint NOT NULL,
    ridership         bigint NOT NULL,
    transfers         bigint NOT NULL,
    source_updated_at timestamptz NOT NULL,
    loaded_at         timestamptz NOT NULL
);

-- Dimensi
CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_station (
    station_complex_id text PRIMARY KEY,
    station_complex    text NOT NULL,
    borough            text NOT NULL,
    transit_mode       text NOT NULL,
    latitude           double precision NOT NULL,
    longitude          double precision NOT NULL
);
CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_date (
    date        date PRIMARY KEY,
    year        smallint NOT NULL,
    month       smallint NOT NULL,
    day         smallint NOT NULL,
    day_of_week smallint NOT NULL,
    is_weekend  boolean NOT NULL
);

-- Fakta (agregat)
CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_ridership_hourly (
    hour              timestamp NOT NULL,
    payment_method    text NOT NULL,
    ridership         bigint NOT NULL,
    transfers         bigint NOT NULL,
    is_dst_transition boolean NOT NULL,
    PRIMARY KEY (hour, payment_method)
);
CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_ridership_daily_station (
    day                date NOT NULL,
    station_complex_id text NOT NULL,
    ridership          bigint NOT NULL,
    transfers          bigint NOT NULL,
    omny_ridership     bigint NOT NULL,
    PRIMARY KEY (day, station_complex_id)
);
CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_ridership_daily_fare_class (
    day                 date NOT NULL,
    fare_class_category text NOT NULL,
    ridership           bigint NOT NULL,
    PRIMARY KEY (day, fare_class_category)
);

-- Hasil inspeksi kualitas per bulan dan log setiap run pipeline
CREATE TABLE IF NOT EXISTS {SCHEMA}.quality_month (
    month date NOT NULL,
    rule  text NOT NULL,
    rows  bigint NOT NULL,
    PRIMARY KEY (month, rule)
);
CREATE TABLE IF NOT EXISTS {SCHEMA}.pipeline_run (
    run_id         uuid PRIMARY KEY,
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz,
    status         text NOT NULL,
    stage          text,
    days_extracted integer,
    rows_extracted bigint,
    months_loaded  text,
    message        text
);
"""

pg = psycopg.connect(application_name="etl-notebook", autocommit=True)
version = pg.execute("SELECT version()").fetchone()[0]
log.info("Terhubung ke target storage: database %s di %s (%s)",
         pg.info.dbname, pg.info.host, version.split(",")[0])
pg.execute(DDL)


def pg_value(sql: str, params: tuple = ()):
    """Satu nilai dari kueri PostgreSQL."""
    return pg.execute(sql, params).fetchone()[0]


def pg_frame(sql: str, columns: list[str], params: tuple = ()) -> pd.DataFrame:
    """Hasil kueri PostgreSQL sebagai DataFrame."""
    return pd.DataFrame(pg.execute(sql, params).fetchall(), columns=columns)


def stored_fingerprints() -> pd.DataFrame:
    """Fingerprint yang tercatat saat load sebelumnya (tabel etl.ingest_day)."""
    return pg_frame(
        f"SELECT day, rows, ridership, transfers, source_updated_at FROM {SCHEMA}.ingest_day",
        ["day", "rows", "ridership", "transfers", "updated_at"])


def month_start(day: dt.date) -> dt.date:
    return day.replace(day=1)


def next_month(month: dt.date) -> dt.date:
    return month_start(month + dt.timedelta(days=32))


def plan_months(server: pd.DataFrame, stored: pd.DataFrame) -> list[dt.date]:
    """Bulan yang perlu diekstraksi ulang: bulan yang memuat hari baru, hari yang berubah,
    atau hari yang hilang dari server."""
    columns = ["rows", "ridership", "transfers", "updated_at"]
    now = server.set_index("day")[columns]
    before = stored.set_index("day")[columns] if len(stored) else now.iloc[0:0]
    joined = now.merge(before, left_index=True, right_index=True, how="outer",
                       suffixes=("", "_before"))
    changed = pd.Series(False, index=joined.index)
    for column in columns:
        changed |= joined[column] != joined[f"{column}_before"]
    return sorted({month_start(day) for day in joined.index[changed]})


stored = stored_fingerprints()
months = plan_months(server, stored)
days_to_extract = [day for day in server.day if month_start(day) in months]
if DAY_LIMIT:  # uji cepat: hanya N hari terakhir
    days_to_extract = days_to_extract[-DAY_LIMIT:]
    months = sorted({month_start(day) for day in days_to_extract})
log.info("%d hari sudah pernah dimuat; perlu ekstraksi %d hari pada %d bulan",
         len(stored), len(days_to_extract), len(months))

# %% [markdown]
# ### 1c. Melakukan ekstraksi data
# Setiap hari diambil per halaman 500.000 baris dan dicek jumlah barisnya terhadap fingerprint server
# (diulang sampai tiga kali bila berbeda). Hasilnya ditulis sebagai Parquet di area *staging*, satu berkas per hari.

# %%
PAGE_SIZE = 500_000  # baris per halaman SODA


def csv_to_table(content: bytes) -> pa.Table:
    """CSV dari SODA → tabel Arrow dengan skema sumber. Halaman kosong → tabel kosong."""
    if not content.strip():
        return SOURCE_SCHEMA.empty_table()
    options = pacsv.ConvertOptions(column_types=SOURCE_SCHEMA, include_columns=COLUMNS,
                                   strings_can_be_null=True)
    table = pacsv.read_csv(io.BytesIO(content), convert_options=options)
    return table.select(COLUMNS).cast(SOURCE_SCHEMA)


def download_day(day: dt.date) -> pa.Table:
    """Semua baris satu hari, diambil per halaman."""
    next_day = day + dt.timedelta(days=1)
    where = (f"transit_timestamp >= '{day}T00:00:00' "
             f"AND transit_timestamp < '{next_day}T00:00:00'")
    pages, offset = [], 0
    while True:
        params = {"$where": where, "$order": ":id", "$limit": PAGE_SIZE, "$offset": offset}
        page = csv_to_table(http_get(SOURCE_URL + ".csv", params).content)
        pages.append(page)
        if page.num_rows < PAGE_SIZE:  # halaman terakhir
            return pa.concat_tables(pages)
        offset += PAGE_SIZE


def extract_day(day: dt.date, expected_rows: int) -> int:
    """Mengunduh satu hari ke berkas Parquet di staging. Bila jumlah barisnya berbeda dengan
    fingerprint server, unduhan diulang (paling banyak tiga kali)."""
    for attempt in range(3):
        table = download_day(day)
        if table.num_rows == expected_rows:
            pq.write_table(table, STAGING / f"{day}.parquet", compression="zstd")
            return table.num_rows
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{day}: jumlah baris tetap berbeda dengan server")


def extract(days: list[dt.date], fingerprints: pd.DataFrame) -> int:
    """Tahap Extract: mengunduh hari-hari yang diminta secara paralel ke area staging."""
    log.info("Proses Extract dimulai: %d hari", len(days))
    shutil.rmtree(STAGING, ignore_errors=True)
    STAGING.mkdir(parents=True)
    expected = fingerprints.set_index("day")["rows"]
    total = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(extract_day, day, int(expected[day])) for day in days]
        for done, future in enumerate(as_completed(futures), start=1):
            total += future.result()
            if done % 50 == 0 or done == len(futures):
                log.info("  %d/%d hari diekstraksi (%s baris)", done, len(futures), f"{total:,}")
    log.info("Data berhasil diekstraksi: %s baris ke %s", f"{total:,}", STAGING)
    return total


started = time.time()
extracted_rows = extract(days_to_extract, server)
extract_seconds = time.time() - started
expected_rows = int(server[server.day.isin(days_to_extract)].rows.sum())  # menurut server
staging_mb = sum(f.stat().st_size for f in STAGING.glob("*.parquet")) / 1e6
print(f"Waktu ekstraksi: {extract_seconds / 60:.1f} menit | "
      f"ukuran berkas staging: {staging_mb:.0f} MB")

# %% [markdown]
# ### 1d. Pemeriksaan awal data
# Pemeriksaan dilakukan dengan DuckDB langsung di atas berkas Parquet *staging* sehingga 47 juta baris
# tidak perlu dimuat seluruhnya ke memori.

# %%
duck = duckdb.connect()
duck.execute("SET memory_limit = '2GB'")
duck.execute("SET threads = 2")
duck.execute("SET preserve_insertion_order = false")
duck.execute("SET enable_progress_bar = false")
duck.execute(f"SET temp_directory = '{STAGING}/.duckdb'")


def duck_value(sql: str, params: list | None = None):
    """Satu nilai dari kueri DuckDB."""
    return duck.execute(sql, params or []).fetchone()[0]


def create_raw_view() -> None:
    """View `raw` = semua berkas Parquet hasil Extract di staging."""
    duck.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{STAGING}/*.parquet')")


create_raw_view()
n_rows = duck_value("SELECT count(*) FROM raw")
raw_columns = duck.execute("DESCRIBE raw").df()[["column_name", "column_type"]]
n_cols = len(raw_columns)
print(f"Jumlah baris: {n_rows:,} | jumlah kolom: {n_cols}")
raw_columns

# %%
print("Contoh 5 baris data hasil ekstraksi:")
duck.execute("SELECT * FROM raw LIMIT 5").df()

# %%
KEY_COLUMNS = ["transit_timestamp", "station_complex_id", "payment_method", "fare_class_category"]


def count_duplicates(columns: list[str], table: str = "raw") -> int:
    """Jumlah baris duplikat pada kolom tertentu.

    Agar hemat memori, yang dihitung lebih dahulu adalah hash 64-bit tiap baris. Hasilnya batas
    atas: bila tidak nol, baris yang hash-nya sama dibandingkan lagi dengan nilai aslinya.
    """
    cols = ", ".join(columns)
    upper_bound = duck_value(f"SELECT count(*) - count(DISTINCT hash({cols})) FROM {table}")
    if upper_bound == 0:
        return 0
    return int(duck_value(f"""
        SELECT coalesce(sum(n - 1), 0)
        FROM (
            SELECT count(*) AS n
            FROM {table}
            WHERE hash({cols}) IN (
                SELECT hash({cols}) FROM {table} GROUP BY 1 HAVING count(*) > 1)
            GROUP BY {cols}
            HAVING count(*) > 1
        )"""))


null_counts = ", ".join(f"count(*) - count({column}) AS {column}" for column in COLUMNS)
missing = duck.execute(f"SELECT {null_counts} FROM raw").df().iloc[0].to_frame("nilai_kosong")
dup_rows = count_duplicates(COLUMNS)
dup_keys = count_duplicates(KEY_COLUMNS)
print(f"Baris duplikat persis: {dup_rows:,} | "
      f"duplikat kunci (timestamp, stasiun, metode, kategori tarif): {dup_keys:,}")
missing.T

# %%
summary_before = {
    "baris": n_rows,
    "kolom": n_cols,
    "nilai_kosong": int(missing["nilai_kosong"].sum()),
    "duplikat_kunci": dup_keys,
    "tipe ridership/transfers": duck_value("SELECT typeof(any_value(ridership)) FROM raw"),
    "total ridership": duck_value("SELECT sum(ridership) FROM raw"),
    "total transfers": duck_value("SELECT sum(transfers) FROM raw"),
}
pd.Series(summary_before, name="kondisi awal")

# %% [markdown]
# ## 2. Transform — Data Transformation
# ### 2a. Data Inspection
# Dua belas aturan kualitas dari bagian IV notebook proyek diperiksa sekaligus. Kolom *dampak* menjelaskan
# akibatnya bila tidak ditangani.

# %%
NY = ZoneInfo("America/New_York")


def dst_transition_hours(first: dt.datetime, last: dt.datetime) -> list[dt.datetime]:
    """Jam lokal New York yang tidak ada atau terjadi dua kali karena peralihan DST."""
    hours = []
    hour = first.replace(minute=0, second=0, microsecond=0)
    while hour <= last:
        offset_first = hour.replace(tzinfo=NY, fold=0).utcoffset()
        offset_second = hour.replace(tzinfo=NY, fold=1).utcoffset()
        if offset_first != offset_second:
            hours.append(hour)
        hour += dt.timedelta(hours=1)
    return hours


def build_station_mode() -> None:
    """Tabel bantu `station_mode`: moda dominan tiap kompleks stasiun."""
    duck.execute("""
        CREATE OR REPLACE TABLE station_mode AS
        SELECT station_complex_id, arg_max(transit_mode, n) AS station_mode
        FROM (
            SELECT station_complex_id, transit_mode, count(*) AS n
            FROM raw
            GROUP BY ALL
        )
        GROUP BY ALL""")


first_ts, last_ts = duck.execute(
    "SELECT min(transit_timestamp), max(transit_timestamp) FROM raw").fetchone()
dst_hours = dst_transition_hours(first_ts, last_ts)
if dst_hours:
    listed = ", ".join(f"TIMESTAMP '{hour:%Y-%m-%d %H:%M:%S}'" for hour in dst_hours)
    IS_DST_HOUR = f"transit_timestamp IN ({listed})"
else:
    IS_DST_HOUR = "FALSE"
build_station_mode()


class Rule(NamedTuple):
    condition: str  # kondisi SQL untuk baris yang melanggar
    impact: str     # akibatnya bila tidak ditangani
    blocking: bool  # True = tidak bisa diperbaiki otomatis, pipeline berhenti


GEO_POINT = "string_split(trim(georeference, 'POINT ()'), ' ')"  # "POINT (lon lat)" → [lon, lat]
RULES = {
    "missing_value": Rule(
        " OR ".join(f"{column} IS NULL" for column in COLUMNS),
        "Baris tidak lengkap tidak bisa diagregasi", blocking=True),
    "invalid_category": Rule(
        "transit_mode NOT IN ('subway', 'staten_island_railway', 'tram') "
        "OR payment_method NOT IN ('omny', 'metrocard') "
        "OR borough NOT IN ('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island')",
        "Kategori salah membuat grup baru", blocking=True),
    "nonpositive_ridership": Rule(
        "ridership <= 0",
        "Baris tanpa penumpang tidak bermakna", blocking=True),
    "negative_transfers": Rule(
        "transfers < 0",
        "Total transfer menjadi lebih kecil dari seharusnya", blocking=False),
    "transfers_exceed_ridership": Rule(
        "transfers > ridership",
        "Transfer tidak mungkin melebihi penumpang", blocking=True),
    "non_integer_count": Rule(
        "ridership <> floor(ridership) OR transfers <> floor(transfers)",
        "Hitungan orang harus bilangan bulat", blocking=True),
    "outside_nyc": Rule(
        "latitude NOT BETWEEN 40.47 AND 40.92 OR longitude NOT BETWEEN -74.26 AND -73.70",
        "Peta stasiun salah", blocking=True),
    "not_on_hour": Rule(
        "date_trunc('hour', transit_timestamp) <> transit_timestamp",
        "Data per jam tidak sejajar", blocking=True),
    "fare_class_payment_mismatch": Rule(
        "lower(split_part(fare_class_category, ' - ', 1)) <> payment_method",
        "Kategori tarif bertentangan dengan metode", blocking=True),
    "station_mode_mismatch": Rule(
        "transit_mode <> station_mode",
        "Satu stasiun tercatat dengan dua moda", blocking=False),
    "georeference_mismatch": Rule(
        f"CAST({GEO_POINT}[1] AS DOUBLE) <> longitude "
        f"OR CAST({GEO_POINT}[2] AS DOUBLE) <> latitude",
        "Koordinat ganda tidak konsisten", blocking=True),
    "dst_transition_hour": Rule(
        IS_DST_HOUR,
        "Jam peralihan DST bisa berisi dua jam atau kosong", blocking=False),
}


def count_violations(where: str = "TRUE", params: list | None = None) -> pd.Series:
    """Jumlah baris yang melanggar tiap aturan, opsional dibatasi kondisi `where`."""
    filters = ", ".join(f"count(*) FILTER ({rule.condition}) AS {name}"
                        for name, rule in RULES.items())
    sql = f"SELECT {filters} FROM raw JOIN station_mode USING (station_complex_id) WHERE {where}"
    return duck.execute(sql, params or []).df().iloc[0]


violations = count_violations()
inspection = pd.DataFrame({
    "pelanggaran": violations,
    "dampak": {name: rule.impact for name, rule in RULES.items()},
})
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
blocking = {name: int(violations[name])
            for name, rule in RULES.items() if rule.blocking and violations[name]}
assert not blocking, f"Pelanggaran yang tidak bisa diperbaiki otomatis: {blocking}"
print("Tidak ada pelanggaran yang menghentikan pipeline.")

# %% [markdown]
# ### 2b–2c. Data Cleaning dan Data Transformation
# Pembersihan dan transformasi dijalankan sebagai satu kueri DuckDB yang menulis data bersih `clean` sebagai berkas
# Parquet di area *staging* (47 juta baris tidak perlu ditampung di memori), lalu agregat dibangun dari berkas tersebut.

# %%
CLEAN_PARQUET = STAGING / "clean" / "clean.parquet"

CLEAN_SQL = """
    SELECT
        transit_timestamp,
        -- fitur turunan
        CAST(transit_timestamp AS DATE) AS date,
        hour(transit_timestamp)         AS hour_of_day,
        isodow(transit_timestamp)       AS day_of_week,
        isodow(transit_timestamp) >= 6  AS is_weekend,
        {is_dst_hour}                   AS is_dst_transition,
        -- standardisasi: moda dominan kompleks stasiun
        station_mode AS transit_mode,
        station_complex_id, station_complex, borough, payment_method, fare_class_category,
        -- konversi tipe; transfers negatif menjadi 0
        CAST(ridership AS INTEGER)              AS ridership,
        CAST(greatest(transfers, 0) AS INTEGER) AS transfers,
        latitude, longitude
        -- georeference dibuang karena isinya sama dengan latitude dan longitude
    FROM raw JOIN station_mode USING (station_complex_id)
"""

AGGREGATES = {
    # jam × metode pembayaran
    "t_hourly": """
        SELECT transit_timestamp AS hour, payment_method,
               sum(ridership)::BIGINT     AS ridership,
               sum(transfers)::BIGINT     AS transfers,
               bool_or(is_dst_transition) AS is_dst_transition
        FROM clean
        GROUP BY ALL""",
    # hari × stasiun, termasuk penumpang OMNY
    "t_daily_station": """
        SELECT date AS day, station_complex_id,
               sum(ridership)::BIGINT AS ridership,
               sum(transfers)::BIGINT AS transfers,
               coalesce(sum(ridership) FILTER (payment_method = 'omny'), 0)::BIGINT
                   AS omny_ridership
        FROM clean
        GROUP BY ALL""",
    # hari × kategori tarif
    "t_daily_fare_class": """
        SELECT date AS day, fare_class_category, sum(ridership)::BIGINT AS ridership
        FROM clean
        GROUP BY ALL""",
    # dimensi stasiun: atribut terbaru tiap stasiun
    "t_dim_station": """
        SELECT station_complex_id,
               arg_max(station_complex, transit_timestamp) AS station_complex,
               arg_max(borough, transit_timestamp)         AS borough,
               any_value(transit_mode)                     AS transit_mode,
               arg_max(latitude, transit_timestamp)        AS latitude,
               arg_max(longitude, transit_timestamp)       AS longitude
        FROM clean
        GROUP BY ALL""",
    # dimensi tanggal
    "t_dim_date": """
        SELECT DISTINCT date, year(date) AS year, month(date) AS month, day(date) AS day,
               day_of_week, is_weekend
        FROM clean""",
}


def transform() -> dict[str, int]:
    """Tahap Transform: membersihkan data mentah, menulis data bersih ke staging, lalu
    membangun tabel agregat dan dimensi. Mengembalikan jumlah baris tiap tabel."""
    log.info("Proses Transform dimulai")
    CLEAN_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    clean_sql = CLEAN_SQL.format(is_dst_hour=IS_DST_HOUR)
    duck.execute(f"COPY ({clean_sql}) TO '{CLEAN_PARQUET}' (FORMAT parquet, COMPRESSION zstd)")
    duck.execute(f"CREATE OR REPLACE VIEW clean AS SELECT * FROM read_parquet('{CLEAN_PARQUET}')")
    for table, sql in AGGREGATES.items():
        duck.execute(f"CREATE OR REPLACE TABLE {table} AS {sql}")
    sizes = {table: duck_value(f"SELECT count(*) FROM {table}") for table in ["clean", *AGGREGATES]}
    log.info("Data berhasil dibersihkan dan ditransformasi: %s",
             {table: f"{n:,}" for table, n in sizes.items()})
    return sizes


sizes = transform()
duck.execute("DESCRIBE clean").df()[["column_name", "column_type"]].T

# %%
print("Contoh data bersih dengan fitur turunan:")
duck.execute("SELECT * FROM clean WHERE is_dst_transition LIMIT 3").df()

# %% [markdown]
# **Transformasi yang diterapkan**
#
# | Transformasi | Hasil | Tujuan |
# |---|---|---|
# | *Feature creation* | `date`, `hour_of_day`, `day_of_week`, `is_weekend`, `is_dst_transition` | Analisis pola jam dan hari |
# | *Data aggregation* | `fact_ridership_hourly` (jam × metode) | Grafik jam sibuk seluruh jaringan |
# | *Data aggregation* | `fact_ridership_daily_station` (hari × stasiun, termasuk OMNY) | Analisis per stasiun |
# | *Data aggregation* | `fact_ridership_daily_fare_class` (hari × kategori tarif) | Analisis kategori tarif |
# | *Attribute transformation* | `dim_station`, `dim_date` | Atribut stasiun dan tanggal tidak diulang di setiap baris fakta |

# %% [markdown]
# ### 2d. Data Validation

# %%
def count_rows(where: str, table: str = "clean") -> int:
    return duck_value(f"SELECT count(*) FROM {table} WHERE {where}")


REQUIRED = ["transit_timestamp", "station_complex_id", "payment_method", "ridership", "transfers"]
clean_types = {row[0]: row[1] for row in duck.execute("DESCRIBE clean").fetchall()}
clean_ridership = duck_value("SELECT sum(ridership) FROM clean")
missing_after = count_rows(" OR ".join(f"{column} IS NULL" for column in REQUIRED))
negative_after = count_rows("transfers < 0")
dup_keys_after = count_duplicates(KEY_COLUMNS, "clean")
modes_per_station = duck_value("""
    SELECT max(n) FROM (
        SELECT count(DISTINCT transit_mode) AS n FROM clean GROUP BY station_complex_id)""")
duplicate_station_days = duck_value(
    "SELECT count(*) - count(DISTINCT (day, station_complex_id)) FROM t_daily_station")

validation = {
    "jumlah baris bersih = jumlah baris mentah": sizes["clean"] == n_rows,
    "tidak ada nilai kosong": missing_after == 0,
    "ridership dan transfers bertipe INTEGER":
        clean_types["ridership"] == clean_types["transfers"] == "INTEGER",
    "tidak ada transfers negatif": negative_after == 0,
    "satu moda per stasiun": modes_per_station == 1,
    "total ridership tidak berubah": clean_ridership == summary_before["total ridership"],
    "total ridership agregat jam = data bersih":
        duck_value("SELECT sum(ridership) FROM t_hourly") == clean_ridership,
    "total ridership harian stasiun = data bersih":
        duck_value("SELECT sum(ridership) FROM t_daily_station") == clean_ridership,
    "kunci agregat unik": duplicate_station_days == 0,
    "tidak ada duplikat kunci": dup_keys_after == 0,
}
pd.Series(validation, name="lulus")

# %% [markdown]
# ### 2e. Perbandingan kondisi data sebelum dan sesudah transformasi

# %%
transfers_after = duck_value("SELECT sum(transfers) FROM clean")
before_after = {  # aspek: (sebelum, sesudah)
    "baris": (f"{n_rows:,}", f"{sizes['clean']:,}"),
    "kolom": (n_cols, len(clean_types)),
    "nilai kosong": (summary_before["nilai_kosong"], missing_after),
    "duplikat kunci": (dup_keys, dup_keys_after),
    # sesudah = 0 karena setiap stasiun tinggal satu moda (validasi "satu moda per stasiun")
    "moda tidak konsisten": (int(violations["station_mode_mismatch"]), 0),
    "transfers negatif": (int(violations["negative_transfers"]), negative_after),
    "tipe ridership/transfers":
        (summary_before["tipe ridership/transfers"], clean_types["ridership"]),
    "total ridership": (f"{summary_before['total ridership']:,.0f}", f"{clean_ridership:,}"),
    "total transfers": (f"{summary_before['total transfers']:,.0f}", f"{transfers_after:,}"),
    "atribut berulang": ("georeference ada", "dibuang (sama dengan lat/lon)"),
    "fitur baru": ("-", "date, hour_of_day, day_of_week, is_weekend, is_dst_transition"),
}
comparison = pd.DataFrame.from_dict(before_after, orient="index", columns=["sebelum", "sesudah"])
comparison

# %% [markdown]
# Jumlah baris tidak berubah karena tidak ada baris yang dihapus. Total `transfers` naik karena nilai negatif
# diubah menjadi 0; total `ridership` tetap sama persis.

# %% [markdown]
# ## 3. Load — Data Loading
# ### 3a–3b. Koneksi dan struktur penyimpanan
# Target storage: **PostgreSQL 16**, database `mta`, skema `etl` (terpisah dari pipeline ELT Minggu 11).

# %%
TABLE_COLUMNS_SQL = """
    SELECT table_name,
           string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position)
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name LIKE %s
    GROUP BY table_name
    ORDER BY table_name"""
structure = pg_frame(TABLE_COLUMNS_SQL, ["tabel", "kolom dan tipe"], (SCHEMA, "%"))
structure

# %% [markdown]
# ### 3c. Proses loading
# * **Tabel fakta**: *upsert* per bulan. Bulan yang diekstraksi dihapus lalu dimuat ulang dalam **satu transaksi**,
#   sehingga revisi mingguan MTA menggantikan nilai lama dan pipeline aman dijalankan ulang tanpa duplikasi.
# * **Dimensi**: *upsert* per baris (`INSERT ... ON CONFLICT DO UPDATE`).
# * **Fingerprint** hari yang dimuat dicatat di `etl.ingest_day` untuk run inkremental berikutnya.

# %%
class Fact(NamedTuple):
    source: str        # tabel hasil transformasi di DuckDB
    date_column: str   # kolom untuk memilih data satu bulan
    columns: list[str]


FACTS = {
    "fact_ridership_hourly": Fact(
        "t_hourly", "hour",
        ["hour", "payment_method", "ridership", "transfers", "is_dst_transition"]),
    "fact_ridership_daily_station": Fact(
        "t_daily_station", "day",
        ["day", "station_complex_id", "ridership", "transfers", "omny_ridership"]),
    "fact_ridership_daily_fare_class": Fact(
        "t_daily_fare_class", "day",
        ["day", "fare_class_category", "ridership"]),
}
DIMENSIONS = {  # tabel PostgreSQL -> (tabel hasil transformasi, kunci)
    "dim_station": ("t_dim_station", "station_complex_id"),
    "dim_date": ("t_dim_date", "date"),
}
UPSERT_FINGERPRINT = f"""
    INSERT INTO {SCHEMA}.ingest_day (day, rows, ridership, transfers, source_updated_at, loaded_at)
    VALUES (%s, %s, %s, %s, %s, now())
    ON CONFLICT (day) DO UPDATE SET
        rows              = EXCLUDED.rows,
        ridership         = EXCLUDED.ridership,
        transfers         = EXCLUDED.transfers,
        source_updated_at = EXCLUDED.source_updated_at,
        loaded_at         = EXCLUDED.loaded_at"""


def replace_month(cur, table: str, fact: Fact, month: dt.date) -> int:
    """Menghapus data satu bulan di tabel fakta lalu memuat hasil transformasi bulan itu."""
    column, period = fact.date_column, (month, next_month(month))
    cur.execute(f"DELETE FROM {SCHEMA}.{table} WHERE {column} >= %s AND {column} < %s", period)
    rows = duck.execute(f"SELECT {', '.join(fact.columns)} FROM {fact.source} "
                        f"WHERE {column} >= ? AND {column} < ?", list(period)).fetchall()
    with cur.copy(f"COPY {SCHEMA}.{table} ({', '.join(fact.columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    return len(rows)


def record_quality(cur, month: dt.date) -> None:
    """Mencatat jumlah pelanggaran tiap aturan pada satu bulan (tabel etl.quality_month)."""
    in_month = "transit_timestamp >= ? AND transit_timestamp < ?"
    month_violations = count_violations(in_month, [month, next_month(month)])
    cur.execute(f"DELETE FROM {SCHEMA}.quality_month WHERE month = %s", (month,))
    cur.executemany(f"INSERT INTO {SCHEMA}.quality_month VALUES (%s, %s, %s)",
                    [(month, rule, int(n)) for rule, n in month_violations.items()])


def upsert_dimension(cur, table: str, source: str, key: str) -> int:
    """Baris dimensi baru ditambahkan, baris yang sudah ada diperbarui."""
    columns = [row[0] for row in duck.execute(f"DESCRIBE {source}").fetchall()]
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != key)
    placeholders = ", ".join(["%s"] * len(columns))
    rows = duck.execute(f"SELECT {', '.join(columns)} FROM {source}").fetchall()
    cur.executemany(f"""
        INSERT INTO {SCHEMA}.{table} ({', '.join(columns)}) VALUES ({placeholders})
        ON CONFLICT ({key}) DO UPDATE SET {updates}""", rows)
    return len(rows)


def load(months: list[dt.date], fingerprints: pd.DataFrame) -> dict[str, int]:
    """Tahap Load ke PostgreSQL. Mengembalikan jumlah baris yang dimuat per tabel.

    Tabel fakta diganti per bulan, satu transaksi per bulan. Dimensi di-upsert, lalu fingerprint
    hari yang dimuat dicatat sebagai dasar run berikutnya.
    """
    log.info("Proses Load dimulai: %d bulan", len(months))
    loaded = {}
    for month in months:
        with pg.transaction(), pg.cursor() as cur:
            for table, fact in FACTS.items():
                loaded[table] = loaded.get(table, 0) + replace_month(cur, table, fact, month)
            record_quality(cur, month)
    with pg.transaction(), pg.cursor() as cur:
        for table, (source, key) in DIMENSIONS.items():
            loaded[table] = upsert_dimension(cur, table, source, key)
        cur.executemany(UPSERT_FINGERPRINT, [
            (row.day, row.rows, row.ridership, row.transfers, row.updated_at.to_pydatetime())
            for row in fingerprints.itertuples()])
    log.info("Data berhasil disimpan ke target storage: %s",
             {table: f"{n:,}" for table, n in loaded.items()})
    return loaded


started = time.time()
loaded = load(months, server[server.day.isin(days_to_extract)])
print(f"Waktu load: {time.time() - started:.0f} detik")
pd.Series(loaded, name="baris dimuat")

# %% [markdown]
# ### 3d. Memeriksa hasil penyimpanan (membaca kembali dari PostgreSQL)

# %%
BATCH_TABLES = [*FACTS, *DIMENSIONS, "ingest_day", "quality_month"]
stored_counts = {table: pg_value(f"SELECT count(*) FROM {SCHEMA}.{table}")
                 for table in BATCH_TABLES}
print(pd.Series(stored_counts, name="baris di PostgreSQL").to_string())
pg_frame(f"""
    SELECT f.day, s.station_complex, s.borough, f.ridership, f.omny_ridership
    FROM {SCHEMA}.fact_ridership_daily_station f
    JOIN {SCHEMA}.dim_station s USING (station_complex_id)
    ORDER BY f.day DESC, f.ridership DESC
    LIMIT 5""", ["day", "station", "borough", "ridership", "omny"])

# %% [markdown]
# ### 3e. Konsistensi data hasil transformasi dan data di target storage

# %%
def compare_fact(table: str, fact: Fact, start: dt.date, end: dt.date) -> dict:
    """Jumlah baris dan total ridership: hasil transformasi (DuckDB) vs PostgreSQL."""
    column = fact.date_column
    duck_rows, duck_total = duck.execute(
        f"SELECT count(*), sum(ridership) FROM {fact.source} "
        f"WHERE {column} >= ? AND {column} < ?", [start, end]).fetchone()
    pg_rows, pg_total = pg.execute(
        f"SELECT count(*), sum(ridership) FROM {SCHEMA}.{table} "
        f"WHERE {column} >= %s AND {column} < %s", (start, end)).fetchone()
    return {
        "tabel": table,
        "baris hasil transformasi": duck_rows,
        "baris di PostgreSQL": pg_rows,
        "total ridership transformasi": duck_total,
        "total ridership PostgreSQL": pg_total,
        "sama": duck_rows == pg_rows and duck_total == pg_total,
    }


period_start, period_end = months[0], next_month(months[-1])
consistency = pd.DataFrame([compare_fact(table, fact, period_start, period_end)
                            for table, fact in FACTS.items()])
consistency

# %%
OUTPUT.mkdir(parents=True, exist_ok=True)
csv_path = OUTPUT / "transformed_dataset.csv"
duck.execute(f"""
    COPY (
        SELECT d.day, d.station_complex_id, s.station_complex, s.borough,
               d.ridership, d.transfers, d.omny_ridership
        FROM t_daily_station d
        JOIN t_dim_station s USING (station_complex_id)
        ORDER BY d.day, d.station_complex_id
    ) TO '{csv_path}' (HEADER)""")
print(f"Dataset hasil transformasi: {csv_path} "
      f"({csv_path.stat().st_size / 1e6:.1f} MB, {sizes['t_daily_station']:,} baris)")

# %% [markdown]
# ## 4. ETL Pipeline Integration
# Seluruh tahapan digabung dalam fungsi `run_etl()`. Transform hanya berjalan bila Extract berhasil, dan Load
# hanya berjalan bila Transform serta validasi berhasil. Setiap run dicatat di `etl.pipeline_run` beserta
# tahap tempat kegagalan terjadi. Area *staging* dihapus setelah Load berhasil (data mentah tidak disimpan).
#
# Run di bawah ini adalah eksekusi kedua dari awal sampai akhir. Karena fingerprint seluruh hari sudah tersimpan,
# hanya bulan yang direvisi MTA sejak run pertama yang diekstraksi ulang.

# %%
def finish_run(run_id, status: str, stage: str, days=None, rows=None, months=None,
               message=None) -> None:
    """Melengkapi catatan satu run di etl.pipeline_run."""
    pg.execute(f"""
        UPDATE {SCHEMA}.pipeline_run
        SET finished_at = now(), status = %s, stage = %s, days_extracted = %s,
            rows_extracted = %s, months_loaded = %s, message = %s
        WHERE run_id = %s""", (status, stage, days, rows, months, message, run_id))


def run_etl() -> dict:
    """Extract → Transform → Load untuk hari yang berubah sejak run sebelumnya.

    Setiap tahap hanya berjalan bila tahap sebelumnya berhasil. Hasil run dan tahap tempat
    kegagalan terjadi dicatat di etl.pipeline_run.
    """
    run_id, stage = uuid.uuid4(), "extract"
    pg.execute(f"INSERT INTO {SCHEMA}.pipeline_run (run_id, started_at, status, stage) "
               "VALUES (%s, now(), 'running', %s)", (run_id, stage))
    result = {"run_id": str(run_id), "bulan": [], "baris_extract": 0}
    try:
        fingerprints, stored = server_fingerprints(), stored_fingerprints()
        if DAY_LIMIT:  # uji cepat: hanya hari yang pernah dimuat yang dibandingkan
            fingerprints = fingerprints[fingerprints.day.isin(stored.day)]
        months = plan_months(fingerprints, stored)
        days = [day for day in fingerprints.day if month_start(day) in months]

        if not months:
            log.info("Tidak ada hari yang berubah; tidak ada yang perlu dimuat")
        else:
            result["baris_extract"] = extract(days, fingerprints)
            create_raw_view()
            build_station_mode()

            stage = "transform"
            if transform()["clean"] != result["baris_extract"]:
                raise RuntimeError("jumlah baris berubah saat transformasi")

            stage = "load"
            load(months, fingerprints[fingerprints.day.isin(days)])
            result["bulan"] = [f"{month:%Y-%m}" for month in months]

        shutil.rmtree(STAGING, ignore_errors=True)  # data mentah tidak disimpan
        finish_run(run_id, "success", "done", days=len(days), rows=result["baris_extract"],
                   months=",".join(result["bulan"]) or None)
        log.info("ETL Pipeline selesai dijalankan")
        return result
    except Exception as error:
        finish_run(run_id, "failed", stage, message=f"{type(error).__name__}: {error}"[:500])
        log.error("ETL Pipeline gagal pada tahap %s: %s", stage, error)
        raise


# Run pertama di atas dijalankan bertahap sel demi sel; catat juga di log run.
pg.execute(f"""
    INSERT INTO {SCHEMA}.pipeline_run (run_id, started_at, finished_at, status, stage,
                                       days_extracted, rows_extracted, months_loaded, message)
    VALUES (%s, now() - make_interval(secs => %s), now(), 'success', 'done', %s, %s, %s, %s)""",
    (uuid.uuid4(), extract_seconds, len(days_to_extract), extracted_rows,
     ",".join(f"{month:%Y-%m}" for month in months), "run pertama (bertahap di notebook)"))
second = run_etl()
second

# %%
# months_loaded "2025-01,2025-02,...,2026-11" ditampilkan sebagai "2025-01 s.d. 2026-11"
run_log = pg_frame(rf"""
    SELECT started_at::timestamp(0),
           finished_at::timestamp(0),
           status,
           stage,
           days_extracted,
           rows_extracted,
           coalesce(regexp_replace(months_loaded, '^([^,]+),.*,([^,]+)$', '\1 s.d. \2'), '-'),
           coalesce(message, 'run_etl()')
    FROM {SCHEMA}.pipeline_run
    ORDER BY started_at""",
    ["mulai", "selesai", "status", "tahap", "hari", "baris", "bulan", "keterangan"])
run_log

# %% [markdown]
# ## 5. ETL Pipeline Testing and Validation

# %%
day_mismatches = pg_value(f"""
    SELECT count(*)
    FROM {SCHEMA}.ingest_day i
    LEFT JOIN (
        SELECT day, sum(ridership) AS ridership
        FROM {SCHEMA}.fact_ridership_daily_station
        GROUP BY day
    ) f USING (day)
    WHERE f.ridership IS DISTINCT FROM i.ridership""")
fact_counts = ", ".join(f"{table}={n:,}" for table, n in stored_counts.items()
                        if table.startswith("fact"))

tests = [  # (tahapan, aspek yang diuji, hasil yang diharapkan, hasil aktual, lulus)
    ("Extract", "Akses sumber data", "SODA API dapat diakses",
     f"{len(server)} hari tersedia", len(server) > 0),
    ("Extract", "Jumlah baris", "Sama dengan fingerprint server",
     f"{extracted_rows:,} dari {expected_rows:,}", extracted_rows == expected_rows),
    ("Extract", "Atribut", "12 kolom sumber",
     f"{n_cols} kolom", n_cols == 12),
    ("Transform", "Nilai kosong", "0 setelah transformasi",
     str(missing_after), missing_after == 0),
    ("Transform", "Duplikasi", "0 duplikat kunci",
     f"{dup_keys} sebelum, {dup_keys_after} sesudah", validation["tidak ada duplikat kunci"]),
    ("Transform", "Transfers negatif", "Diubah menjadi 0",
     f"{int(violations['negative_transfers'])} baris diperbaiki",
     validation["tidak ada transfers negatif"]),
    ("Transform", "Moda stasiun", "Satu moda per stasiun",
     f"{int(violations['station_mode_mismatch'])} baris diseragamkan",
     validation["satu moda per stasiun"]),
    ("Transform", "Tipe data", "ridership/transfers INTEGER",
     clean_types["ridership"], validation["ridership dan transfers bertipe INTEGER"]),
    ("Transform", "Kehilangan data", "Total ridership tetap",
     f"{summary_before['total ridership']:,.0f}", validation["total ridership tidak berubah"]),
    ("Load", "Koneksi", "Terhubung ke PostgreSQL",
     version.split(",")[0], True),
    ("Load", "Penyimpanan", "Semua tabel terisi",
     fact_counts, all(stored_counts[table] > 0 for table in FACTS)),
    ("Load", "Pembacaan kembali", "Jumlah dan total sama",
     f"{int(consistency['sama'].sum())}/{len(consistency)} tabel sama",
     bool(consistency["sama"].all())),
    ("End-to-End", "Eksekusi pipeline", "Seluruh tahapan berjalan",
     f"run kedua: {second['bulan'] or 'tidak ada perubahan'}", True),
    ("End-to-End", "Kesesuaian dengan server", "Total harian = fingerprint server",
     f"{day_mismatches} hari berbeda", day_mismatches == 0),
]
TEST_COLUMNS = ["Tahapan", "Aspek yang diuji", "Hasil yang diharapkan", "Hasil aktual", "Status"]
test_table = pd.DataFrame(tests, columns=TEST_COLUMNS)
test_table["Status"] = test_table["Status"].map({True: "Berhasil", False: "Gagal"})
test_table

# %%
fact_rows_after_transform = sum(sizes[fact.source] for fact in FACTS.values())
row_flow = pd.DataFrame([
    ("Setelah Extract", f"{extracted_rows:,}",
     "Baris mentah dari SODA API (berkas staging)"),
    ("Setelah Transform", f"{sizes['clean']:,}",
     "Baris bersih; tidak ada baris yang dihapus"),
    ("Setelah Transform (agregat)", f"{fact_rows_after_transform:,}",
     "Baris tabel fakta hasil agregasi (jam, harian stasiun, harian tarif)"),
    ("Setelah Load", f"{sum(stored_counts[table] for table in FACTS):,}",
     "Baris tabel fakta yang tersimpan di PostgreSQL"),
], columns=["Tahapan", "Jumlah baris", "Keterangan"])
row_flow

# %% [markdown]
# Perbedaan antara jumlah baris setelah Transform dan setelah Load terjadi karena yang dimuat ke PostgreSQL adalah
# **agregat**, sesuai rancangan ETL Minggu 9 (gudang data hanya berisi data bersih dan agregat). Kesesuaiannya
# dibuktikan dengan total `ridership` yang sama persis antara data bersih, tabel fakta, dan fingerprint server.

# %%
passed = (test_table["Status"] == "Berhasil").sum()
failed = (test_table["Status"] == "Gagal").sum()
print(f"Jumlah uji: {len(test_table)}, berhasil: {passed}, gagal: {failed}")

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

# Kode yang sama dengan yang dijalankan layanan streaming di klaster
sys.path.insert(0, str(Path(os.environ.get("STREAMING_CODE", "streaming"))))
import producer   # Extract: Deployment mta-gtfs-producer
import spark_etl  # Transform dan Load: Deployment mta-spark-streaming


def as_utc_series(values) -> pd.Series:
    """Waktu UTC berpresisi mikrodetik, agar bisa dibandingkan dengan data dari PostgreSQL."""
    return pd.to_datetime(pd.Series(values), utc=True).astype("datetime64[us, UTC]")


KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ.get("KAFKA_TOPIC", "mta-gtfs-stream")
mongo = pymongo.MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=15000)["mta"]
print(f"Kafka {KAFKA_BOOTSTRAP}, topik {TOPIC} | "
      f"MongoDB versi {mongo.client.server_info()['version']}")

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
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
partitions = admin.list_topics(TOPIC, timeout=20).topics[TOPIC].partitions
topic_resource = ConfigResource("topic", TOPIC)
topic_config = admin.describe_configs([topic_resource])[topic_resource].result()
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": f"etl-notebook-{uuid.uuid4().hex[:8]}",  # grup sementara, tidak mengganggu Spark
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})
PARTITION = TopicPartition(TOPIC, 0)
low, high = consumer.get_watermark_offsets(PARTITION, timeout=20)


def read_batch_log() -> pd.DataFrame:
    """Log micro-batch Spark (etl.gtfs_stream_batch), satu baris per batch."""
    columns = ["batch_id", "processed_at", "messages", "skipped_snapshots", "offset_from",
               "offset_to", "feed_time_from", "feed_time_to", "minute_rows", "hour_rows",
               "arrivals", "alerts", "seconds"]
    batches = pg_frame(f"SELECT {', '.join(columns)} FROM {SCHEMA}.gtfs_stream_batch "
                       "ORDER BY batch_id", columns)
    return batches.rename(columns={"skipped_snapshots": "skipped"})


WINDOW = 320  # pesan yang diperiksa: ±20 menit (8 feed × 2 putaran per menit)
PRIME = 240   # pesan sebelumnya, untuk menyiapkan state seperti milik Spark
batch_log = read_batch_log()
window_end = int(batch_log.offset_to.max()) + 1     # offset pertama yang belum diproses Spark
first_offset = int(batch_log.offset_from.min())     # offset pertama yang tercatat di log Spark
window_start = max(first_offset, window_end - WINDOW)
prime_start = max(first_offset, window_start - PRIME)
pd.Series({
    "partisi": len(partitions),
    "retensi (jam)": int(topic_config["retention.ms"].value) / 3.6e6,
    "offset terendah tersimpan": low,
    "offset tertinggi": high,
    "pesan tersimpan": high - low,
    "micro-batch Spark tercatat": len(batch_log),
    "jendela pemeriksaan": f"offset {window_start}–{window_end - 1}",
}, name="Kafka")

# %%
def read_messages(start: int, end: int) -> list:
    """Pesan Kafka dengan start ≤ offset < end."""
    consumer.assign([TopicPartition(TOPIC, 0, start)])
    messages = []
    while len(messages) < end - start:
        message = consumer.poll(15)
        if message is None:
            break
        if message.error():
            raise RuntimeError(message.error())
        if message.offset() >= end:
            break
        messages.append(message)
    return messages


def parse_feed(value: bytes):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(value)
    return feed


def has_direction(trip_update) -> bool:
    """Apakah ekstensi NYCT mengisi arah perjalanan."""
    nyct = trip_update.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor]
    return nyct.HasField("direction")


def profile(message) -> dict:
    """Ringkasan satu pesan mentah (satu snapshot feed), tanpa transformasi apa pun."""
    feed = parse_feed(message.value())
    feed_time = feed.header.timestamp
    trips = [entity.trip_update for entity in feed.entity if entity.HasField("trip_update")]
    vehicles = [entity.vehicle for entity in feed.entity if entity.HasField("vehicle")]
    updates = [update for trip in trips for update in trip.stop_time_update]

    def is_past(update) -> bool:  # prediksi yang sudah lewat
        return spark_etl.predicted_time(update) < feed_time - 30

    return {
        "offset": message.offset(),
        "feed": message.key().decode(),
        "kb": round(len(message.value()) / 1024, 1),
        "fetched_at": int(dict(message.headers())["fetched_at"]),
        "feed_timestamp": feed_time,
        "entitas": len(feed.entity),
        "trip_update": len(trips),
        "vehicle": len(vehicles),
        "alert": sum(entity.HasField("alert") for entity in feed.entity),
        "stop_time_update": len(updates),
        "arah_kosong": sum(not has_direction(trip) for trip in trips),
        "status_kosong": sum(not vehicle.HasField("current_status") for vehicle in vehicles),
        "prediksi_lampau": sum(is_past(update) for update in updates),
        "trip_tanpa_prediksi": sum(all(is_past(u) for u in t.stop_time_update) for t in trips),
        "trip_id_tidak_baku": sum(not spark_etl.TRIP_KEY.match(t.trip.trip_id) for t in trips),
    }


def is_repeated(timestamps: pd.Series) -> pd.Series:
    """Snapshot berulang: tidak lebih baru dari snapshot sebelumnya pada feed yang sama."""
    return timestamps <= timestamps.cummax().shift(fill_value=0)


started = time.time()
messages = read_messages(prime_start, window_end)
raw_profile = pd.DataFrame(profile(message) for message in messages)
raw_profile["snapshot_berulang"] = (raw_profile.groupby("feed")["feed_timestamp"]
                                   .transform(is_repeated))
minute_start = raw_profile.feed_timestamp - raw_profile.feed_timestamp % 60
raw_profile["menit"] = as_utc_series(pd.to_datetime(minute_start, unit="s", utc=True))

prime = [(m.key(), m.value()) for m in messages if m.offset() < window_start]
window = [(m.key(), m.value()) for m in messages if m.offset() >= window_start]
window_profile = raw_profile[raw_profile.offset >= window_start].reset_index(drop=True)
log.info("Extract streaming: %d pesan dibaca dari Kafka dalam %.1f detik "
         "(%d untuk state, %d jendela)", len(messages), time.time() - started,
         len(prime), len(window))
print("Delapan pesan terakhir di jendela:")
window_profile.tail(8)[["offset", "feed", "kb", "fetched_at", "feed_timestamp",
                        "entitas", "trip_update", "vehicle", "alert"]]

# %% [markdown]
# ### Pemeriksaan awal data
# Satu pesan berisi satu *snapshot* feed. Contoh isi satu `trip_update` setelah di-decode:

# %%
def trip_updates(value: bytes) -> list:
    return [entity.trip_update for entity in parse_feed(value).entity
            if entity.HasField("trip_update")]


def local_time(seconds: int) -> str | None:
    return dt.datetime.fromtimestamp(seconds, NY).strftime("%H:%M:%S") if seconds else None


# perjalanan terakhir di jendela yang memiliki minimal tiga prediksi stasiun
sample_trip = next(trip for _, value in reversed(window) for trip in trip_updates(value)
                   if len(trip.stop_time_update) >= 3)
pd.DataFrame([
    {
        "trip_id": sample_trip.trip.trip_id,
        "route_id": sample_trip.trip.route_id,
        "start_date": sample_trip.trip.start_date,
        "stop_id": update.stop_id,
        "arrival": local_time(update.arrival.time),
        "departure": local_time(update.departure.time),
    }
    for update in sample_trip.stop_time_update[:5]
])

# %%
ISSUE_COLUMNS = ["snapshot_berulang", "arah_kosong", "status_kosong", "prediksi_lampau",
                 "trip_tanpa_prediksi", "trip_id_tidak_baku"]
summary_stream = pd.Series({
    "pesan (snapshot feed)": len(window_profile),
    "feed berbeda": window_profile.feed.nunique(),
    "ukuran total (MB)": round(window_profile.kb.sum() / 1024, 1),
    "rentang waktu feed":
        f"{window_profile.menit.min():%H:%M}–{window_profile.menit.max():%H:%M} UTC",
    "entitas": int(window_profile.entitas.sum()),
    "trip_update": int(window_profile.trip_update.sum()),
    "vehicle": int(window_profile.vehicle.sum()),
    "alert": int(window_profile.alert.sum()),
    "stop_time_update": int(window_profile.stop_time_update.sum()),
    **{column: int(window_profile[column].sum()) for column in ISSUE_COLUMNS},
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
# | Feed tidak memiliki kolom keterlambatan | Dihitung: prediksi − jadwal statis GTFS (*feature creation*) | Metrik utama analisis keterlambatan |
# | Keterlambatan lebih dari 3 jam | Dikosongkan (*outlier*) | Bukan perjalanan yang sama (batas hari layanan) |
# | Trip tidak ada di jadwal statis (pola `trip_id` lain, mis. SIR, atau trip tambahan) | Keterlambatan dibiarkan kosong; observasi tetap dihitung | Tidak ada jadwal pembanding |
# | `stop_id` berakhiran arah (`127N`) | Dipotong menjadi kode stasiun (`127`) (*standardization*) | Agregat per stasiun |
#
# Fungsi transformasi yang dijalankan Spark pada setiap *micro-batch*:

# %%
for function in [spark_etl.decode, spark_etl.observe_trip, spark_etl.delay_seconds,
                 spark_etl.MinuteDelay, spark_etl.aggregate]:
    print(inspect.getsource(function))

# %%
def reset_state() -> None:
    """Mengosongkan state lalu memproses pesan sebelum jendela, sehingga state notebook sama
    dengan state Spark saat jendela dimulai."""
    spark_etl.trip_state.clear()
    spark_etl.last_snapshot.clear()
    spark_etl.aggregate(prime)


log.info("Proses Transform streaming dimulai")
spark_etl.schedule.refresh()

# Lintasan 1: catatan per observasi (sebelum agregasi), untuk inspeksi.
reset_state()
observations, duplicates_seen = [], 0
for key, value in window:
    for kind, record in spark_etl.decode(value, key.decode()):
        if kind == "obs":
            observations.append(record)
        elif kind == "duplicate":
            duplicates_seen += 1
obs = pd.DataFrame(observations, columns=["minute", "route_id", "direction", "stop", "delay_s"])

# Lintasan 2: agregasi dengan fungsi yang sama persis dengan Spark.
reset_state()
agg = spark_etl.aggregate(window)
log.info("Data streaming ditransformasi: %d observasi, %d baris menit, %d baris jam, "
         "%d kedatangan, %d gangguan", len(obs), len(agg["minute"]), len(agg["hour"]),
         len(agg["arrivals"]), len(agg["alerts"]))
print("Contoh observasi sebelum agregasi:")
obs.dropna().sample(5, random_state=1)

# %%
MINUTE_COLUMNS = ["minute", "route_id", "direction", "observations", "delay_observations",
                  "late_observations", "sum_delay_s", "max_delay_s", "stopped_at"]
nb_minute = pd.DataFrame(spark_etl.as_rows(agg["minute"]), columns=MINUTE_COLUMNS)
nb_minute["minute"] = as_utc_series(nb_minute.minute)

by_route = (obs.dropna()
            .groupby("route_id").delay_s
            .agg(observasi="size", median_s="median", p90_s=lambda s: s.quantile(0.9))
            .round(0))
print("Keterlambatan per rute pada jendela (detik; negatif = lebih awal dari jadwal):")
by_route.sort_values("observasi", ascending=False).head(10).T

# %% [markdown]
# ### Validasi dan perbandingan sebelum–sesudah

# %%
used = window_profile[~window_profile.snapshot_berulang]  # snapshot yang benar-benar diproses
running_trips = int((used.trip_update - used.trip_tanpa_prediksi).sum())
counts_ordered = ((nb_minute.late_observations <= nb_minute.delay_observations)
                  & (nb_minute.delay_observations <= nb_minute.observations))
validation_stream = {
    "snapshot berulang dilewati":
        duplicates_seen == int(window_profile.snapshot_berulang.sum()) == agg["skipped"],
    "satu observasi per trip yang masih berjalan": len(obs) == running_trips,
    "arah hanya N atau S": set(obs.direction) <= {"N", "S"},
    "rute dan stasiun tidak kosong": bool((obs.route_id != "").all() and (obs.stop != "").all()),
    "keterlambatan dalam ±3 jam": bool(obs.delay_s.dropna().abs().max() <= 3 * 3600),
    "observasi terlambat ≤ observasi berketerlambatan ≤ observasi": bool(counts_ordered.all()),
    "kedatangan unik per _id": len(agg["arrivals"]) == len({a["_id"] for a in agg["arrivals"]}),
}
pd.Series(validation_stream, name="lulus")

# %%
matched = obs.delay_s.notna().mean()  # porsi observasi yang punya jadwal pembanding
raw_totals = window_profile[["trip_update", "stop_time_update", "snapshot_berulang",
                             "arah_kosong", "status_kosong", "prediksi_lampau"]].sum().astype(int)
before_after = {  # aspek: (sebelum, sesudah)
    "data": (f"{len(window_profile)} pesan", f"{len(used)} snapshot dipakai"),
    "snapshot berulang": (raw_totals.snapshot_berulang, 0),
    "unit": (f"{raw_totals.trip_update:,} trip_update", f"{len(obs):,} observasi"),
    "prediksi stasiun": (f"{raw_totals.stop_time_update:,}", "1 per trip (stasiun berikutnya)"),
    "arah kosong": (raw_totals.arah_kosong, 0),
    "status kosong": (raw_totals.status_kosong, 0),
    "prediksi lampau": (f"{raw_totals.prediksi_lampau:,}", 0),
    "keterlambatan": ("tidak ada", f"ada untuk {matched:.0%} observasi (cocok dengan jadwal)"),
    "bentuk data": ("protobuf biner per feed",
                    f"{len(agg['minute']):,} baris menit, {len(agg['hour']):,} baris jam, "
                    f"{len(agg['arrivals'])} kedatangan"),
}
comparison_stream = pd.DataFrame.from_dict(
    before_after, orient="index",
    columns=["sebelum (pesan mentah)", "sesudah (hasil transformasi)"])
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
structure_stream = pg_frame(TABLE_COLUMNS_SQL, ["tabel / view", "kolom dan tipe"],
                            (SCHEMA, "gtfs%"))
mongo_indexes = {
    collection: {name: (spec["key"], spec.get("expireAfterSeconds"))
                 for name, spec in mongo[collection].index_information().items()}
    for collection in ["arrivals", "alerts"]
}
print("Indeks MongoDB (kunci, TTL detik):")
for collection, indexes in mongo_indexes.items():
    print(f"  {collection}: {indexes}")
structure_stream

# %% [markdown]
# ### Membaca kembali dari PostgreSQL dan MongoDB

# %%
stream_counts = {
    "etl.gtfs_delay_minute": pg_value(f"SELECT count(*) FROM {SCHEMA}.gtfs_delay_minute"),
    "etl.gtfs_delay_hourly": pg_value(f"SELECT count(*) FROM {SCHEMA}.gtfs_delay_hourly"),
    "etl.gtfs_stream_batch": pg_value(f"SELECT count(*) FROM {SCHEMA}.gtfs_stream_batch"),
    "mta.arrivals": mongo.arrivals.count_documents({}),
    "mta.alerts": mongo.alerts.count_documents({}),
}
print(pd.Series(stream_counts, name="baris / dokumen").to_string())
print("\nRata-rata keterlambatan 30 menit terakhir per rute (dari view etl.gtfs_delay_minute_v):")
pg_frame(f"""
    SELECT route_id,
           sum(observations)       AS observasi,
           round(avg(avg_delay_s)) AS rata2_terlambat_s,
           round(100.0 * sum(late_observations) / nullif(sum(observations), 0), 1) AS persen
    FROM {SCHEMA}.gtfs_delay_minute_v
    WHERE minute >= now() - interval '30 minutes'
    GROUP BY route_id
    ORDER BY observasi DESC
    LIMIT 10""", ["rute", "observasi", "rata-rata terlambat (s)", "% terlambat > 5 menit"])

# %%
print("Kedatangan terbaru di MongoDB:")
arrival_fields = {"_id": 0, "route_id": 1, "direction": 1, "stop_name": 1, "arrived_at": 1,
                  "delay_s": 1, "trip_id": 1}
pd.DataFrame(mongo.arrivals.find({}, arrival_fields).sort("arrived_at", -1).limit(5))

# %%
print("Gangguan layanan aktif di MongoDB:")
alert_fields = {"_id": 0, "routes": 1, "header": 1, "last_seen": 1}
pd.DataFrame(mongo.alerts.find({}, alert_fields).sort("last_seen", -1).limit(5))

# %% [markdown]
# ### Konsistensi hasil transformasi dan data di target storage
# Menit di tepi jendela dapat berisi pesan di luar jendela, sehingga perbandingan memakai menit lengkap
# (dua menit pertama dan dua menit terakhir jendela tidak dipakai).

# %%
full_minutes = sorted(nb_minute.minute.unique())[2:-2]
KEY_COLS = ["minute", "route_id", "direction"]
VALUE_COLS = ["observations", "delay_observations", "late_observations", "stopped_at",
              "sum_delay_s"]

pg_minute = pg_frame(
    f"SELECT {', '.join(KEY_COLS + VALUE_COLS)} FROM {SCHEMA}.gtfs_delay_minute "
    "WHERE minute = ANY(%s)",
    KEY_COLS + VALUE_COLS, ([minute.to_pydatetime() for minute in full_minutes],))
pg_minute["minute"] = as_utc_series(pg_minute.minute)
both = (nb_minute[nb_minute.minute.isin(full_minutes)]
        .merge(pg_minute, on=KEY_COLS, how="outer", suffixes=("_nb", "_pg"), indicator=True))

# Satu kunci berbeda bila hanya ada di satu sisi atau ada nilai yang tidak sama.
different = both["_merge"] != "both"
for column in VALUE_COLS:
    tolerance = 1e-6 if column == "sum_delay_s" else 0  # selisih pembulatan desimal
    gap = (both[f"{column}_nb"] - both[f"{column}_pg"]).abs().fillna(1)
    different |= gap > tolerance

# Kedatangan pada menit yang sama harus ada di MongoDB.
full_minute_set = set(full_minutes)
arrival_ids = [arrival["_id"] for arrival in agg["arrivals"]
               if pd.Timestamp(arrival["observed_at"]).floor("min") in full_minute_set]
arrivals_found = mongo.arrivals.count_documents({"_id": {"$in": arrival_ids}})

consistency_stream = (
    both.groupby("minute")
    .agg(baris_notebook=("observations_nb", "count"),
         baris_postgres=("observations_pg", "count"),
         observasi_notebook=("observations_nb", "sum"),
         observasi_postgres=("observations_pg", "sum"))
    .astype(int)
)
keys_different_per_minute = both.assign(different=different).groupby("minute").different.sum()
consistency_stream["sama"] = keys_different_per_minute.eq(0)
consistency_stream.index = consistency_stream.index.strftime("%H:%M")
print(f"Kunci (menit × rute × arah) yang berbeda: {int(different.sum())} dari {len(both)} | "
      f"kedatangan ditemukan di MongoDB: {arrivals_found} dari {len(arrival_ids)}")
consistency_stream

# %% [markdown]
# ## B4. Integrasi Jalur Streaming
# Ketiga layanan berjalan sebagai Deployment/StatefulSet Kubernetes (dimulai ulang otomatis bila gagal).
# Spark menyimpan *checkpoint* offset di volume persisten, sehingga setelah *restart* pemrosesan berlanjut dari
# offset terakhir; log `etl.gtfs_stream_batch` mencatat setiap *micro-batch* yang berhasil dimuat.

# %%
batch_log = read_batch_log()
_, high_now = consumer.get_watermark_offsets(PARTITION, timeout=20)
latency = (batch_log.processed_at - batch_log.feed_time_to).dt.total_seconds()
next_from = batch_log.offset_from.iloc[1:].values
previous_to = batch_log.offset_to.iloc[:-1].values
integration_stream = pd.Series({
    "micro-batch tercatat": len(batch_log),
    "batch_id unik": batch_log.batch_id.is_unique,
    "offset berurutan tanpa celah": bool((next_from == previous_to + 1).all()),
    "pesan diproses": int(batch_log.messages.sum()),
    "snapshot berulang dilewati": int(batch_log.skipped.sum()),
    "rata-rata durasi micro-batch (s)": round(batch_log.seconds.mean(), 2),
    "durasi terlama (s)": round(batch_log.seconds.max(), 2),
    "latensi median feed → PostgreSQL (s)": round(latency.median(), 0),
    "lag Kafka saat ini (pesan)": int(high_now - (batch_log.offset_to.max() + 1)),
}, name="integrasi streaming")
print(integration_stream.to_string())
batch_log.tail(5).drop(columns=["feed_time_from", "feed_time_to"])

# %% [markdown]
# ## B5. Pengujian Jalur Streaming

# %%
lag = integration_stream["lag Kafka saat ini (pesan)"]
exactly_once = (integration_stream["batch_id unik"]
                and integration_stream["offset berurutan tanpa celah"])
ttl_on_every_collection = all(any(ttl for _, ttl in indexes.values())
                              for indexes in mongo_indexes.values())
keys_different = int(different.sum())

stream_tests = [  # (tahapan, aspek yang diuji, hasil yang diharapkan, hasil aktual, lulus)
    ("Extract", "Akses feed GTFS-RT", "8 feed terkirim setiap putaran",
     f"{window_profile.feed.nunique()} feed, {len(window_profile)} pesan di jendela",
     window_profile.feed.nunique() == 8),
    ("Extract", "Penyimpanan di Kafka", "Pesan tersimpan dan terbaca",
     f"{high - low:,} pesan tersimpan, {len(messages)} dibaca",
     len(messages) == window_end - prime_start),
    ("Extract", "Format data", "Seluruh pesan dapat di-decode",
     f"{len(raw_profile)} dari {len(messages)}", len(raw_profile) == len(messages)),
    ("Transform", "Snapshot berulang", "Dilewati",
     f"{agg['skipped']} snapshot dilewati", validation_stream["snapshot berulang dilewati"]),
    ("Transform", "Nilai kosong (arah, status)", "Diisi nilai bawaan",
     f"{raw_totals.arah_kosong} arah, {raw_totals.status_kosong} status diisi",
     validation_stream["arah hanya N atau S"]),
    ("Transform", "Prediksi lampau", "Dibuang",
     f"{raw_totals.prediksi_lampau:,} prediksi dibuang",
     validation_stream["satu observasi per trip yang masih berjalan"]),
    ("Transform", "Keterlambatan", "Dihitung dari jadwal statis",
     f"{matched:.0%} observasi cocok jadwal", matched > 0.2),
    ("Transform", "Outlier", "Keterlambatan dalam ±3 jam",
     f"maks {obs.delay_s.abs().max():,.0f} detik",
     validation_stream["keterlambatan dalam ±3 jam"]),
    ("Load", "PostgreSQL", "Tabel terisi",
     f"menit={stream_counts['etl.gtfs_delay_minute']:,}, "
     f"jam={stream_counts['etl.gtfs_delay_hourly']:,}",
     stream_counts["etl.gtfs_delay_minute"] > 0 and stream_counts["etl.gtfs_delay_hourly"] > 0),
    ("Load", "MongoDB", "Koleksi terisi dengan TTL",
     f"arrivals={stream_counts['mta.arrivals']:,}, alerts={stream_counts['mta.alerts']:,}",
     stream_counts["mta.arrivals"] > 0 and ttl_on_every_collection),
    ("Load", "Konsistensi PostgreSQL", "Hasil notebook = hasil Spark",
     f"{len(full_minutes)} menit, {keys_different} kunci berbeda", keys_different == 0),
    ("Load", "Konsistensi MongoDB", "Kedatangan ditemukan",
     f"{arrivals_found} dari {len(arrival_ids)}", arrivals_found == len(arrival_ids)),
    ("End-to-End", "Exactly-once", "batch_id unik, offset tanpa celah",
     f"{len(batch_log)} batch", bool(exactly_once)),
    ("End-to-End", "Latensi", "Kurang dari 2 menit",
     f"median {latency.median():.0f} detik", latency.median() < 120),
    ("End-to-End", "Lag", "Spark mengikuti producer",
     f"{lag} pesan", lag <= 400),
]
stream_test_table = pd.DataFrame(stream_tests, columns=TEST_COLUMNS)
stream_test_table["Status"] = stream_test_table["Status"].map({True: "Berhasil", False: "Gagal"})
stream_test_table

# %%
in_full = window_profile[window_profile.menit.isin(full_minutes)]
obs_in_full = obs[as_utc_series(obs.minute).isin(full_minutes).values]
aggregated_rows = int(both._merge.isin(["both", "left_only"]).sum())
stream_row_flow = pd.DataFrame([
    ("Setelah Extract", f"{len(in_full)} pesan; {int(in_full.entitas.sum()):,} entitas",
     "Snapshot feed di Kafka untuk menit lengkap"),
    ("Setelah Transform", f"{len(obs_in_full):,} observasi",
     "Satu observasi per trip yang masih berjalan per snapshot"),
    ("Setelah Transform (agregat)", f"{aggregated_rows:,} baris",
     "Agregat menit × rute × arah"),
    ("Setelah Load", f"{len(pg_minute):,} baris",
     "Baris etl.gtfs_delay_minute di PostgreSQL untuk menit yang sama"),
], columns=["Tahapan", "Jumlah", "Keterangan"])
stream_row_flow

# %%
consumer.close()
all_tests = pd.concat([test_table.assign(Jalur="Batch"),
                       stream_test_table.assign(Jalur="Streaming")])
print(all_tests.groupby("Jalur").Status.value_counts().unstack(fill_value=0).to_string())
passed = (all_tests.Status == "Berhasil").sum()
failed = (all_tests.Status == "Gagal").sum()
print(f"\nTotal uji: {len(all_tests)}, berhasil: {passed}, gagal: {failed}")
