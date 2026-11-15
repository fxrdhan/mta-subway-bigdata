# Tugas Minggu 10 — Implementasi ETL Pipeline

Implementasi rancangan ETL Minggu 9 untuk data MTA Subway, dijalankan di klaster Kubernetes laboratorium
(namespace `mta-pipeline`).

| Jalur | Extract | Transform | Load |
|---|---|---|---|
| Batch | SODA API → Parquet per hari di area *staging* | DuckDB: 12 aturan kualitas, pembersihan, fitur, agregasi | PostgreSQL skema `etl`, *upsert* per bulan |
| Streaming | `producer.py`: 8 feed GTFS-RT tiap 30 detik → Kafka | `spark_etl.py`: Spark Structured Streaming, keterlambatan vs jadwal statis | PostgreSQL `etl.gtfs_*` (*exactly-once*) dan MongoDB `mta.arrivals`/`mta.alerts` |

## Isi folder

| Berkas | Keterangan |
|---|---|
| `ETL_Pipeline.py` | Sumber *notebook* (format jupytext *percent*) |
| `ETL_Pipeline.ipynb` | *Notebook* beserta hasil eksekusinya di klaster (dibuat dari `ETL_Pipeline.py` dengan jupytext) |
| `screenshots/` | Tangkapan hasil eksekusi dan diagram implementasi |
| `streaming/producer.py` | Extract jalur streaming (Deployment `mta-gtfs-producer`) |
| `streaming/spark_etl.py` | Transform dan Load jalur streaming (Deployment `mta-spark-streaming`); fungsi `decode()` dan `aggregate()` juga dipakai *notebook* |
| `k8s/streaming.yaml` | Kafka (KRaft), MongoDB, *producer*, dan Spark |
| `k8s/etl-notebook-job.yaml` | Job yang mengeksekusi *notebook* dengan `jupyter nbconvert --execute` |
| `diagram/make_diagram.py` | Diagram implementasi (gaya slide Minggu 9) |

## Menjalankan

Secret `mta-postgres` (username, password) dan `mta-mongo` (username, password, uri) harus sudah ada di namespace.

```bash
kubectl -n mta-pipeline create configmap streaming-code --from-file=streaming/producer.py --from-file=streaming/spark_etl.py
```

```bash
kubectl apply -f k8s/streaming.yaml
```

```bash
kubectl -n mta-pipeline create configmap etl-notebook --from-file=ETL_Pipeline.ipynb
```

```bash
kubectl apply -f k8s/etl-notebook-job.yaml
```

Job menulis `ETL_Pipeline_executed.ipynb` dan `output/transformed_dataset.csv` di `/work`, lalu menunggu dua jam agar
berkas dapat disalin dengan `kubectl cp`. Variabel `ETL_DAY_LIMIT` membatasi jumlah hari untuk uji cepat.

Dokumentasi lengkap: `docs/TugasMinggu10_BIG-DATA_TK47G06_101032330094-101032300131.pdf`. Paket pengumpulan dibuat di `dist/` (tidak di-commit).
