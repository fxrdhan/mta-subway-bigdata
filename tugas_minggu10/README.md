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
| `build/shots/` | Pembuat tangkapan layar dari *notebook* dan dari keluaran perintah di klaster |
| `build/report/` | Pembuat dokumentasi DOCX/PDF (docx-js, daftar isi dua tahap, pemeriksaan tabel terpotong) |
| `build/package/` | Penyusun paket pengumpulan `dist/Tugas_Minggu10_Smile.zip` |

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

## Membangun ulang tangkapan layar, dokumentasi, dan paket

Perintah dijalankan dari folder `tugas_minggu10/build`; hasil antara ditulis ke `build/out/` (tidak di-commit).
Diperlukan `uv`, Node.js, LibreOffice (`soffice`), dan Google Chrome (dipakai Playwright).

Tangkapan layar *notebook* (01–08, 12, 13, 16) dari `ETL_Pipeline.ipynb` yang sudah dieksekusi:

```bash
uvx --from jupyter-core --with nbconvert --with jupyterlab-pygments jupyter nbconvert --to html ../ETL_Pipeline.ipynb --output-dir out
```

```bash
uv run --no-project --with playwright --with pillow python shots/nb_shots.py out/ETL_Pipeline.html
```

Tangkapan layar klaster: 09 diambil selama Job `etl-notebook` berjalan; 10, 11, 14, dan 15 memerlukan *pod* bantu `etl-psql`.

```bash
uv run --no-project --with playwright --with pillow python shots/term_shots.py job
```

```bash
kubectl apply -f shots/etl-psql.yaml
```

```bash
uv run --no-project --with playwright --with pillow python shots/term_shots.py
```

```bash
kubectl -n mta-pipeline delete pod etl-psql
```

```bash
cp ../diagram/implementasi_etl.png ../screenshots/00_diagram_implementasi.png
```

Dokumentasi: angka diambil dari keluaran *notebook*, lalu `make10.py` menyusun DOCX dua kali (tahap kedua mengisi nomor
halaman daftar isi), mengonversinya ke PDF, dan memeriksa bahwa tidak ada tabel yang terpotong antarhalaman.

```bash
uv run --no-project --with pandas --with lxml python report/extract_results.py ../ETL_Pipeline.ipynb out/results10.json
```

```bash
npm ci --prefix report
```

```bash
uv run --no-project --with pypdf python report/make10.py ../.. out/results10.json ../screenshots out
```

```bash
cp out/Dokumentasi_ETL.pdf ../../docs/TugasMinggu10_BIG-DATA_TK47G06_101032330094-101032300131.pdf
```

```bash
cp out/Dokumentasi_ETL.docx ../../docs/TugasMinggu10_BIG-DATA_TK47G06_101032330094-101032300131.docx
```

Paket pengumpulan, dengan `transformed_dataset.csv` yang disalin dari Job (`kubectl cp <pod>:/work/output/transformed_dataset.csv`):

```bash
sh package/package.sh transformed_dataset.csv
```
