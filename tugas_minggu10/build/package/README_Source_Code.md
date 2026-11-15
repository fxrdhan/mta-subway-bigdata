# Source Code — Tugas Minggu 10 (Kelompok Smile)

- `ETL_Pipeline.ipynb`: notebook ETL (Bagian A batch, Bagian B streaming) beserta hasil eksekusinya di klaster laboratorium.
- `streaming/producer.py`: Extract jalur streaming (8 feed GTFS-Realtime → Kafka).
- `streaming/spark_etl.py`: Transform dan Load jalur streaming (Spark Structured Streaming → PostgreSQL dan MongoDB).
  Notebook memakai fungsi `decode()` dan `aggregate()` dari modul ini.
- `k8s/streaming.yaml`: Kafka, MongoDB, producer, dan Spark di Kubernetes.
- `k8s/etl-notebook-job.yaml`: Job yang mengeksekusi notebook dengan `jupyter nbconvert --execute`.

Kredensial dibaca dari Secret Kubernetes (`mta-postgres`, `mta-mongo`) melalui variabel lingkungan.
