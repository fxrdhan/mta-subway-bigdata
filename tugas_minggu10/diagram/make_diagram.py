# Implementation diagram for Tugas Minggu 10: what actually runs in the lab cluster (batch Job and streaming services).
# Reuses the slide style of the Week 9 diagrams (docs/diagrams/pipeline_etl_elt/slide/make_slides.py).
# Usage: python make_diagram.py  -> writes implementasi_etl.html/.png next to this file.
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SLIDE = HERE.parents[1] / "docs" / "diagrams" / "pipeline_etl_elt" / "slide"
sys.path.insert(0, str(SLIDE))
import make_slides as ms  # noqa: E402

ms.HERE = SLIDE  # icons are read from the Week 9 folder

# Figures from the executed notebook (ETL_Pipeline_executed.ipynb).
N = {
    "days": "674 hari", "rows": "47,7 juta baris", "facts": "326.611 baris fakta",
    "csv": "transformed_dataset.csv", "tests_batch": 14, "tests_stream": 15,
}


def build(n=N):
    bw, bh = 200, 172
    batch = ms.lane("JALUR BATCH · Job <b>etl-notebook</b> (Jupyter nbconvert) · sekali jalan, inkremental",
                    ms.box("orange", "download", "Extract", "requests, 6 thread; <i>fingerprint</i> per hari; Parquet per hari di <i>staging</i>",
                           badge=1, width=bw, height=bh, sm=True)
                    + ms.ARROW
                    + ms.box("green", "settings", "Transform", "DuckDB: 12 aturan kualitas, <code>clean.parquet</code>, 3 fakta + 2 dimensi",
                             badge=2, width=bw, height=bh, sm=True)
                    + ms.ARROW
                    + ms.box("teal", "upload", "Load", "psycopg COPY; ganti per bulan dalam satu transaksi; <i>upsert</i> dimensi",
                             badge=3, width=bw, height=bh, sm=True, bid="load"))
    stream = ms.lane("JALUR STREAMING · Deployment dan StatefulSet · terus-menerus, <i>micro-batch</i> 30 detik",
                     ms.box("orange", "radio-tower", "Extract", "<b>mta-gtfs-producer</b>: 8 feed tiap 30 detik, confluent-kafka",
                            badge=1, width=bw, height=bh, sm=True)
                     + ms.ARROW
                     + ms.box("slate", "server", "Kafka", "<b>mta-kafka</b> (KRaft): topik mta-gtfs-stream, retensi 24 jam",
                              width=bw, height=bh, sm=True, bid="kafka")
                     + ms.ARROW
                     + ms.box("green", "zap", "Transform + Load", "<b>mta-spark-streaming</b> (Spark 4.2): snapshot ganda dibuang, keterlambatan vs jadwal, agregasi",
                              badge=2, width=bw, height=bh, sm=True, bid="spark"))
    cluster = ('<div class="cont gray" style="padding-top:38px"><div class="clbl">Klaster Kubernetes laboratorium · namespace mta-pipeline</div>'
               f'<div class="col" style="gap:16px">{batch}{stream}</div></div>')
    store = ('<div class="col" style="gap:22px;align-items:center">'
             + ms.cyl("warehouse", "PostgreSQL 16", "skema <b>etl</b>",
                      ["fact_ridership_* (3)", "dim_station, dim_date", "ingest_day, quality_month", "pipeline_run",
                       "gtfs_delay_minute / hourly", "gtfs_stream_batch"], width=230, bid="pg")
             + ms.box("ink", "file-json", "MongoDB 8", "<b>mta.arrivals</b>, <b>mta.alerts</b> · TTL 7 hari", width=230, sm=True, bid="mongo")
             + ms.box("dark", "archive", "Dataset CSV", f"ekspor {n['csv']} (harian × stasiun)", width=230, sm=True, bid="csv")
             + "</div>")
    main = ms.SOURCES + ms.ARROW + cluster + ms.gap(70) + store
    conns = [["load", "pg", {"ty": 0.3}], ["load", "csv", {"fy": 0.8, "dash": True, "elbow": "late"}],
             ["spark", "pg", {"ty": 0.72, "fy": 0.35}], ["spark", "mongo", {"fy": 0.7}]]
    cards = [
        ("Cara dijalankan", "Batch: Job sekali jalan di node Jupyter. Stream: tiga layanan yang dimulai ulang otomatis bila gagal."),
        ("Volume data", f"Batch: {n['days']}, {n['rows']} menjadi {n['facts']}. Stream: 16 pesan per menit."),
        ("Ketahanan", "<i>Fingerprint</i> per hari, transaksi per bulan, <i>checkpoint</i> Spark, log <code>batch_id</code> (<i>exactly-once</i>)."),
        ("Pencatatan", "<code>etl.pipeline_run</code>, <code>etl.quality_month</code>, <code>etl.gtfs_stream_batch</code>, log tiap tahap."),
        ("Pengujian", f"{n['tests_batch']} uji batch dan {n['tests_stream']} uji streaming di notebook; hasil dibaca ulang dari target."),
    ]
    return ms.page("Implementasi ETL Pipeline Data Subway MTA", "Yang <em>benar-benar dijalankan</em> di klaster laboratorium",
                   main, cards, "#2E3A4B", main_h=600, conns=conns)


if __name__ == "__main__":
    import subprocess
    import tempfile
    from urllib.parse import quote
    out = HERE / "implementasi_etl.html"
    out.write_text(build())
    with tempfile.TemporaryDirectory() as profile:
        subprocess.run([ms.CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--user-data-dir={profile}",
                        "--force-device-scale-factor=2", f"--window-size={ms.W},935", "--virtual-time-budget=10000",
                        f"--screenshot={HERE / 'implementasi_etl.png'}", "file://" + quote(str(out))], check=True, capture_output=True)
    print("wrote implementasi_etl.png")
