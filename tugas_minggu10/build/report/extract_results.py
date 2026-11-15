"""Extracts the figures used by Dokumentasi_ETL from the executed notebook into results10.json.
Usage: python extract_results.py <executed.ipynb> <results10.json> [--stream-only]"""
import ast
import datetime as dt
import io
import json
import re
import sys

import pandas as pd

nb = json.load(open(sys.argv[1]))
STREAM_ONLY = "--stream-only" in sys.argv
cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
MONTHS = "Januari Februari Maret April Mei Juni Juli Agustus September Oktober November Desember".split()


def cell(marker):
    for c in cells:
        if marker in "".join(c["source"]):
            return c
    raise KeyError(marker)


def text(marker):
    out = []
    for o in cell(marker).get("outputs", []):
        if o["output_type"] == "stream":
            out.append("".join(o["text"]))
        elif o["output_type"] in ("execute_result", "display_data") and "text/html" not in o["data"]:
            out.append("".join(o["data"].get("text/plain", "")))
    return "\n".join(out)


def frame(marker, k=-1):
    htmls = ["".join(o["data"]["text/html"]) for o in cell(marker).get("outputs", [])
             if o["output_type"] in ("execute_result", "display_data") and "text/html" in o["data"]]
    return pd.read_html(io.StringIO(htmls[k]), index_col=0, thousands=None, keep_default_na=False)[0]


def series(block):
    """Parses a printed pandas Series ('key   value' lines)."""
    out = {}
    for line in block.splitlines():
        m = re.match(r"^(\S.*?)\s{2,}(\S.*)$", line)
        if m and not line.startswith(("Name:", "dtype")):
            out[m.group(1).strip()] = m.group(2).strip()
    return out


num = lambda s: int(str(s).replace(",", "").replace(".", "")) if re.fullmatch(r"[\d,\.]+", str(s)) and "," in str(s) or str(s).isdigit() else float(str(s).replace(",", ""))
intc = lambda s: int(float(str(s).replace(",", "")))
day_id = lambda d: f"{d.day} {MONTHS[d.month - 1]} {d.year}"
rows_of = lambda df: [[str(v) for v in r] for r in df.itertuples(index=False)]
errors = [o for c in cells for o in c.get("outputs", []) if o["output_type"] == "error"]
assert not errors, f"notebook has errors: {[e['ename'] for e in errors]}"

times = [c["metadata"].get("execution", {}) for c in cells]
start = min(pd.Timestamp(t["iopub.execute_input"]) for t in times if "iopub.execute_input" in t)
end = max(pd.Timestamp(t["shell.execute_reply"]) for t in times if "shell.execute_reply" in t)
R = {"run_date_id": day_id(start.date()), "total_min": round((end - start).total_seconds() / 60)}

if not STREAM_ONLY:
    B = {"workers": 6}
    m = re.search(r"Sumber dapat diakses: (\d+) hari \((\S+) s\.d\. (\S+)\), ([\d,]+) baris", text("server = server_fingerprints()"))
    B["server_days"], B["server_rows"] = int(m.group(1)), intc(m.group(4))
    first, last = dt.date.fromisoformat(m.group(2)), dt.date.fromisoformat(m.group(3))
    B["first_day_id"], B["last_day_id"] = day_id(first), day_id(last)
    m = re.search(r"perlu ekstraksi (\d+) hari pada (\d+) bulan", text("months = plan_months(server, stored)"))
    B["days"], B["months_loaded"] = int(m.group(1)), int(m.group(2))
    t = text("extracted_rows = extract(days_to_extract, server)")
    B["rows"] = intc(re.search(r"Data berhasil diekstraksi: ([\d,]+) baris", t).group(1))
    B["extract_min"] = float(re.search(r"Waktu ekstraksi: ([\d.]+) menit", t).group(1))
    m = re.search(r"ukuran berkas staging: (\d+) MB", t)
    B["staging_mb"] = int(m.group(1)) if m else 0
    t = text("n_rows = duck_value")
    B["cols"] = int(re.search(r"jumlah kolom: (\d+)", t).group(1))
    t = text("dup_keys = count_duplicates(KEY_COLUMNS)")
    m = re.search(r"Baris duplikat persis: ([\d,]+) \| duplikat kunci .*?: ([\d,]+)", t)
    B["dup_rows"], B["dup_keys"] = intc(m.group(1)), intc(m.group(2))
    sb = series(text("summary_before = {"))
    B["missing"], B["type_before"] = intc(sb["nilai_kosong"]), sb["tipe ridership/transfers"]
    B["total_ridership"], B["total_transfers"] = intc(sb["total ridership"]), intc(sb["total transfers"])
    insp = frame("inspection = pd.DataFrame")
    B["inspection"] = [[k, intc(r["pelanggaran"]), r["dampak"]] for k, r in insp.iterrows()]
    B["counts"] = {k: intc(r["pelanggaran"]) for k, r in insp.iterrows()}
    t = text("sizes = transform()")
    sizes = ast.literal_eval(re.search(r"ditransformasi: (\{.*\})", t).group(1))
    B["sizes"] = {k: intc(v) for k, v in sizes.items()}
    B["validation"] = [[k, v == "True"] for k, v in series(text("validation = {")).items()]
    comp = frame("comparison = pd.DataFrame")
    B["comparison"] = [[k, str(r["sebelum"]), str(r["sesudah"])] for k, r in comp.iterrows()]
    B["total_transfers_after"] = intc(comp.loc["total transfers", "sesudah"])
    t = text("loaded = load(months")
    B["load_s"] = int(re.search(r"Waktu load: (\d+) detik", t).group(1))
    loaded = {k: intc(v) for k, v in series(t).items() if k.startswith(("fact", "dim"))}
    B["loaded_facts"] = sum(v for k, v in loaded.items() if k.startswith("fact"))
    stored = {k: intc(v) for k, v in series(text("stored_counts = {")).items() if re.fullmatch(r"[\d,]+", v)}
    B["stored"], B["stations"] = stored, stored["dim_station"]
    cons = frame("consistency = pd.DataFrame")
    B["consistency"] = [[r["tabel"], intc(r["baris hasil transformasi"]), intc(r["baris di PostgreSQL"]),
                         intc(r["total ridership transformasi"]), intc(r["total ridership PostgreSQL"]), str(r["sama"]) == "True"]
                        for _, r in cons.iterrows()]
    m = re.search(r"\(([\d.]+) MB, ([\d,]+) baris\)", text("csv_path = OUTPUT"))
    B["csv_mb"], B["csv_rows"] = float(m.group(1)), intc(m.group(2))
    second = ast.literal_eval(re.search(r"(\{'run_id'.*\})", text("second = run_etl()"), re.S).group(1))
    if second["bulan"]:
        B["second_text"] = (f"*Run* kedua hanya mengekstraksi ulang {second['baris_extract']:,} baris pada bulan "
                            f"{', '.join(second['bulan'])} yang direvisi MTA sejak *run* pertama.").replace(",", ".")
    else:
        B["second_text"] = ("Karena *fingerprint* seluruh hari tidak berubah sejak *run* pertama, *run* kedua tidak mengekstraksi "
                            "data apa pun dan selesai tanpa mengubah isi gudang data, sehingga terbukti aman dijalankan ulang.")
    runs = frame("run_log = pg_frame")
    B["runs"] = [[str(r["mulai"]), str(r["selesai"]), r["status"], r["tahap"], intc(r["hari"]) if str(r["hari"]) not in ("", "nan") else 0,
                  intc(r["baris"]) if str(r["baris"]) not in ("", "nan") else 0, str(r["bulan"]),
                  str(r["keterangan"])] for _, r in runs.iterrows()]
    B["tests"] = rows_of(frame("test_table = pd.DataFrame(tests"))
    B["row_flow"] = rows_of(frame("row_flow = pd.DataFrame"))
    R["batch"] = B

S = {}
k = series(text("WINDOW = 320"))
S["kafka_stored"] = intc(k["pesan tersimpan"])
S["window_range"] = k["jendela pemeriksaan"]
sm = series(text("summary_stream = pd.Series"))
S["window_msgs"] = intc(sm["pesan (snapshot feed)"])
for key, col in [("entities", "entitas"), ("trip_updates", "trip_update"), ("vehicles", "vehicle"), ("alerts_raw", "alert"),
                 ("stop_time_updates", "stop_time_update"), ("arah_kosong", "arah_kosong"), ("status_kosong", "status_kosong"),
                 ("prediksi_lampau", "prediksi_lampau"), ("snapshot_berulang", "snapshot_berulang")]:
    S[key] = intc(sm[col])
S["mb_per_hour"] = round(float(sm["ukuran total (MB)"]) / (S["window_msgs"] / 16) * 60, 1)
m = re.search(r"ditransformasi: (\d+) observasi", text("agg = spark_etl.aggregate(window)"))
S["obs"] = int(m.group(1))
comp = frame("comparison_stream = pd.DataFrame")
S["comparison"] = [[k, str(r.iloc[0]), str(r.iloc[1])] for k, r in comp.iterrows()]
S["matched_pct"] = int(re.search(r"ada untuk (\d+)%", comp.loc["keterlambatan"].iloc[1]).group(1))
counts = series(text("stream_counts = {"))
S["pg_minute"], S["pg_hourly"] = intc(counts["etl.gtfs_delay_minute"]), intc(counts["etl.gtfs_delay_hourly"])
S["mongo_arrivals"], S["mongo_alerts"] = intc(counts["mta.arrivals"]), intc(counts["mta.alerts"])
t = text("consistency_stream = (")
m = re.search(r"berbeda: (\d+) dari (\d+) \| kedatangan ditemukan di MongoDB: (\d+) dari (\d+)", t)
S["keys_diff"], S["keys_compared"], S["arrivals_found"], S["arrival_ids"] = map(int, m.groups())
S["full_minutes"] = len(frame("consistency_stream = ("))
integ = series(text("integration_stream = pd.Series"))
S["batches"], S["lag"] = intc(integ["micro-batch tercatat"]), intc(integ["lag Kafka saat ini (pesan)"])
S["avg_batch_s"], S["latency_s"] = float(integ["rata-rata durasi micro-batch (s)"]), float(integ["latensi median feed → PostgreSQL (s)"])
S["tests"] = rows_of(frame("stream_test_table = pd.DataFrame"))
S["row_flow"] = rows_of(frame("stream_row_flow = pd.DataFrame"))
R["stream"] = S
json.dump(R, open(sys.argv[2], "w"), indent=1, ensure_ascii=False)
print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items() if not isinstance(vv, list)}) for k, v in R.items()},
                 indent=1, ensure_ascii=False)[:3000])
