"""Screenshots of the executed notebook (rendered to HTML).
Usage: python nb_shots.py <executed.html> [out_dir]   (out_dir defaults to tugas_minggu10/screenshots)"""
import sys
from pathlib import Path

import shots

HTML = Path(sys.argv[1])
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parents[2] / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)
O = lambda marker: (marker, "out")

SPECS = {
    "01_extract.png": [[O("server = server_fingerprints()")], [O("months = plan_months(server, stored)")],
                       [O("extracted_rows = extract(days_to_extract, server)")]],
    "02_initial_check.png": [[O("n_rows = duck_value")], [O("Contoh 5 baris data hasil ekstraksi")],
                             [O("dup_keys = count_duplicates(KEY_COLUMNS)")], [O("summary_before = {")]],
    "03_inspection.png": [[O("inspection = pd.DataFrame")], [O("check_blocking(violations)")]],
    "04_comparison.png": [[O("validation = validate(")], [O("comparison = pd.DataFrame")]],
    "05_load.png": [[O("loaded = load(months")], [O("stored_counts = {")]],
    "06_consistency.png": [["consistency = pd.DataFrame"]],
    "07_pipeline_run.png": [[O("second = run_etl()")], ["run_log = pg_frame"]],
    "08_tests.png": [[O("test_table = pd.DataFrame(tests")], [O("row_flow = pd.DataFrame")], [O("Jumlah uji: {len(test_table)}")]],
    "12_stream_transform.png": [[O("agg = spark_etl.aggregate(window)")], [O("validation_stream = {")], [O("comparison_stream = pd.DataFrame")]],
    "13_stream_consistency.png": [[O("consistency_stream = (")]],
    "16_stream_tests.png": [[O("stream_test_table = pd.DataFrame")], [O("stream_row_flow = pd.DataFrame")], [O("all_tests = pd.concat")]],
}
WIDE = {"02_initial_check.png": 1720}
for name, groups in SPECS.items():
    shots.notebook(HTML, OUT / name, groups, width=WIDE.get(name, 1180))
shots.close()
