"""Cetak ringkasan 10 baris ke terminal. Semua angka dibaca dari file hasil (state/, reports/*.json, logs/)."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
J = lambda p: json.loads((ROOT / p).read_text())  # noqa: E731

fc, cut = J("data/state/final_check.json"), J("data/state/cutoff.json")
prof, st, rel, val = J("reports/profile.json"), J("reports/stream_checks.json"), J("reports/related_dataset.json"), J("reports/snapshot_value_check.json")
log = (ROOT / "logs/download_full.log").read_text()
real = float(re.search(r"([\d.]+)\s+real", log).group(1))
rss = int(re.search(r"(\d+)\s+maximum resident set size", log).group(1))
dry = st["dryrun_by_day"]
la, lat, rv = st["late_arrival"], st["latency"], st["revisions"]
real_cycle = J("data/state/stream_watermark.json")["last_cycle"]

lines = [
    f"1. Snapshot: {fc['parquet_rows']:,} baris Parquet == count server sampai cutoff {cut['cutoff_ts']} -> {'COCOK' if fc['match'] else 'TIDAK COCOK'}; "
    f"{val['n_days_with_difference']} dari {val['n_days_server']} hari beda pada count/sum(ridership)/sum(transfers).",
    f"2. Bentuk: {prof['n_cols']} kolom, {prof['n_files']} file Parquet zstd = {prof['parquet_bytes'] / 1e6:,.1f} MB; null = {sum(prof['nulls'].values())}; duplikat key = {prof['key_uniqueness']['n_key_duplikat']}.",
    f"3. Unduh: {real:,.0f} s (~{real / 60:.1f} menit, 20 bulan + 1 bulan uji terpisah), RSS puncak {rss / 1e9:.2f} GB; profil RSS puncak {prof['peak_rss_bytes'] / 1e9:.2f} GB (batas 16 GB).",
    f"4. Kualitas: ridership min {prof['stats_ridership']['min']:.0f} / max {prof['stats_ridership']['max']:,.0f} / median {prof['stats_ridership']['median']:.0f}; "
    f"transfers negatif {prof['stats_transfers']['n_negatif']} baris; jam hilang {len(prof['missing_hours'])} (DST 2026-03-08 02:00); kepadatan grid {prof['grid_density'][0]['pct_terisi']}%.",
    f"5. Streaming (siklus nyata terakhir): {real_cycle['rows_written']} baris ditulis di atas watermark {real_cycle['watermark_before']}; dry-run cutoff-3 hari: {sum(d['baris'] for d in dry):,} baris, ridership null {sum(d['ridership_null'] for d in dry)}, "
    f"identik dengan snapshot (selisih {st['stream_vs_snapshot_window']['stream_minus_snapshot']}/{st['stream_vs_snapshot_window']['snapshot_minus_stream']}).",
    f"6. Skema API == Parquet snapshot == Parquet stream: {st['names_equal_api_vs_parquet'] and st['schema_equal_snapshot_vs_stream']}. Data terbaru {lat['server_max_ts_local']}, lag {lat['lag_now_vs_data_end_hours'] / 24:.1f} hari vs sekarang.",
    f"7. Update: {st['update_evidence']['created_at_loads']['n_distinct_created_at'] - 1} rilis sejak bulk load, tiap hari Rabu (jarak {st['update_evidence']['created_at_loads']['weekly_gap_days_min']}-{st['update_evidence']['created_at_loads']['weekly_gap_days_max']} hari), terakhir {st['update_evidence']['created_at_loads']['last_load']}.",
    f"8. RISIKO STREAMING: {la['late_rows']:,} baris ({la['pct_late']}%) rilis mingguan berstempel >6 hari lebih tua, dan {rv['rows_updated_after_created']:,} baris ({rv['pct_of_total']}%) punya :updated_at > :created_at -> watermark transit_timestamp melewatkan baris terlambat/revisi.",
    f"9. Dataset terkait {rel['related_id']}: {rel['count']['rows']:,} baris ({rel['count']['min_ts'][:10]} s/d {rel['count']['max_ts'][:10]}), skema sama={rel['schema']['regular_columns_identical_names_types_order']}, "
    f"stasiun sama {rel['values']['stations_common']}/{rel['values']['stations_new']}; menyambung tanpa celah, gabungan {rel['join']['total_rows_if_combined']:,} baris.",
    "10. TIDAK TERVERIFIKASI: lisensi (field kosong), kadensi update sebelum 2026-03-11, zona waktu eksplisit, apakah nilai baris lama benar-benar direvisi, isi rilis berikutnya. Laporan: reports/mta_data_report.md",
]
print("\n".join(lines))
