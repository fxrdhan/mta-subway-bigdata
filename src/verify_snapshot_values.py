"""Verifikasi nilai snapshot terhadap server (per hari) dan deteksi revisi antar rilis.

Membandingkan count(*), sum(ridership), sum(transfers) per hari: server (agregat SODA, transit_timestamp <= cutoff)
vs Parquet lokal. Angka disimpan sebagai sidik jari harian (data/state/daily_fingerprint.json); jalankan ulang
setelah rilis mingguan berikutnya untuk melihat hari mana yang berubah di server (revisi baris lama).

Keluaran: reports/snapshot_value_check.md, reports/snapshot_value_check.json
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import duckdb

import soda

ROOT = Path(__file__).resolve().parent.parent
SNAP = str(ROOT / "data" / "parquet" / "*" / "*" / "*.parquet")
FP = ROOT / "data" / "state" / "daily_fingerprint.json"
cutoff = json.loads((ROOT / "data" / "state" / "cutoff.json").read_text())["cutoff_ts"]

srv_rows = soda.query_json(
    {
        "$select": "date_trunc_ymd(transit_timestamp) AS d, count(*) AS n, sum(ridership) AS r, sum(transfers) AS t",
        "$where": f"transit_timestamp <= {soda.soql_ts(cutoff)}",
        "$group": "d", "$order": "d", "$limit": 5000,
    }
)
srv = {r["d"][:10]: (int(r["n"]), float(r["r"]), float(r["t"])) for r in srv_rows}
con = duckdb.connect()
loc = {
    str(d): (n, float(r), float(t))
    for d, n, r, t in con.sql(
        f"SELECT transit_timestamp::DATE, count(*), sum(ridership), sum(transfers) FROM read_parquet('{SNAP}', hive_partitioning=false) GROUP BY 1"
    ).fetchall()
}
days = sorted(set(srv) | set(loc))
diff = [
    {"day": d, "server": srv.get(d), "local": loc.get(d)}
    for d in days
    if srv.get(d) != loc.get(d)
]
res = {
    "checked_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    "cutoff_ts": cutoff, "n_days_server": len(srv), "n_days_local": len(loc),
    "n_days_with_difference": len(diff), "differences": diff[:50],
    "total_rows_server": sum(v[0] for v in srv.values()), "total_rows_local": sum(v[0] for v in loc.values()),
    "sum_ridership_server": sum(v[1] for v in srv.values()), "sum_ridership_local": sum(v[1] for v in loc.values()),
    "sum_transfers_server": sum(v[2] for v in srv.values()), "sum_transfers_local": sum(v[2] for v in loc.values()),
}
prev = json.loads(FP.read_text()) if FP.exists() else None
if prev:
    changed = [d for d in prev["days"] if prev["days"][d] != list(srv.get(d, ()))]
    res["changed_on_server_since_first_fingerprint"] = {"since": prev["created_at_utc"], "n_days_changed": len(changed), "days": changed[:100]}
else:
    FP.write_text(json.dumps({"created_at_utc": res["checked_at_utc"], "cutoff_ts": cutoff, "days": {d: list(v) for d, v in srv.items()}}))

md = ["# Verifikasi nilai snapshot vs server (per hari)", "",
      f"Dijalankan {res['checked_at_utc']} dengan cutoff {cutoff}. Server = agregat SODA; lokal = agregat DuckDB di Parquet.", "",
      f"- Hari di server: {res['n_days_server']}; di Parquet: {res['n_days_local']}",
      f"- Hari dengan perbedaan (count, sum ridership, sum transfers): **{res['n_days_with_difference']}**",
      f"- Total baris server/lokal: {res['total_rows_server']:,} / {res['total_rows_local']:,}",
      f"- sum(ridership) server/lokal: {res['sum_ridership_server']:,.0f} / {res['sum_ridership_local']:,.0f}",
      f"- sum(transfers) server/lokal: {res['sum_transfers_server']:,.0f} / {res['sum_transfers_local']:,.0f}", ""]
if diff:
    md += ["| hari | server (n, ridership, transfers) | lokal |", "|---|---|---|"] + [f"| {x['day']} | {x['server']} | {x['local']} |" for x in diff[:50]]
if prev:
    c = res["changed_on_server_since_first_fingerprint"]
    md += ["", f"Perubahan di server sejak sidik jari pertama ({c['since']}): **{c['n_days_changed']} hari** berubah: {', '.join(c['days'][:30])}"]
else:
    md += ["", f"Sidik jari harian pertama disimpan di `{FP.relative_to(ROOT)}`. Jalankan ulang skrip ini setelah rilis berikutnya "
           "untuk mendeteksi hari-hari lama yang direvisi server."]
(ROOT / "reports" / "snapshot_value_check.md").write_text("\n".join(md) + "\n")
(ROOT / "reports" / "snapshot_value_check.json").write_text(json.dumps(res, indent=1))
print("\n".join(md))
