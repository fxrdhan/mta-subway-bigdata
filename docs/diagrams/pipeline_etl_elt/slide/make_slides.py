# Builds the ETL and ELT diagrams in the visual style of the Week 9 lecture slides
# (Lato, dark source boxes, numbered coloured steps, dashed containers, five question cards).
# Each design covers both the batch ridership path and the GTFS-RT streaming path.
# Usage: python make_slides.py  -> writes 01_etl.html/.png and 02_elt.html/.png here.
# Rendering uses headless Google Chrome; the Lato font is loaded from Google Fonts.
# Icons: Lucide (lucide-static 0.460.0, ISC licence), stored in icons/.
import json
import re
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
W, H = 1600, 930


def icon(name, size=40, cls="ic"):
    svg = (HERE / "icons" / f"{name}.svg").read_text()
    inner = re.search(r"<svg[^>]*>(.*)</svg>", svg, re.S).group(1)
    return (f'<svg class="{cls}" width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
            f'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">{inner}</svg>')


ARROW = ('<div class="arrow"><svg width="30" height="14" viewBox="0 0 30 14"><line x1="2" y1="7" x2="22" y2="7" '
         'stroke="#445469" stroke-width="2.4"/><path d="M20 2 L28 7 L20 12 Z" fill="#445469"/></svg></div>')

CSS = """
* { box-sizing: border-box; }
body { margin: 0; width: %dpx; background: #fff; color: #2E3A4B;
       font-family: 'Lato', 'Helvetica Neue', Arial, sans-serif; }
.wrap { padding: 24px 36px 0; }
h1 { margin: 0; text-align: center; font-size: 42px; font-weight: 900; letter-spacing: -0.3px; color: #2E3A4B; }
.sub { margin-top: 4px; text-align: center; font-size: 22px; color: #445469; }
.sub em { font-style: normal; font-weight: 700; color: #C00000; }
.bar { width: 80px; height: 5px; margin: 10px auto 0; background: #C00000; }
.main { position: relative; display: flex; align-items: center; justify-content: center; margin-top: 14px; }
.wires { position: absolute; left: 0; top: 0; width: 100%%; height: 100%%; overflow: visible; pointer-events: none; }
.gap { flex: none; }
.sources { width: 215px; flex: none; display: flex; flex-direction: column; gap: 12px;
           padding-right: 14px; border-right: 6px solid #DDE3EA; }
.sources .lbl { text-align: center; font-size: 13px; font-weight: 700; letter-spacing: 1.6px; color: #6B7686; }
.src { display: flex; align-items: center; gap: 10px; padding: 12px; border-radius: 12px; background: #222A34; color: #fff; }
.src svg { flex: none; }
.src b { display: block; font-size: 16px; font-weight: 900; }
.src span { display: block; margin-top: 2px; font-size: 12.5px; color: #C9D1DC; line-height: 1.25; }
.arrow { width: 32px; flex: none; display: flex; justify-content: center; }
.box { position: relative; display: flex; flex-direction: column; align-items: center; justify-content: center;
       gap: 3px; padding: 18px 14px; border-radius: 20px; color: #fff; text-align: center; }
.box h3 { margin: 2px 0 0; font-size: 21px; font-weight: 900; line-height: 1.15; }
.box p { margin: 0; font-size: 15px; line-height: 1.3; }
.box.sm { padding: 12px 12px; border-radius: 16px; }
.box.sm h3 { font-size: 18px; }
.box.sm p { font-size: 13.5px; }
.badge { position: absolute; top: -12px; left: -12px; width: 34px; height: 34px; border-radius: 50%%;
         background: #fff; display: flex; align-items: center; justify-content: center;
         font-size: 17px; font-weight: 900; box-shadow: 0 1px 3px rgba(0,0,0,.15); }
.orange { background: #F29B26; } .orange .badge { color: #F29B26; }
.green { background: #9BBB5C; } .green .badge { color: #6E8E35; }
.teal { background: #1EA185; } .teal .badge { color: #1EA185; }
.slate { background: #445469; } .slate .badge { color: #445469; }
.red { background: #BD392F; } .red .badge { color: #BD392F; }
.dark { background: #222A34; } .dark .badge { color: #222A34; }
.ink { background: #2E3A4B; }
ul.b { margin: 6px 0 0; padding-left: 18px; text-align: left; font-size: 15px; line-height: 1.38; }
.cont { position: relative; padding: 36px 14px 14px; border: 2.5px dashed #9BBB5C; border-radius: 18px; }
.cont .clbl { position: absolute; top: 9px; left: 0; right: 0; text-align: center;
              font-size: 14px; font-weight: 700; color: #6E8E35; }
.cont.gray { border-color: #445469; background: #E4E8EE; }
.cont.gray .clbl { color: #2E3A4B; }
.cyl { position: relative; margin-top: 22px; padding: 44px 12px 30px; background: #445469; color: #fff; text-align: center;
       border-radius: 0 0 50%% 50%% / 0 0 24px 24px; }
.cyl::before { content: ''; position: absolute; top: -22px; left: 0; right: 0; height: 44px; border-radius: 50%%; background: #8D98A9; }
.cyl h3 { margin: 4px 0 2px; font-size: 20px; font-weight: 900; }
.cyl p { margin: 0; font-size: 14px; }
.mono { margin-top: 6px; font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 12px; line-height: 1.4; }
.col { display: flex; flex-direction: column; gap: 12px; }
.row { display: flex; align-items: center; }
.cards { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-top: 16px; }
.card .h { padding: 10px 6px; border-radius: 8px 8px 0 0; color: #fff; text-align: center; font-size: 17px; font-weight: 900; }
.card .t { min-height: 112px; padding: 12px 14px; border-radius: 0 0 8px 8px; background: #F4F6F9;
           font-size: 16px; line-height: 1.4; color: #2E3A4B; }
""" % W


def page(title, sub, main, cards, header_color, main_h=430, conns=()):
    card_html = "".join(f'<div class="card"><div class="h" style="background:{header_color}">{q}</div>'
                        f'<div class="t">{a}</div></div>' for q, a in cards)
    return f"""<!doctype html><html lang="id"><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Lato:wght@400;700;900&family=JetBrains+Mono:wght@400&display=block" rel="stylesheet">
<style>{CSS}</style></head><body><div class="wrap">
<h1>{title}</h1><div class="sub">{sub}</div><div class="bar"></div>
<div class="main" style="height:{main_h}px">{main}<svg class="wires"></svg></div><div class="cards">{card_html}</div></div>
<script>{WIRE_JS}
wire({json.dumps(list(conns))});</script></body></html>"""


def src(ic, name, detail):
    return f'<div class="src">{icon(ic, 26)}<div><b>{name}</b><span>{detail}</span></div></div>'


def sources(*items):
    return '<div class="sources"><div class="lbl">SUMBER</div>' + "".join(items) + "</div>"


def box(color, ic, title, text, badge=None, width=170, height=None, sm=False, extra="", bid=None):
    style = f"width:{width}px;" + (f"height:{height}px;" if height else "")
    b = f'<div class="badge">{badge}</div>' if badge else ""
    size = 30 if sm else 40
    did = f' data-id="{bid}"' if bid else ""
    return (f'<div class="box {color}{" sm" if sm else ""}"{did} style="{style}">{b}{icon(ic, size)}'
            f'<h3>{title}</h3>{f"<p>{text}</p>" if text else ""}{extra}</div>')


def cyl(ic, title, text, mono, width=190, bid=None):
    did = f' data-id="{bid}"' if bid else ""
    return (f'<div class="cyl"{did} style="width:{width}px">{icon(ic, 36)}<h3>{title}</h3><p>{text}</p>'
            f'<div class="mono">{"<br>".join(mono)}</div></div>')


RIDERSHIP = src("train-front", "Hourly Ridership", "SODA API · CSV<br>diperbarui mingguan")
SOURCES = sources(RIDERSHIP,
                  src("radio", "GTFS-Realtime", "8 feed · protobuf<br>tiap 30 detik"),
                  src("calendar-days", "GTFS statis", "jadwal acuan<br>ZIP berisi CSV"))
DASHED = ('<div class="arrow" style="width:{w}px"><svg width="{w}" height="30" viewBox="0 0 {w} 30">'
          '<line x1="2" y1="15" x2="{x2}" y2="15" stroke="#445469" stroke-width="2.4" stroke-dasharray="6 5"/>'
          '<path d="M{x3} 10 L{x4} 15 L{x3} 20 Z" fill="#445469"/>'
          '<text x="{mid}" y="9" text-anchor="middle" font-size="12" fill="#6B7686" font-family="Lato">{label}</text></svg></div>')


def dashed(w, label):
    return DASHED.format(w=w, x2=w - 10, x3=w - 12, x4=w - 2, mid=w // 2, label=label)


def lane(label, inner, gray=False):
    return f'<div class="cont{" gray" if gray else ""}" style="padding-top:34px"><div class="clbl">{label}</div><div class="row">{inner}</div></div>'


def serving(pg_text, ml_from, ml_label, pg_width=165, badge=4):
    """PostgreSQL feeding Streamlit and Grafana; the ML notebook row is fed by `ml_from` (an arrow cell)."""
    return (
        f'<div style="display:grid;grid-template-columns:{pg_width}px 32px 170px;grid-template-rows:repeat(3,auto);row-gap:12px;align-items:center">'
        f'<div style="grid-row:1/3;grid-column:1">{cyl("warehouse", "PostgreSQL", pg_text, [], width=pg_width)}</div>'
        f'<div style="grid-row:1;grid-column:2">{ARROW}</div>'
        f'<div style="grid-row:1;grid-column:3">{box("red", "layout-dashboard", "Streamlit", "dashboard dan prediksi untuk dosen", badge=badge, width=170, sm=True)}</div>'
        f'<div style="grid-row:2;grid-column:2">{ARROW}</div>'
        f'<div style="grid-row:2;grid-column:3">{box("slate", "gauge", "Grafana", "pemantauan resource dan pipeline", width=170, sm=True)}</div>'
        f'<div style="grid-row:3;grid-column:1/3">{ml_from}</div>'
        f'<div style="grid-row:3;grid-column:3">{box("dark", "brain-circuit", "Notebook ML", ml_label, width=170, sm=True)}</div>'
        "</div>")


# Orthogonal connectors between boxes: [from, to, options]. Options: ty/fy = anchor height as a fraction
# of the box (default 0.5), elbow = "mid" | "late" (bend near the target), dash = true, label = text.
WIRE_JS = """
function wire(conns) {
  const main = document.querySelector('.main'), svg = main.querySelector('.wires'), m = main.getBoundingClientRect();
  const ns = 'http://www.w3.org/2000/svg';
  svg.innerHTML = '<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    + '<path d="M0,0 L10,5 L0,10 z" fill="#445469"/></marker></defs>';
  for (const [a, b, o = {}] of conns) {
    const ra = document.querySelector('[data-id="' + a + '"]').getBoundingClientRect();
    const rb = document.querySelector('[data-id="' + b + '"]').getBoundingClientRect();
    const x1 = ra.right - m.left + 2, y1 = ra.top - m.top + ra.height * (o.fy ?? 0.5);
    const x2 = rb.left - m.left - 3, y2 = rb.top - m.top + rb.height * (o.ty ?? 0.5);
    const xm = o.elbow === 'late' ? x2 - 22 : (o.elbow === 'early' ? x1 + 18 : (x1 + x2) / 2);
    const d = Math.abs(y1 - y2) < 1.5 ? `M${x1},${y1} H${x2}` : `M${x1},${y1} H${xm} V${y2} H${x2}`;
    const path = document.createElementNS(ns, 'path');
    path.setAttribute('d', d); path.setAttribute('fill', 'none'); path.setAttribute('stroke', '#445469');
    path.setAttribute('stroke-width', '2.4'); path.setAttribute('marker-end', 'url(#ah)');
    if (o.dash) path.setAttribute('stroke-dasharray', '6 5');
    svg.appendChild(path);
    if (o.label) {
      const t = document.createElementNS(ns, 'text');
      t.setAttribute('x', (x1 + xm) / 2); t.setAttribute('y', y1 - 7); t.setAttribute('text-anchor', 'middle');
      t.setAttribute('font-size', '12'); t.setAttribute('fill', '#6B7686'); t.setAttribute('font-family', 'Lato');
      t.textContent = o.label; svg.appendChild(t);
    }
  }
}
"""


def gap(w):
    return f'<div class="gap" style="width:{w}px"></div>'


def outputs(badge, ml_label, w=170):
    return ('<div class="col" style="gap:16px">'
            + box("red", "layout-dashboard", "Streamlit", "dashboard dan prediksi untuk dosen", badge=badge, width=w, sm=True, bid="st")
            + box("slate", "gauge", "Grafana", "pemantauan resource dan pipeline", width=w, sm=True, bid="gf")
            + box("dark", "brain-circuit", "Notebook ML", ml_label, width=w, sm=True, bid="ml")
            + "</div>")


def etl():
    bw = 168
    batch = lane("JALUR BATCH · harian · staging Python + DuckDB",
                 box("orange", "download", "Extract", "CronJob 01.00 UTC, hari yang berubah", badge=1, width=bw, height=158, sm=True)
                 + ARROW + box("green", "settings", "Transform", "DuckDB: 12 aturan kualitas, tanda DST, agregasi", badge=2, width=bw, height=158, sm=True)
                 + ARROW + box("teal", "upload", "Load", "<i>upsert</i> per bulan, aman diulang", badge=3, width=bw, height=158, sm=True, bid="load"))
    stream = lane("JALUR STREAMING · tiap 30 detik · Kafka + Spark",
                  box("orange", "radio-tower", "Extract", "<i>producer</i> Python, 8 feed", badge=1, width=bw, height=158, sm=True)
                  + ARROW + box("slate", "server", "Kafka", "penampung pesan, retensi 24 jam", width=bw, height=158, sm=True)
                  + ARROW + box("green", "zap", "Transform + Load", "Spark: dekode, gabung jadwal, hitung keterlambatan", badge=2, width=bw, height=158, sm=True, bid="spark"))
    lanes = f'<div class="col" style="gap:16px">{batch}{stream}</div>'
    store = ('<div class="col" style="gap:44px;align-items:center">'
             + cyl("warehouse", "PostgreSQL", "warehouse: agregat ridership dan keterlambatan", [], width=170, bid="pg")
             + box("ink", "file-json", "MongoDB", "basis data dokumen: kedatangan kereta dan alert, TTL 7 hari", width=170, sm=True, bid="mongo")
             + "</div>")
    main = SOURCES + ARROW + lanes + gap(64) + store + gap(64) + outputs(4, "pelatihan model")
    conns = [["load", "pg", {"ty": 0.4}], ["spark", "pg", {"ty": 0.62}], ["spark", "mongo", {"fy": 0.75}],
             ["pg", "st"], ["pg", "gf"], ["pg", "ml"]]
    cards = [
        ("Dari mana data berasal?", "SODA API (ridership), 8 feed GTFS-RT tiap 30 detik, dan GTFS statis."),
        ("Bagaimana dikumpulkan?", "Batch: job harian inkremental (<i>fingerprint</i>). Stream: <i>producer</i> polling 30 detik ke Kafka."),
        ("Kapan ditransformasi?", "<b>Sebelum</b> disimpan: DuckDB di staging (batch), Spark sebelum PostgreSQL (stream)."),
        ("Di mana disimpan?", "PostgreSQL (agregat) dan MongoDB (kedatangan, alert); data mentah tidak disimpan."),
        ("Bagaimana digunakan?", "Streamlit (dashboard dan prediksi), Grafana (pemantauan), notebook ML."),
    ]
    return page("Arsitektur ETL untuk Data Subway MTA", "Transformasi <em>sebelum</em> data disimpan",
                main, cards, "#BD392F", main_h=560, conns=conns)


def elt():
    ingest_batch = lane("JALUR BATCH · harian",
                        box("orange", "cloud-upload", "Extract &amp; Load", "CronJob, hari yang berubah disalin apa adanya", badge=1, width=300, height=150, sm=True, bid="elb"))
    ingest_stream = lane("JALUR STREAMING · tiap 30 detik",
                         box("orange", "radio-tower", "Extract &amp; Load", "<i>producer</i> Python, 8 feed", badge=1, width=134, height=150, sm=True)
                         + ARROW + box("slate", "server", "Kafka", "pesan disalin mentah ke MinIO", width=134, height=150, sm=True, bid="kafka"))
    ingest = f'<div class="col" style="gap:16px">{ingest_batch}{ingest_stream}</div>'
    lake = ('<div class="cont gray"><div class="clbl">Data lake · MinIO + DuckDB/Spark</div><div class="row">'
            '<div class="col" style="width:138px">'
            + box("slate", "database", "Bronze", "ridership harian (Parquet)", width=138, height=150, sm=True, bid="bz1")
            + box("slate", "archive", "Bronze", "snapshot GTFS-RT mentah", width=138, height=150, sm=True, bid="bz2")
            + "</div>" + gap(40)
            + box("green", "settings", "Transform", "DuckDB (batch) dan Spark (stream) membaca data mentah", badge=2, width=150, height=312, sm=True, bid="tr")
            + gap(40) + '<div class="col" style="width:132px">'
            + box("orange", "layers", "Gold", "agregat siap pakai", width=132, height=150, sm=True, bid="gold")
            + box("teal", "layers", "Silver", "baris bersih", width=132, height=150, sm=True, bid="silver")
            + "</div></div></div>")
    main = (SOURCES + ARROW + ingest + gap(46) + lake + gap(46)
            + cyl("warehouse", "PostgreSQL", "warehouse: salinan gold", [], width=150, bid="pg") + gap(46)
            + outputs(3, "latih model dari silver", w=165))
    conns = [["elb", "bz1"], ["kafka", "bz2"], ["bz1", "tr", {"ty": 0.25}], ["bz2", "tr", {"ty": 0.75}],
             ["tr", "gold", {"fy": 0.25}], ["tr", "silver", {"fy": 0.75}], ["gold", "pg"],
             ["pg", "st"], ["pg", "gf"], ["silver", "ml", {"dash": True, "elbow": "late", "label": "baca silver", "fy": 0.9}]]
    cards = [
        ("Dari mana data berasal?", "Sama: SODA API (ridership), 8 feed GTFS-RT, dan GTFS statis."),
        ("Bagaimana dikumpulkan?", "Batch harian dan polling 30 detik ke Kafka; semua data disalin apa adanya ke MinIO."),
        ("Kapan ditransformasi?", "<b>Setelah</b> dimuat: DuckDB (batch) dan Spark (stream) membaca data mentah di MinIO."),
        ("Di mana disimpan?", "MinIO: data mentah, silver, gold (mentah GTFS-RT ±26 GB sampai Januari 2027); agregat di PostgreSQL."),
        ("Bagaimana digunakan?", "Streamlit dan Grafana membaca PostgreSQL, notebook ML membaca silver; data mentah bisa diproses ulang."),
    ]
    return page("Arsitektur ELT untuk Data Subway MTA", "Muat mentah dulu, transformasi <em>di dalam</em> data lake",
                main, cards, "#1EA185", main_h=560, conns=conns)


def render(name, html, height=H):
    src_path = HERE / f"{name}.html"
    src_path.write_text(html)
    with tempfile.TemporaryDirectory() as profile:
        subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--user-data-dir={profile}",
                        "--force-device-scale-factor=2", f"--window-size={W},{height}", "--virtual-time-budget=10000",
                        f"--screenshot={HERE / (name + '.png')}", "file://" + quote(str(src_path))],
                       check=True, capture_output=True)
    print("wrote", name + ".png")


if __name__ == "__main__":
    render("01_etl", etl())
    render("02_elt", elt())
