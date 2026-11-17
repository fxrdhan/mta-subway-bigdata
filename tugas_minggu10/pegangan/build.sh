#!/bin/sh
# Membangun ulang diagram SVG dan PDF pegangan alur pipeline Minggu 10.
# Perlu plantuml, pandoc (>= 3), dan Google Chrome. Jalankan: sh pegangan/build.sh
set -eu
cd "$(dirname "$0")"
CHROME=${CHROME:-"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

plantuml -tsvg diagram/0*.puml
# Judul dokumen diambil dari heading pertama; bab utama (##) dimulai di halaman baru kecuali bab 1.
pandoc ALUR_PIPELINE.md --from gfm --shift-heading-level-by=-1 --standalone --embed-resources \
  --toc --toc-depth=2 --css style.css --metadata lang=id -o "$TMP/pegangan.html"
python3 - "$TMP/pegangan.html" <<'PY'
import re, sys
path = sys.argv[1]
html = open(path, encoding="utf-8").read()
# Bab 2 dan seterusnya diberi kelas "page" agar dimulai di halaman baru.
html = re.sub(r'<h1 id="([^"]+)">(?!1\.)', r'<h1 id="\1" class="page">', html)
# Daftar isi dipindah ke bawah paragraf pembuka, tepat sebelum bab 1.
toc = re.search(r'<nav id="TOC".*?</nav>\n?', html, re.S).group(0)
html = html.replace(toc, "", 1)
first = html.index('<h1 id=', html.index("</header>"))
html = html[:first] + toc + html[first:]
open(path, "w", encoding="utf-8").write(html)
PY
"$CHROME" --headless=new --disable-gpu --no-pdf-header-footer --print-to-pdf="$TMP/out.pdf" \
  "file://$TMP/pegangan.html" 2>/dev/null
mv "$TMP/out.pdf" Pegangan_Alur_Pipeline_Minggu10.pdf
echo "wrote $(pwd)/Pegangan_Alur_Pipeline_Minggu10.pdf"
