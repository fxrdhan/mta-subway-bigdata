"""Two-pass build of Dokumentasi_ETL: pass 1 renders the document to find each heading's page, pass 2 writes
the table of contents with those pages. Then checks that no table is split across pages and that TOC links exist.
Usage: uv run --with pypdf python make10.py <repo> <results10.json> <shots dir> <out_dir>
"""
import json
import re
import subprocess
import sys
from pathlib import Path

from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
repo, results, shots, out_dir = (Path(a).resolve() for a in sys.argv[1:5])  # build10.js runs with cwd=HERE
repo, results, shots = str(repo), str(results), str(shots)
render = HERE / "render10"
render.mkdir(exist_ok=True)
NAME = "Dokumentasi_ETL"


def build(docx: Path, toc_pages: Path | None = None) -> Path:
    cmd = ["node", str(HERE / "build10.js"), repo, results, shots, str(docx)] + ([str(toc_pages)] if toc_pages else [])
    subprocess.run(cmd, check=True, cwd=HERE)
    subprocess.run(["zip", "-d", str(docx), "*/"], capture_output=True)
    subprocess.run(["soffice", "--headless", "--convert-to", "pdf", "--outdir", str(docx.parent), str(docx)],
                   check=True, capture_output=True)
    return docx.with_suffix(".pdf")


def pages_text(pdf: Path) -> list[str]:
    return [p.extract_text() or "" for p in PdfReader(str(pdf)).pages]


squash = lambda s: re.sub(r"\s+", "", s)


def heading_pages(pdf: Path) -> dict[str, int]:
    texts = [squash(t) for t in pages_text(pdf)]
    toc = json.loads((HERE / "toc10_entries.json").read_text())
    # content page 1 = first page after the TOC that starts chapter I
    start = next(i for i, t in enumerate(texts) if i > 1 and "BABI" in t[:12] and "PENDAHULUAN" in t[:40])
    pages, cursor = {}, start
    for e in toc:
        key = squash(e["search"])
        i = next((j for j in range(cursor, len(texts)) if key in texts[j]), None)
        if i is None:
            raise SystemExit(f"heading not found: {e['text']}")
        pages[e["text"]] = str(i - start + 1)
        cursor = i
    return pages


pass1 = build(render / f"{NAME}_pass1.docx")
toc_pages = render / "toc10_pages.json"
toc_pages.write_text(json.dumps(heading_pages(pass1), indent=1, ensure_ascii=False))
final = build(render / f"{NAME}.docx", toc_pages)
check = heading_pages(final)
assert check == json.loads(toc_pages.read_text()), "page numbers moved between passes"

texts = [squash(t) for t in pages_text(final)]
problems = []
for t in json.loads((HERE / "table10_checks.json").read_text()):
    cap = squash(t["caption"])
    p = next((i for i, x in enumerate(texts) if cap in x), None)
    last = next((squash(c)[:20] for c in t["last"] if squash(c)), "")
    if p is None:
        problems.append(f"caption not found: {t['caption']}")
    elif last and last not in texts[p]:
        problems.append(f"split or moved table: {t['caption']} (last row '{last}' not on page {p + 1})")
reader = PdfReader(str(final))
toc_links = sum(1 for a in (reader.pages[1].get("/Annots") or []) if a.get_object().get("/Subtype") == "/Link")
print(f"pages: {len(reader.pages)} | TOC links on first TOC page: {toc_links} | table problems: {len(problems)}")
for p in problems:
    print("  ", p)
out_dir.mkdir(parents=True, exist_ok=True)
for ext in ("docx", "pdf"):
    (out_dir / f"{NAME}.{ext}").write_bytes((render / f"{NAME}.{ext}").read_bytes())
print("wrote", out_dir / f"{NAME}.pdf")
