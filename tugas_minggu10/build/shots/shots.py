"""Screenshot helpers: crop cells of an executed notebook (rendered to HTML) and render real terminal output.

notebook(html, out_png, markers): screenshots the union of the notebook cells whose source contains each marker
(a marker may be (text, "out") to capture only that cell's output area).
terminal(out_png, title, blocks): renders [(command, output), ...] in a terminal-style window and screenshots it.
"""
from __future__ import annotations

import html as _html
from pathlib import Path

from playwright.sync_api import sync_playwright

_pw = _browser = None


def _page(width=1180):
    global _pw, _browser
    if _browser is None:
        _pw = sync_playwright().start()
        _browser = _pw.chromium.launch(channel="chrome")
    return _browser.new_page(viewport={"width": width, "height": 900}, device_scale_factor=2)


FIND = """(markers) => {
  const cells = [...document.querySelectorAll('.jp-Cell')];
  const boxes = [];
  for (const [text, part] of markers) {
    const cell = cells.find(c => (c.querySelector('.jp-InputArea, .jp-MarkdownOutput') || c).innerText.includes(text));
    if (!cell) throw new Error('marker not found: ' + text);
    const el = part === 'out' ? (cell.querySelector('.jp-Cell-outputWrapper') || cell) : cell;
    const r = el.getBoundingClientRect();
    boxes.push([r.left + scrollX, r.top + scrollY, r.right + scrollX, r.bottom + scrollY]);
  }
  return [Math.min(...boxes.map(b => b[0])), Math.min(...boxes.map(b => b[1])),
          Math.max(...boxes.map(b => b[2])), Math.max(...boxes.map(b => b[3]))];
}"""

CSS = """
body { background: #fff; }
.jp-Notebook { padding: 8px 18px !important; }
.jp-InputPrompt, .jp-OutputPrompt { min-width: 52px !important; }
.jp-RenderedHTMLCommon table { font-size: 12.5px; }
"""


def notebook(html_path: Path, out: Path, groups, pad: int = 8, width: int = 1180) -> None:
    """groups: list of marker lists; each group is cropped as one region and the regions are stacked vertically."""
    from PIL import Image
    page = _page(width)
    page.goto(Path(html_path).resolve().as_uri())
    page.add_style_tag(content=CSS)
    page.wait_for_timeout(300)
    parts = []
    for i, markers in enumerate(groups):
        marks = [m if isinstance(m, (list, tuple)) else (m, "cell") for m in markers]
        x0, y0, x1, y1 = page.evaluate(FIND, marks)
        tmp = out.with_name(f"{out.stem}_part{i}.png")
        page.screenshot(path=str(tmp), full_page=True,
                        clip={"x": max(0, x0 - pad), "y": max(0, y0 - pad), "width": min(width, x1 - x0 + 2 * pad), "height": y1 - y0 + 2 * pad})
        parts.append(tmp)
    page.close()
    imgs = [Image.open(t) for t in parts]
    gap = 18
    canvas = Image.new("RGB", (max(i.width for i in imgs), sum(i.height for i in imgs) + gap * (len(imgs) - 1)), "white")
    y = 0
    for im in imgs:
        canvas.paste(im, (0, y))
        y += im.height + gap
    canvas.save(out)
    for t in parts:
        t.unlink()
    print("wrote", out.name, canvas.size)


TERM = """<!doctype html><html><head><meta charset="utf-8"><style>
body {{ margin: 0; padding: 18px; background: #fff; font-family: -apple-system, 'Helvetica Neue', Arial, sans-serif; }}
.win {{ display: inline-block; min-width: 900px; max-width: {maxw}px; border-radius: 10px; overflow: hidden;
        box-shadow: 0 4px 18px rgba(0,0,0,.25); background: #1E2229; }}
.bar {{ height: 30px; background: #2C313A; display: flex; align-items: center; padding: 0 12px; gap: 8px; }}
.dot {{ width: 12px; height: 12px; border-radius: 50%; }}
.t {{ flex: 1; text-align: center; color: #AEB6C2; font-size: 13px; margin-right: 52px; }}
pre {{ margin: 0; padding: 14px 18px 16px; color: #D7DCE3; font: 13px/1.45 'JetBrains Mono', Menlo, 'Courier New', monospace;
       white-space: pre-wrap; overflow-wrap: anywhere; }}
.p {{ color: #7EC07A; }} .c {{ color: #F0F3F6; font-weight: 600; }}
</style></head><body><div class="win"><div class="bar"><span class="dot" style="background:#FF5F57"></span>
<span class="dot" style="background:#FEBC2E"></span><span class="dot" style="background:#28C840"></span>
<span class="t">{title}</span></div><pre>{body}</pre></div></body></html>"""


def terminal(out: Path, title: str, blocks, maxw: int = 1500) -> None:
    body = "\n".join(f'<span class="p">$</span> <span class="c">{_html.escape(cmd)}</span>\n{_html.escape(output.rstrip())}\n'
                     for cmd, output in blocks)
    page = _page(maxw + 60)
    page.set_content(TERM.format(title=_html.escape(title), body=body.rstrip(), maxw=maxw))
    page.locator(".win").screenshot(path=str(out))
    page.close()
    print("wrote", out.name)


def close() -> None:
    if _browser:
        _browser.close()
        _pw.stop()
