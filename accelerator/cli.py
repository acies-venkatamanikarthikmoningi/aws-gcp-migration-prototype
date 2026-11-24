"""Command line: run the end-to-end demo and export a self-contained console.

  python -m accelerator.cli run      [--out runs/demo]
  python -m accelerator.cli export   [--out runs/demo]   -> runs/demo/console.html (open in any browser, no server)
  python -m accelerator.cli catalogue                     -> knowledge_base/aws_gcp_mapping.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from . import catalogue
from .orchestrator import ROOT, Orchestrator


def export_console(workdir: Path) -> Path:
    st = json.loads((workdir / "state.json").read_text())
    html = (ROOT / "ui" / "index.html").read_text(encoding="utf-8")
    blob = json.dumps(st, default=str).replace("</", "<\\/")
    html = html.replace('<script>\n"use strict";', f'<script>window.__STATE__={blob};</script>\n<script>\n"use strict";', 1)
    out = workdir / "console.html"
    out.write_text(html, encoding="utf-8")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(prog="accelerator")
    ap.add_argument("cmd", choices=["run", "export", "catalogue"])
    ap.add_argument("--out", default=str(ROOT / "runs" / "demo"))
    a = ap.parse_args()
    out = Path(a.out)
    if a.cmd == "catalogue":
        p = ROOT / "knowledge_base" / "aws_gcp_mapping.json"
        catalogue.export(p)
        print(p)
        return
    if a.cmd == "run":
        if out.exists():
            import shutil
            shutil.rmtree(out)
        Orchestrator(out).run_all()
    print("console:", export_console(out))


if __name__ == "__main__":
    main()
