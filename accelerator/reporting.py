"""Reporting engine: consolidated migration & reconciliation report (HTML + PDF), XLSX workbook and CSV extracts."""
from __future__ import annotations

import csv
import html
import json
from pathlib import Path


def _t(rows: list[dict], cols: list[str], heads: list[str] | None = None) -> str:
    heads = heads or cols
    h = "".join(f"<th>{html.escape(x)}</th>" for x in heads)
    b = "".join("<tr>" + "".join(f"<td>{html.escape(str(r.get(c, '')) if r.get(c) is not None else '')}</td>" for c in cols) + "</tr>" for r in rows)
    return f"<table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>"


def build_html(st: dict) -> str:
    k = st["kpis"]
    p = st["project"]
    risk = list(st["risk"].values())
    mv = st["migration"].get("validations") or {}
    ids = [mv[k]["id"] for k in ("orders_final", "customer") if k in mv]
    non_pass = [r for v in st["validations"] if v["id"] in ids for r in v["results"] if r["status"] != "PASS"]
    ran = bool(ids)
    sample = (st.get("stages") or {}).get("sample", True)
    gates = [{"application": a["application"], "readiness": a["readiness"], "score": a["score"], "class": a["classification"],
              "blockers": "; ".join(f"{g['gate']}: {g['detail']}" for g in a["gates"] if g["status"] == "BLOCKED")} for a in risk]
    story = st["migration"]["story"]
    av = (st.get("availability") or {}).get("datasets", [])
    av_rows = [{"dataset": d["name"], "source_access": d["access_text"], "source_rows": (d["result"] or {}).get("source", {}).get("rows"),
                "target_rows": (d["result"] or {}).get("target", {}).get("rows") if (d["result"] or {}).get("target") else "none",
                "check": (d["result"] or {}).get("status", "NOT_RUN"), "where_it_stands": d["clearance"], "reason": d["clearance_reason"]} for d in av]
    av_html = (_t(av_rows, ["dataset", "source_access", "source_rows", "target_rows", "check", "where_it_stands", "reason"]) +
               "<p>Counts and keys are evidence of availability, not proof that every record is identical. Where the source was only visible as a staging copy, "
               "the data owner has to confirm that every expected file arrived.</p>") if av else "<p>No datasets were added to the availability check for this project.</p>"
    css = ("body{font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#1f2933;margin:24px}h1{font-size:20px;margin:0}"
           "h2{font-size:14px;border-bottom:2px solid #1a73e8;padding-bottom:3px;margin-top:22px}table{border-collapse:collapse;width:100%;margin:6px 0}"
           "th,td{border:1px solid #d0d7de;padding:3px 5px;text-align:left;vertical-align:top}th{background:#eef3fb}"
           ".k{display:inline-block;border:1px solid #d0d7de;padding:6px 10px;margin:3px;border-radius:4px}.k b{display:block;font-size:15px}"
           ".note{background:#fff8e1;border-left:4px solid #f9a825;padding:6px 8px}")
    kp = "".join(f"<span class='k'><b>{html.escape(str(v))}</b>{html.escape(n)}</span>" for n, v in [
        ("resources discovered", k["resources_discovered"]), ("relationships", k["relationships"]), ("conflicts flagged", k["conflicts"]),
        ("mappings approved", f"{k['mappings_approved']}/{k['mappings_total']}"), ("artefacts converted", k["conversions"]),
        ("batches loaded", k["batches_loaded"]), ("orders validation", k["validation_orders"]), ("customer validation", k["validation_customer"]),
        ("apps blocked", f"{k['apps_blocked']}/{k['apps_total']}")])
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>Migration Readiness Report</title><style>{css}</style></head><body>
<h1>AWS → GCP Migration Readiness &amp; Reconciliation Report</h1>
<p>{html.escape(p['customer'])} · {html.escape(p['name'])} · project {p['id']} · generated {st['generated_at']}<br>
AWS {p['aws_account']} {p['aws_region']} → GCP {p['gcp_project']} {p['gcp_region']} · mapping catalogue {st['catalogue']['version']}</p>
<p class="note">{"Prototype run on synthetic data with simulated execution. Approvals marked “SIMULATED” are demo stand-ins for authorised users. " if sample else "Assessment of the uploaded files only. No data was loaded or compared, so there is no reconciliation evidence in this report. "}No infrastructure was deployed and no e-mail was sent.</p>
<h2>1. Summary</h2>{kp}
<h2>2. Readiness gates by application</h2>{_t(gates, ['application', 'readiness', 'score', 'class', 'blockers'])}
<h2>3. Discovery</h2><p>{json.dumps(st['discovery']['stats'])}</p>
{_t(st['discovery']['resources'], ['resource_id', 'name', 'service', 'application', 'evidence_status', 'confidence'])}
<h2>4. Service mapping &amp; 7R strategy</h2>{_t(st['mappings'], ['resource_id', 'aws_service', 'target_service', 'strategy', 'effort', 'score', 'approval_status'])}
<h2>5. Data movement plan</h2>{_t(st['plan']['movement'], ['name', 'service', 'size_gb', 'estimated_transfer_hours', 'bulk_method', 'cutover_strategy'])}
<h2>6. Migration waves</h2>{_t([{**w, 'applications': ', '.join(w['applications']), 'resources': ', '.join(w['resources'])} for w in st['plan']['waves']], ['wave', 'name', 'applications', 'resources'])}
<h2>7. Conversion</h2>{_t(st['conversions'], ['source_file', 'converter', 'confidence', 'version'])}
<h2>8. Historical load &amp; validation timeline</h2>{_t([{'step': s['step'], 'detail': json.dumps({k: v for k, v in s.items() if k not in ('step', 'at')}, default=str)[:300]} for s in story], ['step', 'detail']) if story else '<p>Not run for this project.</p>'}
<h2>9. Reconciliation – non-PASS checks (final)</h2>{_t(non_pass, ['object', 'partition', 'dimension', 'rule', 'source_value', 'target_value', 'tolerance', 'status']) if ran else '<p>Not run for this project: no source and target data was compared.</p>'}
<h2>10. Stakeholders</h2>{_t(st['stakeholders'], ['application', 'name', 'email', 'type', 'verified', 'owner_source'])}
<h2>11. Data availability before conversion</h2>{av_html}
<h2>12. Audit</h2><p>{st['audit']['chain']['events']} events · hash chain valid: {st['audit']['chain']['valid']}</p>
</body></html>"""


def write_reports(st: dict, outdir: Path) -> list[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    paths = []
    h = outdir / "migration_readiness_report.html"
    h.write_text(build_html(st), encoding="utf-8")
    paths.append(h)
    try:
        import pymupdf
        pdf = outdir / "migration_readiness_report.pdf"
        story = pymupdf.Story(html=build_html(st))
        writer = pymupdf.DocumentWriter(str(pdf))
        mediabox = pymupdf.paper_rect("a4-l")
        where = mediabox + (24, 24, -24, -24)
        more = 1
        while more:
            dev = writer.begin_page(mediabox)
            more, _ = story.place(where)
            story.draw(dev)
            writer.end_page()
        writer.close()
        paths.append(pdf)
    except Exception:  # noqa: BLE001 – PDF is optional; HTML is canonical
        pass
    tables = {
        "resources": (st["discovery"]["resources"], ["resource_id", "name", "service", "category", "application", "region", "evidence_status", "confidence"]),
        "mappings": (st["mappings"], ["resource_id", "aws_service", "target_service", "strategy", "effort", "score", "approval_status", "catalogue_version"]),
        "movement_plan": (st["plan"]["movement"], ["name", "service", "size_gb", "estimated_transfer_hours", "bulk_method", "catch_up_method", "cutover_strategy", "data_pattern"]),
        "batches": ([{**b, "run_type": r["run_type"]} for r in st["migration"]["runs"] for b in r["batches"]],
                    ["run_id", "run_type", "batch_key", "partition", "status", "attempt", "source_count", "extracted", "loaded", "rejected", "checksum"]),
        "validation_results": ([r | {"run_type": v["run_type"]} for v in st["validations"] for r in v.get("results", [])],
                               ["validation_run_id", "run_type", "object", "partition", "dimension", "rule", "source_value", "target_value", "tolerance", "status", "severity"]),
        "risks": ([{"application": a, "dimension": d, **x} for a, v in st["risk"].items() for d, x in v["dimensions"].items()],
                  ["application", "dimension", "score", "weight", "rationale"]),
        "availability": ([{"dataset": d["name"], "source_access": d["source_access"], "check": (d["result"] or {}).get("status", "NOT_RUN"),
                           "where_it_stands": d["clearance"], "reason": d["clearance_reason"],
                           "source_rows": ((d["result"] or {}).get("source") or {}).get("rows"),
                           "target_rows": ((d["result"] or {}).get("target") or {}).get("rows"),
                           "decision": (d["decision"] or {}).get("decision"), "decided_by": (d["decision"] or {}).get("approver"),
                           "owner_confirmation": (d["confirmation"] or {}).get("status")} for d in (st.get("availability") or {}).get("datasets", [])],
                         ["dataset", "source_access", "check", "where_it_stands", "reason", "source_rows", "target_rows", "decision", "decided_by", "owner_confirmation"]),
        "stakeholders": (st["stakeholders"], ["application", "name", "email", "type", "verified", "owner_source"]),
        "notifications": (st["notifications"], ["event", "recipient", "subject", "status"]),
        "audit": (st["audit"]["events"], ["seq", "ts", "actor", "action", "entity_type", "entity_id", "hash"]),
    }
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    wb = Workbook()
    wb.remove(wb.active)
    for name, (rows, cols) in tables.items():
        c = outdir / f"{name}.csv"
        with c.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows([{k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in r.items()} for r in rows])
        paths.append(c)
        ws = wb.create_sheet(name[:31])
        ws.append(cols)
        for cell in ws[1]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="1A73E8")
        for r in rows:
            ws.append([json.dumps(r.get(x)) if isinstance(r.get(x), (dict, list)) else r.get(x) for x in cols])
        for i, col in enumerate(cols, 1):
            ws.column_dimensions[ws.cell(1, i).column_letter].width = min(60, max(12, len(col) + 4))
        ws.freeze_panes = "A2"
    x = outdir / "migration_report.xlsx"
    wb.save(x)
    paths.append(x)
    return paths
