"""Version history, review decisions and person-made edits for converted code (Round 7; blueprint "approve generated code").

Every assessment run writes a GENERATED version of each converted file set. A person can save an EDITED version (their
change to one output file, the rest carried over) and approve or reject a specific version. A decision belongs to the
exact content it was made on (a hash of every output file): a later version with different content needs its own
review, and a regenerated version identical to an approved one keeps that approval.
"""
from __future__ import annotations

import difflib
import hashlib
import json

from .store import Store, new_id, now

DECISIONS = ("APPROVED", "REJECTED")


def outputs_hash(outputs: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(outputs, sort_keys=True).encode()).hexdigest()[:16]


def versions(store: Store, pid: str, source_file: str) -> list[dict]:
    rows = store.query("SELECT * FROM conversion_runs WHERE project_id=? AND source_file=? ORDER BY created_at, rowid", (pid, source_file))
    for i, r in enumerate(rows):
        r["n"] = i + 1
        r["has_content"] = bool(r.get("outputs"))
    return rows


def view(r: dict) -> dict:
    return {k: r.get(k) for k in ("id", "n", "version", "kind", "created_at", "confidence", "outputs_hash", "approval_status",
                                  "approved_by", "approval_note", "approved_at", "edited_by", "note", "parent_id", "has_content")}


def carry_approval(store: Store, pid: str, row: dict) -> None:
    """A regenerated version identical to an approved earlier version keeps that approval (same content, same decision)."""
    prev = [v for v in versions(store, pid, row["source_file"]) if v["id"] != row["id"] and v.get("outputs_hash") == row["outputs_hash"]
            and v.get("approval_status") in DECISIONS]
    if prev:
        p = prev[-1]
        store.update("conversion_runs", row["id"], approval_status=p["approval_status"], approved_by=p["approved_by"],
                     approval_note=f"Carried over from version {p['n']} (identical output). {p.get('approval_note') or ''}".strip(),
                     approved_at=p["approved_at"])


def _row(store: Store, pid: str, cid: str) -> dict:
    r = store.get("conversion_runs", cid)
    if not r or r["project_id"] != pid:
        raise KeyError(cid)
    return r


def decide(store: Store, pid: str, cid: str, approver: str, decision: str, note: str) -> dict:
    r = _row(store, pid, cid)
    if decision not in DECISIONS:
        raise ValueError("Decision must be APPROVED or REJECTED.")
    approver, note = (approver or "").strip(), (note or "").strip()
    if len(approver) < 2 or len(note) < 10:
        raise ValueError("Enter your name and a review note of at least 10 characters.")
    if not r.get("outputs"):
        raise ValueError("This version was generated before versions were kept; its content is not available to review. "
                         "Run the assessment again to create a reviewable version.")
    latest = versions(store, pid, r["source_file"])[-1]
    if latest["id"] != cid:
        raise ValueError(f"Only the latest version (version {latest['n']}) can be reviewed.")
    store.update("conversion_runs", cid, approval_status=decision, approved_by=approver, approval_note=note, approved_at=now())
    store.record_approval(pid, "conversion", cid, approver, decision, note, requested_by="conversion-agent")
    return store.get("conversion_runs", cid)


def edit(store: Store, pid: str, cid: str, path: str, content: str, by: str, note: str, workdir) -> dict:
    """Save a person's change to one output file as a new EDITED version; every other file is carried over unchanged."""
    r = _row(store, pid, cid)
    if not r.get("outputs"):
        raise ValueError("This version's content was not kept, so it cannot be edited. Run the assessment again first.")
    latest = versions(store, pid, r["source_file"])[-1]
    if latest["id"] != cid:
        raise ValueError(f"Edit the latest version (version {latest['n']}).")
    outs = dict(r["outputs"])
    if path not in outs:
        raise ValueError(f"{path} is not one of this version's files.")
    by, note = (by or "").strip(), (note or "").strip()
    if len(by) < 2 or len(note) < 5:
        raise ValueError("Enter your name and a short note saying what you changed.")
    if content == outs[path]:
        raise ValueError("Nothing changed.")
    outs[path] = content
    nid = new_id("CNV-")
    outdir = workdir / "output" / "conversions" / "edits" / nid
    for rel, text in outs.items():
        p = outdir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    row = {k: r.get(k) for k in ("project_id", "source_file", "converter", "converter_version", "upload_id", "warnings", "unsupported", "checks")}
    row.update(id=nid, output_path=str(outdir), confidence=r.get("confidence"), status="EDITED", approval_status="PENDING_REVIEW",
               version=f"{r.get('version') or ''}+edit", kind="EDITED", outputs=outs, outputs_hash=outputs_hash(outs),
               parent_id=cid, edited_by=by, note=f"{path}: {note}", created_at=now())
    store.insert("conversion_runs", row)
    store.audit(pid, by, "CODE_EDITED", "conversion_run", nid, {"parent": cid, "file": path, "note": note})
    return store.get("conversion_runs", nid)


def diff(store: Store, pid: str, a_id: str, b_id: str) -> dict:
    a, b = _row(store, pid, a_id), _row(store, pid, b_id)
    if not a.get("outputs") or not b.get("outputs"):
        raise ValueError("One of these versions was generated before versions were kept, so there is nothing to compare.")
    files, same = {}, []
    for rel in sorted(set(a["outputs"]) | set(b["outputs"])):
        x, y = a["outputs"].get(rel), b["outputs"].get(rel)
        if x == y:
            same.append(rel)
            continue
        files[rel] = "".join(difflib.unified_diff((x or "").splitlines(True), (y or "").splitlines(True),
                                                 fromfile=f"{rel} (from)", tofile=f"{rel} (to)", n=2)) or "(only whitespace changed)"
        if x is None:
            files[rel] = f"(file added)\n" + files[rel]
        elif y is None:
            files[rel] = f"(file removed)\n" + files[rel]
    return {"from": a_id, "to": b_id, "changed": files, "unchanged": same}
