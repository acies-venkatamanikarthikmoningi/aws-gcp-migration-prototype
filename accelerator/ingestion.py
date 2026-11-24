"""Multi-source ingestion (blueprint 4.2 / 15): allowlists, archive safety, hashing, immutable storage."""
from __future__ import annotations

import hashlib
import shutil
import zipfile
from pathlib import Path, PurePosixPath

from .store import Store, new_id, now

MAX_FILE_BYTES = 50 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 2000
MAX_ARCHIVE_EXPANDED = 500 * 1024 * 1024

ALLOWED = {
    # extension: (category, file_type)
    ".csv": ("inventory", "csv"), ".xlsx": ("inventory", "xlsx"), ".json": ("inventory", "json"),
    ".tf": ("code", "terraform"), ".yaml": ("code", "yaml"), ".yml": ("code", "yaml"),
    ".py": ("code", "python"), ".java": ("code", "java"), ".sql": ("code", "sql"), ".js": ("code", "javascript"),
    ".ts": ("code", "typescript"), ".properties": ("code", "config"), ".conf": ("code", "config"),
    ".jsonl": ("data", "jsonl"), ".parquet": ("data", "parquet"),
    ".pdf": ("document", "pdf"), ".docx": ("document", "docx"), ".pptx": ("document", "pptx"),
    ".md": ("document", "markdown"), ".txt": ("document", "text"),
    ".drawio": ("diagram", "drawio"), ".mmd": ("diagram", "mermaid"), ".png": ("diagram", "image"),
    ".jpg": ("diagram", "image"), ".jpeg": ("diagram", "image"), ".svg": ("diagram", "svg"),
    ".zip": ("archive", "zip"),
}
BLOCKED_EXT = {".exe", ".dll", ".so", ".bat", ".cmd", ".sh", ".ps1", ".msi", ".jar", ".com", ".scr", ".dylib"}
MAGIC = {  # minimal content sniffing: extension must agree with content
    "xlsx": b"PK", "docx": b"PK", "pptx": b"PK", "zip": b"PK", "pdf": b"%PDF", "png": b"\x89PNG",
}
MALWARE_SIGNATURES = [b"EICAR-STANDARD-ANTIVIRUS-TEST-FILE", b"ACCEL-DEMO-MALWARE-SIGNATURE"]
EXEC_MAGIC = [b"MZ", b"\x7fELF", b"\xcf\xfa\xed\xfe", b"#!"]


class RejectedFile(Exception):
    pass


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def classify(path: Path) -> tuple[str, str]:
    ext = path.suffix.lower()
    name = path.name.lower()
    if ext in BLOCKED_EXT:
        raise RejectedFile(f"blocked executable extension {ext}")
    if ext not in ALLOWED:
        raise RejectedFile(f"extension {ext or '(none)'} not in allowlist")
    cat, ftype = ALLOWED[ext]
    # refine category by content/name
    if ext == ".csv" and ("owner" in name or "consumer" in name):
        cat = "ownership"
    elif ext == ".csv" and "depend" in name:
        cat = "dependency"
    elif ext == ".csv" and any(k in str(path).lower() for k in ("/data/", "source_", "target_", "order_month")):
        cat = "data"
    elif ext == ".csv":
        # A CSV is only an AWS inventory if its header says so. Data files uploaded for the availability check or a
        # validation object (any header, often with an "id" column) must not be read as resources.
        import csv as _csv
        with path.open(encoding="utf-8", errors="ignore", newline="") as f:
            hdr = {h.strip().lower() for h in next(_csv.reader(f), [])}
        if {"source_resource", "target_resource"} <= hdr:
            cat = "dependency"
        elif not (hdr & {"service", "resource_type", "aws_service"} or ({"type"} <= hdr and hdr & {"resource_id", "resourceid", "arn"})):
            cat = "data"
    elif ext == ".xlsx" and "depend" in name:
        cat = "dependency"
    elif ext == ".json":
        head = path.read_text(encoding="utf-8", errors="ignore")[:4000]
        if '"States"' in head and '"StartAt"' in head:
            cat, ftype = "code", "stepfunctions"
        elif '"Statement"' in head and '"Version"' in head:
            cat, ftype = "code", "iam_policy"
        elif '"Item"' in head:
            cat, ftype = "data", "dynamodb_export"
    elif ext in (".yaml", ".yml"):
        head = path.read_text(encoding="utf-8", errors="ignore")[:4000]
        if "AWSTemplateFormatVersion" in head or "AWS::" in head:
            ftype = "cloudformation"
    elif ext == ".py":
        head = path.read_text(encoding="utf-8", errors="ignore")
        if "awsglue" in head:
            ftype = "glue_pyspark"
        elif "from airflow" in head:
            ftype = "airflow_dag"
        elif "lambda_handler" in head:
            ftype = "lambda_python"
    return cat, ftype


def check_content(path: Path, ftype: str) -> None:
    size = path.stat().st_size
    if size > MAX_FILE_BYTES:
        raise RejectedFile(f"file exceeds {MAX_FILE_BYTES} bytes")
    with path.open("rb") as f:
        head = f.read(8)
    if ftype in MAGIC and not head.startswith(MAGIC[ftype]):
        raise RejectedFile(f"content does not match declared type {ftype} (MIME/magic mismatch)")
    if ftype not in ("python", "lambda_python", "glue_pyspark", "airflow_dag") and any(
            head.startswith(m) for m in EXEC_MAGIC):
        raise RejectedFile("executable payload detected")
    # placeholder for AV hook (ClamAV / Cloud DLP / customer scanner) in production.
    # The demo uses a harmless marker instead of the real EICAR string so host antivirus is not triggered.
    if any(sig in path.read_bytes()[:4096] for sig in MALWARE_SIGNATURES):
        raise RejectedFile("malware signature detected (demo scanner)")


def safe_extract(zip_path: Path, dest: Path) -> list[Path]:
    out: list[Path] = []
    with zipfile.ZipFile(zip_path) as z:
        members = z.infolist()
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise RejectedFile("archive has too many members")
        if sum(m.file_size for m in members) > MAX_ARCHIVE_EXPANDED:
            raise RejectedFile("archive expands beyond limit (zip bomb protection)")
        for m in members:
            pp = PurePosixPath(m.filename)
            if pp.is_absolute() or ".." in pp.parts:
                raise RejectedFile(f"path traversal entry {m.filename}")
            if m.is_dir():
                continue
            if Path(m.filename).suffix.lower() in BLOCKED_EXT:
                raise RejectedFile(f"archive contains blocked file {m.filename}")
            target = dest / pp
            target.parent.mkdir(parents=True, exist_ok=True)
            with z.open(m) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            out.append(target)
    return out


def ingest_file(store: Store, project_id: str, path: Path, vault: Path, user: str = "demo.user") -> dict:
    """Validate, hash and store the original artefact unchanged. Returns upload row (status ACCEPTED/REJECTED)."""
    uid = new_id("UPL-")
    row = {"id": uid, "project_id": project_id, "filename": str(path.name), "uploaded_by": user,
           "created_at": now(), "size_bytes": 0}
    try:
        row["size_bytes"] = path.stat().st_size
    except OSError:
        pass
    try:
        cat, ftype = classify(path)
        check_content(path, ftype)
        digest = sha256_file(path)
        dup = store.query("SELECT id FROM uploads WHERE project_id=? AND sha256=? AND status='ACCEPTED'",
                          (project_id, digest))
        stored = vault / digest[:2] / digest / path.name  # immutable, content-addressed, original name kept
        stored.parent.mkdir(parents=True, exist_ok=True)
        if not stored.exists():
            shutil.copy2(path, stored)
        row.update(category=cat, file_type=ftype, sha256=digest, storage_path=str(stored),
                   status="DUPLICATE" if dup else "ACCEPTED", error=f"duplicate of {dup[0]['id']}" if dup else None)
    except RejectedFile as e:
        row.update(category="rejected", file_type=path.suffix.lower(), sha256=None, storage_path=None,
                   status="REJECTED", error=str(e))
    except OSError as e:  # e.g. Windows Defender quarantined the file between write and read
        row.update(category="rejected", file_type=path.suffix.lower(), sha256=None, storage_path=None,
                   status="REJECTED", error=f"file unreadable (blocked by host antivirus or OS): {e.strerror or e}")
    store.insert("uploads", row)
    store.audit(project_id, user, f"UPLOAD_{row['status']}", "upload", uid,
                {"filename": path.name, "sha256": row.get("sha256"), "error": row.get("error")})
    return row


def ingest_tree(store: Store, project_id: str, root: Path, vault: Path, user: str = "demo.user") -> list[dict]:
    rows = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        if p.suffix.lower() == ".zip":
            tmp = vault / "_unzipped" / p.stem
            try:
                for m in safe_extract(p, tmp):
                    rows.append(ingest_file(store, project_id, m, vault, user))
            except RejectedFile as e:
                r = ingest_file(store, project_id, p, vault, user)
                store.update("uploads", r["id"], status="REJECTED", error=str(e))
                r.update(status="REJECTED", error=str(e))
                rows.append(r)
            continue
        r = ingest_file(store, project_id, p, vault, user)
        r["original_path"] = str(p)
        rows.append(r)
    return rows
