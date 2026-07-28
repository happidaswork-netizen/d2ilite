# -*- coding: utf-8 -*-
"""Queue original-image + EXIF audit (Hermes P0 self-check).

Checks image_downloads.jsonl rows for:
- sha256 present
- saved_path / named_path exists
- optional re-hash match
- source_photo_taken_at / exif_present consistency with on-disk EXIF
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.task_service import normalize_public_task_root

logger = logging.getLogger("d2i.cloud.image_audit")


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.is_file():
        return []
    out: List[Dict[str, Any]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _resolve_path(raw: Any, *, output_root: Path) -> Path:
    text = str(raw or "").strip()
    if not text:
        return Path()
    p = Path(text)
    if p.is_absolute():
        return p
    return (output_root / p).resolve()


def audit_queue_images(
    *,
    queue_id: str = "",
    output_root: str = "",
    limit: int = 500,
    rehash: bool = True,
    re_read_exif: bool = True,
) -> Dict[str, Any]:
    """Audit one queue's download warehouse / manifest."""
    from cloud import jobs_db
    from image_source_meta import inspect_source_image, sha256_file

    root_text = str(output_root or "").strip()
    qid = str(queue_id or "").strip()
    if not root_text and qid:
        row = jobs_db.get_queue(qid)
        if not row:
            raise KeyError(f"queue not found: {qid}")
        root_text = str(row.get("output_root") or "").strip()
        if not root_text and isinstance(row.get("meta"), dict):
            root_text = str(row["meta"].get("output_root") or "").strip()
    root_norm = normalize_public_task_root(root_text) or root_text
    root = Path(root_norm) if root_norm else Path()
    if not root_norm or not root.exists():
        raise FileNotFoundError(f"output_root missing: {root_text or qid or '(empty)'}")

    manifest_path = root / "downloads" / "image_downloads.jsonl"
    images_dir = root / "downloads" / "images"
    rows = _read_jsonl(manifest_path)
    max_n = max(1, min(int(limit or 500), 5000))
    sample = rows[:max_n]

    counts = {
        "rows": len(rows),
        "audited": 0,
        "ok": 0,
        "missing_sha": 0,
        "missing_path": 0,
        "path_missing_on_disk": 0,
        "sha_mismatch": 0,
        "exif_present": 0,
        "exif_absent": 0,
        "has_source_date": 0,
        "exif_date_not_recorded": 0,
        "warehouse_dir_present": images_dir.is_dir(),
        "manifest_present": manifest_path.is_file(),
    }
    problems: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []

    for row in sample:
        counts["audited"] += 1
        name = str(row.get("name") or "")
        sha = str(row.get("sha256") or row.get("image_sha256") or "").strip().lower().removeprefix("sha256:")
        saved = _resolve_path(row.get("saved_path"), output_root=root)
        named = _resolve_path(row.get("named_path"), output_root=root)
        check_path = saved if saved.is_file() else named
        item: Dict[str, Any] = {
            "name": name,
            "sha256": sha,
            "saved_path": str(saved) if saved else "",
            "named_path": str(named) if named else "",
            "ok": True,
            "issues": [],
            "exif_present": row.get("exif_present"),
            "source_photo_taken_at": str(row.get("source_photo_taken_at") or ""),
            "photo_taken_at_source": str(row.get("photo_taken_at_source") or ""),
        }

        if not sha:
            counts["missing_sha"] += 1
            item["ok"] = False
            item["issues"].append("missing_sha256")
        if not str(row.get("saved_path") or row.get("named_path") or "").strip():
            counts["missing_path"] += 1
            item["ok"] = False
            item["issues"].append("missing_path")
        if not check_path or not check_path.is_file():
            counts["path_missing_on_disk"] += 1
            item["ok"] = False
            item["issues"].append("path_missing_on_disk")
        else:
            if rehash and sha:
                try:
                    actual = sha256_file(check_path)
                    if actual.lower() != sha.lower():
                        counts["sha_mismatch"] += 1
                        item["ok"] = False
                        item["issues"].append("sha256_mismatch")
                        item["actual_sha256"] = actual
                except OSError as exc:
                    item["ok"] = False
                    item["issues"].append(f"rehash_error:{exc}")
            if re_read_exif:
                try:
                    info = inspect_source_image(check_path, expected_sha256=sha)
                    disk_present = bool(info.get("exif_present"))
                    disk_date = str(info.get("source_photo_taken_at") or "")
                    item["disk_exif_present"] = disk_present
                    item["disk_source_photo_taken_at"] = disk_date
                    item["disk_photo_taken_at_source"] = str(info.get("photo_taken_at_source") or "unknown")
                    if disk_present:
                        counts["exif_present"] += 1
                    else:
                        counts["exif_absent"] += 1
                    if disk_date:
                        counts["has_source_date"] += 1
                    # Gate: EXIF date on disk but not recorded on row / audit.
                    recorded = str(row.get("source_photo_taken_at") or row.get("photo_taken_at") or "").strip()
                    if disk_date and not recorded:
                        counts["exif_date_not_recorded"] += 1
                        item["ok"] = False
                        item["issues"].append("exif_date_not_recorded")
                    # Prefer filling missing provenance from disk for the response
                    if item.get("exif_present") is None:
                        item["exif_present"] = disk_present
                    if not item.get("source_photo_taken_at") and disk_date:
                        item["source_photo_taken_at"] = disk_date
                        item["photo_taken_at_source"] = "source_exif"
                except Exception as exc:  # noqa: BLE001
                    item["issues"].append(f"exif_read_error:{type(exc).__name__}")
            else:
                if row.get("exif_present"):
                    counts["exif_present"] += 1
                else:
                    counts["exif_absent"] += 1
                if item.get("source_photo_taken_at"):
                    counts["has_source_date"] += 1

        if item["ok"]:
            counts["ok"] += 1
        else:
            problems.append(
                {
                    "name": name,
                    "issues": list(item["issues"]),
                    "saved_path": item["saved_path"],
                    "sha256": sha[:16] if sha else "",
                }
            )
        items.append(item)

    complete = (
        counts["manifest_present"]
        and counts["missing_sha"] == 0
        and counts["missing_path"] == 0
        and counts["path_missing_on_disk"] == 0
        and counts["sha_mismatch"] == 0
        and counts["exif_date_not_recorded"] == 0
    )
    return {
        "ok": bool(complete),
        "schema": "d2i-cloud-image-audit-v1",
        "queue_id": qid,
        "output_root": str(root),
        "manifest_path": str(manifest_path),
        "images_dir": str(images_dir),
        "counts": counts,
        "complete_ingest": complete,
        "problems": problems[:100],
        "items": items[: min(50, len(items))],
        "truncated": len(rows) > max_n,
        "note": (
            "complete_ingest requires sha256+path on disk, hash match, and "
            "any on-disk EXIF date must be recorded (empty date + exif_present=false is OK)"
        ),
    }


def audit_from_queue_id(queue_id: str, **kwargs: Any) -> Dict[str, Any]:
    return audit_queue_images(queue_id=queue_id, **kwargs)
