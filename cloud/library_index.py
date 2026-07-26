# -*- coding: utf-8 -*-
"""Library item index: jsonl remains source of truth; SQLite accelerates browse/search.

Indexer reads each queue's workspace progress rows (same path as queue_items) and
upserts into jobs.sqlite.library_items. library_list can then do real SQL paging
instead of loading every queue into memory with per-queue caps.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cloud import jobs_db, queue_service


def ensure_schema(db_path: Optional[Path] = None) -> None:
    jobs_db.init_db(db_path)
    jobs_db.ensure_library_items_schema(db_path)


def index_queue(
    queue_id: str,
    *,
    progress_limit: int = 5000,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Full reindex of one queue from workspace progress files."""
    ensure_schema(db_path)
    qid = str(queue_id or "").strip()
    if not qid:
        raise ValueError("queue_id required")
    record, root, items = queue_service._iter_queue_item_rows(qid, progress_limit=progress_limit)
    qname = str(record.get("name") or qid).strip() or qid
    now = time.time()
    rows: List[Dict[str, Any]] = []
    for item in items:
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        rows.append(
            {
                "library_id": f"{qid}:{item_id}",
                "queue_id": qid,
                "queue_name": qname,
                "item_id": item_id,
                "name": str(item.get("name") or "").strip(),
                "detail_url": str(item.get("detail_url") or "").strip(),
                "image_path": str(item.get("image_path") or "").strip(),
                "bucket": str(item.get("bucket") or "").strip() or "pending",
                "status": str(item.get("status") or "").strip(),
                "reason": str(item.get("reason") or "").strip(),
                "has_preview": 1 if item.get("has_preview") else 0,
                "output_root": root or str(record.get("output_root") or ""),
                "indexed_at": now,
            }
        )
    written = jobs_db.replace_library_items_for_queue(qid, rows, db_path=db_path)
    return {
        "ok": True,
        "queue_id": qid,
        "queue_name": qname,
        "items": written,
        "output_root": root,
    }


def index_all_queues(
    *,
    queue_limit: int = 500,
    progress_limit: int = 5000,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    ensure_schema(db_path)
    records = jobs_db.list_queues(limit=max(1, int(queue_limit or 500)), db_path=db_path)
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    total = 0
    for record in records:
        qid = str(record.get("id") or "").strip()
        if not qid:
            continue
        try:
            out = index_queue(qid, progress_limit=progress_limit, db_path=db_path)
            total += int(out.get("items") or 0)
            results.append(out)
        except Exception as exc:
            errors.append({"queue_id": qid, "error": str(exc)})
    return {
        "ok": not errors,
        "queues": len(results),
        "items": total,
        "errors": errors,
        "results": results,
    }


def library_list_from_index(
    *,
    limit: int = 60,
    offset: int = 0,
    status: str = "",
    q: str = "",
    queue_id: str = "",
    db_path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """SQL-backed library browse. Returns None if the index table is empty
    (caller should fall back to the legacy file-scan path)."""
    ensure_schema(db_path)
    counts = jobs_db.library_items_stats(db_path=db_path)
    if int(counts.get("total") or 0) <= 0:
        return None

    status_filter = str(status or "").strip().lower()
    query = str(q or "").strip()
    only_queue = str(queue_id or "").strip()
    start = max(0, int(offset or 0))
    size = max(1, min(int(limit or 60), 500))

    page = jobs_db.query_library_items(
        limit=size,
        offset=start,
        status=status_filter,
        q=query,
        queue_id=only_queue,
        db_path=db_path,
    )
    items: List[Dict[str, Any]] = []
    for row in page.get("items") or []:
        item = dict(row)
        qid = str(item.get("queue_id") or "")
        item_id = str(item.get("item_id") or "")
        item["id"] = item_id
        item["library_id"] = str(item.get("library_id") or f"{qid}:{item_id}")
        item["has_preview"] = bool(item.get("has_preview"))
        if item["has_preview"] and qid and item_id:
            item["preview_url"] = f"/api/v1/queues/{qid}/items/{item_id}/preview"
        else:
            item["preview_url"] = ""
        # Match legacy shape expected by the frontend.
        item["flags"] = {
            "detail_ok": False,
            "image_ok": bool(item["has_preview"]),
            "meta_ok": False,
        }
        items.append(item)

    summaries = jobs_db.library_queue_summaries(db_path=db_path)
    return {
        "total": int(page.get("total") or 0),
        "offset": start,
        "limit": size,
        "previewable": int(page.get("previewable") or 0),
        "items": items,
        "queues": summaries,
        "errors": [],
        "truncated": False,
        "per_queue_limit": 0,
        "source": "index",
        "index": counts,
    }
