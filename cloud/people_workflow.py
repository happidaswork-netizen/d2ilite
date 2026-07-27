# -*- coding: utf-8 -*-
"""People photo workflow marks: confirm no-photo, hold, resume, unusable asset.

Canonical columns (already on people.sqlite; we do not invent parallel tables):

- source_page_image_status
    has_photo | no_photo_on_source | placeholder_avatar | unknown | …
- image_status
    official_photo | has_photo | no_photo | no_image | …
- repair_status
    none | hold | abandoned_no_usable_photo | titi_written | …
- file_status
    available | ok | missing | abandoned | reject | …
- source_image_status
    ok | no_photo_on_source | source_too_small_or_corrupt | placeholder_avatar | …

Workflow actions (API/CLI):

- no_photo   → confirmed source has no usable portrait; stop crawl+vision
- hold       → temporary pause; optional hold_until ISO date in notes/meta
- resume     → clear hold / re-open for inventory
- unusable   → local asset bad (tiny/truncated); stay out of vision until recrawl
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from cloud.paths import people_db_path

# --- canonical values ---
NO_PHOTO_PAGE = "no_photo_on_source"
NO_PHOTO_IMAGE = "no_photo"
HOLD_REPAIR = "hold"
UNUSABLE_SOURCE = "source_too_small_or_corrupt"
UNUSABLE_FILE = "abandoned"
ABANDONED_REPAIR = "abandoned_no_usable_photo"

# Terminal / gated states that inventory & enqueue must skip by default.
BLOCK_VISION_PAGE_STATUSES = frozenset(
    {
        NO_PHOTO_PAGE,
        "placeholder_avatar",
        "missing_on_source",
    }
)
BLOCK_VISION_IMAGE_STATUSES = frozenset(
    {
        NO_PHOTO_IMAGE,
        "no_image",
    }
)
BLOCK_VISION_REPAIR_STATUSES = frozenset(
    {
        HOLD_REPAIR,
        ABANDONED_REPAIR,
        "hold",
        "deferred",
    }
)
BLOCK_VISION_FILE_STATUSES = frozenset(
    {
        UNUSABLE_FILE,
        "reject",
        "missing",
    }
)
BLOCK_VISION_SOURCE_IMAGE_STATUSES = frozenset(
    {
        NO_PHOTO_PAGE,
        UNUSABLE_SOURCE,
        "placeholder_avatar",
        "missing_on_source",
    }
)

ACTIONS = frozenset({"no_photo", "hold", "resume", "unusable"})


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _open(db: Optional[Path] = None) -> sqlite3.Connection:
    path = Path(db) if db else people_db_path()
    if not path.is_file():
        raise FileNotFoundError(f"people db not found: {path}")
    conn = sqlite3.connect(str(path), timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def _cols(conn: sqlite3.Connection) -> set[str]:
    return {str(r[1]) for r in conn.execute("PRAGMA table_info(people)")}


def _append_note(existing: Any, line: str) -> str:
    base = str(existing or "").strip()
    stamp = _utc_stamp()
    entry = f"[{stamp}] {line}"
    if not base:
        return entry
    # keep notes bounded
    merged = base + "\n" + entry
    if len(merged) > 4000:
        merged = merged[-4000:]
    return merged


def workflow_state_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Derive a single workflow label for UI / gates from people columns."""
    page = str(row.get("source_page_image_status") or "").strip()
    image = str(row.get("image_status") or "").strip()
    repair = str(row.get("repair_status") or "").strip()
    file_st = str(row.get("file_status") or "").strip()
    src_img = str(row.get("source_image_status") or "").strip()
    path = str(row.get("primary_image_path") or "").strip()

    if repair in BLOCK_VISION_REPAIR_STATUSES and repair != ABANDONED_REPAIR:
        return {
            "workflow": "hold",
            "label": "暂挂",
            "blocks_vision": True,
            "blocks_recrawl": True,
            "terminal": False,
        }
    if (
        page in {NO_PHOTO_PAGE, "missing_on_source"}
        or image in {NO_PHOTO_IMAGE, "no_image"}
        or repair == ABANDONED_REPAIR
    ):
        return {
            "workflow": "no_photo_confirmed",
            "label": "确认无图",
            "blocks_vision": True,
            "blocks_recrawl": True,
            "terminal": True,
        }
    if (
        page == "placeholder_avatar"
        or src_img in {UNUSABLE_SOURCE, "placeholder_avatar"}
        or file_st in {UNUSABLE_FILE, "reject"}
    ):
        return {
            "workflow": "unusable_asset",
            "label": "图不可用",
            "blocks_vision": True,
            "blocks_recrawl": False,
            "terminal": False,
        }
    if path:
        return {
            "workflow": "has_photo",
            "label": "有图",
            "blocks_vision": False,
            "blocks_recrawl": False,
            "terminal": False,
        }
    return {
        "workflow": "open",
        "label": "待处理",
        "blocks_vision": False,
        "blocks_recrawl": False,
        "terminal": False,
    }


def sql_exclude_blocked_vision(cols: set[str], *, param_prefix: str = "") -> tuple[str, list[Any]]:
    """AND-clauses that drop confirmed-no-photo / hold / unusable from vision inventory.

    Returns (sql_fragment, params). Fragment starts with AND … or is empty.
    """
    parts: List[str] = []
    params: List[Any] = []
    if "source_page_image_status" in cols:
        placeholders = ",".join("?" for _ in BLOCK_VISION_PAGE_STATUSES)
        parts.append(
            f"(source_page_image_status IS NULL OR source_page_image_status = '' "
            f"OR source_page_image_status NOT IN ({placeholders}))"
        )
        params.extend(sorted(BLOCK_VISION_PAGE_STATUSES))
    if "image_status" in cols:
        placeholders = ",".join("?" for _ in BLOCK_VISION_IMAGE_STATUSES)
        parts.append(
            f"(image_status IS NULL OR image_status = '' OR image_status NOT IN ({placeholders}))"
        )
        params.extend(sorted(BLOCK_VISION_IMAGE_STATUSES))
    if "repair_status" in cols:
        placeholders = ",".join("?" for _ in BLOCK_VISION_REPAIR_STATUSES)
        parts.append(
            f"(repair_status IS NULL OR repair_status = '' OR repair_status NOT IN ({placeholders}))"
        )
        params.extend(sorted(BLOCK_VISION_REPAIR_STATUSES))
    if "file_status" in cols:
        # only block hard rejects/abandoned — keep available/ok/downloaded
        hard = sorted({UNUSABLE_FILE, "reject"})
        placeholders = ",".join("?" for _ in hard)
        parts.append(
            f"(file_status IS NULL OR file_status = '' OR file_status NOT IN ({placeholders}))"
        )
        params.extend(hard)
    if "source_image_status" in cols:
        placeholders = ",".join("?" for _ in BLOCK_VISION_SOURCE_IMAGE_STATUSES)
        parts.append(
            f"(source_image_status IS NULL OR source_image_status = '' "
            f"OR source_image_status NOT IN ({placeholders}))"
        )
        params.extend(sorted(BLOCK_VISION_SOURCE_IMAGE_STATUSES))
    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), params


def get_person(person_id: str, *, people_db: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    pid = str(person_id or "").strip()
    if not pid:
        return None
    conn = _open(people_db)
    try:
        cols = _cols(conn)
        want = [
            c
            for c in (
                "person_id",
                "name",
                "gender",
                "unit_name",
                "province",
                "city",
                "primary_image_path",
                "source_url",
                "image_status",
                "source_page_image_status",
                "source_image_status",
                "file_status",
                "repair_status",
                "photo_kind",
                "visual_gender",
                "has_official_photo",
                "notes",
            )
            if c in cols
        ]
        if "person_id" not in want:
            return None
        row = conn.execute(
            f"SELECT {', '.join(want)} FROM people WHERE person_id = ?",
            (pid,),
        ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["workflow"] = workflow_state_from_row(data)
        return data
    finally:
        conn.close()


def mark_person(
    *,
    person_id: str = "",
    name: str = "",
    unit_like: str = "",
    action: str,
    reason: str = "",
    hold_until: str = "",
    clear_primary_path: bool = True,
    dry_run: bool = False,
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    """Apply a workflow mark to one person. Returns before/after snapshot."""
    act = str(action or "").strip().lower()
    if act not in ACTIONS:
        raise ValueError(f"action must be one of {sorted(ACTIONS)}, got {action!r}")

    conn = _open(people_db)
    try:
        cols = _cols(conn)
        if "person_id" not in cols:
            raise RuntimeError("people.person_id column missing")

        row = None
        pid = str(person_id or "").strip()
        if pid:
            row = conn.execute("SELECT * FROM people WHERE person_id = ?", (pid,)).fetchone()
        elif name:
            nm = str(name).strip()
            if unit_like and "unit_name" in cols:
                row = conn.execute(
                    "SELECT * FROM people WHERE name = ? AND unit_name LIKE ? "
                    "ORDER BY rowid DESC LIMIT 1",
                    (nm, f"%{unit_like}%"),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT * FROM people WHERE name = ? ORDER BY rowid DESC LIMIT 1",
                    (nm,),
                ).fetchone()
        if row is None:
            raise KeyError(f"person not found: person_id={person_id!r} name={name!r}")

        before = dict(row)
        pid = str(before.get("person_id") or "")
        updates: Dict[str, Any] = {}
        reason_s = str(reason or "").strip() or act

        if act == "no_photo":
            if "source_page_image_status" in cols:
                updates["source_page_image_status"] = NO_PHOTO_PAGE
            if "image_status" in cols:
                updates["image_status"] = NO_PHOTO_IMAGE
            if "source_image_status" in cols:
                updates["source_image_status"] = NO_PHOTO_PAGE
            if "repair_status" in cols:
                updates["repair_status"] = ABANDONED_REPAIR
            if "has_official_photo" in cols:
                updates["has_official_photo"] = 0
            if clear_primary_path and "primary_image_path" in cols:
                # Explicit empty — do not leave a fake path pretending to have a photo.
                updates["primary_image_path"] = ""
            if "file_status" in cols:
                updates["file_status"] = "missing"
        elif act == "hold":
            if "repair_status" in cols:
                updates["repair_status"] = HOLD_REPAIR
            # keep paths; just gate inventory
        elif act == "unusable":
            if "source_image_status" in cols:
                updates["source_image_status"] = UNUSABLE_SOURCE
            if "file_status" in cols:
                updates["file_status"] = UNUSABLE_FILE
            if "repair_status" in cols and str(before.get("repair_status") or "") not in {
                HOLD_REPAIR,
                ABANDONED_REPAIR,
            }:
                # soft mark — still recrawl-eligible
                updates["repair_status"] = str(before.get("repair_status") or "") or "none"
            if "source_page_image_status" in cols and not str(
                before.get("source_page_image_status") or ""
            ).strip():
                updates["source_page_image_status"] = "unknown"
        elif act == "resume":
            # Clear hold / no_photo / unusable gates so person re-enters open workflow.
            if "repair_status" in cols:
                cur = str(before.get("repair_status") or "")
                if cur in {HOLD_REPAIR, ABANDONED_REPAIR, "deferred", "none"}:
                    updates["repair_status"] = ""
            if "source_page_image_status" in cols:
                cur = str(before.get("source_page_image_status") or "")
                if cur in BLOCK_VISION_PAGE_STATUSES:
                    updates["source_page_image_status"] = "unknown"
            if "image_status" in cols:
                cur = str(before.get("image_status") or "")
                if cur in BLOCK_VISION_IMAGE_STATUSES:
                    updates["image_status"] = ""
            if "source_image_status" in cols:
                cur = str(before.get("source_image_status") or "")
                if cur in BLOCK_VISION_SOURCE_IMAGE_STATUSES:
                    updates["source_image_status"] = ""
            if "file_status" in cols:
                cur = str(before.get("file_status") or "")
                if cur in {UNUSABLE_FILE, "reject", "missing"}:
                    updates["file_status"] = ""

        note_bits = [f"workflow:{act}", reason_s]
        if hold_until and act == "hold":
            note_bits.append(f"hold_until={hold_until}")
        note_line = " · ".join(x for x in note_bits if x)
        if "notes" in cols:
            updates["notes"] = _append_note(before.get("notes"), note_line)
        # optional free-form JSON side channel if column exists
        if "workflow_meta_json" in cols:
            meta = {}
            try:
                meta = json.loads(str(before.get("workflow_meta_json") or "") or "{}")
            except Exception:
                meta = {}
            if not isinstance(meta, dict):
                meta = {}
            meta.update(
                {
                    "action": act,
                    "reason": reason_s,
                    "at": _utc_stamp(),
                    "hold_until": str(hold_until or "") if act == "hold" else "",
                }
            )
            updates["workflow_meta_json"] = json.dumps(meta, ensure_ascii=False)

        if not updates:
            raise RuntimeError("no writable workflow columns on people table")

        if dry_run:
            after = dict(before)
            after.update(updates)
            return {
                "ok": True,
                "dry_run": True,
                "action": act,
                "person_id": pid,
                "name": before.get("name"),
                "before": {
                    k: before.get(k)
                    for k in (
                        "image_status",
                        "source_page_image_status",
                        "repair_status",
                        "file_status",
                        "source_image_status",
                        "primary_image_path",
                    )
                },
                "after": {
                    k: after.get(k)
                    for k in (
                        "image_status",
                        "source_page_image_status",
                        "repair_status",
                        "file_status",
                        "source_image_status",
                        "primary_image_path",
                    )
                },
                "workflow": workflow_state_from_row(after),
            }

        sets = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [pid]
        conn.execute(f"UPDATE people SET {sets} WHERE person_id = ?", values)
        conn.commit()
        after_row = conn.execute("SELECT * FROM people WHERE person_id = ?", (pid,)).fetchone()
        after = dict(after_row) if after_row else {**before, **updates}
        return {
            "ok": True,
            "dry_run": False,
            "action": act,
            "person_id": pid,
            "name": after.get("name") or before.get("name"),
            "updated_fields": sorted(updates.keys()),
            "before": {
                k: before.get(k)
                for k in (
                    "image_status",
                    "source_page_image_status",
                    "repair_status",
                    "file_status",
                    "source_image_status",
                    "primary_image_path",
                )
            },
            "after": {
                k: after.get(k)
                for k in (
                    "image_status",
                    "source_page_image_status",
                    "repair_status",
                    "file_status",
                    "source_image_status",
                    "primary_image_path",
                )
            },
            "workflow": workflow_state_from_row(after),
        }
    finally:
        conn.close()


def mark_many(
    *,
    person_ids: Sequence[str],
    action: str,
    reason: str = "",
    hold_until: str = "",
    clear_primary_path: bool = True,
    dry_run: bool = False,
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for raw in person_ids:
        pid = str(raw or "").strip()
        if not pid:
            continue
        try:
            results.append(
                mark_person(
                    person_id=pid,
                    action=action,
                    reason=reason,
                    hold_until=hold_until,
                    clear_primary_path=clear_primary_path,
                    dry_run=dry_run,
                    people_db=people_db,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors.append({"person_id": pid, "error": f"{type(exc).__name__}:{exc}"})
    return {
        "ok": not errors,
        "action": action,
        "dry_run": dry_run,
        "count_ok": len(results),
        "count_error": len(errors),
        "results": results,
        "errors": errors,
    }


def list_marked(
    *,
    workflow: str = "",
    limit: int = 200,
    province: str = "",
    city: str = "",
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    """List people currently in no_photo / hold / unusable states."""
    conn = _open(people_db)
    try:
        cols = _cols(conn)
        select = [
            c
            for c in (
                "person_id",
                "name",
                "unit_name",
                "province",
                "city",
                "primary_image_path",
                "image_status",
                "source_page_image_status",
                "source_image_status",
                "file_status",
                "repair_status",
                "visual_gender",
            )
            if c in cols
        ]
        if not select:
            return {"ok": True, "items": [], "total": 0}
        where = ["1=1"]
        params: List[Any] = []
        wf = str(workflow or "").strip().lower()
        if wf in {"no_photo", "no_photo_confirmed"}:
            bits = []
            if "source_page_image_status" in cols:
                bits.append("source_page_image_status = ?")
                params.append(NO_PHOTO_PAGE)
            if "image_status" in cols:
                bits.append("image_status = ?")
                params.append(NO_PHOTO_IMAGE)
            if "repair_status" in cols:
                bits.append("repair_status = ?")
                params.append(ABANDONED_REPAIR)
            if bits:
                where.append("(" + " OR ".join(bits) + ")")
        elif wf == "hold":
            if "repair_status" in cols:
                where.append("repair_status = ?")
                params.append(HOLD_REPAIR)
        elif wf == "unusable":
            bits = []
            if "source_image_status" in cols:
                bits.append("source_image_status = ?")
                params.append(UNUSABLE_SOURCE)
            if "file_status" in cols:
                bits.append("file_status IN (?,?)")
                params.extend([UNUSABLE_FILE, "reject"])
            if bits:
                where.append("(" + " OR ".join(bits) + ")")
        else:
            # any blocked
            bits = []
            if "repair_status" in cols:
                bits.append("repair_status IN (?,?)")
                params.extend([HOLD_REPAIR, ABANDONED_REPAIR])
            if "source_page_image_status" in cols:
                bits.append("source_page_image_status IN (?,?,?)")
                params.extend([NO_PHOTO_PAGE, "placeholder_avatar", "missing_on_source"])
            if "image_status" in cols:
                bits.append("image_status IN (?,?)")
                params.extend([NO_PHOTO_IMAGE, "no_image"])
            if bits:
                where.append("(" + " OR ".join(bits) + ")")
        if province and "province" in cols:
            where.append("province = ?")
            params.append(province)
        if city and "city" in cols:
            where.append("city = ?")
            params.append(city)
        lim = max(1, min(int(limit or 200), 2000))
        sql = (
            f"SELECT {', '.join(select)} FROM people WHERE {' AND '.join(where)} "
            f"ORDER BY rowid DESC LIMIT ?"
        )
        params.append(lim)
        items = []
        for row in conn.execute(sql, tuple(params)):
            data = dict(row)
            data["workflow"] = workflow_state_from_row(data)
            items.append(data)
        return {
            "ok": True,
            "workflow_filter": wf or "any_blocked",
            "total": len(items),
            "items": items,
        }
    finally:
        conn.close()
