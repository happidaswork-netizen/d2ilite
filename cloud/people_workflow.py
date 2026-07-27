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
import re
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


def derive_source_bucket(row: Dict[str, Any]) -> str:
    """P1-2: cloud_queue vs legacy_import.

    Cloud queue rows usually carry queue_id in notes/raw or non-legacy path forms.
    Legacy scrape paths still look like /data/photos/originals/… with no queue trail.
    """
    path = str(row.get("primary_image_path") or "").replace("\\", "/")
    notes = str(row.get("notes") or "")
    raw = str(row.get("raw_json") or row.get("extra_json") or "")
    blob = f"{notes}\n{raw}"
    if re.search(r"\bq_[0-9a-f]{6,}\b", blob) or "queue_id" in blob or "promoted_at" in blob:
        return "cloud_queue"
    if path.startswith("/data/photos/originals/") or "山东公开官员" in path:
        return "legacy_import"
    if path.startswith("/runtime/portrait/") or "/角色肖像/" in path or path.startswith("/vol1/1001/角色肖像"):
        # could be either; prefer cloud if notes mention workflow promote
        if "promoted" in blob.lower() or "workflow:rebind" in blob:
            return "cloud_queue"
        # default: path under portrait without queue trail → still treat as legacy if no visual
        if not str(row.get("visual_gender") or "").strip() and not re.search(r"q_", blob):
            return "legacy_import"
        return "cloud_queue"
    if not path:
        return "legacy_import"
    return "legacy_import"


def enrich_path_info(row: Dict[str, Any]) -> Dict[str, Any]:
    """P1-1: resolved_path / on_disk / width / height / size_bytes for API consumers."""
    from cloud.vision_service import resolve_image_path

    raw = str(row.get("primary_image_path") or "").strip()
    info: Dict[str, Any] = {
        "primary_image_path": raw,
        "resolved_path": "",
        "on_disk": False,
        "width": 0,
        "height": 0,
        "min_edge": 0,
        "size_bytes": 0,
        "error_code": "",
    }
    if not raw:
        info["error_code"] = "no_primary_path"
        return info
    resolved = resolve_image_path(raw)
    if resolved is None:
        info["error_code"] = "image_not_found"
        return info
    info["resolved_path"] = str(resolved).replace("\\", "/")
    inspect = _inspect_image_file(resolved)
    info["on_disk"] = bool(inspect.get("exists"))
    info["width"] = int(inspect.get("width") or 0)
    info["height"] = int(inspect.get("height") or 0)
    info["min_edge"] = int(inspect.get("min_edge") or 0)
    info["size_bytes"] = int(inspect.get("size_bytes") or 0)
    if not inspect.get("ok"):
        info["error_code"] = str(inspect.get("error_code") or "image_invalid")
    return info


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
        data["source_bucket"] = derive_source_bucket(data)
        data["path_info"] = enrich_path_info(data)
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
        result = {
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
        # P0-1: mark no_photo/unusable/hold auto-resolves open recrawl-inbox entries.
        if act in {"no_photo", "unusable", "hold"}:
            try:
                from cloud import vision_service

                result["inbox"] = vision_service.resolve_inbox_for_person(
                    pid,
                    action=act,
                    reason=reason_s,
                    status="resolved",
                )
            except Exception as exc:  # noqa: BLE001
                result["inbox_error"] = f"{type(exc).__name__}:{exc}"
        return result
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
        # enrich lightly
        for data in items:
            data["source_bucket"] = derive_source_bucket(data)
        return {
            "ok": True,
            "workflow_filter": wf or "any_blocked",
            "total": len(items),
            "items": items,
        }
    finally:
        conn.close()


def search_people(
    *,
    q: str = "",
    province: str = "",
    city: str = "",
    unit_like: str = "",
    workflow: str = "",
    source_bucket: str = "",
    limit: int = 50,
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    """P1-5: light people search — not a CRM.

    Returns rows + same-name collision hints + dirty unit_name flags.
    """
    conn = _open(people_db)
    try:
        cols = _cols(conn)
        select = [
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
                "visual_gender",
            )
            if c in cols
        ]
        if "person_id" not in select or "name" not in select:
            return {"ok": False, "error": "people schema incomplete", "items": [], "total": 0}
        where = ["1=1"]
        params: List[Any] = []
        qq = str(q or "").strip()
        if qq:
            where.append("(name LIKE ? OR unit_name LIKE ? OR person_id = ?)")
            params.extend([f"%{qq}%", f"%{qq}%", qq])
        if province and "province" in cols:
            where.append("province = ?")
            params.append(province)
        if city and "city" in cols:
            where.append("city = ?")
            params.append(city)
        if unit_like and "unit_name" in cols:
            where.append("unit_name LIKE ?")
            params.append(f"%{unit_like}%")
        # workflow coarse filter reuses list_marked semantics via post-filter
        lim = max(1, min(int(limit or 50), 500))
        sql = (
            f"SELECT {', '.join(select)} FROM people WHERE {' AND '.join(where)} "
            f"ORDER BY rowid DESC LIMIT ?"
        )
        params.append(lim * 3)  # over-fetch then filter
        raw_items: List[Dict[str, Any]] = []
        for row in conn.execute(sql, tuple(params)):
            data = dict(row)
            data["workflow"] = workflow_state_from_row(data)
            data["source_bucket"] = derive_source_bucket(data)
            raw_items.append(data)

        wf = str(workflow or "").strip().lower()
        sb = str(source_bucket or "").strip().lower()
        items: List[Dict[str, Any]] = []
        name_counts: Dict[str, int] = {}
        for data in raw_items:
            if wf:
                w = str((data.get("workflow") or {}).get("workflow") or "")
                if wf in {"no_photo", "no_photo_confirmed"} and w != "no_photo_confirmed":
                    continue
                if wf == "hold" and w != "hold":
                    continue
                if wf == "unusable" and w != "unusable_asset":
                    continue
                if wf == "open" and w not in {"open", "has_photo"}:
                    continue
            if sb and data.get("source_bucket") != sb:
                continue
            nm = str(data.get("name") or "")
            name_counts[nm] = int(name_counts.get(nm) or 0) + 1
            unit = str(data.get("unit_name") or "")
            data["dirty_unit"] = bool(
                unit
                and (
                    len(unit) > 40
                    or re.search(r"(要闻|动态|首页|导航|菜单|undefined|null|测试)", unit)
                    or unit in {"未知", "未命名", "(空)"}
                )
            )
            items.append(data)
            if len(items) >= lim:
                break
        collisions = [
            {"name": n, "count": c}
            for n, c in sorted(name_counts.items(), key=lambda kv: -kv[1])
            if c >= 2 and n
        ][:20]
        for data in items:
            nm = str(data.get("name") or "")
            data["name_collision"] = int(name_counts.get(nm) or 0) >= 2
        return {
            "ok": True,
            "total": len(items),
            "items": items,
            "name_collisions": collisions,
            "filters": {
                "q": qq,
                "province": province,
                "city": city,
                "unit_like": unit_like,
                "workflow": wf,
                "source_bucket": sb,
                "limit": lim,
            },
        }
    finally:
        conn.close()


# Minimum edge length for a usable primary (20×20 placeholders must fail).
REBIND_MIN_EDGE = 40


def _inspect_image_file(path: Path) -> Dict[str, Any]:
    """Read basic geometry; reject truncated / unreadable images."""
    from PIL import Image

    info: Dict[str, Any] = {
        "path": str(path),
        "exists": path.is_file(),
        "size_bytes": 0,
        "width": 0,
        "height": 0,
        "min_edge": 0,
        "ok": False,
        "error": "",
        "error_code": "",
    }
    if not path.is_file():
        info["error"] = "image_not_found"
        info["error_code"] = "image_not_found"
        return info
    try:
        info["size_bytes"] = int(path.stat().st_size)
    except OSError as exc:
        info["error"] = f"stat_failed:{exc}"
        info["error_code"] = "image_invalid"
        return info
    if info["size_bytes"] <= 0:
        info["error"] = "0 bytes not processed"
        info["error_code"] = "image_truncated"
        return info
    try:
        with Image.open(path) as im:
            im.load()  # force full decode → catch truncated
            w, h = im.size
        info["width"] = int(w)
        info["height"] = int(h)
        info["min_edge"] = min(int(w), int(h))
    except Exception as exc:  # noqa: BLE001
        msg = f"{type(exc).__name__}:{exc}"
        low = msg.lower()
        info["error"] = msg
        if "truncated" in low or "oserror" in low:
            info["error_code"] = "image_truncated"
        else:
            info["error_code"] = "image_invalid"
        return info
    if info["min_edge"] < REBIND_MIN_EDGE:
        info["error"] = f"too small: {info['width']}x{info['height']} (min_edge>={REBIND_MIN_EDGE})"
        info["error_code"] = "image_too_small"
        return info
    info["ok"] = True
    return info


def rebind_primary(
    *,
    person_id: str,
    path: str,
    dry_run: bool = False,
    reason: str = "",
    people_db: Optional[Path] = None,
    clear_gates: bool = True,
) -> Dict[str, Any]:
    """Point primary_image_path at a verified local portrait and reopen vision gates.

    P0-2: server validates file exists, min edge, non-truncated, not protected path.
    """
    from cloud.vision_service import resolve_image_path, resolve_inbox_for_person

    pid = str(person_id or "").strip()
    raw_path = str(path or "").strip()
    if not pid:
        raise ValueError("person_id required")
    if not raw_path:
        raise ValueError("path required")

    resolved = resolve_image_path(raw_path)
    if resolved is None:
        raise ValueError(
            f"path rejected or not found under portrait/task roots (protected paths blocked): {raw_path}"
        )
    inspect = _inspect_image_file(resolved)
    if not inspect.get("ok"):
        raise ValueError(
            f"rebind rejected ({inspect.get('error_code') or 'invalid'}): {inspect.get('error')}"
        )

    # Prefer container-stable portrait form when under host portrait.
    store_path = str(resolved).replace("\\", "/")
    try:
        from cloud.paths import portrait_root

        root = portrait_root().resolve()
        try:
            rel = resolved.resolve().relative_to(root)
            store_path = f"/runtime/portrait/{rel.as_posix()}"
        except ValueError:
            host = Path("/vol1/1001/角色肖像")
            try:
                rel = resolved.resolve().relative_to(host)
                store_path = f"/runtime/portrait/{rel.as_posix()}"
            except ValueError:
                pass
    except Exception:
        pass

    conn = _open(people_db)
    try:
        cols = _cols(conn)
        if "person_id" not in cols:
            raise RuntimeError("people.person_id column missing")
        row = conn.execute("SELECT * FROM people WHERE person_id = ?", (pid,)).fetchone()
        if row is None:
            raise KeyError(f"person not found: {pid}")
        before = dict(row)
        updates: Dict[str, Any] = {}
        if "primary_image_path" in cols:
            updates["primary_image_path"] = store_path
        if "source_page_image_status" in cols:
            updates["source_page_image_status"] = "has_photo"
        if "image_status" in cols:
            # keep official_photo if already set; otherwise has_photo
            cur_img = str(before.get("image_status") or "").strip()
            if cur_img in {"", NO_PHOTO_IMAGE, "no_image"}:
                updates["image_status"] = "has_photo"
        if "file_status" in cols:
            updates["file_status"] = "available"
        if "source_image_status" in cols:
            updates["source_image_status"] = "ok"
        if "has_official_photo" in cols:
            updates["has_official_photo"] = 1
        if clear_gates:
            if "repair_status" in cols:
                cur = str(before.get("repair_status") or "")
                if cur in {HOLD_REPAIR, ABANDONED_REPAIR, "deferred"}:
                    updates["repair_status"] = ""
        reason_s = str(reason or "").strip() or "rebind-primary"
        if "notes" in cols:
            updates["notes"] = _append_note(
                before.get("notes"),
                f"workflow:rebind · {reason_s} · {store_path}",
            )

        if dry_run:
            after = dict(before)
            after.update(updates)
            return {
                "ok": True,
                "dry_run": True,
                "action": "rebind",
                "person_id": pid,
                "name": before.get("name"),
                "path_requested": raw_path,
                "path_resolved": str(resolved),
                "path_stored": store_path,
                "inspect": inspect,
                "before": {"primary_image_path": before.get("primary_image_path")},
                "after": {"primary_image_path": store_path},
                "workflow": workflow_state_from_row(after),
            }

        sets = ", ".join(f"{k} = ?" for k in updates.keys())
        conn.execute(f"UPDATE people SET {sets} WHERE person_id = ?", (*updates.values(), pid))
        conn.commit()
        after_row = conn.execute("SELECT * FROM people WHERE person_id = ?", (pid,)).fetchone()
        after = dict(after_row) if after_row else {**before, **updates}
        result = {
            "ok": True,
            "dry_run": False,
            "action": "rebind",
            "person_id": pid,
            "name": after.get("name") or before.get("name"),
            "path_requested": raw_path,
            "path_resolved": str(resolved),
            "path_stored": store_path,
            "inspect": inspect,
            "updated_fields": sorted(updates.keys()),
            "before": {
                k: before.get(k)
                for k in (
                    "primary_image_path",
                    "image_status",
                    "source_page_image_status",
                    "repair_status",
                    "file_status",
                    "source_image_status",
                )
            },
            "after": {
                k: after.get(k)
                for k in (
                    "primary_image_path",
                    "image_status",
                    "source_page_image_status",
                    "repair_status",
                    "file_status",
                    "source_image_status",
                )
            },
            "workflow": workflow_state_from_row(after),
        }
        try:
            result["inbox"] = resolve_inbox_for_person(
                pid, action="rebound", reason=reason_s, status="resolved"
            )
        except Exception as exc:  # noqa: BLE001
            result["inbox_error"] = f"{type(exc).__name__}:{exc}"
        return result
    finally:
        conn.close()
