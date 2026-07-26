# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from cloud.paths import jobs_db_path

_LOCK = threading.RLock()


def _connect(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = Path(db_path or jobs_db_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    with _LOCK:
        conn = _connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS queues (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL DEFAULT '',
                  template_id TEXT NOT NULL DEFAULT '',
                  template_path TEXT NOT NULL DEFAULT '',
                  start_url TEXT NOT NULL DEFAULT '',
                  output_root TEXT NOT NULL,
                  speed_tier TEXT NOT NULL DEFAULT 'safe',
                  speed_tier_reason TEXT NOT NULL DEFAULT '',
                  desired_state TEXT NOT NULL DEFAULT 'created',
                  last_error TEXT NOT NULL DEFAULT '',
                  notes TEXT NOT NULL DEFAULT '',
                  meta_json TEXT NOT NULL DEFAULT '{}',
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_queues_updated ON queues(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_queues_output ON queues(output_root);
                """
            )
            conn.commit()
        finally:
            conn.close()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    meta_raw = data.pop("meta_json", "{}")
    try:
        meta = json.loads(meta_raw) if meta_raw else {}
    except Exception:
        meta = {}
    if not isinstance(meta, dict):
        meta = {}
    data["meta"] = meta
    return data


def create_queue_record(
    *,
    name: str,
    template_id: str,
    template_path: str,
    start_url: str,
    output_root: str,
    speed_tier: str = "safe",
    speed_tier_reason: str = "",
    notes: str = "",
    meta: Optional[Dict[str, Any]] = None,
    queue_id: str = "",
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    now = time.time()
    qid = str(queue_id or "").strip() or f"q_{uuid.uuid4().hex[:12]}"
    payload = {
        "id": qid,
        "name": str(name or "").strip() or qid,
        "template_id": str(template_id or "").strip(),
        "template_path": str(template_path or "").strip(),
        "start_url": str(start_url or "").strip(),
        "output_root": str(output_root or "").strip(),
        "speed_tier": str(speed_tier or "safe").strip() or "safe",
        "speed_tier_reason": str(speed_tier_reason or "").strip(),
        "desired_state": "created",
        "last_error": "",
        "notes": str(notes or "").strip(),
        "meta_json": json.dumps(meta if isinstance(meta, dict) else {}, ensure_ascii=False),
        "created_at": now,
        "updated_at": now,
    }
    with _LOCK:
        conn = _connect(db_path)
        try:
            conn.execute(
                """
                INSERT INTO queues (
                  id, name, template_id, template_path, start_url, output_root,
                  speed_tier, speed_tier_reason, desired_state, last_error, notes,
                  meta_json, created_at, updated_at
                ) VALUES (
                  :id, :name, :template_id, :template_path, :start_url, :output_root,
                  :speed_tier, :speed_tier_reason, :desired_state, :last_error, :notes,
                  :meta_json, :created_at, :updated_at
                )
                """,
                payload,
            )
            conn.commit()
        finally:
            conn.close()
    return get_queue(qid, db_path=db_path) or payload


def list_queues(db_path: Optional[Path] = None, *, limit: int = 200) -> List[Dict[str, Any]]:
    with _LOCK:
        conn = _connect(db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM queues ORDER BY updated_at DESC LIMIT ?",
                (max(1, int(limit or 200)),),
            ).fetchall()
            return [_row_to_dict(row) for row in rows]
        finally:
            conn.close()


def get_queue(queue_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    qid = str(queue_id or "").strip()
    if not qid:
        return None
    with _LOCK:
        conn = _connect(db_path)
        try:
            row = conn.execute("SELECT * FROM queues WHERE id = ?", (qid,)).fetchone()
            return _row_to_dict(row) if row else None
        finally:
            conn.close()


def get_queue_by_output_root(output_root: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    root = str(output_root or "").strip()
    if not root:
        return None
    with _LOCK:
        conn = _connect(db_path)
        try:
            row = conn.execute(
                "SELECT * FROM queues WHERE output_root = ? ORDER BY updated_at DESC LIMIT 1",
                (root,),
            ).fetchone()
            return _row_to_dict(row) if row else None
        finally:
            conn.close()


def update_queue(
    queue_id: str,
    *,
    desired_state: Optional[str] = None,
    last_error: Optional[str] = None,
    notes: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    speed_tier: Optional[str] = None,
    speed_tier_reason: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    current = get_queue(queue_id, db_path=db_path)
    if not current:
        return None
    fields: Dict[str, Any] = {"id": queue_id, "updated_at": time.time()}
    sets = ["updated_at = :updated_at"]
    if desired_state is not None:
        fields["desired_state"] = str(desired_state)
        sets.append("desired_state = :desired_state")
    if last_error is not None:
        fields["last_error"] = str(last_error)
        sets.append("last_error = :last_error")
    if notes is not None:
        fields["notes"] = str(notes)
        sets.append("notes = :notes")
    if speed_tier is not None:
        fields["speed_tier"] = str(speed_tier)
        sets.append("speed_tier = :speed_tier")
    if speed_tier_reason is not None:
        fields["speed_tier_reason"] = str(speed_tier_reason)
        sets.append("speed_tier_reason = :speed_tier_reason")
    if meta is not None:
        merged = dict(current.get("meta") or {})
        if isinstance(meta, dict):
            merged.update(meta)
        fields["meta_json"] = json.dumps(merged, ensure_ascii=False)
        sets.append("meta_json = :meta_json")
    with _LOCK:
        conn = _connect(db_path)
        try:
            conn.execute(f"UPDATE queues SET {', '.join(sets)} WHERE id = :id", fields)
            conn.commit()
        finally:
            conn.close()
    return get_queue(queue_id, db_path=db_path)
