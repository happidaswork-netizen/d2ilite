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

                CREATE TABLE IF NOT EXISTS vision_jobs (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'queued',
                  priority INTEGER NOT NULL DEFAULT 100,
                  batch_key TEXT NOT NULL DEFAULT '',
                  province TEXT NOT NULL DEFAULT '',
                  city TEXT NOT NULL DEFAULT '',
                  total INTEGER NOT NULL DEFAULT 0,
                  done_count INTEGER NOT NULL DEFAULT 0,
                  ok_count INTEGER NOT NULL DEFAULT 0,
                  failed_count INTEGER NOT NULL DEFAULT 0,
                  skipped_count INTEGER NOT NULL DEFAULT 0,
                  missing_count INTEGER NOT NULL DEFAULT 0,
                  force INTEGER NOT NULL DEFAULT 0,
                  write_people INTEGER NOT NULL DEFAULT 1,
                  dry_run INTEGER NOT NULL DEFAULT 0,
                  items_json TEXT NOT NULL DEFAULT '[]',
                  result_json TEXT NOT NULL DEFAULT '{}',
                  error TEXT NOT NULL DEFAULT '',
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL,
                  started_at REAL,
                  finished_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_vision_jobs_status
                  ON vision_jobs(status, priority ASC, created_at ASC);
                CREATE INDEX IF NOT EXISTS idx_vision_jobs_updated
                  ON vision_jobs(updated_at DESC);
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


def _vision_row(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    for key, default in (("items_json", "[]"), ("result_json", "{}")):
        raw = data.pop(key, default)
        try:
            parsed = json.loads(raw) if raw else ([] if key == "items_json" else {})
        except Exception:
            parsed = [] if key == "items_json" else {}
        data[key.replace("_json", "")] = parsed
    data["force"] = bool(int(data.get("force") or 0))
    data["write_people"] = bool(int(data.get("write_people") if data.get("write_people") is not None else 1))
    data["dry_run"] = bool(int(data.get("dry_run") or 0))
    return data


def create_vision_job(
    *,
    name: str = "",
    batch_key: str = "",
    province: str = "",
    city: str = "",
    items: Optional[List[Dict[str, Any]]] = None,
    priority: int = 100,
    force: bool = False,
    write_people: bool = True,
    dry_run: bool = False,
    job_id: str = "",
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    now = time.time()
    jid = str(job_id or "").strip() or f"vj_{uuid.uuid4().hex[:12]}"
    payload_items = list(items or [])
    row = {
        "id": jid,
        "name": str(name or "").strip() or jid,
        "status": "queued",
        "priority": int(priority if priority is not None else 100),
        "batch_key": str(batch_key or "").strip(),
        "province": str(province or "").strip(),
        "city": str(city or "").strip(),
        "total": len(payload_items),
        "done_count": 0,
        "ok_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "missing_count": 0,
        "force": 1 if force else 0,
        "write_people": 1 if write_people else 0,
        "dry_run": 1 if dry_run else 0,
        "items_json": json.dumps(payload_items, ensure_ascii=False),
        "result_json": "{}",
        "error": "",
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
    }
    with _LOCK:
        conn = _connect(db_path)
        try:
            conn.execute(
                """
                INSERT INTO vision_jobs (
                  id, name, status, priority, batch_key, province, city, total,
                  done_count, ok_count, failed_count, skipped_count, missing_count,
                  force, write_people, dry_run, items_json, result_json, error,
                  created_at, updated_at, started_at, finished_at
                ) VALUES (
                  :id, :name, :status, :priority, :batch_key, :province, :city, :total,
                  :done_count, :ok_count, :failed_count, :skipped_count, :missing_count,
                  :force, :write_people, :dry_run, :items_json, :result_json, :error,
                  :created_at, :updated_at, :started_at, :finished_at
                )
                """,
                row,
            )
            conn.commit()
        finally:
            conn.close()
    return get_vision_job(jid, db_path=db_path) or _vision_row_from_insert(row)


def _vision_row_from_insert(row: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(row)
    try:
        data["items"] = json.loads(data.pop("items_json", "[]") or "[]")
    except Exception:
        data["items"] = []
        data.pop("items_json", None)
    try:
        data["result"] = json.loads(data.pop("result_json", "{}") or "{}")
    except Exception:
        data["result"] = {}
        data.pop("result_json", None)
    data["force"] = bool(int(data.get("force") or 0))
    data["write_people"] = bool(int(data.get("write_people") if data.get("write_people") is not None else 1))
    data["dry_run"] = bool(int(data.get("dry_run") or 0))
    return data


def list_vision_jobs(
    db_path: Optional[Path] = None,
    *,
    limit: int = 100,
    status: str = "",
) -> List[Dict[str, Any]]:
    st = str(status or "").strip().lower()
    with _LOCK:
        conn = _connect(db_path)
        try:
            if st:
                rows = conn.execute(
                    "SELECT * FROM vision_jobs WHERE status=? ORDER BY priority ASC, created_at ASC LIMIT ?",
                    (st, max(1, min(int(limit or 100), 500))),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM vision_jobs ORDER BY updated_at DESC LIMIT ?",
                    (max(1, min(int(limit or 100), 500)),),
                ).fetchall()
            return [_vision_row(r) for r in rows]
        finally:
            conn.close()


def get_vision_job(job_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    jid = str(job_id or "").strip()
    if not jid:
        return None
    with _LOCK:
        conn = _connect(db_path)
        try:
            row = conn.execute("SELECT * FROM vision_jobs WHERE id=?", (jid,)).fetchone()
            return _vision_row(row) if row else None
        finally:
            conn.close()


def claim_next_vision_job(db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Atomically move the highest-priority queued job to running."""
    now = time.time()
    with _LOCK:
        conn = _connect(db_path)
        try:
            row = conn.execute(
                """
                SELECT * FROM vision_jobs
                WHERE status='queued'
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            jid = str(row["id"])
            conn.execute(
                """
                UPDATE vision_jobs
                SET status='running', started_at=?, updated_at=?, error=''
                WHERE id=? AND status='queued'
                """,
                (now, now, jid),
            )
            conn.commit()
            claimed = conn.execute("SELECT * FROM vision_jobs WHERE id=?", (jid,)).fetchone()
            if claimed and str(claimed["status"]) == "running":
                return _vision_row(claimed)
            return None
        finally:
            conn.close()


def update_vision_job(
    job_id: str,
    *,
    status: Optional[str] = None,
    done_count: Optional[int] = None,
    ok_count: Optional[int] = None,
    failed_count: Optional[int] = None,
    skipped_count: Optional[int] = None,
    missing_count: Optional[int] = None,
    result: Optional[Dict[str, Any]] = None,
    items: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
    finished: bool = False,
    mark_started: bool = False,
    expected_status: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """Update one vision job row.

    expected_status(F2 CAS): 传入时 UPDATE 带 WHERE status=?,当前状态不匹配
    则一行都不改(如取消后终态写不进,保留 cancelled)。返回值始终是重查后的
    最新行,调用方通过 status 判断写入是否生效。
    """
    current = get_vision_job(job_id, db_path=db_path)
    if not current:
        return None
    fields: Dict[str, Any] = {"id": job_id, "updated_at": time.time()}
    sets = ["updated_at = :updated_at"]
    if status is not None:
        fields["status"] = str(status)
        sets.append("status = :status")
        if str(status) == "running" or mark_started:
            if not current.get("started_at"):
                fields["started_at"] = time.time()
                sets.append("started_at = :started_at")
    elif mark_started and not current.get("started_at"):
        fields["started_at"] = time.time()
        sets.append("started_at = :started_at")
    for key, val in (
        ("done_count", done_count),
        ("ok_count", ok_count),
        ("failed_count", failed_count),
        ("skipped_count", skipped_count),
        ("missing_count", missing_count),
    ):
        if val is not None:
            fields[key] = int(val)
            sets.append(f"{key} = :{key}")
    if result is not None:
        fields["result_json"] = json.dumps(result if isinstance(result, dict) else {}, ensure_ascii=False)
        sets.append("result_json = :result_json")
    if items is not None:
        payload_items = list(items or [])
        fields["items_json"] = json.dumps(payload_items, ensure_ascii=False)
        fields["total"] = len(payload_items)
        sets.append("items_json = :items_json")
        sets.append("total = :total")
    if error is not None:
        fields["error"] = str(error)
        sets.append("error = :error")
    if finished:
        fields["finished_at"] = time.time()
        sets.append("finished_at = :finished_at")
    with _LOCK:
        conn = _connect(db_path)
        try:
            where = "id = :id"
            if expected_status is not None:
                fields["expected_status"] = str(expected_status)
                where += " AND status = :expected_status"
            conn.execute(f"UPDATE vision_jobs SET {', '.join(sets)} WHERE {where}", fields)
            conn.commit()
        finally:
            conn.close()
    return get_vision_job(job_id, db_path=db_path)


def vision_job_counts(db_path: Optional[Path] = None) -> Dict[str, int]:
    with _LOCK:
        conn = _connect(db_path)
        try:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM vision_jobs GROUP BY status"
            ).fetchall()
            out = {str(r["status"]): int(r["n"]) for r in rows}
            out["total"] = sum(out.values())
            return out
        finally:
            conn.close()


def normalize_vision_job_statuses(db_path: Optional[Path] = None) -> Dict[str, Any]:
    """One-shot repair: finished jobs whose status lies about their counts.

    completed + ok=0 + failed>0        -> failed
    completed + ok>0 + failed>0        -> completed_with_errors
    """
    now = time.time()
    with _LOCK:
        conn = _connect(db_path)
        try:
            to_failed = conn.execute(
                "SELECT id FROM vision_jobs WHERE status='completed' AND ok_count=0 AND failed_count>0"
            ).fetchall()
            to_partial = conn.execute(
                "SELECT id FROM vision_jobs WHERE status='completed' AND ok_count>0 AND failed_count>0"
            ).fetchall()
            conn.execute(
                "UPDATE vision_jobs SET status='failed', updated_at=? "
                "WHERE status='completed' AND ok_count=0 AND failed_count>0",
                (now,),
            )
            conn.execute(
                "UPDATE vision_jobs SET status='completed_with_errors', updated_at=? "
                "WHERE status='completed' AND ok_count>0 AND failed_count>0",
                (now,),
            )
            conn.commit()
            return {
                "ok": True,
                "to_failed": [str(r["id"]) for r in to_failed],
                "to_completed_with_errors": [str(r["id"]) for r in to_partial],
                "counts": None,
            }
        finally:
            conn.close()


def cancel_vision_job(job_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    current = get_vision_job(job_id, db_path=db_path)
    if not current:
        return None
    st = str(current.get("status") or "")
    if st not in {"queued", "running"}:
        return current
    # CAS:检查到写入之间状态变了(比如恰好跑完)就不覆盖,返回真实终态。
    # running 态的协作取消由 vision_service.run_vision_job 逐片检查完成。
    return update_vision_job(
        job_id,
        status="cancelled",
        error="cancelled by operator",
        finished=True,
        expected_status=st,
        db_path=db_path,
    )


def requeue_stale_running_vision_jobs(
    db_path: Optional[Path] = None,
    *,
    reason: str = "reset stale running after restart",
    stale_after_seconds: float = 1800.0,
) -> int:
    """Move orphaned running vision jobs back to queued (e.g. after container restart).

    F1: 只有 started_at/updated_at 均早于 stale_after_seconds(默认 30 分钟)
    的 running 行才算 stale——活着的泵每片都会刷新 updated_at,在跑的批
    不会被打回 queued 重复计费。传 0 表示不设阈值(显式恢复场景)。
    """
    now = time.time()
    threshold = now - max(0.0, float(stale_after_seconds or 0.0))
    with _LOCK:
        conn = _connect(db_path)
        try:
            cur = conn.execute(
                """
                UPDATE vision_jobs
                SET status='queued',
                    started_at=NULL,
                    error=?,
                    updated_at=?
                WHERE status='running'
                  AND MAX(
                        COALESCE(started_at, 0),
                        COALESCE(updated_at, 0)
                      ) < ?
                """,
                (str(reason or "reset stale running"), now, threshold),
            )
            conn.commit()
            return int(cur.rowcount or 0)
        finally:
            conn.close()
