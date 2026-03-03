# -*- coding: utf-8 -*-
"""Task domain services for public scraper orchestration."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from services.scraper_monitor_service import read_jsonl_rows


@dataclass
class TaskRuntimeState:
    """Minimal runtime state model for task-oriented panels."""

    task_id: str
    status: str
    message: str = ""
    started_at_ts: Optional[float] = None
    finished_at_ts: Optional[float] = None


def normalize_public_task_root(output_root: Any) -> str:
    raw = str(output_root or "").strip()
    return os.path.abspath(raw) if raw else ""


def is_process_running(proc: Any) -> bool:
    return bool(proc and (proc.poll() is None))


def public_scraper_pause_flag_path(output_root: Any) -> str:
    root = normalize_public_task_root(output_root)
    if not root:
        return ""
    return os.path.join(root, "state", "manual_pause.flag")


def normalize_existing_path(path_value: Any) -> str:
    path = str(path_value or "").strip()
    if not path:
        return ""
    try:
        normalized = os.path.abspath(path)
    except Exception:
        normalized = path
    return normalized if os.path.isfile(normalized) else ""


def read_json_file(path: Any) -> Dict[str, Any]:
    text = str(path or "").strip()
    if (not text) or (not os.path.exists(text)):
        return {}
    try:
        with open(text, "r", encoding="utf-8", errors="ignore") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def safe_positive_int(value: Any) -> int:
    try:
        num = int(value)
        return num if num > 0 else 0
    except Exception:
        return 0


def count_jsonl_rows_no_cache(path: Any) -> int:
    text = str(path or "").strip()
    if (not text) or (not os.path.exists(text)):
        return 0
    try:
        stat = os.stat(text)
    except Exception:
        return 0
    count = 0
    try:
        with open(text, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                count += chunk.count(b"\n")
        if stat.st_size > 0:
            with open(text, "rb") as f:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b"\n":
                    count += 1
    except Exception:
        return 0
    return max(0, int(count))


def estimate_scraper_total_target(
    output_root: Any,
    *,
    count_jsonl_rows_fn: Optional[Callable[[str], int]] = None,
) -> int:
    root = normalize_public_task_root(output_root)
    if not root:
        return 0

    count_rows = count_jsonl_rows_fn or count_jsonl_rows_no_cache
    candidates: List[int] = []

    crawl_report = read_json_file(os.path.join(root, "reports", "crawl_report.json"))
    if crawl_report:
        metrics = crawl_report.get("metrics_this_run")
        if isinstance(metrics, dict):
            candidates.append(safe_positive_int(metrics.get("detail_requests_enqueued")))
        totals = crawl_report.get("totals_on_disk")
        if isinstance(totals, dict):
            candidates.append(safe_positive_int(totals.get("profiles")))

    seen_detail_urls: set[str] = set()
    list_path = os.path.join(root, "raw", "list_records.jsonl")
    for row in read_jsonl_rows(list_path, max_rows=0):
        if not isinstance(row, dict):
            continue
        detail_url = str(row.get("detail_url", "")).strip()
        if detail_url:
            seen_detail_urls.add(detail_url)

    candidates.append(len(seen_detail_urls))
    candidates.append(safe_positive_int(count_rows(os.path.join(root, "raw", "profiles.jsonl"))))
    candidates.append(safe_positive_int(count_rows(os.path.join(root, "downloads", "image_downloads.jsonl"))))
    return max(candidates) if candidates else 0


def get_scraper_record_path(output_root: Any) -> str:
    root = normalize_public_task_root(output_root)
    if not root:
        return ""
    path = os.path.join(root, "crawl_record.json")
    return path if os.path.exists(path) else ""


def read_scraper_backoff_state(output_root: Any) -> Dict[str, str]:
    default_payload = {"blocked_until": "", "blocked_reason": ""}
    root = normalize_public_task_root(output_root)
    if not root:
        return default_payload
    path = os.path.join(root, "state", "backoff_state.json")
    if not os.path.exists(path):
        return default_payload
    payload = read_json_file(path)
    return {
        "blocked_until": str(payload.get("blocked_until", "")).strip(),
        "blocked_reason": str(payload.get("blocked_reason", "")).strip(),
    }


def default_public_tasks_root(app_file: Any) -> str:
    app_path = str(app_file or "").strip()
    if not app_path:
        return os.path.abspath(os.path.join(os.getcwd(), "data", "public_archive"))
    app_dir = os.path.dirname(os.path.abspath(app_path))
    return os.path.abspath(os.path.join(app_dir, "data", "public_archive"))


def discover_public_task_roots(base_root: Any) -> List[str]:
    base = normalize_public_task_root(base_root)
    if (not base) or (not os.path.isdir(base)):
        return []
    roots: List[str] = []
    for root, dirs, _files in os.walk(base):
        runtime_cfg = os.path.join(root, "state", "runtime_config.json")
        if os.path.exists(runtime_cfg):
            roots.append(os.path.abspath(root))
            dirs[:] = []
            continue
        dirs[:] = [d for d in dirs if d not in {"raw", "downloads", "reports", "state", "__pycache__"}]
    return sorted({os.path.abspath(x) for x in roots})


def count_latest_metadata_status(output_root: Any) -> Tuple[int, int]:
    root = normalize_public_task_root(output_root)
    if not root:
        return 0, 0
    path = os.path.join(root, "raw", "metadata_write_results.jsonl")
    latest: Dict[str, str] = {}
    for row in read_jsonl_rows(path, max_rows=0):
        if not isinstance(row, dict):
            continue
        detail_url = str(row.get("detail_url", "")).strip()
        if not detail_url:
            continue
        latest[detail_url] = str(row.get("status", "")).strip().lower()
    ok_count = sum(1 for status in latest.values() if status == "ok")
    fail_count = sum(1 for status in latest.values() if status and status != "ok")
    return ok_count, fail_count


def retry_requires_crawl_phase(
    rows: Iterable[Dict[str, Any]],
    *,
    is_row_completed_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> bool:
    for row in rows:
        completed = False
        if is_row_completed_fn is not None:
            try:
                completed = bool(is_row_completed_fn(row))
            except Exception:
                completed = False
        else:
            completed = str(row.get("meta", "")).strip() in {"√", "✓"}
        if completed:
            continue
        detail_status = str(row.get("detail", "")).strip()
        if detail_status not in {"√", "✓"}:
            return True
    return False


def derive_public_task_status(
    *,
    root: str,
    entry: Optional[Dict[str, Any]],
    current_active_root: str,
    pause_flag_exists: bool,
    backoff_state: Dict[str, str],
    profile_rows: int,
    pending_rows: int,
    metadata_failed: int,
    activity_total: int,
) -> str:
    status = "初始化"
    task_entry = entry if isinstance(entry, dict) else {}
    running_in_session = is_process_running(task_entry.get("proc"))
    if running_in_session:
        paused = bool(task_entry.get("manual_paused", False))
        if paused and current_active_root == root:
            return "手动暂停(当前)"
        return "手动暂停" if paused else "运行中"

    if pause_flag_exists:
        return "手动暂停"

    if str(backoff_state.get("blocked_until", "")).strip():
        return "风控暂停"
    if profile_rows > 0 and pending_rows == 0 and metadata_failed == 0:
        return "已完成"
    if activity_total > 0:
        return "未完成"

    runtime_state = str(task_entry.get("runtime_state", "")).strip()
    return runtime_state or status


def suggest_public_scraper_output_root(app_file: Any, site_name: Any) -> str:
    root = str(site_name or "").strip() or "public_task"
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(str(app_file or ""))), "data", "public_archive", root))


def public_scraper_templates_dir(app_file: Any) -> str:
    root_dir = os.path.dirname(os.path.abspath(str(app_file or "")))
    path = os.path.join(root_dir, "scraper", "templates")
    os.makedirs(path, exist_ok=True)
    return path


def public_scraper_template_state_path(app_file: Any) -> str:
    root_dir = os.path.dirname(os.path.abspath(str(app_file or "")))
    state_dir = os.path.join(root_dir, "scraper", "state")
    os.makedirs(state_dir, exist_ok=True)
    return os.path.join(state_dir, "template_run_state.json")


def load_public_scraper_template_states(app_file: Any) -> Dict[str, Dict[str, str]]:
    path = public_scraper_template_state_path(app_file)
    if not os.path.exists(path):
        return {}
    payload = read_json_file(path)
    templates_obj = payload.get("templates")
    if not isinstance(templates_obj, dict):
        return {}
    states: Dict[str, Dict[str, str]] = {}
    for key, value in templates_obj.items():
        abs_key = os.path.abspath(str(key or "").strip())
        if not abs_key:
            continue
        if isinstance(value, dict):
            status = str(value.get("status", "")).strip().lower()
            updated_at = str(value.get("updated_at", "")).strip()
            states[abs_key] = {"status": status, "updated_at": updated_at}
        else:
            status = str(value or "").strip().lower()
            if status:
                states[abs_key] = {"status": status, "updated_at": ""}
    return states


def save_public_scraper_template_states(app_file: Any, states: Dict[str, Dict[str, str]]) -> None:
    normalized: Dict[str, Dict[str, str]] = {}
    for key, value in dict(states or {}).items():
        abs_key = os.path.abspath(str(key or "").strip())
        if not abs_key:
            continue
        status = str((value or {}).get("status", "")).strip().lower()
        updated_at = str((value or {}).get("updated_at", "")).strip()
        if not status:
            continue
        normalized[abs_key] = {
            "status": status,
            "updated_at": updated_at or datetime.now().isoformat(timespec="seconds"),
        }
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "templates": normalized,
    }
    path = public_scraper_template_state_path(app_file)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def set_public_scraper_template_state(app_file: Any, template_path: Any, status: Any) -> None:
    path = os.path.abspath(str(template_path or "").strip())
    status_text = str(status or "").strip().lower()
    if (not path) or (not status_text):
        return
    states = load_public_scraper_template_states(app_file)
    states[path] = {
        "status": status_text,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    save_public_scraper_template_states(app_file, states)


def list_public_scraper_templates(app_file: Any) -> List[Tuple[str, str]]:
    unfinished_pairs: List[Tuple[str, str]] = []
    done_pairs: List[Tuple[str, str]] = []
    root_dir = os.path.dirname(os.path.abspath(str(app_file or "")))
    templates_dir = public_scraper_templates_dir(app_file)
    template_states = load_public_scraper_template_states(app_file)

    seen: set[str] = set()
    for folder in [templates_dir, os.path.join(root_dir, "scraper")]:
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder), key=lambda x: x.lower()):
            if not name.lower().endswith(".json"):
                continue
            full = os.path.abspath(os.path.join(folder, name))
            if full in seen:
                continue
            seen.add(full)
            if name.lower() == "template_run_state.json":
                continue
            if "config." not in name.lower() and folder != templates_dir:
                continue
            rel = os.path.relpath(full, root_dir)
            raw_status = str((template_states.get(full, {}) or {}).get("status", "")).strip().lower()
            is_done = raw_status in {"done", "completed", "finished", "success"}
            label = f"{'已完成' if is_done else '未完成'} | {rel}"
            if is_done:
                done_pairs.append((label, full))
            else:
                unfinished_pairs.append((label, full))
    return unfinished_pairs + done_pairs
