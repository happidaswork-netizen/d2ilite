# -*- coding: utf-8 -*-
"""Task domain services for public scraper orchestration."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from services.scraper_monitor_service import (
    humanize_scraper_reason,
    merge_status_reason,
    normalize_person_key,
    read_jsonl_rows,
)


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


def count_jsonl_rows(path: Any, cache: Optional[Dict[str, Tuple[int, float, int]]] = None) -> int:
    text = str(path or "").strip()
    if (not text) or (not os.path.exists(text)):
        return 0
    try:
        stat = os.stat(text)
    except Exception:
        return 0

    cache_key = os.path.abspath(text)
    if isinstance(cache, dict):
        cached = cache.get(cache_key)
        if (
            isinstance(cached, tuple)
            and len(cached) == 3
            and cached[0] == stat.st_size
            and cached[1] == stat.st_mtime
        ):
            try:
                return int(cached[2])
            except Exception:
                pass

    count = count_jsonl_rows_no_cache(text)
    if isinstance(cache, dict):
        cache[cache_key] = (stat.st_size, stat.st_mtime, count)
    return count


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


def collect_scraper_progress_rows(
    output_root: Any,
    *,
    max_rows: int = 3000,
    read_jsonl_rows_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
    read_json_file_fn: Optional[Callable[[str], Dict[str, Any]]] = None,
    normalize_existing_path_fn: Optional[Callable[[Any], str]] = None,
    extract_live_actions_fn: Optional[Callable[[str], Any]] = None,
    is_row_completed_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> List[Dict[str, str]]:
    root = normalize_public_task_root(output_root)
    if not root:
        return []

    read_jsonl = read_jsonl_rows_fn or read_jsonl_rows
    read_json = read_json_file_fn or read_json_file
    normalize_existing = normalize_existing_path_fn or normalize_existing_path
    max_rows = max(1, int(max_rows or 3000))

    list_path = os.path.join(root, "raw", "list_records.jsonl")
    profile_path = os.path.join(root, "raw", "profiles.jsonl")
    manifest_path = os.path.join(root, "downloads", "image_downloads.jsonl")
    metadata_queue_path = os.path.join(root, "raw", "metadata_queue.jsonl")
    metadata_result_path = os.path.join(root, "raw", "metadata_write_results.jsonl")
    review_path = os.path.join(root, "raw", "review_queue.jsonl")
    failures_path = os.path.join(root, "raw", "failures.jsonl")
    image_url_index_path = os.path.join(root, "state", "image_url_index.json")
    image_sha_index_path = os.path.join(root, "state", "image_sha_index.json")

    image_url_index_raw = read_json(image_url_index_path)
    image_sha_index_raw = read_json(image_sha_index_path)
    image_url_index: Dict[str, str] = {}
    image_sha_index: Dict[str, str] = {}
    if isinstance(image_url_index_raw, dict):
        for k, v in image_url_index_raw.items():
            kk = str(k or "").strip()
            vv = str(v or "").strip()
            if kk and vv:
                image_url_index[kk] = vv
    if isinstance(image_sha_index_raw, dict):
        for k, v in image_sha_index_raw.items():
            kk = str(k or "").strip()
            vv = normalize_existing(v)
            if kk and vv:
                image_sha_index[kk] = vv

    rows: List[Dict[str, Any]] = []
    detail_index: Dict[str, int] = {}
    detail_seen: set[str] = set()

    def _append_row(name: str, detail_url: str) -> int:
        idx = len(rows) + 1
        row = {
            "idx": str(idx),
            "name": str(name or "").strip() or f"未命名_{idx}",
            "detail_url": str(detail_url or "").strip(),
            "detail": "…",
            "image": "…",
            "meta": "…",
            "reason": "",
            "image_path": "",
            "_has_image_url": False,
            "_image_url": "",
        }
        rows.append(row)
        if row["detail_url"]:
            detail_index[row["detail_url"]] = len(rows) - 1
        return len(rows) - 1

    for item in read_jsonl(list_path, max_rows=max_rows * 2):
        name = str(item.get("name", "")).strip()
        detail_url = str(item.get("detail_url", "")).strip()
        if detail_url and detail_url in detail_seen:
            continue
        if detail_url:
            detail_seen.add(detail_url)
        row_pos = _append_row(name, detail_url)
        if not detail_url:
            rows[row_pos]["detail"] = "×"
            rows[row_pos]["image"] = "-"
            rows[row_pos]["meta"] = "-"
            merge_status_reason(rows[row_pos], "列表缺少详情链接")
        if len(rows) >= max_rows:
            break

    for item in read_jsonl(profile_path, max_rows=max_rows * 2):
        detail_url = str(item.get("detail_url", "")).strip()
        if not detail_url:
            continue
        row_pos = detail_index.get(detail_url)
        if row_pos is None:
            row_pos = _append_row(str(item.get("name", "")).strip(), detail_url)
        row = rows[row_pos]
        if (not str(row.get("name", "")).strip()) and str(item.get("name", "")).strip():
            row["name"] = str(item.get("name", "")).strip()
        row["detail"] = "√"
        image_url = str(item.get("image_url", "")).strip()
        row["_has_image_url"] = bool(image_url)
        if image_url:
            row["_image_url"] = image_url
        if not image_url and row["image"] != "√":
            row["image"] = "×"
            merge_status_reason(row, "详情缺少图片链接")
        if len(rows) >= max_rows and detail_url not in detail_index:
            break

    for item in read_jsonl(manifest_path, max_rows=max_rows * 3):
        detail_url = str(item.get("detail_url", "")).strip()
        if not detail_url:
            continue
        row_pos = detail_index.get(detail_url)
        if row_pos is None:
            row_pos = _append_row(str(item.get("name", "")).strip(), detail_url)
        row = rows[row_pos]
        row["image"] = "√"
        candidate = normalize_existing(item.get("named_path")) or normalize_existing(item.get("saved_path"))
        if candidate:
            row["image_path"] = candidate

    for item in read_jsonl(metadata_queue_path, max_rows=max_rows * 3):
        detail_url = str(item.get("detail_url", "")).strip()
        if not detail_url:
            continue
        row_pos = detail_index.get(detail_url)
        if row_pos is None:
            row_pos = _append_row(str(item.get("name", "")).strip(), detail_url)
        candidate = normalize_existing(item.get("local_image_path"))
        if candidate:
            rows[row_pos]["image_path"] = candidate
            rows[row_pos]["image"] = "√"
        image_url_q = str(item.get("image_url", "")).strip()
        if image_url_q and (not rows[row_pos].get("_image_url")):
            rows[row_pos]["_image_url"] = image_url_q
            rows[row_pos]["_has_image_url"] = True

    meta_summary: Dict[str, Dict[str, Any]] = {}
    for item in read_jsonl(metadata_result_path, max_rows=max_rows * 3):
        detail_url = str(item.get("detail_url", "")).strip()
        if not detail_url:
            continue
        state = meta_summary.get(detail_url)
        if state is None:
            state = {"ok": False, "failed": False, "error": "", "output_path": ""}
            meta_summary[detail_url] = state

        status = str(item.get("status", "")).strip().lower()
        if status == "ok":
            state["ok"] = True
            candidate = normalize_existing(item.get("output_path"))
            if candidate:
                state["output_path"] = candidate
            continue
        if status:
            state["failed"] = True
            err = str(item.get("error", "")).strip() or f"元数据失败({status})"
            if err:
                state["error"] = err

    for detail_url, state in meta_summary.items():
        row_pos = detail_index.get(detail_url)
        if row_pos is None:
            row_pos = _append_row("", detail_url)
        row = rows[row_pos]
        if state.get("ok"):
            row["meta"] = "√"
            candidate = str(state.get("output_path", "")).strip()
            if candidate:
                row["image_path"] = candidate
                row["image"] = "√"
        elif state.get("failed"):
            row["meta"] = "×"
            merge_status_reason(row, str(state.get("error", "")).strip() or "元数据写入失败")

    for item in read_jsonl(review_path, max_rows=max_rows * 3):
        reason = str(item.get("reason", "")).strip()
        detail_url = str(item.get("detail_url", "")).strip()
        if not detail_url:
            record = item.get("record")
            if isinstance(record, dict):
                detail_url = str(record.get("detail_url", "")).strip()
        if not detail_url:
            continue
        row_pos = detail_index.get(detail_url)
        if row_pos is None:
            row_pos = _append_row("", detail_url)
        row = rows[row_pos]
        lower_reason = reason.lower()
        if lower_reason.startswith("image_"):
            if row["image"] != "√":
                row["image"] = "×"
            elif lower_reason != "image_ok":
                continue
        if lower_reason.startswith("metadata_"):
            if row["meta"] != "√":
                row["meta"] = "×"
            elif not lower_reason.startswith("audit_missing_metadata_fields"):
                continue
        if "missing_required_fields" in lower_reason and row["detail"] != "√":
            row["detail"] = "×"
        merge_status_reason(row, reason)

    for item in read_jsonl(failures_path, max_rows=max_rows * 3):
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        row_pos = detail_index.get(url)
        if row_pos is None:
            continue
        row = rows[row_pos]
        context = item.get("context")
        phase = str((context or {}).get("phase", "")).strip().lower() if isinstance(context, dict) else ""
        if phase == "detail":
            row["detail"] = "×"
        merge_status_reason(row, str(item.get("reason", "")).strip())

    for row in rows:
        if str(row.get("image", "")).strip() == "√":
            continue
        existing_path = normalize_existing(row.get("image_path", ""))
        if existing_path:
            row["image_path"] = existing_path
            row["image"] = "√"
            continue
        image_url = str(row.get("_image_url", "")).strip()
        if not image_url:
            continue
        sha = image_url_index.get(image_url, "")
        if not sha:
            continue
        candidate = image_sha_index.get(sha, "")
        if candidate:
            row["image_path"] = candidate
            row["image"] = "√"

    live_by_person: Dict[str, str] = {}
    if extract_live_actions_fn is not None:
        try:
            live_result = extract_live_actions_fn(root)
            if isinstance(live_result, tuple) and len(live_result) >= 1 and isinstance(live_result[0], dict):
                live_by_person = live_result[0]
        except Exception:
            live_by_person = {}

    output: List[Dict[str, str]] = []
    ok_tokens = {"√", "✓"}
    for row in rows[:max_rows]:
        detail_status = str(row.get("detail", ""))
        image_status = str(row.get("image", ""))
        meta_status = str(row.get("meta", ""))
        if detail_status == "√" and row.get("_has_image_url") and image_status == "…":
            image_status = "⌛"
        if image_status == "√" and meta_status == "…":
            meta_status = "⌛"
        reason_text = humanize_scraper_reason(str(row.get("reason", "")).strip())

        row_name = str(row.get("name", "")).strip()
        row_live_action = live_by_person.get(normalize_person_key(row_name), "")
        if is_row_completed_fn is not None:
            try:
                row_completed = bool(
                    is_row_completed_fn(
                        {
                            "detail": detail_status,
                            "image": image_status,
                            "meta": meta_status,
                        }
                    )
                )
            except Exception:
                row_completed = False
        else:
            row_completed = (
                detail_status in ok_tokens
                and image_status in ok_tokens
                and meta_status in ok_tokens
            )
        if row_live_action and (not row_completed):
            if (row_live_action == "正在下载图片") and (image_status not in ok_tokens):
                image_status = "⌛"
            elif (row_live_action == "正在写入元数据") and (meta_status not in ok_tokens):
                meta_status = "⌛"
            elif (row_live_action == "正在抓取详情页") and (detail_status not in ok_tokens):
                detail_status = "⌛"
            if reason_text:
                if not reason_text.startswith(row_live_action):
                    reason_text = f"{row_live_action} | {reason_text}"
            else:
                reason_text = row_live_action

        output.append(
            {
                "idx": str(row.get("idx", "")),
                "name": row_name,
                "detail": detail_status,
                "image": image_status,
                "meta": meta_status,
                "reason": reason_text,
                "detail_url": str(row.get("detail_url", "")).strip(),
                "image_path": str(row.get("image_path", "")).strip(),
            }
        )
    return output


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
