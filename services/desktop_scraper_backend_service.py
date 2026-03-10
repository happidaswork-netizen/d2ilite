# -*- coding: utf-8 -*-
"""Desktop scraper backend service for desktop-next runtime adapters."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

from services.scraper_monitor_service import extract_scraper_live_actions, read_text_tail
from services.task_orchestration_service import resolve_named_images_dir
from services.task_service import (
    build_public_scraper_progress_text,
    build_public_task_manager_list_view,
    count_jsonl_rows,
    default_public_tasks_root,
    discover_public_task_roots,
    estimate_scraper_total_target,
    normalize_public_task_root,
    public_task_manager_status_text,
    read_json_file,
    resolve_public_task_log_path,
    collect_scraper_progress_rows,
    split_scraper_progress_rows,
    summarize_public_task,
    summarize_scraper_progress_rows,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_FILE = str(PROJECT_ROOT / "app.py")
BACKEND_PROVIDER = "python-cli"
BACKEND_VERSION = "scraper-backend-v1"


def build_ping_payload() -> Dict[str, Any]:
    return {
        "provider": BACKEND_PROVIDER,
        "version": BACKEND_VERSION,
    }


def build_default_base_root_payload() -> Dict[str, Any]:
    return {
        "base_root": default_public_tasks_root(APP_FILE),
    }


def _serialize_task_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    data = row if isinstance(row, dict) else {}
    return {
        "root": str(data.get("root", "") or "").strip(),
        "task": str(data.get("task", "") or "").strip(),
        "status": str(data.get("status", "") or "").strip(),
        "profiles": max(0, int(data.get("profiles", 0) or 0)),
        "images": max(0, int(data.get("images", 0) or 0)),
        "metadata_ok": max(0, int(data.get("metadata_ok", 0) or 0)),
        "pending": max(0, int(data.get("pending", 0) or 0)),
        "review": max(0, int(data.get("review", 0) or 0)),
        "failures": max(0, int(data.get("failures", 0) or 0)),
        "updated_at": str(data.get("updated_at", "") or "").strip(),
    }


def _serialize_progress_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    output: List[Dict[str, str]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        output.append(
            {
                "idx": str(row.get("idx", "") or "").strip(),
                "name": str(row.get("name", "") or "").strip(),
                "detail": str(row.get("detail", "") or "").strip(),
                "image": str(row.get("image", "") or "").strip(),
                "meta": str(row.get("meta", "") or "").strip(),
                "reason": str(row.get("reason", "") or "").strip(),
                "detail_url": str(row.get("detail_url", "") or "").strip(),
                "image_path": str(row.get("image_path", "") or "").strip(),
            }
        )
    return output


def _build_task_summary(root: str) -> Dict[str, Any]:
    return summarize_public_task(root, count_jsonl_rows_fn=count_jsonl_rows)


def _resolve_selected_root(base_root: str, selected_root: str, rows: List[Dict[str, Any]]) -> str:
    normalized_selected = normalize_public_task_root(selected_root)
    row_roots = {str(item.get("root", "") or "").strip() for item in rows if isinstance(item, dict)}
    if normalized_selected and normalized_selected in row_roots:
        return normalized_selected
    if rows:
        return str(rows[0].get("root", "") or "").strip()
    return ""


def build_scraper_workspace_payload(
    base_root: str,
    *,
    selected_root: str = "",
    progress_limit: int = 300,
    log_lines: int = 80,
) -> Dict[str, Any]:
    base = normalize_public_task_root(base_root) or default_public_tasks_root(APP_FILE)
    view = build_public_task_manager_list_view(
        base,
        discover_task_roots_fn=discover_public_task_roots,
        summarize_task_fn=_build_task_summary,
    )
    rows = [_serialize_task_summary(item) for item in list(view.get("rows") or []) if isinstance(item, dict)]
    active_root = _resolve_selected_root(base, selected_root, rows)
    selected_task = next((item for item in rows if item.get("root") == active_root), None)

    detail: Dict[str, Any] | None = None
    if active_root:
        progress_rows = collect_scraper_progress_rows(active_root, max_rows=max(20, int(progress_limit or 300)))
        pending_rows, done_rows = split_scraper_progress_rows(progress_rows)
        counts = summarize_scraper_progress_rows(progress_rows)
        list_rows = count_jsonl_rows(os.path.join(active_root, "raw", "list_records.jsonl"))
        profile_rows = count_jsonl_rows(os.path.join(active_root, "raw", "profiles.jsonl"))
        image_rows = count_jsonl_rows(os.path.join(active_root, "downloads", "image_downloads.jsonl"))
        metadata_rows = count_jsonl_rows(os.path.join(active_root, "raw", "metadata_write_results.jsonl"))
        total_target = max(
            max(0, int(counts.get("discovered_rows", 0) or 0)),
            estimate_scraper_total_target(active_root, count_jsonl_rows_fn=count_jsonl_rows),
        )
        log_path = resolve_public_task_log_path(active_root)
        log_tail = read_text_tail(log_path, max_lines=max(20, int(log_lines or 80)))
        _by_person, _by_detail, latest_action = extract_scraper_live_actions(
            active_root,
            max_lines=max(60, int(log_lines or 80) * 2),
        )

        runtime_config = read_json_file(os.path.join(active_root, "state", "runtime_config.json"))
        runtime_rules = runtime_config.get("rules") if isinstance(runtime_config.get("rules"), dict) else {}
        named_dir = resolve_named_images_dir(active_root, runtime_rules)

        detail = {
            "root": active_root,
            "task": str((selected_task or {}).get("task", "") or os.path.basename(active_root)).strip(),
            "status": str((selected_task or {}).get("status", "") or "").strip(),
            "updated_at": str((selected_task or {}).get("updated_at", "") or "").strip(),
            "output_path": named_dir or active_root,
            "log_path": log_path or os.path.join(active_root, "reports", "gui_public_scraper.log"),
            "progress_text": build_public_scraper_progress_text(
                discovered_rows=int(counts.get("discovered_rows", 0) or 0),
                downloaded_rows=int(counts.get("downloaded_rows", 0) or 0),
                completed_rows=int(counts.get("completed_rows", 0) or 0),
                total_target=total_target,
                list_rows=list_rows,
                profile_rows=profile_rows,
                image_rows=image_rows,
                metadata_rows=metadata_rows,
            ),
            "latest_action": latest_action,
            "total_target": total_target,
            "discovered_rows": max(0, int(counts.get("discovered_rows", 0) or 0)),
            "downloaded_rows": max(0, int(counts.get("downloaded_rows", 0) or 0)),
            "completed_rows": max(0, int(counts.get("completed_rows", 0) or 0)),
            "list_rows": max(0, int(list_rows or 0)),
            "profile_rows": max(0, int(profile_rows or 0)),
            "image_rows": max(0, int(image_rows or 0)),
            "metadata_rows": max(0, int(metadata_rows or 0)),
            "review_rows": max(0, int((selected_task or {}).get("review", 0) or 0)),
            "failure_rows": max(0, int((selected_task or {}).get("failures", 0) or 0)),
            "pending_rows": _serialize_progress_rows(pending_rows),
            "done_rows": _serialize_progress_rows(done_rows),
            "log_tail": str(log_tail or ""),
        }

    return {
        "base_root": base,
        "task_count": len(rows),
        "status_text": public_task_manager_status_text(len(rows)),
        "tasks": rows,
        "selected_root": active_root,
        "selected_task": selected_task,
        "detail": detail,
    }
