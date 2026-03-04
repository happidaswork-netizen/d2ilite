# -*- coding: utf-8 -*-
"""Task action orchestration helpers (UI-independent)."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional


def normalize_continue_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = options if isinstance(options, dict) else {}
    return {
        "mode": str(data.get("mode", "") or "").strip(),
        "auto_fallback": bool(data.get("auto_fallback", True)),
        "disable_page_images": bool(data.get("disable_page_images", True)),
    }


def build_continue_start_existing_task_args(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    opts = normalize_continue_options(options)
    return {
        "skip_crawl": False,
        "skip_images": False,
        "skip_metadata": False,
        "show_success_dialog": True,
        "success_title": "继续任务",
        "runtime_state": "继续运行中",
        "mode_override": opts["mode"],
        "auto_fallback_override": opts["auto_fallback"],
        "disable_page_images_override": opts["disable_page_images"],
    }


def build_retry_start_existing_task_args(
    need_crawl: bool,
    options: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    opts = normalize_continue_options(options)
    return {
        "skip_crawl": (not bool(need_crawl)),
        "skip_images": False,
        "skip_metadata": False,
        "show_success_dialog": True,
        "success_title": ("重试失败（含详情重抓）" if need_crawl else "重试失败"),
        "runtime_state": ("继续运行中" if need_crawl else "失败重试中"),
        "mode_override": opts["mode"],
        "auto_fallback_override": opts["auto_fallback"],
        "disable_page_images_override": opts["disable_page_images"],
    }


def build_rewrite_metadata_start_existing_task_args() -> Dict[str, Any]:
    return {
        "skip_crawl": True,
        "skip_images": True,
        "skip_metadata": False,
        "show_success_dialog": True,
        "success_title": "重写元数据",
        "runtime_state": "元数据重写中",
    }


def retry_started_status_text(need_crawl: bool) -> str:
    return "重试任务已启动（自动包含详情重抓）" if need_crawl else "重试任务已启动（失败优先）"


def continue_action_for_active_entry(
    active_entry: Optional[Dict[str, Any]],
    *,
    is_process_running_fn: Callable[[Any], bool],
) -> str:
    entry = active_entry if isinstance(active_entry, dict) else {}
    if not is_process_running_fn(entry.get("proc")):
        return "start_new"
    if bool(entry.get("manual_paused", False)):
        return "resume_paused"
    return "already_running"
