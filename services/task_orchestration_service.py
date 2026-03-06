# -*- coding: utf-8 -*-
"""Task action orchestration helpers (UI-independent)."""

from __future__ import annotations

import os
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


def prepare_existing_task_runtime_config(
    runtime_config: Optional[Dict[str, Any]],
    *,
    output_root: str,
    mode_override: str = "",
    auto_fallback_override: Optional[bool] = None,
    disable_page_images_override: Optional[bool] = None,
) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    output_root_abs = os.path.abspath(str(output_root or "").strip())
    config["output_root"] = output_root_abs

    rules = config.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    rules["named_images_dir"] = ""
    rules["final_output_root"] = ""
    rules["record_root"] = ""
    rules.setdefault("retry_failed_first", True)
    rules.setdefault("metadata_write_retries", 3)
    rules.setdefault("metadata_write_retry_delay_seconds", 1.2)
    rules.setdefault("metadata_write_retry_backoff_factor", 1.5)

    mode = str(mode_override or "").strip().lower()
    if mode in {"requests_jsl", "browser"}:
        rules["image_download_mode"] = mode
        if mode == "browser":
            rules["download_images_during_crawl"] = True
    if auto_fallback_override is not None:
        rules["auto_fallback_to_browser"] = bool(auto_fallback_override)
    if disable_page_images_override is not None:
        rules["disable_page_images_during_crawl"] = bool(disable_page_images_override)
    rules.pop("llm_api_key", None)
    config["rules"] = rules
    return config


def resolve_named_images_dir(output_root: Any, runtime_rules: Optional[Dict[str, Any]]) -> str:
    output_root_abs = os.path.abspath(str(output_root or "").strip())
    rules = runtime_rules if isinstance(runtime_rules, dict) else {}
    named_dir_raw = str(rules.get("named_images_dir", "") or "").strip()
    if not named_dir_raw:
        return output_root_abs
    named_dir = named_dir_raw if os.path.isabs(named_dir_raw) else os.path.join(output_root_abs, named_dir_raw)
    return os.path.abspath(named_dir)


def build_public_scraper_log_banner(run_label: str, started_at_text: str) -> str:
    label = str(run_label or "").strip() or "Run"
    timestamp = str(started_at_text or "").strip()
    return f"\n\n=== D2I Public Scraper {label} {timestamp} ===\n"


def build_public_scraper_command(
    python_exec: str,
    script_path: str,
    config_path: str,
    output_root: str,
    *,
    skip_crawl: bool = False,
    skip_images: bool = False,
    skip_metadata: bool = False,
) -> list[str]:
    cmd = [
        str(python_exec or "").strip(),
        "-X",
        "utf8",
        str(script_path or "").strip(),
        "--config",
        str(config_path or "").strip(),
        "--output-root",
        os.path.abspath(str(output_root or "").strip()),
    ]
    if skip_crawl:
        cmd.append("--skip-crawl")
    if skip_images:
        cmd.append("--skip-images")
    if skip_metadata:
        cmd.append("--skip-metadata")
    return cmd


def resolve_active_template_path(primary_template_path: str, runtime_rules: Optional[Dict[str, Any]]) -> str:
    rules = runtime_rules if isinstance(runtime_rules, dict) else {}
    active_template_path = (
        str(primary_template_path or "").strip()
        or str(rules.get("template_source_path", "")).strip()
        or str(rules.get("generated_template_path", "")).strip()
    )
    return os.path.abspath(active_template_path) if active_template_path else ""


def build_started_task_message(
    *,
    pid: int,
    named_dir: str,
    log_path: str,
    runtime_rules: Optional[Dict[str, Any]],
) -> str:
    rules = runtime_rules if isinstance(runtime_rules, dict) else {}
    used_template_path = str(rules.get("template_source_path", "")).strip()
    generated_template_path = str(rules.get("generated_template_path", "")).strip()
    image_mode = str(rules.get("image_download_mode", "requests_jsl")).strip().lower()
    image_mode_text = "浏览器模式(慢稳)" if image_mode == "browser" else "请求模式(快)"
    folder_msg = ""
    resolved_subdir = str(rules.get("resolved_output_subdir", "")).strip()
    if resolved_subdir:
        folder_msg = f"\n任务子目录：{resolved_subdir}\n"
    template_msg = ""
    if used_template_path:
        template_msg = f"\n模板：\n{used_template_path}"
    elif generated_template_path:
        template_msg = f"\n模板（本次生成）：\n{generated_template_path}"
    return (
        "抓取任务已在后台启动。\n"
        f"任务进程 PID: {pid}\n\n"
        f"图片下载方式：{image_mode_text}\n\n"
        f"{folder_msg}"
        f"最终图片会输出到：\n{named_dir}\n\n"
        f"运行日志：\n{log_path}{template_msg}"
    )


def build_existing_task_started_message(
    *,
    pid: int,
    output_root: str,
    named_dir: str,
    log_path: str,
    skip_crawl: bool,
) -> str:
    mode_hint = "（仅重试失败阶段）" if skip_crawl else ""
    return (
        "已按已有配置继续抓取任务。\n\n"
        f"{mode_hint}\n"
        f"任务进程 PID: {pid}\n\n"
        f"任务目录：\n{output_root}\n\n"
        f"最终图片目录：\n{named_dir}\n\n"
        f"运行日志：\n{log_path}"
    )


def build_public_scraper_task_exit_plan(
    root: Any,
    task: Optional[Dict[str, Any]],
    code: int,
    *,
    current_active_root: str,
    record_path: str = "",
    backoff_state: Optional[Dict[str, Any]] = None,
    updated_at_ts: Optional[float] = None,
) -> Dict[str, Any]:
    raw_root = str(root or "").strip()
    root_abs = os.path.abspath(raw_root) if raw_root else ""
    current_root = str(current_active_root or "").strip()
    current_root_abs = os.path.abspath(current_root) if current_root else ""
    task_data = dict(task or {})
    named_dir = str(task_data.get("named_dir", "")).strip()
    active_template_path = str(task_data.get("active_template_path", "")).strip()
    log_path = str(task_data.get("log_path", "")).strip()
    log_handle = task_data.get("log_handle")
    is_active = bool(root_abs) and (current_root_abs == root_abs)
    backoff = backoff_state if isinstance(backoff_state, dict) else {}

    outcome = decide_task_exit_outcome(
        int(code),
        is_active=is_active,
        named_dir=named_dir,
        active_template_path=active_template_path,
        log_path=log_path,
        record_path=str(record_path or "").strip(),
        blocked_until=str(backoff.get("blocked_until", "")).strip(),
        blocked_reason=str(backoff.get("blocked_reason", "")).strip(),
    )

    next_task = dict(task_data)
    next_task["proc"] = None
    next_task["log_handle"] = None
    next_task["manual_paused"] = False
    next_task["last_exit_code"] = int(code)
    next_task["updated_at_ts"] = float(updated_at_ts or 0.0)
    runtime_state = str(outcome.get("runtime_state", "")).strip()
    if runtime_state:
        next_task["runtime_state"] = runtime_state

    return {
        "root": root_abs,
        "task": next_task,
        "log_handle": log_handle,
        "active_template_path": active_template_path,
        "is_active": is_active,
        "template_state": str(outcome.get("template_state", "")).strip(),
        "status_text": str(outcome.get("status_text", "")).strip(),
        "dialog_kind": str(outcome.get("dialog_kind", "")).strip().lower(),
        "dialog_title": str(outcome.get("dialog_title", "")).strip(),
        "dialog_message": str(outcome.get("dialog_message", "")).strip(),
    }


def build_public_scraper_poll_plan(
    current_active_root: Any,
    tasks: Dict[str, Any],
    *,
    is_process_running_fn: Callable[[Any], bool],
) -> Dict[str, Any]:
    active_root_raw = str(current_active_root or "").strip()
    active_root = pick_next_active_root(
        active_root_raw,
        tasks,
        is_process_running_fn=is_process_running_fn,
    )
    running_roots = []
    for root, entry in dict(tasks or {}).items():
        if isinstance(entry, dict) and is_process_running_fn(entry.get("proc")):
            running_roots.append(str(root or "").strip())
    return {
        "active_root": str(active_root or "").strip(),
        "running_roots": running_roots,
        "running_any": bool(running_roots),
        "should_schedule_poll": bool(running_roots),
    }


def collect_running_public_scraper_tasks(
    tasks: Dict[str, Any],
    *,
    is_process_running_fn: Callable[[Any], bool],
) -> list[Dict[str, Any]]:
    running: list[Dict[str, Any]] = []
    for root, entry in dict(tasks or {}).items():
        if not isinstance(entry, dict):
            continue
        proc = entry.get("proc")
        if not is_process_running_fn(proc):
            continue
        root_text = str(root or "").strip()
        running.append(
            {
                "root": (os.path.abspath(root_text) if root_text else ""),
                "task": entry,
                "proc": proc,
            }
        )
    return running


def build_app_close_confirmation(task_count: int) -> Dict[str, str]:
    count = max(0, int(task_count or 0))
    return {
        "title": "关闭确认",
        "message": f"仍有 {count} 个抓取任务在运行。\n\n关闭软件将停止这些任务。\n是否继续关闭？",
    }


def build_public_scraper_task_close_cleanup_plan(root: Any, task: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw_root = str(root or "").strip()
    root_abs = os.path.abspath(raw_root) if raw_root else ""
    task_data = task if isinstance(task, dict) else {}
    return {
        "root": root_abs,
        "proc": task_data.get("proc"),
        "log_handle": task_data.get("log_handle"),
        "active_template_path": str(task_data.get("active_template_path", "")).strip(),
    }


def collect_public_scraper_log_handles(tasks: Dict[str, Any]) -> list[Any]:
    handles: list[Any] = []
    seen: set[int] = set()
    for entry in dict(tasks or {}).values():
        if not isinstance(entry, dict):
            continue
        handle = entry.get("log_handle")
        if handle is None:
            continue
        key = id(handle)
        if key in seen:
            continue
        seen.add(key)
        handles.append(handle)
    return handles


def build_public_scraper_close_reset_state() -> Dict[str, Any]:
    return {
        "tasks": {},
        "active_task_root": "",
        "output_root": "",
        "proc": None,
        "named_dir": "",
        "last_progress_text": "",
        "started_at": None,
        "runtime_state": "空闲",
        "active_template_path": "",
    }


def build_public_scraper_app_close_plan(
    tasks: Dict[str, Any],
    *,
    is_process_running_fn: Callable[[Any], bool],
) -> Dict[str, Any]:
    running_tasks = collect_running_public_scraper_tasks(tasks, is_process_running_fn=is_process_running_fn)
    cleanup_plans = [
        build_public_scraper_task_close_cleanup_plan(item.get("root", ""), item.get("task"))
        for item in running_tasks
    ]
    confirmation = build_app_close_confirmation(len(running_tasks))
    return {
        "running_tasks": running_tasks,
        "running_task_count": len(running_tasks),
        "should_confirm": bool(running_tasks),
        "confirm_title": confirmation["title"],
        "confirm_message": confirmation["message"],
        "cleanup_plans": cleanup_plans,
        "all_log_handles": collect_public_scraper_log_handles(tasks),
        "reset_state": build_public_scraper_close_reset_state(),
    }


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


def decide_task_exit_outcome(
    code: int,
    *,
    is_active: bool,
    named_dir: str = "",
    active_template_path: str = "",
    log_path: str = "",
    record_path: str = "",
    blocked_until: str = "",
    blocked_reason: str = "",
) -> Dict[str, Any]:
    c = int(code)
    result: Dict[str, Any] = {
        "runtime_state": "",
        "template_state": "",
        "status_text": "",
        "dialog_kind": "",
        "dialog_title": "",
        "dialog_message": "",
    }

    if c == 0:
        result["runtime_state"] = "已完成"
        result["template_state"] = "done" if str(active_template_path or "").strip() else ""
        if is_active:
            result["status_text"] = "抓取任务完成"
            if str(named_dir or "").strip():
                tail_msg = f"\n\n抓取记录：\n{record_path}" if str(record_path or "").strip() else ""
                result["dialog_kind"] = "info"
                result["dialog_title"] = "完成"
                result["dialog_message"] = (
                    "抓取任务已完成。\n\n"
                    f"最终图片目录：\n{named_dir}{tail_msg}"
                )
        return result

    if c == 2:
        result["runtime_state"] = "已暂停(风控等待)"
        result["template_state"] = "pending" if str(active_template_path or "").strip() else ""
        if is_active:
            result["status_text"] = "抓取任务已暂停，等待 backoff 后继续"
            detail_lines = ["抓取任务已自动暂停（风控 backoff）。"]
            if str(blocked_until or "").strip():
                detail_lines.append(f"恢复时间：{blocked_until}")
            if str(blocked_reason or "").strip():
                detail_lines.append(f"原因：{blocked_reason}")
            reason_lower = str(blocked_reason or "").lower()
            if "suspected_block_consecutive" in reason_lower:
                detail_lines.append("提示：检测到连续提取失败，建议先手动打开目标网页检查是否触发风控或页面结构变化。")
            detail_lines.append("")
            detail_lines.append("当前进度已归档，可在稍后点击“继续任务”。")
            result["dialog_kind"] = "info"
            result["dialog_title"] = "任务已暂停"
            result["dialog_message"] = "\n".join(detail_lines)
        return result

    result["runtime_state"] = f"异常结束({c})"
    result["template_state"] = "pending" if str(active_template_path or "").strip() else ""
    if is_active:
        result["status_text"] = "抓取任务异常结束"
        detail = (
            f"抓取任务异常结束，退出码：{c}\n\n抓取记录：\n{record_path}"
            if str(record_path or "").strip()
            else f"抓取任务异常结束，退出码：{c}\n\n运行日志：\n{log_path}"
        )
        result["dialog_kind"] = "warning"
        result["dialog_title"] = "任务结束"
        result["dialog_message"] = detail
    return result


def pick_next_active_root(
    current_active_root: str,
    tasks: Dict[str, Any],
    *,
    is_process_running_fn: Callable[[Any], bool],
) -> str:
    active_root = str(current_active_root or "").strip()
    if active_root:
        return active_root

    entries = dict(tasks or {})
    for root, entry in entries.items():
        if isinstance(entry, dict) and is_process_running_fn(entry.get("proc")):
            return str(root or "").strip()
    try:
        return str(next(iter(entries.keys())) or "").strip()
    except Exception:
        return ""
