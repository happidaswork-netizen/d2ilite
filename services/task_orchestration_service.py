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
