# -*- coding: utf-8 -*-
"""Desktop scraper backend service for desktop-next runtime adapters."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from services.scraper_monitor_service import extract_scraper_live_actions, read_text_tail
from services.runtime_service import build_utf8_subprocess_env, resolve_python_cli_executable
from services.settings_service import load_app_settings
from services.task_orchestration_service import (
    build_continue_start_existing_task_args,
    build_existing_task_started_message,
    build_public_scraper_command,
    build_public_scraper_log_banner,
    build_public_scraper_poll_plan,
    build_public_scraper_task_exit_plan,
    build_retry_start_existing_task_args,
    build_rewrite_metadata_start_existing_task_args,
    continue_action_for_active_entry,
    prepare_existing_task_runtime_config,
    resolve_active_template_path,
    resolve_named_images_dir,
)
from services.task_service import (
    build_public_scraper_progress_text,
    build_public_task_manager_list_view,
    collect_scraper_progress_rows,
    count_jsonl_rows,
    default_public_tasks_root,
    discover_public_task_roots,
    estimate_scraper_total_target,
    get_scraper_record_path,
    normalize_public_task_root,
    public_task_manager_status_text,
    read_json_file,
    read_scraper_backoff_state,
    resolve_public_task_log_path,
    retry_requires_crawl_phase,
    set_public_scraper_manual_pause_flag,
    split_scraper_progress_rows,
    summarize_public_task,
    summarize_scraper_progress_rows,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_FILE = str(PROJECT_ROOT / "app.py")
BACKEND_PROVIDER = "python-cli"
BACKEND_VERSION = "scraper-backend-v2"
REGISTRY_PATH = PROJECT_ROOT / ".tmp" / "desktop-next" / "scraper-runtime-registry.json"
RUNNING_LIKE_STATES = {"运行中", "继续运行中", "失败重试中", "元数据重写中"}
CONTROL_DEFAULTS = {
    "mode": "requests_jsl",
    "auto_fallback": True,
    "disable_page_images": True,
}


class _PidProcProxy:
    def __init__(self, pid: int) -> None:
        self.pid = max(0, int(pid or 0))

    def poll(self) -> Optional[int]:
        return None if _is_pid_running(self.pid) else 1


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_api_base(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        from llm_client import normalize_api_base  # type: ignore

        if callable(normalize_api_base):
            return str(normalize_api_base(text)).strip()
    except Exception:
        pass
    return text.rstrip("/")


def _apply_llm_env(
    env: Dict[str, str],
    *,
    api_base: str = "",
    api_key: str = "",
    model: str = "",
) -> Dict[str, str]:
    data = dict(env or os.environ)
    base = str(api_base or "").strip()
    key = str(api_key or "").strip()
    mdl = str(model or "").strip()
    if base:
        data["D2I_LLM_API_BASE"] = base
    if key:
        data["D2I_LLM_API_KEY"] = key
    if mdl:
        data["D2I_LLM_MODEL"] = mdl
    return data


def _registry_default_payload() -> Dict[str, Any]:
    return {
        "active_root": "",
        "tasks": {},
        "updated_at": "",
    }


def _registry_path() -> Path:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    return REGISTRY_PATH


def _normalize_registry_entry(entry: Any) -> Dict[str, Any]:
    data = entry if isinstance(entry, dict) else {}
    return {
        "pid": max(0, _safe_int(data.get("pid"), 0)),
        "config_path": str(data.get("config_path", "") or "").strip(),
        "log_path": str(data.get("log_path", "") or "").strip(),
        "named_dir": str(data.get("named_dir", "") or "").strip(),
        "runtime_state": str(data.get("runtime_state", "") or "").strip(),
        "manual_paused": bool(data.get("manual_paused", False)),
        "active_template_path": str(data.get("active_template_path", "") or "").strip(),
        "last_exit_code": _safe_int(data.get("last_exit_code"), 0) if str(data.get("last_exit_code", "")).strip() else None,
        "updated_at_ts": _safe_float(data.get("updated_at_ts"), 0.0),
        "started_at": _safe_float(data.get("started_at"), 0.0),
    }


def _load_runtime_registry() -> Dict[str, Any]:
    path = _registry_path()
    if not path.exists():
        return _registry_default_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _registry_default_payload()
    tasks_obj = payload.get("tasks")
    tasks: Dict[str, Dict[str, Any]] = {}
    if isinstance(tasks_obj, dict):
        for root, entry in tasks_obj.items():
            root_abs = normalize_public_task_root(root)
            if not root_abs:
                continue
            tasks[root_abs] = _normalize_registry_entry(entry)
    return {
        "active_root": normalize_public_task_root(payload.get("active_root")),
        "tasks": tasks,
        "updated_at": str(payload.get("updated_at", "") or "").strip(),
    }


def _save_runtime_registry(registry: Dict[str, Any]) -> None:
    path = _registry_path()
    tasks_obj = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    payload = {
        "active_root": normalize_public_task_root(registry.get("active_root")),
        "tasks": {root: _normalize_registry_entry(entry) for root, entry in tasks_obj.items()},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_pid_running(pid: int) -> bool:
    target = max(0, int(pid or 0))
    if target <= 0:
        return False
    try:
        os.kill(target, 0)
        return True
    except PermissionError:
        return True
    except Exception:
        if os.name != "nt":
            return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {target}", "/FO", "CSV", "/NH"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="ignore",
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return str(target) in str(result.stdout or "")
        except Exception:
            return False
    return False


def _entry_running(entry: Optional[Dict[str, Any]]) -> bool:
    data = entry if isinstance(entry, dict) else {}
    return _is_pid_running(_safe_int(data.get("pid"), 0))


def _entry_to_task_payload(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = _normalize_registry_entry(entry)
    proc = _PidProcProxy(data["pid"]) if data["pid"] > 0 else None
    return {
        "proc": proc,
        "named_dir": data["named_dir"],
        "log_path": data["log_path"],
        "manual_paused": data["manual_paused"],
        "runtime_state": data["runtime_state"],
        "active_template_path": data["active_template_path"],
        "last_exit_code": data["last_exit_code"],
        "started_at": data["started_at"],
        "updated_at_ts": data["updated_at_ts"],
    }


def _serialize_task_summary(row: Dict[str, Any], entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = row if isinstance(row, dict) else {}
    runtime = _normalize_registry_entry(entry)
    session_running = _entry_running(runtime)
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
        "pid": runtime["pid"] if session_running else 0,
        "session_running": session_running,
        "manual_paused": bool(runtime.get("manual_paused", False)),
        "runtime_state": str(runtime.get("runtime_state", "") or "").strip(),
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


def _resolve_selected_root(base_root: str, selected_root: str, rows: List[Dict[str, Any]]) -> str:
    normalized_selected = normalize_public_task_root(selected_root)
    row_roots = {str(item.get("root", "") or "").strip() for item in rows if isinstance(item, dict)}
    if normalized_selected and normalized_selected in row_roots:
        return normalized_selected
    if rows:
        return str(rows[0].get("root", "") or "").strip()
    return ""


def _infer_exit_code(root: str, entry: Dict[str, Any]) -> int:
    if bool(entry.get("manual_paused", False)):
        return int(entry.get("last_exit_code") or 1 or 1)
    backoff = read_scraper_backoff_state(root)
    if str(backoff.get("blocked_until", "")).strip():
        return 2
    summary = summarize_public_task(root, count_jsonl_rows_fn=count_jsonl_rows)
    if str(summary.get("status", "")).strip() == "已完成":
        return 0
    last_exit_code = entry.get("last_exit_code")
    if isinstance(last_exit_code, int):
        return int(last_exit_code)
    return 1


def _refresh_runtime_registry(registry: Dict[str, Any]) -> Dict[str, Any]:
    data = {
        "active_root": normalize_public_task_root(registry.get("active_root")),
        "tasks": {
            normalize_public_task_root(root): _normalize_registry_entry(entry)
            for root, entry in dict(registry.get("tasks") or {}).items()
            if normalize_public_task_root(root)
        },
    }
    tasks = data["tasks"]

    for root in list(tasks.keys()):
        entry = tasks[root]
        if _entry_running(entry):
            if bool(entry.get("manual_paused", False)):
                entry["runtime_state"] = "已暂停(手动)"
            elif str(entry.get("runtime_state", "") or "").strip() not in RUNNING_LIKE_STATES:
                entry["runtime_state"] = "运行中"
            continue

        entry["pid"] = 0
        if bool(entry.get("manual_paused", False)):
            entry["runtime_state"] = "已暂停(手动)"
            continue

        if str(entry.get("runtime_state", "") or "").strip() in RUNNING_LIKE_STATES:
            inferred_code = _infer_exit_code(root, entry)
            exit_plan = build_public_scraper_task_exit_plan(
                root,
                _entry_to_task_payload(entry),
                inferred_code,
                current_active_root=str(data.get("active_root", "") or ""),
                record_path=get_scraper_record_path(root),
                backoff_state=(read_scraper_backoff_state(root) if inferred_code == 2 else {"blocked_until": "", "blocked_reason": ""}),
                updated_at_ts=time.time(),
            )
            next_entry = _normalize_registry_entry(exit_plan.get("task") or {})
            next_entry["pid"] = 0
            tasks[root] = next_entry
            set_public_scraper_manual_pause_flag(root, paused=False)

    poll_plan = build_public_scraper_poll_plan(
        str(data.get("active_root", "") or ""),
        {root: _entry_to_task_payload(entry) for root, entry in tasks.items()},
        is_process_running_fn=lambda proc: bool(proc and proc.poll() is None),
    )
    data["active_root"] = normalize_public_task_root(poll_plan.get("active_root"))
    return data


def _resolve_scraper_runner_script() -> str:
    override = str(os.environ.get("D2I_DESKTOP_SCRAPER_RUNNER", "") or "").strip()
    if override:
        return os.path.abspath(override)
    return os.path.abspath(os.path.join(PROJECT_ROOT, "scraper", "run_public_scraper.py"))


def _load_runtime_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("配置内容不是 JSON 对象")
    return payload


def _load_global_llm_settings() -> Dict[str, Any]:
    settings = load_app_settings()
    llm = settings.get("llm") if isinstance(settings, dict) else {}
    return dict(llm) if isinstance(llm, dict) else {}


def _start_existing_task(
    output_root: str,
    *,
    skip_crawl: bool = False,
    skip_images: bool = False,
    skip_metadata: bool = False,
    runtime_state: str = "继续运行中",
    mode_override: str = "",
    auto_fallback_override: Optional[bool] = None,
    disable_page_images_override: Optional[bool] = None,
    **_unused: Any,
) -> Dict[str, Any]:
    root_abs = normalize_public_task_root(output_root)
    if not root_abs:
        raise RuntimeError("任务目录无效")

    script_path = _resolve_scraper_runner_script()
    if not os.path.exists(script_path):
        raise RuntimeError(f"未找到抓取脚本: {script_path}")

    config_path = os.path.join(root_abs, "state", "runtime_config.json")
    if not os.path.exists(config_path):
        raise RuntimeError(f"未找到运行配置文件: {config_path}")

    runtime_config = _load_runtime_config(config_path)
    runtime_config = prepare_existing_task_runtime_config(
        runtime_config,
        output_root=root_abs,
        mode_override=mode_override,
        auto_fallback_override=auto_fallback_override,
        disable_page_images_override=disable_page_images_override,
    )
    rules = runtime_config.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(runtime_config, handle, ensure_ascii=False, indent=2)

    named_dir = resolve_named_images_dir(root_abs, runtime_config.get("rules"))
    log_path = os.path.join(root_abs, "reports", "gui_public_scraper.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    set_public_scraper_manual_pause_flag(root_abs, paused=False)

    run_label = "Retry" if skip_crawl else "Continue"
    with open(log_path, "a", encoding="utf-8") as log_handle:
        log_handle.write(build_public_scraper_log_banner(run_label, time.strftime("%Y-%m-%d %H:%M:%S")))
        log_handle.flush()

        python_exec = resolve_python_cli_executable()
        cmd = build_public_scraper_command(
            python_exec,
            script_path,
            config_path,
            root_abs,
            skip_crawl=skip_crawl,
            skip_images=skip_images,
            skip_metadata=skip_metadata,
        )

        global_llm = _load_global_llm_settings()
        llm_model = str(rules.get("llm_model", "")).strip() or str(global_llm.get("model", "")).strip()
        llm_api_base = _normalize_api_base(
            str(rules.get("llm_api_base", "")).strip() or str(global_llm.get("api_base", "")).strip()
        )
        llm_api_key = str(rules.get("llm_api_key", "")).strip() or str(global_llm.get("api_key", "")).strip()
        env = _apply_llm_env(build_utf8_subprocess_env(), api_base=llm_api_base, api_key=llm_api_key, model=llm_model)

        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(script_path) or ".",
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=creationflags,
        )

    active_template_path_abs = resolve_active_template_path("", rules)
    entry = {
        "pid": proc.pid,
        "config_path": config_path,
        "log_path": log_path,
        "named_dir": named_dir,
        "runtime_state": str(runtime_state or "继续运行中").strip() or "继续运行中",
        "manual_paused": False,
        "active_template_path": active_template_path_abs,
        "last_exit_code": None,
        "updated_at_ts": time.time(),
        "started_at": time.time(),
    }
    message = build_existing_task_started_message(
        pid=proc.pid,
        output_root=root_abs,
        named_dir=named_dir,
        log_path=log_path,
        skip_crawl=skip_crawl,
    )
    return {
        "root": root_abs,
        "entry": entry,
        "message": message,
    }


def _pause_task(registry: Dict[str, Any], root: str) -> str:
    tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    entry = tasks.get(root)
    if not isinstance(entry, dict) or not _entry_running(entry):
        raise RuntimeError("当前没有运行中的抓取任务。")
    if bool(entry.get("manual_paused", False)):
        raise RuntimeError("当前任务已处于手动暂停状态。")
    if not set_public_scraper_manual_pause_flag(root, paused=True):
        raise RuntimeError("无法写入暂停标记文件，请检查目录写权限。")
    entry["manual_paused"] = True
    entry["runtime_state"] = "已暂停(手动)"
    entry["updated_at_ts"] = time.time()
    registry["active_root"] = root
    return "抓取任务已手动暂停，可点击继续运行"


def _continue_task(registry: Dict[str, Any], root: str, options: Optional[Dict[str, Any]]) -> str:
    tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    entry = tasks.get(root)
    action = continue_action_for_active_entry(
        _entry_to_task_payload(entry if isinstance(entry, dict) else {}),
        is_process_running_fn=lambda proc: bool(proc and proc.poll() is None),
    )
    if action == "resume_paused":
        if not set_public_scraper_manual_pause_flag(root, paused=False):
            raise RuntimeError("无法移除暂停标记文件，请检查目录写权限。")
        if isinstance(entry, dict):
            entry["manual_paused"] = False
            entry["runtime_state"] = "运行中"
            entry["updated_at_ts"] = time.time()
        registry["active_root"] = root
        return "抓取任务已继续运行"
    if action == "already_running":
        raise RuntimeError("该任务已在运行中。")

    start_kwargs = build_continue_start_existing_task_args(options if isinstance(options, dict) else CONTROL_DEFAULTS)
    result = _start_existing_task(root, **start_kwargs)
    tasks[root] = _normalize_registry_entry(result.get("entry") or {})
    registry["active_root"] = root
    return str(result.get("message", "") or "已按已有配置继续抓取任务。")


def _retry_task(registry: Dict[str, Any], root: str, options: Optional[Dict[str, Any]]) -> str:
    tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    entry = tasks.get(root)
    if isinstance(entry, dict) and _entry_running(entry):
        raise RuntimeError("当前任务正在运行，请先暂停后再重试失败项。")

    rows = collect_scraper_progress_rows(root, max_rows=3000)
    need_crawl = retry_requires_crawl_phase(rows)
    start_kwargs = build_retry_start_existing_task_args(need_crawl, options if isinstance(options, dict) else CONTROL_DEFAULTS)
    result = _start_existing_task(root, **start_kwargs)
    tasks[root] = _normalize_registry_entry(result.get("entry") or {})
    registry["active_root"] = root
    return str(result.get("message", "") or "")


def _rewrite_task(registry: Dict[str, Any], root: str) -> str:
    tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    entry = tasks.get(root)
    if isinstance(entry, dict) and _entry_running(entry):
        raise RuntimeError("当前任务正在运行，请先暂停后再重写元数据。")
    start_kwargs = build_rewrite_metadata_start_existing_task_args()
    result = _start_existing_task(root, **start_kwargs)
    tasks[root] = _normalize_registry_entry(result.get("entry") or {})
    registry["active_root"] = root
    return str(result.get("message", "") or "")


def build_ping_payload() -> Dict[str, Any]:
    return {
        "provider": BACKEND_PROVIDER,
        "version": BACKEND_VERSION,
    }


def build_default_base_root_payload() -> Dict[str, Any]:
    return {
        "base_root": default_public_tasks_root(APP_FILE),
    }


def build_control_defaults_payload() -> Dict[str, Any]:
    return dict(CONTROL_DEFAULTS)


def build_scraper_workspace_payload(
    base_root: str,
    *,
    selected_root: str = "",
    progress_limit: int = 300,
    log_lines: int = 80,
) -> Dict[str, Any]:
    registry = _refresh_runtime_registry(_load_runtime_registry())
    _save_runtime_registry(registry)

    base = normalize_public_task_root(base_root) or default_public_tasks_root(APP_FILE)
    active_root_in_registry = str(registry.get("active_root", "") or "").strip()

    def _build_task_summary(root: str) -> Dict[str, Any]:
        entry = (registry.get("tasks") or {}).get(root) if isinstance(registry.get("tasks"), dict) else None
        return summarize_public_task(
            root,
            count_jsonl_rows_fn=count_jsonl_rows,
            entry=_entry_to_task_payload(entry if isinstance(entry, dict) else {}),
            current_active_root=active_root_in_registry,
        )

    view = build_public_task_manager_list_view(
        base,
        discover_task_roots_fn=discover_public_task_roots,
        summarize_task_fn=_build_task_summary,
    )
    registry_tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    rows = [
        _serialize_task_summary(item, registry_tasks.get(str(item.get("root", "") or "").strip()))
        for item in list(view.get("rows") or [])
        if isinstance(item, dict)
    ]
    active_root = _resolve_selected_root(base, selected_root or active_root_in_registry, rows)
    selected_task = next((item for item in rows if item.get("root") == active_root), None)
    selected_entry = registry_tasks.get(active_root) if active_root else None

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
        runtime = _normalize_registry_entry(selected_entry)
        session_running = _entry_running(runtime)
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
            "pid": runtime["pid"] if session_running else 0,
            "session_running": session_running,
            "manual_paused": bool(runtime.get("manual_paused", False)),
            "runtime_state": str(runtime.get("runtime_state", "") or "").strip(),
            "can_pause": bool(session_running and (not runtime.get("manual_paused", False))),
            "can_continue": bool(
                runtime.get("manual_paused", False)
                or ((not session_running) and str((selected_task or {}).get("status", "") or "").strip() != "已完成")
            ),
            "can_retry": bool((not session_running) and (int((selected_task or {}).get("failures", 0) or 0) > 0)),
            "can_rewrite_metadata": bool((not session_running) and (profile_rows > 0 or metadata_rows > 0)),
        }

    return {
        "base_root": base,
        "task_count": len(rows),
        "status_text": public_task_manager_status_text(len(rows)),
        "tasks": rows,
        "selected_root": active_root,
        "selected_task": selected_task,
        "detail": detail,
        "control_defaults": build_control_defaults_payload(),
    }


def execute_scraper_control_action(
    action: str,
    *,
    output_root: str,
    base_root: str = "",
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    root = normalize_public_task_root(output_root)
    if not root:
        raise RuntimeError("请先选择一个抓取任务。")

    registry = _refresh_runtime_registry(_load_runtime_registry())
    tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
    tasks.setdefault(root, _normalize_registry_entry({}))
    registry["active_root"] = root

    action_text = str(action or "").strip().lower()
    if action_text == "pause":
        message = _pause_task(registry, root)
    elif action_text == "continue":
        message = _continue_task(registry, root, options)
    elif action_text == "retry":
        message = _retry_task(registry, root, options)
    elif action_text == "rewrite":
        message = _rewrite_task(registry, root)
    else:
        raise RuntimeError(f"不支持的抓取动作: {action_text}")

    _save_runtime_registry(registry)
    return {
        "action": action_text,
        "message": message,
        "workspace": build_scraper_workspace_payload(base_root or default_public_tasks_root(APP_FILE), selected_root=root),
    }
