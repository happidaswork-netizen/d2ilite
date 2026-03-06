# -*- coding: utf-8 -*-
"""D2I Lite: 本地图片查看与元数据全量读写工具。"""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from collections import deque
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox

import ttkbootstrap as ttk
from PIL import Image, ImageTk

try:
    import requests  # type: ignore

    HAS_REQUESTS = True
except Exception:
    requests = None
    HAS_REQUESTS = False

try:
    from llm_client import OpenAICompatibleClient, normalize_api_base  # type: ignore

    HAS_LLM_CLIENT = True
except Exception:
    OpenAICompatibleClient = None  # type: ignore
    normalize_api_base = None  # type: ignore
    HAS_LLM_CLIENT = False

try:
    import urllib3
except Exception:
    urllib3 = None

from metadata_manager import (
    ImageMetadataInfo,
    clean_keywords,
    read_image_metadata,
    suggest_metadata_fill,
    update_metadata_preserve_others,
)
from services.image_service import (
    list_images_in_folder as _svc_list_images_in_folder,
    read_image_basic_info as _svc_read_image_basic_info,
)
from services.editor_text_service import (
    extract_json_payload_from_llm as _svc_extract_json_payload_from_llm,
    normalize_gender_text as _svc_normalize_gender_text,
    normalize_multiline_editor_text as _svc_normalize_multiline_editor_text,
    normalize_profile_for_editor as _svc_normalize_profile_for_editor,
    normalize_single_line_text as _svc_normalize_single_line_text,
    prune_empty_profile_values as _svc_prune_empty_profile_values,
)
from services.metadata_service import (
    build_structured_payload as _svc_build_structured_payload,
    normalize_http_url as _svc_normalize_http_url,
    parse_keywords as _svc_parse_keywords,
    read_raw_with_pyexiv2 as _svc_read_raw_with_pyexiv2,
    write_raw_with_pyexiv2 as _svc_write_raw_with_pyexiv2,
)
from services.public_scraper_config_service import (
    apply_public_scraper_setup_to_runtime_config as _svc_apply_public_scraper_setup_to_runtime_config,
    build_public_scraper_runtime_config as _svc_build_public_scraper_runtime_config,
    build_public_scraper_setup_initial_state as _svc_build_public_scraper_setup_initial_state,
    build_public_scraper_setup_submit_plan as _svc_build_public_scraper_setup_submit_plan,
    build_public_scraper_template_selection_state as _svc_build_public_scraper_template_selection_state,
    guess_public_site_name as _svc_guess_public_site_name,
    register_generated_template as _svc_register_generated_template,
    save_generated_public_scraper_template as _svc_save_generated_public_scraper_template,
)
from services.scraper_monitor_service import (
    extract_scraper_live_actions as _svc_extract_scraper_live_actions,
    extract_runtime_log_field as _svc_extract_runtime_log_field,
    format_elapsed as _svc_format_elapsed,
    humanize_scraper_reason as _svc_humanize_scraper_reason,
    merge_status_reason as _svc_merge_status_reason,
    normalize_person_key as _svc_normalize_person_key,
    read_jsonl_rows as _svc_read_jsonl_rows,
    read_text_tail as _svc_read_text_tail,
    repair_mojibake_utf8_latin1 as _svc_repair_mojibake_utf8_latin1,
    write_jsonl_rows as _svc_write_jsonl_rows,
)
from services.runtime_service import (
    build_utf8_subprocess_env as _svc_build_utf8_subprocess_env,
    resolve_python_cli_executable as _svc_resolve_python_cli_executable,
)
from services.settings_service import (
    app_settings_path as _svc_app_settings_path,
    default_app_settings as _svc_default_app_settings,
    load_app_settings as _svc_load_app_settings,
    save_app_settings as _svc_save_app_settings,
)
from services.task_service import (
    build_retry_detail_plan as _svc_build_retry_detail_plan,
    build_progress_context_menu_plan as _svc_build_progress_context_menu_plan,
    build_public_task_manager_list_view as _svc_build_public_task_manager_list_view,
    build_scraper_task_view_rows as _svc_build_scraper_task_view_rows,
    build_public_scraper_progress_text as _svc_build_public_scraper_progress_text,
    build_progress_selection_sync_plan as _svc_build_progress_selection_sync_plan,
    collect_detail_urls_from_progress_values as _svc_collect_detail_urls_from_progress_values,
    collect_other_progress_table_refs as _svc_collect_other_progress_table_refs,
    collect_scraper_error_row_ids as _svc_collect_scraper_error_row_ids,
    collect_scraper_progress_rows as _svc_collect_scraper_progress_rows,
    collect_progress_values as _svc_collect_progress_values,
    count_jsonl_rows as _svc_count_jsonl_rows,
    count_latest_metadata_status as _svc_count_latest_metadata_status,
    default_public_tasks_root as _svc_default_public_tasks_root,
    discover_public_task_roots as _svc_discover_public_task_roots,
    execute_retry_detail_cleanup as _svc_execute_retry_detail_cleanup,
    estimate_scraper_total_target as _svc_estimate_scraper_total_target,
    get_scraper_record_path as _svc_get_scraper_record_path,
    is_process_running as _svc_is_process_running,
    is_scraper_row_completed as _svc_is_scraper_row_completed,
    is_scraper_row_image_downloaded as _svc_is_scraper_row_image_downloaded,
    list_public_scraper_templates as _svc_list_public_scraper_templates,
    load_public_scraper_template_states as _svc_load_public_scraper_template_states,
    extract_detail_url_from_progress_values as _svc_extract_detail_url_from_progress_values,
    normalize_existing_path as _svc_normalize_existing_path,
    normalize_public_task_root as _svc_normalize_public_task_root,
    public_scraper_template_state_path as _svc_public_scraper_template_state_path,
    public_scraper_templates_dir as _svc_public_scraper_templates_dir,
    public_scraper_pause_flag_path as _svc_public_scraper_pause_flag_path,
    read_json_file as _svc_read_json_file,
    read_scraper_backoff_state as _svc_read_scraper_backoff_state,
    missing_audit_fields_from_metadata_snapshot as _svc_missing_audit_fields_from_metadata_snapshot,
    parse_task_root_from_values as _svc_parse_task_root_from_values,
    public_task_manager_status_text as _svc_public_task_manager_status_text,
    public_task_summary_to_tree_values as _svc_public_task_summary_to_tree_values,
    pick_first_progress_values as _svc_pick_first_progress_values,
    scraper_progress_row_to_table_values as _svc_scraper_progress_row_to_table_values,
    scraper_progress_snapshot as _svc_scraper_progress_snapshot,
    scraper_progress_values_has_error as _svc_scraper_progress_values_has_error,
    save_public_scraper_template_states as _svc_save_public_scraper_template_states,
    set_public_scraper_manual_pause_flag as _svc_set_public_scraper_manual_pause_flag,
    split_scraper_progress_rows as _svc_split_scraper_progress_rows,
    summarize_scraper_progress_rows as _svc_summarize_scraper_progress_rows,
    sort_public_task_summaries as _svc_sort_public_task_summaries,
    retry_requires_crawl_phase as _svc_retry_requires_crawl_phase,
    task_entry_status_text as _svc_task_entry_status_text,
    reconcile_task_entry_runtime_state as _svc_reconcile_task_entry_runtime_state,
    resolve_public_task_directory as _svc_resolve_public_task_directory,
    resolve_public_task_log_path as _svc_resolve_public_task_log_path,
    resolve_openable_image_path_from_progress_values as _svc_resolve_openable_image_path_from_progress_values,
    scraper_error_selection_status_text as _svc_scraper_error_selection_status_text,
    set_public_scraper_template_state as _svc_set_public_scraper_template_state,
    summarize_public_task as _svc_summarize_public_task,
    suggest_public_scraper_output_root as _svc_suggest_public_scraper_output_root,
    sync_scraper_audit_review_queue as _svc_sync_scraper_audit_review_queue,
)
from services.task_orchestration_service import (
    build_public_scraper_app_close_plan as _svc_build_public_scraper_app_close_plan,
    build_public_scraper_poll_plan as _svc_build_public_scraper_poll_plan,
    build_public_scraper_close_reset_state as _svc_build_public_scraper_close_reset_state,
    build_public_scraper_task_exit_plan as _svc_build_public_scraper_task_exit_plan,
    build_existing_task_started_message as _svc_build_existing_task_started_message,
    build_continue_start_existing_task_args as _svc_build_continue_start_existing_task_args,
    build_public_scraper_command as _svc_build_public_scraper_command,
    build_public_scraper_log_banner as _svc_build_public_scraper_log_banner,
    build_retry_start_existing_task_args as _svc_build_retry_start_existing_task_args,
    build_rewrite_metadata_start_existing_task_args as _svc_build_rewrite_metadata_start_existing_task_args,
    build_started_task_message as _svc_build_started_task_message,
    continue_action_for_active_entry as _svc_continue_action_for_active_entry,
    prepare_existing_task_runtime_config as _svc_prepare_existing_task_runtime_config,
    resolve_active_template_path as _svc_resolve_active_template_path,
    resolve_named_images_dir as _svc_resolve_named_images_dir,
    retry_started_status_text as _svc_retry_started_status_text,
)
from services.viewer_load_service import (
    load_metadata_snapshot as _svc_load_metadata_snapshot,
    load_preview_image as _svc_load_preview_image,
)

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD  # type: ignore

    HAS_TK_DND = True
except Exception:
    DND_FILES = None
    TkinterDnD = None
    HAS_TK_DND = False

try:
    import pyexiv2

    HAS_PYEXIV2 = True
except Exception:
    pyexiv2 = None
    HAS_PYEXIV2 = False

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
BaseWindow = TkinterDnD.Tk if HAS_TK_DND else tk.Tk

# 从 D2I 复用：常用真实浏览器 UA + 敏感域名策略。
STEALTH_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]
SENSITIVE_DOMAINS = [
    ".gov.cn",
    ".edu.cn",
    ".mil.cn",
    ".org.cn",
    "12371.cn",
    "people.com.cn",
    "xinhuanet.com",
]

# 修复场景采用更激进的快超时，避免长时间卡住。
REPAIR_WARMUP_TIMEOUT_SEC = 6
REPAIR_REQUEST_TIMEOUT_SEC = 15
REPAIR_MAX_CANDIDATES = 20
REPAIR_BROWSER_PAGELOAD_TIMEOUT_SEC = 25
REPAIR_CHALLENGE_WAIT_SEC = 1.0
REPAIR_CHALLENGE_WARM_ROUNDS = 2
REPAIR_CHALLENGE_MAIN_ROUNDS = 2

if HAS_REQUESTS and (urllib3 is not None):
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass


def _safe_json_dumps(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception:
        return json.dumps(str(data), ensure_ascii=False, indent=2)


def _format_value_short(value: Any, limit: int = 220) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            s = json.dumps(value, ensure_ascii=False)
        except Exception:
            s = str(value)
    elif isinstance(value, (bytes, bytearray)):
        s = f"<bytes {len(value)}>"
    else:
        s = str(value)
    s = s.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    return s if len(s) <= limit else (s[: limit - 3] + "...")


def _format_value_full(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return _safe_json_dumps(value)
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return repr(value)
    return str(value)


def _list_images_in_folder(folder: str) -> List[str]:
    return _svc_list_images_in_folder(folder, IMAGE_EXTS)


def _parse_keywords(text: str) -> List[str]:
    return _svc_parse_keywords(text)


def _normalize_http_url(text: Any) -> str:
    return _svc_normalize_http_url(text)


def _read_image_basic_info(filepath: str) -> Dict[str, Any]:
    return _svc_read_image_basic_info(filepath)


def _read_raw_with_pyexiv2(filepath: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    return _svc_read_raw_with_pyexiv2(filepath)


def _write_raw_with_pyexiv2(
    filepath: str,
    *,
    xmp_data: Optional[Dict[str, Any]] = None,
    exif_data: Optional[Dict[str, Any]] = None,
    iptc_data: Optional[Dict[str, Any]] = None,
) -> None:
    _svc_write_raw_with_pyexiv2(
        filepath,
        xmp_data=xmp_data,
        exif_data=exif_data,
        iptc_data=iptc_data,
    )


class KeyValueViewer(ttk.Frame):
    """通用 key/value 查看器。"""

    def __init__(self, parent, title: str):
        super().__init__(parent)
        self._data: Dict[str, Any] = {}

        top = ttk.Frame(self)
        top.pack(fill=tk.X, padx=8, pady=(8, 4))

        ttk.Label(top, text=title).pack(side=tk.LEFT)
        ttk.Label(top, text="过滤:").pack(side=tk.LEFT, padx=(10, 5))
        self.filter_var = tk.StringVar()
        filter_entry = ttk.Entry(top, textvariable=self.filter_var, width=30)
        filter_entry.pack(side=tk.LEFT)
        filter_entry.bind("<KeyRelease>", lambda _e: self._refresh())

        self.count_label = ttk.Label(top, text="0 项")
        self.count_label.pack(side=tk.RIGHT)

        mid = ttk.Frame(self)
        mid.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        self.tree = ttk.Treeview(mid, columns=("value",), show="tree headings", height=12)
        self.tree.heading("#0", text="Key")
        self.tree.heading("value", text="Value")
        self.tree.column("#0", width=320)
        self.tree.column("value", width=600)

        y = ttk.Scrollbar(mid, orient=tk.VERTICAL, command=self.tree.yview)
        x = ttk.Scrollbar(mid, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y.set, xscrollcommand=x.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        y.grid(row=0, column=1, sticky="ns")
        x.grid(row=1, column=0, sticky="ew")
        mid.grid_rowconfigure(0, weight=1)
        mid.grid_columnconfigure(0, weight=1)

        bottom = ttk.Labelframe(self, text="完整值")
        bottom.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        # 使用按字符换行，避免长 URL/长无空格串无法自动换行。
        self.detail = tk.Text(bottom, height=10, wrap=tk.CHAR)
        detail_scroll = ttk.Scrollbar(bottom, orient=tk.VERTICAL, command=self.detail.yview)
        self.detail.configure(yscrollcommand=detail_scroll.set)
        self.detail.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        detail_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    def set_data(self, data: Dict[str, Any]):
        self._data = {str(k): v for k, v in (data or {}).items()}
        self.filter_var.set("")
        self._refresh()

    def _refresh(self):
        self.tree.delete(*self.tree.get_children())
        keyword = (self.filter_var.get() or "").strip().lower()

        items = []
        for k, v in (self._data or {}).items():
            ks = str(k)
            vs = _format_value_short(v)
            if keyword and (keyword not in ks.lower()) and (keyword not in vs.lower()):
                continue
            items.append((ks, v))

        items.sort(key=lambda t: t[0].lower())

        for k, v in items:
            self.tree.insert("", tk.END, text=k, values=(_format_value_short(v),))

        self.count_label.config(text=f"{len(items)} 项")
        self.detail.delete("1.0", tk.END)

    def _on_select(self, _event=None):
        sel = self.tree.selection()
        if not sel:
            return
        item_id = sel[0]
        key = self.tree.item(item_id, "text")
        value = self._data.get(key)
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", _format_value_full(value))


class D2ILiteApp(BaseWindow):
    def __init__(self, start_target: Optional[str] = None):
        super().__init__()

        self.style = ttk.Style("flatly")
        self.title("D2I Lite - 本地看图与元数据")
        self.geometry("1580x940")
        self.minsize(980, 620)

        self._app_settings: Dict[str, Any] = self._load_app_settings()
        self._global_settings_window: Optional[tk.Toplevel] = None

        self.current_path: Optional[str] = None
        self.current_folder: str = ""
        self.folder_images: List[str] = []
        self.current_index: int = -1

        self._preview_pil: Optional[Image.Image] = None
        self._preview_tk = None
        self._preview_resize_after: Optional[str] = None
        self._load_current_token: int = 0

        self._last_info: Optional[ImageMetadataInfo] = None
        self._last_basic: Dict[str, Any] = {}
        self._last_xmp: Dict[str, Any] = {}
        self._last_exif: Dict[str, Any] = {}
        self._last_iptc: Dict[str, Any] = {}
        self._folder_index_ready: bool = False
        self._jsonl_count_cache: Dict[str, Tuple[int, float, int]] = {}

        self._snapshot_dirty: bool = True
        self._all_view_dirty: bool = True
        self._camera_view_dirty: bool = True
        self._xmp_view_dirty: bool = True
        self._exif_view_dirty: bool = True
        self._iptc_view_dirty: bool = True
        self._png_view_dirty: bool = True
        self._raw_editors_dirty: bool = True
        self._ctx_menu: Optional[tk.Menu] = None
        self._ctx_widget: Optional[Any] = None
        self._http_session = None
        self._public_scraper_proc: Optional[subprocess.Popen] = None
        self._public_scraper_poll_after: Optional[str] = None
        self._public_scraper_output_root: str = ""
        self._public_scraper_named_dir: str = ""
        self._public_scraper_config_path: str = ""
        self._public_scraper_log_path: str = ""
        self._public_scraper_log_handle: Optional[Any] = None
        self._public_scraper_last_progress_text: str = ""
        self._public_scraper_started_at: Optional[float] = None
        self._public_scraper_runtime_state: str = "空闲"
        self._public_scraper_manual_paused: bool = False
        self._public_scraper_active_template_path: str = ""
        self._public_scraper_active_task_root: str = ""
        self._public_scraper_tasks: Dict[str, Dict[str, Any]] = {}
        self._public_scraper_panel: Optional[tk.Toplevel] = None
        self._public_task_manager_window: Optional[tk.Toplevel] = None
        self._scraper_start_btn: Optional[ttk.Button] = None
        self._scraper_stop_btn: Optional[ttk.Button] = None
        self._scraper_resume_btn: Optional[ttk.Button] = None
        self._scraper_retry_btn: Optional[ttk.Button] = None
        self._scraper_monitor_state_var: Optional[tk.StringVar] = None
        self._scraper_monitor_pid_var: Optional[tk.StringVar] = None
        self._scraper_monitor_elapsed_var: Optional[tk.StringVar] = None
        self._scraper_monitor_counts_var: Optional[tk.StringVar] = None
        self._scraper_monitor_progress_var: Optional[tk.StringVar] = None
        self._scraper_monitor_progress_bar: Optional[ttk.Progressbar] = None
        self._scraper_monitor_paths_var: Optional[tk.StringVar] = None
        self._scraper_monitor_log_text: Optional[tk.Text] = None
        self._scraper_monitor_pending_box: Optional[Any] = None
        self._scraper_monitor_done_box: Optional[Any] = None
        self._scraper_monitor_progress_table: Optional[ttk.Treeview] = None
        self._scraper_monitor_progress_done_table: Optional[ttk.Treeview] = None
        self._scraper_monitor_last_log_snapshot: str = ""
        self._scraper_monitor_last_progress_snapshot: str = ""
        self._scraper_monitor_last_opened_path: str = ""
        self._scraper_monitor_total_hint: int = 0
        self._scraper_monitor_log_tail_lines: int = 120
        self._scraper_progress_selection_syncing: bool = False
        self._scraper_row_open_pending: bool = False
        self._scraper_row_opening: bool = False
        self._scraper_task_tree: Optional[ttk.Treeview] = None
        self._scraper_task_status_var: Optional[tk.StringVar] = None
        self._edit_llm_busy: bool = False
        self._llm_progress_var: Optional[tk.StringVar] = None
        self._llm_progress_bar: Optional[Any] = None
        self._llm_progress_reset_after: Optional[str] = None
        self._main_scroll_canvas: Optional[tk.Canvas] = None
        self._main_scroll_host: Optional[ttk.Frame] = None
        self._main_scroll_window_id: Optional[int] = None
        self._main_scroll_refresh_after: Optional[str] = None

        self._build_ui()
        self._setup_edit_shortcuts_and_menu()
        self.protocol("WM_DELETE_WINDOW", self._on_app_close)

        if start_target:
            self._load_target(start_target)

    def _build_ui(self):
        shell = ttk.Frame(self)
        shell.pack(fill=tk.BOTH, expand=True)
        content = ttk.Frame(shell)
        content.pack(fill=tk.BOTH, expand=True)
        # 2026-03: 主界面不再套全局 Canvas 滚动壳，避免拖拽分栏/窗口尺寸变化时频繁重排导致闪烁。
        self._main_scroll_canvas = None
        self._main_scroll_host = None
        self._main_scroll_window_id = None

        top = ttk.Frame(content, padding=10)
        top.pack(fill=tk.X)

        top.columnconfigure(0, weight=1)

        path_row = ttk.Frame(top)
        path_row.grid(row=0, column=0, sticky="ew")
        path_row.columnconfigure(1, weight=1)

        action_row = ttk.Frame(top)
        action_row.grid(row=1, column=0, sticky="ew", pady=(8, 0))

        ttk.Label(path_row, text="路径:").grid(row=0, column=0, sticky=tk.W)
        self.path_var = tk.StringVar()
        path_entry = ttk.Entry(path_row, textvariable=self.path_var, width=96)
        path_entry.grid(row=0, column=1, sticky="ew", padx=6)
        path_entry.bind("<Return>", lambda _e: self._load_target(self.path_var.get().strip()))

        ttk.Button(path_row, text="打开图片", command=self._browse_image).grid(row=0, column=2, sticky=tk.W, padx=(6, 0))
        ttk.Button(path_row, text="打开文件夹", command=self._browse_folder).grid(row=0, column=3, sticky=tk.W, padx=6)

        ttk.Button(action_row, text="上一张", command=self._goto_prev).pack(side=tk.LEFT)
        ttk.Button(action_row, text="下一张", command=self._goto_next).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_row, text="刷新", command=self._refresh_current).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_row, text="全局设置", command=self._open_global_settings_dialog).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_row, text="批量下载器(旧版)", command=self._open_batch_downloader).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_row,
            text="公共抓取面板",
            command=self._open_public_scraper_panel,
        ).pack(side=tk.LEFT, padx=(6, 0))

        info_bar = ttk.Frame(content, padding=(10, 0, 10, 8))
        info_bar.pack(fill=tk.X)

        self.position_var = tk.StringVar(value="0 / 0")
        ttk.Label(info_bar, textvariable=self.position_var, width=16).pack(side=tk.LEFT)

        backend_text = "pyexiv2: 已启用" if HAS_PYEXIV2 else "pyexiv2: 未安装（写入不可用）"
        ttk.Label(info_bar, text=backend_text).pack(side=tk.LEFT, padx=(4, 14))

        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(info_bar, textvariable=self.status_var).pack(side=tk.LEFT)

        self._llm_progress_var = tk.StringVar(value="AI: 空闲")
        ttk.Label(info_bar, textvariable=self._llm_progress_var, bootstyle="secondary").pack(side=tk.RIGHT, padx=(8, 0))
        self._llm_progress_bar = ttk.Progressbar(info_bar, mode="indeterminate", length=100)
        self._llm_progress_bar.pack(side=tk.RIGHT, padx=(10, 0))

        dnd_text = "拖拽打开: 已启用" if HAS_TK_DND else "拖拽打开: 未启用（安装 tkinterdnd2）"
        ttk.Label(info_bar, text=dnd_text).pack(side=tk.RIGHT)

        main_pane = ttk.Panedwindow(content, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        left = ttk.Labelframe(main_pane, text="图片预览", padding=8)
        right = ttk.Frame(main_pane)
        main_pane.add(left, weight=3)
        main_pane.add(right, weight=5)

        self.preview_label = ttk.Label(left, text="打开图片后显示预览", anchor=tk.CENTER)
        self.preview_label.pack(fill=tk.BOTH, expand=True)
        self.preview_label.bind("<Configure>", self._on_preview_resize)

        preview_btns = ttk.Frame(left)
        preview_btns.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(preview_btns, text="系统打开文件", command=self._open_current_file).pack(side=tk.LEFT)
        ttk.Button(preview_btns, text="打开所在文件夹", command=self._open_current_folder).pack(side=tk.LEFT, padx=6)

        self.right_notebook = ttk.Notebook(right)
        self.right_notebook.pack(fill=tk.BOTH, expand=True)

        self.edit_tab = ttk.Frame(self.right_notebook)
        self.snapshot_tab = ttk.Frame(self.right_notebook)
        self.adv_tab = ttk.Frame(self.right_notebook)
        self.all_view = KeyValueViewer(self.right_notebook, "全部元数据")
        self.camera_view = KeyValueViewer(self.right_notebook, "相机信息")

        self.xmp_view = KeyValueViewer(self.right_notebook, "XMP 全量")
        self.exif_view = KeyValueViewer(self.right_notebook, "EXIF 全量")
        self.iptc_view = KeyValueViewer(self.right_notebook, "IPTC 全量")
        self.png_view = KeyValueViewer(self.right_notebook, "PNG text / info")

        self.right_notebook.add(self.edit_tab, text="编辑")
        self.right_notebook.add(self.snapshot_tab, text="结构化")
        self.right_notebook.add(self.xmp_view, text="XMP")
        self.right_notebook.add(self.exif_view, text="EXIF")
        self.right_notebook.add(self.iptc_view, text="IPTC")
        self.right_notebook.add(self.png_view, text="PNG text")
        # 用户要求：全部放在“高级写入”之前，并额外增加“相机”标签。
        self.right_notebook.add(self.all_view, text="全部")
        self.right_notebook.add(self.camera_view, text="相机")
        self.right_notebook.add(self.adv_tab, text="高级写入")
        self.right_notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        self._build_edit_tab(self.edit_tab)
        self._build_snapshot_tab(self.snapshot_tab)
        self._build_advanced_tab(self.adv_tab)
        self._setup_drag_drop(path_entry, self.preview_label, self.right_notebook, self)
        def _init_main_split() -> None:
            try:
                total = int(main_pane.winfo_width() or 0)
                if total > 900:
                    main_pane.sashpos(0, int(total * 0.42))
            except Exception:
                pass

        self.after(120, _init_main_split)

    def _schedule_main_scroll_refresh(self, delay_ms: int = 36) -> None:
        if self._main_scroll_refresh_after:
            try:
                self.after_cancel(self._main_scroll_refresh_after)
            except Exception:
                pass
        self._main_scroll_refresh_after = self.after(max(1, int(delay_ms)), self._refresh_main_scrollregion)

    def _refresh_main_scrollregion(self):
        self._main_scroll_refresh_after = None
        canvas = self._main_scroll_canvas
        if canvas is None:
            return
        try:
            bbox = canvas.bbox("all")
            if bbox:
                canvas.configure(scrollregion=bbox)
        except Exception:
            pass

    def _on_main_canvas_configure(self, event):
        canvas = self._main_scroll_canvas
        host = self._main_scroll_host
        window_id = self._main_scroll_window_id
        if canvas is None or host is None or window_id is None:
            return
        try:
            req_width = int(host.winfo_reqwidth() or 0)
            view_width = int(getattr(event, "width", 0) or 0)
            target_width = view_width if req_width <= view_width else req_width
            canvas.itemconfigure(window_id, width=target_width)
        except Exception:
            pass
        self._schedule_main_scroll_refresh()

    def _build_edit_tab(self, parent):
        wrap = ttk.Frame(parent, padding=10)
        wrap.pack(fill=tk.BOTH, expand=True)

        btns = ttk.Frame(wrap)
        btns.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(btns, text="AI自动补全", command=self._ai_autofill_current_metadata).pack(side=tk.LEFT)
        ttk.Button(btns, text="AI自动小传", command=self._ai_generate_biography_for_current).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="一键整理键内容", command=self._normalize_current_keys_content).pack(side=tk.LEFT)
        ttk.Button(btns, text="自动填空(手动)", command=self._apply_autofill_suggestion).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="保存并下一张", command=self._save_structured_and_next).pack(side=tk.RIGHT)
        ttk.Button(btns, text="保存元数据", command=self._save_structured).pack(side=tk.RIGHT, padx=6)

        sections = ttk.Panedwindow(wrap, orient=tk.VERTICAL)
        sections.pack(fill=tk.BOTH, expand=True)

        basic_box = ttk.Labelframe(sections, text="基础字段", padding=8)
        desc_box = ttk.Labelframe(sections, text="描述", padding=6)
        extra_box = ttk.Labelframe(sections, text="扩展字段（自适应）", padding=6)
        sections.add(basic_box, weight=3)
        sections.add(desc_box, weight=4)
        sections.add(extra_box, weight=5)

        form = ttk.Frame(basic_box)
        form.pack(fill=tk.BOTH, expand=True)

        self.edit_vars: Dict[str, tk.StringVar] = {
            "title": tk.StringVar(),
            "person": tk.StringVar(),
            "gender": tk.StringVar(),
            "position": tk.StringVar(),
            "city": tk.StringVar(),
            "source": tk.StringVar(),
            "image_url": tk.StringVar(),
            "keywords": tk.StringVar(),
            "titi_asset_id": tk.StringVar(),
            "titi_world_id": tk.StringVar(),
        }

        rows = [
            ("标题", "title"),
            ("人物", "person"),
            ("性别", "gender"),
            ("职务", "position"),
            ("城市", "city"),
            ("来源", "source"),
            ("原图链接", "image_url"),
            ("关键词", "keywords"),
            ("Asset ID", "titi_asset_id"),
            ("World ID", "titi_world_id"),
        ]

        for row_index, (label, key) in enumerate(rows):
            ttk.Label(form, text=f"{label}:", width=10, anchor=tk.E).grid(
                row=row_index,
                column=0,
                padx=(0, 8),
                pady=4,
                sticky=tk.E,
            )
            entry = ttk.Entry(form, textvariable=self.edit_vars[key])
            entry.grid(row=row_index, column=1, padx=0, pady=4, sticky="ew")

            if key == "source":
                ttk.Button(form, text="打开", width=8, command=self._open_source_url).grid(
                    row=row_index,
                    column=2,
                    padx=(6, 0),
                    pady=4,
                    sticky=tk.W,
                )
            elif key == "image_url":
                tail = ttk.Frame(form)
                tail.grid(row=row_index, column=2, padx=(6, 0), pady=4, sticky=tk.W)
                ttk.Button(tail, text="打开", width=8, command=self._open_image_url).pack(side=tk.LEFT)
                ttk.Button(tail, text="下载修复", command=self._repair_from_image_url_via_browser).pack(
                    side=tk.LEFT, padx=(4, 0)
                )
                ttk.Button(tail, text="直连修复", command=self._repair_from_image_url).pack(side=tk.LEFT, padx=(4, 0))

        form.columnconfigure(1, weight=1)
        self.desc_text = tk.Text(desc_box, wrap=tk.WORD)
        desc_scroll = ttk.Scrollbar(desc_box, orient=tk.VERTICAL, command=self.desc_text.yview)
        self.desc_text.configure(yscrollcommand=desc_scroll.set)
        self.desc_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        desc_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.extra_profile_rows: List[Dict[str, Any]] = []
        self.extra_profile_rows_frame: Optional[Any] = None
        self.extra_profile_rows_canvas: Optional[Any] = None

        extra_tools = ttk.Frame(extra_box)
        extra_tools.pack(fill=tk.X)
        ttk.Button(
            extra_tools,
            text="新增字段",
            width=10,
            command=self._on_add_adaptive_field_clicked,
        ).pack(side=tk.LEFT)
        ttk.Button(
            extra_tools,
            text="清空字段",
            width=10,
            command=self._clear_adaptive_profile_rows,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Label(
            extra_tools,
            text="读取时自动识别字段；警察场景会自动显示警号（可空/待填写）",
            bootstyle="secondary",
        ).pack(side=tk.LEFT, padx=(10, 0))

        header = ttk.Frame(extra_box)
        header.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(header, text="字段名", width=18, anchor=tk.W).pack(side=tk.LEFT)
        ttk.Label(header, text="字段值", anchor=tk.W).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 0))

        rows_wrap = ttk.Frame(extra_box)
        rows_wrap.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
        rows_canvas = tk.Canvas(rows_wrap, highlightthickness=0, borderwidth=0)
        rows_scroll = ttk.Scrollbar(rows_wrap, orient=tk.VERTICAL, command=rows_canvas.yview)
        rows_canvas.configure(yscrollcommand=rows_scroll.set)
        rows_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        rows_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        rows_holder = ttk.Frame(rows_canvas)
        rows_holder_window = rows_canvas.create_window((0, 0), window=rows_holder, anchor="nw")
        rows_holder.bind("<Configure>", lambda _e: self._refresh_adaptive_profile_scrollregion())

        def _sync_adaptive_rows_width(event):
            try:
                rows_canvas.itemconfigure(rows_holder_window, width=int(event.width))
            except Exception:
                pass

        rows_canvas.bind("<Configure>", _sync_adaptive_rows_width)
        self.extra_profile_rows_frame = rows_holder
        self.extra_profile_rows_canvas = rows_canvas

        ttk.Label(
            extra_box,
            text='值支持普通文本；如需结构化可填 JSON（例如 {"rank":"三级警督"}）',
            bootstyle="secondary",
        ).pack(fill=tk.X, pady=(6, 0))

        hint = "提示：结构化保存会更新 XMP + titi:meta，并保留未知字段。"
        ttk.Label(wrap, text=hint).pack(fill=tk.X, pady=(8, 0))

        def _init_edit_split() -> None:
            try:
                total = int(sections.winfo_height() or 0)
                if total > 420:
                    sections.sashpos(0, int(total * 0.33))
                    sections.sashpos(1, int(total * 0.62))
            except Exception:
                pass

        self.after(120, _init_edit_split)

    def _build_snapshot_tab(self, parent):
        wrap = ttk.Frame(parent, padding=10)
        wrap.pack(fill=tk.BOTH, expand=True)

        self.snapshot_text = tk.Text(wrap, wrap=tk.CHAR)
        y = ttk.Scrollbar(wrap, orient=tk.VERTICAL, command=self.snapshot_text.yview)
        x = ttk.Scrollbar(wrap, orient=tk.HORIZONTAL, command=self.snapshot_text.xview)
        self.snapshot_text.configure(yscrollcommand=y.set, xscrollcommand=x.set)

        self.snapshot_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y.pack(side=tk.RIGHT, fill=tk.Y)
        x.pack(side=tk.BOTTOM, fill=tk.X)

    def _build_advanced_tab(self, parent):
        wrap = ttk.Frame(parent, padding=10)
        wrap.pack(fill=tk.BOTH, expand=True)

        tip = (
            "高级写入会直接覆盖对应命名空间。\n"
            "请先确认 JSON 格式正确；建议先在结构化方式保存，再做局部高级调整。"
        )
        ttk.Label(wrap, text=tip, bootstyle="warning").pack(fill=tk.X)

        editor_nb = ttk.Notebook(wrap)
        editor_nb.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.xmp_editor = self._build_json_editor(editor_nb, "XMP JSON", "xmp")
        self.exif_editor = self._build_json_editor(editor_nb, "EXIF JSON", "exif")
        self.iptc_editor = self._build_json_editor(editor_nb, "IPTC JSON", "iptc")

        editor_nb.add(self.xmp_editor["frame"], text="XMP")
        editor_nb.add(self.exif_editor["frame"], text="EXIF")
        editor_nb.add(self.iptc_editor["frame"], text="IPTC")

    def _set_scraper_control_buttons(self, running: bool):
        paused = bool(getattr(self, "_public_scraper_manual_paused", False))
        has_task_root = bool(str(self._public_scraper_output_root or "").strip())
        start_state = tk.NORMAL
        stop_state = tk.NORMAL if (running and (not paused)) else tk.DISABLED
        resume_state = tk.NORMAL if ((running and paused) or (not running)) else tk.DISABLED
        retry_state = tk.NORMAL if ((not running) and has_task_root) else tk.DISABLED
        for btn, state in [
            (self._scraper_start_btn, start_state),
            (self._scraper_stop_btn, stop_state),
            (self._scraper_resume_btn, resume_state),
            (self._scraper_retry_btn, retry_state),
        ]:
            if btn is None:
                continue
            try:
                btn.configure(state=state)
            except Exception:
                pass
        if self._scraper_resume_btn is not None:
            try:
                self._scraper_resume_btn.configure(text=("继续运行" if (running and paused) else "继续任务"))
            except Exception:
                pass

    def _on_public_scraper_panel_close(self):
        panel = self._public_scraper_panel
        self._public_scraper_panel = None
        self._scraper_start_btn = None
        self._scraper_stop_btn = None
        self._scraper_resume_btn = None
        self._scraper_retry_btn = None
        self._scraper_monitor_state_var = None
        self._scraper_monitor_pid_var = None
        self._scraper_monitor_elapsed_var = None
        self._scraper_monitor_counts_var = None
        self._scraper_monitor_progress_var = None
        self._scraper_monitor_progress_bar = None
        self._scraper_monitor_paths_var = None
        self._scraper_monitor_log_text = None
        self._scraper_monitor_pending_box = None
        self._scraper_monitor_done_box = None
        self._scraper_monitor_progress_table = None
        self._scraper_monitor_progress_done_table = None
        self._scraper_monitor_last_log_snapshot = ""
        self._scraper_monitor_last_progress_snapshot = ""
        self._scraper_monitor_last_opened_path = ""
        self._scraper_monitor_total_hint = 0
        self._scraper_progress_selection_syncing = False
        self._scraper_row_open_pending = False
        self._scraper_row_opening = False
        self._scraper_task_tree = None
        self._scraper_task_status_var = None
        if panel is not None:
            try:
                panel.destroy()
            except Exception:
                pass

    def _open_public_scraper_panel(self):
        panel = self._public_scraper_panel
        if panel is not None:
            try:
                if panel.winfo_exists():
                    panel.deiconify()
                    panel.lift()
                    panel.focus_force()
                    self._refresh_scraper_monitor_panel()
                    return
            except Exception:
                pass

        panel = tk.Toplevel(self)
        panel.title("公共抓取")
        panel.geometry("1100x640")
        panel.minsize(960, 560)
        self._public_scraper_panel = panel

        top = ttk.Frame(panel, padding=(10, 10, 10, 6))
        top.pack(fill=tk.X)
        self._scraper_start_btn = ttk.Button(
            top,
            text="开始抓取",
            command=self._start_public_scraper_from_gui,
        )
        self._scraper_start_btn.pack(side=tk.LEFT)
        self._scraper_stop_btn = ttk.Button(
            top,
            text="暂停任务",
            command=self._pause_public_scraper_from_gui,
        )
        self._scraper_stop_btn.pack(side=tk.LEFT, padx=(8, 0))
        self._scraper_resume_btn = ttk.Button(
            top,
            text="继续任务",
            command=self._continue_public_scraper_from_gui,
        )
        self._scraper_resume_btn.pack(side=tk.LEFT, padx=(8, 0))
        self._scraper_retry_btn = ttk.Button(
            top,
            text="重试失败",
            command=self._retry_public_scraper_from_gui,
        )
        self._scraper_retry_btn.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(
            top,
            text="任务管理",
            command=self._open_public_task_manager,
        ).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(
            top,
            text="打开选中",
            command=self._open_selected_scraper_result,
        ).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(
            top,
            text="关闭面板",
            command=self._on_public_scraper_panel_close,
        ).pack(side=tk.RIGHT)

        self._build_scraper_monitor_panel(panel)
        running = bool(self._public_scraper_proc and (self._public_scraper_proc.poll() is None))
        self._set_scraper_control_buttons(running=running)
        self._refresh_scraper_monitor_panel()

        panel.protocol("WM_DELETE_WINDOW", self._on_public_scraper_panel_close)
        panel.lift()
        panel.focus_force()

    def _build_scraper_monitor_panel(self, parent: Any):
        panel = ttk.Labelframe(parent, text="抓取监控", padding=(10, 6))
        panel.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        task_box = ttk.Labelframe(panel, text="任务管理（会话内，点击切换下方监控）", padding=(6, 4))
        task_box.pack(fill=tk.X, pady=(0, 6))
        task_wrap = ttk.Frame(task_box)
        task_wrap.pack(fill=tk.X)
        task_cols = ("status", "pid", "task", "root")
        task_tree = ttk.Treeview(task_wrap, columns=task_cols, show="headings", height=3)
        self._scraper_task_tree = task_tree
        task_tree.heading("status", text="状态")
        task_tree.heading("pid", text="PID")
        task_tree.heading("task", text="任务")
        task_tree.heading("root", text="目录")
        task_tree.column("status", width=120, stretch=False, anchor=tk.W)
        task_tree.column("pid", width=80, stretch=False, anchor=tk.CENTER)
        task_tree.column("task", width=180, stretch=False, anchor=tk.W)
        task_tree.column("root", width=560, stretch=True, anchor=tk.W)
        y_task = ttk.Scrollbar(task_wrap, orient=tk.VERTICAL, command=task_tree.yview)
        task_tree.configure(yscrollcommand=y_task.set)
        task_tree.pack(side=tk.LEFT, fill=tk.X, expand=True)
        y_task.pack(side=tk.RIGHT, fill=tk.Y)
        task_tree.bind("<<TreeviewSelect>>", self._on_scraper_task_selected)
        self._scraper_task_status_var = tk.StringVar(value="会话任务: 0")
        ttk.Label(task_box, textvariable=self._scraper_task_status_var, bootstyle="secondary").pack(
            fill=tk.X, pady=(3, 0)
        )

        line1 = ttk.Frame(panel)
        line1.pack(fill=tk.X)
        self._scraper_monitor_state_var = tk.StringVar(value="状态: 空闲")
        self._scraper_monitor_pid_var = tk.StringVar(value="PID: -")
        self._scraper_monitor_elapsed_var = tk.StringVar(value="运行时长: 00:00:00")
        ttk.Label(line1, textvariable=self._scraper_monitor_state_var).pack(side=tk.LEFT)
        ttk.Label(line1, textvariable=self._scraper_monitor_pid_var).pack(side=tk.LEFT, padx=(16, 0))
        ttk.Label(line1, textvariable=self._scraper_monitor_elapsed_var).pack(side=tk.LEFT, padx=(16, 0))

        line2 = ttk.Frame(panel)
        line2.pack(fill=tk.X, pady=(4, 0))
        self._scraper_monitor_counts_var = tk.StringVar(
            value="进度: 总目标 0 / 已发现 0 (0.0%) / 已下载 0 (0.0%) / 已完成 0 / 列表 0 / 详情 0 / 图片 0 / 元数据 0"
        )
        ttk.Label(line2, textvariable=self._scraper_monitor_counts_var).pack(side=tk.LEFT)

        line3 = ttk.Frame(panel)
        line3.pack(fill=tk.X, pady=(4, 0))
        self._scraper_monitor_progress_var = tk.StringVar(value="下载进度：0 / 0 (0.0%)")
        ttk.Label(line3, textvariable=self._scraper_monitor_progress_var, width=22).pack(side=tk.LEFT)
        self._scraper_monitor_progress_bar = ttk.Progressbar(
            line3,
            orient=tk.HORIZONTAL,
            mode="determinate",
            maximum=100.0,
        )
        self._scraper_monitor_progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 0))

        line4 = ttk.Frame(panel)
        line4.pack(fill=tk.X, pady=(4, 0))
        self._scraper_monitor_paths_var = tk.StringVar(value="输出: -")
        ttk.Label(line4, textvariable=self._scraper_monitor_paths_var).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(line4, text="打开图片目录", command=self._open_scraper_named_dir, width=12).pack(side=tk.RIGHT)
        ttk.Button(line4, text="重试失败", command=self._retry_public_scraper_from_gui, width=10).pack(
            side=tk.RIGHT, padx=(0, 6)
        )
        ttk.Button(line4, text="打开日志", command=self._open_scraper_log_path, width=10).pack(side=tk.RIGHT, padx=(0, 6))

        split = ttk.Panedwindow(panel, orient=tk.HORIZONTAL)
        split.pack(fill=tk.BOTH, expand=True, pady=(6, 0))

        progress_box = ttk.Labelframe(split, text="任务明细（按发现顺序）", padding=4)
        logs_box = ttk.Labelframe(
            split,
            text=f"运行日志(最近{self._scraper_monitor_log_tail_lines}行)",
            padding=4,
        )
        split.add(progress_box, weight=7)
        split.add(logs_box, weight=3)

        progress_split = ttk.Panedwindow(progress_box, orient=tk.VERTICAL)
        progress_split.pack(fill=tk.BOTH, expand=True)
        pending_box = ttk.Labelframe(progress_split, text="待处理条目（0）", padding=4)
        done_box = ttk.Labelframe(progress_split, text="已完成条目（0）", padding=4)
        self._scraper_monitor_pending_box = pending_box
        self._scraper_monitor_done_box = done_box
        progress_split.add(pending_box, weight=3)
        progress_split.add(done_box, weight=2)
        self._scraper_monitor_progress_table = self._build_scraper_progress_tree(pending_box, height=9)
        self._scraper_monitor_progress_done_table = self._build_scraper_progress_tree(done_box, height=6)

        log_wrap = ttk.Frame(logs_box)
        log_wrap.pack(fill=tk.BOTH, expand=True)
        self._scraper_monitor_log_text = tk.Text(log_wrap, height=12, wrap=tk.WORD)
        log_scroll = ttk.Scrollbar(log_wrap, orient=tk.VERTICAL, command=self._scraper_monitor_log_text.yview)
        self._scraper_monitor_log_text.configure(yscrollcommand=log_scroll.set, state=tk.DISABLED)
        self._scraper_monitor_log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    def _build_scraper_progress_tree(self, parent: Any, height: int = 8) -> ttk.Treeview:
        table_wrap = ttk.Frame(parent)
        table_wrap.pack(fill=tk.BOTH, expand=True)
        columns = ("idx", "name", "detail", "image", "meta", "reason", "detail_url", "image_path")
        table = ttk.Treeview(
            table_wrap,
            columns=columns,
            show="headings",
            height=max(4, int(height or 8)),
            selectmode="extended",
        )
        table.heading("idx", text="#")
        table.heading("name", text="姓名")
        table.heading("detail", text="详情")
        table.heading("image", text="图片")
        table.heading("meta", text="元数据")
        table.heading("reason", text="当前状态/说明")
        table.heading("detail_url", text="")
        table.heading("image_path", text="")
        table.column("idx", width=56, anchor=tk.CENTER, stretch=False)
        table.column("name", width=160, anchor=tk.W, stretch=False)
        table.column("detail", width=64, anchor=tk.CENTER, stretch=False)
        table.column("image", width=64, anchor=tk.CENTER, stretch=False)
        table.column("meta", width=72, anchor=tk.CENTER, stretch=False)
        table.column("reason", width=560, anchor=tk.W, stretch=True)
        table.column("detail_url", width=0, stretch=False, anchor=tk.W)
        table.column("image_path", width=0, stretch=False, anchor=tk.W)
        y_table = ttk.Scrollbar(table_wrap, orient=tk.VERTICAL, command=table.yview)
        x_table = ttk.Scrollbar(table_wrap, orient=tk.HORIZONTAL, command=table.xview)
        table.configure(yscrollcommand=y_table.set, xscrollcommand=x_table.set)
        table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        table.bind("<<TreeviewSelect>>", self._on_scraper_progress_row_selected)
        table.bind("<Button-3>", self._on_scraper_progress_context_menu)
        y_table.pack(side=tk.RIGHT, fill=tk.Y)
        x_table.pack(side=tk.BOTTOM, fill=tk.X)
        return table

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        return _svc_format_elapsed(seconds)

    @staticmethod
    def _repair_mojibake_utf8_latin1(text: str) -> str:
        return _svc_repair_mojibake_utf8_latin1(text)

    @staticmethod
    def _read_text_tail(path: str, max_lines: int = 30) -> str:
        return _svc_read_text_tail(path, max_lines=max_lines)

    @staticmethod
    def _read_jsonl_rows(path: str, max_rows: int = 0) -> List[Dict[str, Any]]:
        return _svc_read_jsonl_rows(path, max_rows=max_rows)

    @staticmethod
    def _write_jsonl_rows(path: str, rows: List[Dict[str, Any]]) -> None:
        _svc_write_jsonl_rows(path, rows)

    @staticmethod
    def _merge_status_reason(entry: Dict[str, Any], msg: str):
        _svc_merge_status_reason(entry, msg)

    @staticmethod
    def _humanize_scraper_reason(text: str) -> str:
        return _svc_humanize_scraper_reason(text)

    @staticmethod
    def _normalize_person_key(name: Any) -> str:
        return _svc_normalize_person_key(name)

    @staticmethod
    def _extract_runtime_log_field(line: str, label: str) -> str:
        return _svc_extract_runtime_log_field(line, label)

    def _extract_scraper_live_actions(
        self,
        output_root: str,
    ) -> Tuple[Dict[str, str], Dict[str, str], str]:
        return _svc_extract_scraper_live_actions(
            output_root,
            read_text_tail_fn=self._read_text_tail,
            repair_text_fn=self._repair_mojibake_utf8_latin1,
            extract_runtime_log_field_fn=self._extract_runtime_log_field,
            normalize_person_key_fn=self._normalize_person_key,
        )

    def _scraper_missing_required_fields_from_info(self, info: ImageMetadataInfo) -> List[str]:
        if not isinstance(info, ImageMetadataInfo):
            return ["gender", "birth_date", "photo_taken_at", "age_at_photo"]
        profile: Dict[str, Any] = {}
        if isinstance(info.titi_json, dict):
            prof_raw = (info.titi_json or {}).get("d2i_profile")
            if isinstance(prof_raw, dict):
                profile = prof_raw
        return _svc_missing_audit_fields_from_metadata_snapshot(
            gender=info.gender,
            profile=profile,
        )

    def _sync_scraper_audit_review_queue_for_detail(
        self,
        output_root: str,
        detail_url: str,
        *,
        missing_fields: List[str],
        name_hint: str = "",
    ) -> bool:
        return _svc_sync_scraper_audit_review_queue(
            output_root,
            detail_url,
            missing_fields=missing_fields,
            name_hint=name_hint,
            read_rows_fn=lambda path: self._read_jsonl_rows(path, max_rows=0),
            write_rows_fn=self._write_jsonl_rows,
            scraped_at=datetime.now().isoformat(timespec="seconds"),
        )

    @staticmethod
    def _normalize_existing_path(path_value: Any) -> str:
        return _svc_normalize_existing_path(path_value)

    @staticmethod
    def _read_json_file(path: str) -> Dict[str, Any]:
        return _svc_read_json_file(path)

    def _estimate_scraper_total_target(self, output_root: str) -> int:
        return _svc_estimate_scraper_total_target(
            output_root,
            count_jsonl_rows_fn=self._count_jsonl_rows,
        )

    def _collect_scraper_progress_rows(self, output_root: str, max_rows: int = 3000) -> List[Dict[str, str]]:
        return _svc_collect_scraper_progress_rows(
            output_root,
            max_rows=max_rows,
            read_jsonl_rows_fn=self._read_jsonl_rows,
            read_json_file_fn=self._read_json_file,
            normalize_existing_path_fn=self._normalize_existing_path,
            extract_live_actions_fn=self._extract_scraper_live_actions,
            is_row_completed_fn=self._is_scraper_row_completed,
        )

    @staticmethod
    def _is_scraper_row_completed(row: Dict[str, Any]) -> bool:
        return _svc_is_scraper_row_completed(row)

    @staticmethod
    def _is_scraper_row_image_downloaded(row: Dict[str, Any]) -> bool:
        return _svc_is_scraper_row_image_downloaded(row)

    def _update_scraper_progress_group_titles(self, pending_count: int, done_count: int) -> None:
        pending_box = self._scraper_monitor_pending_box
        done_box = self._scraper_monitor_done_box
        if pending_box is not None:
            try:
                pending_box.configure(text=f"待处理条目（{max(0, int(pending_count or 0))}）")
            except Exception:
                pass
        if done_box is not None:
            try:
                done_box.configure(text=f"已完成条目（{max(0, int(done_count or 0))}）")
            except Exception:
                pass

    def _refresh_scraper_progress_table(self, output_root: str, rows: Optional[List[Dict[str, Any]]] = None):
        pending_table = self._scraper_monitor_progress_table
        done_table = self._scraper_monitor_progress_done_table
        if (pending_table is None) and (done_table is None):
            self._update_scraper_progress_group_titles(0, 0)
            return
        if rows is None:
            rows = self._collect_scraper_progress_rows(output_root)
        pending_rows, done_rows = _svc_split_scraper_progress_rows(
            rows,
            is_row_completed_fn=self._is_scraper_row_completed,
        )
        self._update_scraper_progress_group_titles(len(pending_rows), len(done_rows))
        snapshot = _svc_scraper_progress_snapshot(pending_rows, done_rows)
        if snapshot == self._scraper_monitor_last_progress_snapshot:
            return
        self._scraper_monitor_last_progress_snapshot = snapshot
        try:
            if pending_table is not None:
                pending_table.delete(*pending_table.get_children())
                for row in pending_rows:
                    pending_table.insert(
                        "",
                        tk.END,
                        values=_svc_scraper_progress_row_to_table_values(row),
                    )
            if done_table is not None:
                done_table.delete(*done_table.get_children())
                for row in done_rows:
                    done_table.insert(
                        "",
                        tk.END,
                        values=_svc_scraper_progress_row_to_table_values(row),
                    )
        except Exception:
            pass

    def _iter_scraper_progress_tables(self) -> List[ttk.Treeview]:
        tables: List[ttk.Treeview] = []
        for table in (self._scraper_monitor_progress_table, self._scraper_monitor_progress_done_table):
            if table is not None:
                tables.append(table)
        return tables

    def _get_selected_scraper_progress_values(self) -> Tuple[Any, ...]:
        focused = None
        try:
            focused = self.focus_get()
        except Exception:
            focused = None

        tables = self._iter_scraper_progress_tables()
        prioritized = []
        if focused in tables:
            prioritized.append(focused)
        for table in tables:
            if table not in prioritized:
                prioritized.append(table)

        prioritized_values: List[Any] = []
        for table in prioritized:
            try:
                selected = table.selection()
                if not selected:
                    continue
                values = table.item(selected[0], "values")
            except Exception:
                continue
            prioritized_values.append(values)
        return _svc_pick_first_progress_values(prioritized_values)

    def _collect_selected_scraper_progress_values(self, table: Optional[ttk.Treeview] = None) -> List[Tuple[Any, ...]]:
        values_list: List[Any] = []
        tables = [table] if table is not None else self._iter_scraper_progress_tables()
        for t in tables:
            if t is None:
                continue
            try:
                selected = list(t.selection() or [])
            except Exception:
                selected = []
            for row_id in selected:
                try:
                    values = t.item(row_id, "values")
                except Exception:
                    continue
                values_list.append(values)
        return _svc_collect_progress_values(values_list)

    @staticmethod
    def _scraper_progress_values_has_error(values: Tuple[Any, ...]) -> bool:
        return _svc_scraper_progress_values_has_error(values)

    def _collect_selected_scraper_detail_urls(self, table: Optional[ttk.Treeview] = None) -> List[str]:
        values_list = self._collect_selected_scraper_progress_values(table)
        return _svc_collect_detail_urls_from_progress_values(values_list)

    def _select_scraper_error_rows(self, table: Optional[ttk.Treeview] = None, *, across_tables: bool = False) -> int:
        target_tables = []
        if across_tables:
            target_tables = self._iter_scraper_progress_tables()
        elif table is not None:
            target_tables = [table]
        else:
            target_tables = self._iter_scraper_progress_tables()
        if not target_tables:
            return 0

        total_selected = 0
        try:
            self._scraper_progress_selection_syncing = True
            if (not across_tables) and target_tables:
                selected_table = target_tables[0]
                for other in _svc_collect_other_progress_table_refs(self._iter_scraper_progress_tables(), selected_table):
                    try:
                        other.selection_remove(other.selection())
                    except Exception:
                        pass

            for t in target_tables:
                try:
                    row_ids = list(t.get_children("") or [])
                except Exception:
                    row_ids = []
                row_items: List[Tuple[Any, Any]] = []
                for row_id in row_ids:
                    try:
                        values = tuple(t.item(row_id, "values") or ())
                    except Exception:
                        continue
                    row_items.append((row_id, values))
                bad_ids = _svc_collect_scraper_error_row_ids(
                    row_items,
                    has_error_fn=self._scraper_progress_values_has_error,
                )
                try:
                    if bad_ids:
                        t.selection_set(bad_ids)
                        t.focus(bad_ids[0])
                    else:
                        t.selection_remove(t.selection())
                except Exception:
                    pass
                total_selected += len(bad_ids)
        finally:
            self._scraper_progress_selection_syncing = False

        self._set_status(_svc_scraper_error_selection_status_text(total_selected))
        return total_selected

    def _resolve_scraper_selected_image_path(self) -> str:
        values = self._get_selected_scraper_progress_values()
        return _svc_resolve_openable_image_path_from_progress_values(
            values,
            normalize_existing_path_fn=self._normalize_existing_path,
        )

    def _open_selected_scraper_result(self):
        target = self._resolve_scraper_selected_image_path()
        if not target:
            messagebox.showinfo("提示", "当前选中项还没有可打开的本地图片。", parent=self)
            return
        self._open_scraper_result_path(target)

    def _open_scraper_result_path(self, target: str):
        if not target:
            return
        if target == self._scraper_monitor_last_opened_path:
            return
        if os.path.abspath(str(self.current_path or "")) == os.path.abspath(target):
            self._scraper_monitor_last_opened_path = target
            return
        if self._scraper_row_opening:
            return
        self._scraper_row_opening = True
        try:
            self._load_target(target)
            self._scraper_monitor_last_opened_path = target
            self._set_status(f"已打开：{os.path.basename(target)}")
            self._focus_main_preview_from_scraper()
            self._queue_scraper_audit_sync_after_open(target)
        except Exception:
            pass
        finally:
            self._scraper_row_opening = False

    def _queue_scraper_audit_sync_after_open(self, target_path: str) -> None:
        # Audit sync can be expensive (reads JSONL + metadata); keep it off the UI thread.
        if self._is_process_running(self._public_scraper_proc):
            return
        output_root = str(self._public_scraper_output_root or "").strip()
        detail_url = str(getattr(self, "_scraper_last_selected_detail_url", "") or "").strip()
        if (not output_root) or (not detail_url):
            return

        target_norm = os.path.abspath(str(target_path or "").strip())
        output_root_norm = os.path.abspath(output_root)
        detail_norm = detail_url
        cached_info = self._last_info if isinstance(self._last_info, ImageMetadataInfo) else None

        import threading

        def _runner() -> None:
            changed = False
            try:
                info = cached_info
                if (info is None) or (os.path.abspath(getattr(info, "filepath", "")) != target_norm):
                    info = read_image_metadata(target_norm)

                missing_fields = self._scraper_missing_required_fields_from_info(info)
                name_hint = str(getattr(info, "person", "") or getattr(info, "title", "") or "").strip()
                changed = self._sync_scraper_audit_review_queue_for_detail(
                    output_root_norm,
                    detail_norm,
                    missing_fields=missing_fields,
                    name_hint=name_hint,
                )
            except Exception:
                changed = False

            def _done() -> None:
                if not changed:
                    return
                self._scraper_monitor_last_progress_snapshot = ""
                self._refresh_scraper_monitor_panel()

            try:
                self.after(0, _done)
            except Exception:
                _done()

        threading.Thread(target=_runner, daemon=True).start()

    def _sync_scraper_audit_hints_after_open(self, target_path: str) -> None:
        # When user opens an item and metadata is already corrected, auto-clear stale audit hints.
        if self._is_process_running(self._public_scraper_proc):
            return
        output_root = str(self._public_scraper_output_root or "").strip()
        detail_url = str(getattr(self, "_scraper_last_selected_detail_url", "") or "").strip()
        if (not output_root) or (not detail_url):
            return
        info = self._last_info if isinstance(self._last_info, ImageMetadataInfo) else None
        if (info is None) or (os.path.abspath(getattr(info, "filepath", "")) != os.path.abspath(target_path)):
            try:
                info = read_image_metadata(target_path)
            except Exception:
                return
        missing_fields = self._scraper_missing_required_fields_from_info(info)
        name_hint = str(getattr(info, "person", "") or getattr(info, "title", "") or "").strip()
        changed = self._sync_scraper_audit_review_queue_for_detail(
            output_root,
            detail_url,
            missing_fields=missing_fields,
            name_hint=name_hint,
        )
        if changed:
            self._scraper_monitor_last_progress_snapshot = ""
            self._refresh_scraper_monitor_panel()

    def _queue_open_selected_scraper_result(self):
        if self._scraper_row_open_pending:
            return
        self._scraper_row_open_pending = True

        def _run():
            self._scraper_row_open_pending = False
            target = self._resolve_scraper_selected_image_path()
            if not target:
                return
            self._open_scraper_result_path(target)

        try:
            self.after_idle(_run)
        except Exception:
            self._scraper_row_open_pending = False

    def _on_scraper_progress_row_selected(self, _event=None):
        if self._scraper_progress_selection_syncing:
            return
        tables = self._iter_scraper_progress_tables()
        event_widget = getattr(_event, "widget", None)
        selected_values: Any = tuple()
        try:
            if event_widget in tables:
                selected = event_widget.selection()
                if selected:
                    selected_values = event_widget.item(selected[0], "values")
        except Exception:
            pass
        plan = _svc_build_progress_selection_sync_plan(tables, event_widget, selected_values)
        detail_url = str(plan.get("detail_url", "") or "").strip()
        if detail_url:
            self._scraper_last_selected_detail_url = detail_url
        try:
            if bool(plan.get("has_active_table")):
                self._scraper_progress_selection_syncing = True
                for table in list(plan.get("clear_other_refs") or []):
                    table.selection_remove(table.selection())
        except Exception:
            pass
        try:
            if bool(plan.get("should_queue_open")):
                self._queue_open_selected_scraper_result()
        finally:
            self._scraper_progress_selection_syncing = False

    def _on_scraper_progress_context_menu(self, event: Any):
        table = getattr(event, "widget", None)
        if table not in self._iter_scraper_progress_tables():
            return "break"
        row_id = ""
        try:
            row_id = str(table.identify_row(event.y) or "").strip()
        except Exception:
            row_id = ""
        if not row_id:
            return "break"

        selected_now: List[Any] = []
        selected_values: List[Any] = []
        values_clicked: Any = tuple()
        try:
            selected_now = list(table.selection() or [])
        except Exception:
            selected_now = []
        for selected_id in selected_now:
            try:
                selected_values.append(table.item(selected_id, "values"))
            except Exception:
                continue
        try:
            values_clicked = table.item(row_id, "values")
        except Exception:
            values_clicked = tuple()

        plan = _svc_build_progress_context_menu_plan(
            row_id,
            selected_now,
            values_clicked,
            selected_values,
        )
        try:
            if bool(plan.get("should_reset_selection")):
                table.selection_set(str(plan.get("row_id", "") or "").strip())
            focus_row_id = str(plan.get("focus_row_id", "") or "").strip()
            if focus_row_id:
                table.focus(focus_row_id)
        except Exception:
            pass

        detail_url = str(plan.get("detail_url", "") or "").strip()
        if detail_url:
            self._scraper_last_selected_detail_url = detail_url

        detail_urls = list(plan.get("detail_urls") or [])
        detail_count = int(plan.get("retry_count", 0) or 0)

        menu = tk.Menu(self, tearoff=False)
        menu.add_command(label="打开选中", command=self._open_selected_scraper_result)
        menu.add_command(
            label="全选错误项（当前列表）",
            command=lambda t=table: self._select_scraper_error_rows(t, across_tables=False),
        )
        menu.add_command(
            label="全选错误项（上下列表）",
            command=lambda: self._select_scraper_error_rows(None, across_tables=True),
        )
        menu.add_separator()
        if detail_count > 0:
            menu.add_command(
                label=f"重试选中 {detail_count} 条（继续任务时生效）",
                command=lambda urls=list(detail_urls): self._retry_scraper_detail_rows(urls),
            )
        else:
            menu.add_command(label="重试选中条目（继续任务时生效）", state=tk.DISABLED)
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                menu.grab_release()
            except Exception:
                pass
        return "break"

    def _retry_scraper_detail_row(self, detail_url: str):
        self._retry_scraper_detail_rows([detail_url])

    def _retry_scraper_detail_rows(self, detail_urls: List[str]):
        retry_plan = _svc_build_retry_detail_plan(detail_urls)
        detail_list = list(retry_plan.get("detail_urls") or [])
        if not bool(retry_plan.get("has_details")):
            messagebox.showinfo(
                "提示",
                str(retry_plan.get("empty_selection_message", "") or "当前选中项缺少详情链接，无法标记重试。"),
                parent=self,
            )
            return

        proc = self._public_scraper_proc
        if proc and (proc.poll() is None):
            messagebox.showinfo("提示", "请先暂停当前任务，再执行重试。", parent=self)
            return

        output_root = str(self._public_scraper_output_root or "").strip()
        if (not output_root) or (not os.path.isdir(output_root)):
            messagebox.showerror("重试失败", "当前任务目录无效，无法执行重试。", parent=self)
            return

        if not messagebox.askyesno(
            str(retry_plan.get("confirm_title", "") or "确认重试"),
            str(retry_plan.get("confirm_message", "") or ""),
            parent=self,
        ):
            return

        cleanup_result = _svc_execute_retry_detail_cleanup(output_root, detail_list)
        if not bool(cleanup_result.get("has_touched")):
            messagebox.showinfo(
                "提示",
                str(cleanup_result.get("empty_cleanup_message", "")),
                parent=self,
            )
        else:
            self._set_status(str(cleanup_result.get("status_text", "")))
            messagebox.showinfo(
                str(cleanup_result.get("dialog_title", "") or "已标记重试"),
                str(cleanup_result.get("dialog_message", "")),
                parent=self,
            )
        self._refresh_scraper_monitor_panel()

    def _focus_main_preview_from_scraper(self):
        try:
            self.deiconify()
            self.lift()
            self.focus_force()
        except Exception:
            pass
        panel = self._public_scraper_panel
        if panel is None:
            return
        try:
            if panel.winfo_exists():
                panel.attributes("-topmost", False)
                panel.lower(self)
        except Exception:
            pass

    def _open_scraper_named_dir(self):
        target = self._public_scraper_named_dir or self._public_scraper_output_root
        if target and os.path.isdir(target):
            os.startfile(target)
            return
        messagebox.showinfo("提示", "当前暂无可打开的图片目录。", parent=self)

    def _open_scraper_log_path(self):
        target = self._public_scraper_log_path
        if (not target) and self._public_scraper_output_root:
            target = os.path.join(self._public_scraper_output_root, "reports", "gui_public_scraper.log")
        if target and os.path.exists(target):
            try:
                # Use a separate process to avoid blocking the GUI thread.
                subprocess.Popen(["notepad.exe", target], close_fds=True)
                return
            except Exception:
                pass
            try:
                os.startfile(os.path.dirname(target))
                return
            except Exception as e:
                messagebox.showerror("打开失败", f"无法打开日志：\n{e}", parent=self)
                return
        messagebox.showinfo("提示", "当前暂无可打开的日志文件。", parent=self)

    @staticmethod
    def _get_scraper_record_path(output_root: str) -> str:
        return _svc_get_scraper_record_path(output_root)

    @staticmethod
    def _read_scraper_backoff_state(output_root: str) -> Dict[str, str]:
        return _svc_read_scraper_backoff_state(output_root)

    @staticmethod
    def _default_public_tasks_root() -> str:
        return _svc_default_public_tasks_root(__file__)

    def _retry_requires_crawl_phase(self, output_root: str) -> bool:
        root = str(output_root or "").strip()
        if (not root) or (not os.path.isdir(root)):
            return False
        try:
            rows = self._collect_scraper_progress_rows(root, max_rows=20000)
        except Exception:
            return False
        return _svc_retry_requires_crawl_phase(
            rows,
            is_row_completed_fn=self._is_scraper_row_completed,
        )

    def _discover_public_task_roots(self, base_root: str) -> List[str]:
        return _svc_discover_public_task_roots(base_root)

    def _count_latest_metadata_status(self, output_root: str) -> Tuple[int, int]:
        return _svc_count_latest_metadata_status(output_root)

    def _summarize_public_task(self, output_root: str) -> Dict[str, Any]:
        root = self._normalize_public_task_root(output_root)
        return _svc_summarize_public_task(
            root,
            count_jsonl_rows_fn=self._count_jsonl_rows,
            entry=self._public_scraper_tasks.get(root),
            current_active_root=self._public_scraper_active_task_root or self._public_scraper_output_root,
        )

    def _on_public_task_manager_close(self):
        win = self._public_task_manager_window
        self._public_task_manager_window = None
        self._public_task_manager_tree = None
        self._public_task_manager_base_var = None
        self._public_task_manager_status_var = None
        if win is not None:
            try:
                win.destroy()
            except Exception:
                pass

    def _refresh_public_task_manager_list(self):
        tree = getattr(self, "_public_task_manager_tree", None)
        base_var = getattr(self, "_public_task_manager_base_var", None)
        status_var = getattr(self, "_public_task_manager_status_var", None)
        if tree is None or base_var is None:
            return
        base_root = str(base_var.get() or "").strip()
        view = _svc_build_public_task_manager_list_view(
            base_root,
            discover_task_roots_fn=self._discover_public_task_roots,
            summarize_task_fn=self._summarize_public_task,
            sort_rows_fn=_svc_sort_public_task_summaries,
            row_to_values_fn=_svc_public_task_summary_to_tree_values,
        )
        rows = list(view.get("rows") or [])
        tree_values = list(view.get("tree_values") or [])
        try:
            tree.delete(*tree.get_children())
        except Exception:
            pass
        for values in tree_values:
            tree.insert("", tk.END, values=values)
        if status_var is not None:
            try:
                status_var.set(_svc_public_task_manager_status_text(len(rows)))
            except Exception:
                pass

    def _public_task_manager_selected_root(self) -> str:
        tree = getattr(self, "_public_task_manager_tree", None)
        if tree is None:
            return ""
        try:
            selected = tree.selection()
            if not selected:
                return ""
            values = tuple(tree.item(selected[0], "values") or ())
            return _svc_parse_task_root_from_values(values, root_index=9)
        except Exception:
            return ""

    def _continue_selected_public_task(self):
        root = self._public_task_manager_selected_root()
        if not root:
            messagebox.showinfo("提示", "请先在任务列表中选择一个任务。", parent=self)
            return
        self._set_active_public_scraper_task(root, refresh=False)
        active_entry = self._public_scraper_tasks.get(os.path.abspath(root))
        action = _svc_continue_action_for_active_entry(
            active_entry if isinstance(active_entry, dict) else None,
            is_process_running_fn=self._is_process_running,
        )
        if action == "resume_paused":
            self._continue_public_scraper_from_gui()
            self._refresh_public_task_manager_list()
            return
        if action == "already_running":
            messagebox.showinfo("提示", "该任务已在运行中。", parent=self)
            self._refresh_public_task_manager_list()
            return
        continue_opts = self._show_public_scraper_continue_options_dialog(root)
        if not continue_opts:
            self._set_status("已取消继续任务")
            return
        start_kwargs = _svc_build_continue_start_existing_task_args(continue_opts)
        ok = self._start_public_scraper_from_existing_task(
            output_root=root,
            **start_kwargs,
        )
        if ok:
            self._refresh_public_task_manager_list()

    def _retry_selected_public_task_failures(self):
        root = self._public_task_manager_selected_root()
        if not root:
            messagebox.showinfo("提示", "请先在任务列表中选择一个任务。", parent=self)
            return
        self._set_active_public_scraper_task(root, refresh=False)
        need_crawl = self._retry_requires_crawl_phase(root)
        retry_opts = self._show_public_scraper_continue_options_dialog(root)
        if not retry_opts:
            self._set_status("已取消失败重试")
            return
        start_kwargs = _svc_build_retry_start_existing_task_args(need_crawl, retry_opts)
        ok = self._start_public_scraper_from_existing_task(
            output_root=root,
            **start_kwargs,
        )
        if ok:
            self._set_status(_svc_retry_started_status_text(need_crawl))
            self._refresh_public_task_manager_list()

    def _rewrite_selected_public_task_metadata(self):
        root = self._public_task_manager_selected_root()
        if not root:
            messagebox.showinfo("提示", "请先在任务列表中选择一个任务。", parent=self)
            return
        self._set_active_public_scraper_task(root, refresh=False)
        start_kwargs = _svc_build_rewrite_metadata_start_existing_task_args()
        ok = self._start_public_scraper_from_existing_task(
            output_root=root,
            **start_kwargs,
        )
        if ok:
            self._refresh_public_task_manager_list()

    def _open_selected_public_task_dir(self):
        root = self._public_task_manager_selected_root()
        if not root:
            messagebox.showinfo("提示", "请先在任务列表中选择一个任务。", parent=self)
            return
        resolved = _svc_resolve_public_task_directory(root)
        if resolved:
            os.startfile(resolved)
            return
        messagebox.showerror("打开失败", f"目录不存在：\n{root}", parent=self)

    def _open_selected_public_task_log(self):
        root = self._public_task_manager_selected_root()
        if not root:
            messagebox.showinfo("提示", "请先在任务列表中选择一个任务。", parent=self)
            return
        target = _svc_resolve_public_task_log_path(root)
        if target and os.path.exists(target):
            try:
                subprocess.Popen(["notepad.exe", target], close_fds=True)
                return
            except Exception:
                pass
            try:
                os.startfile(os.path.dirname(target))
                return
            except Exception as e:
                messagebox.showerror("打开失败", f"无法打开日志：\n{e}", parent=self)
                return
        messagebox.showinfo("提示", "该任务暂无日志文件。", parent=self)

    def _open_selected_task_in_monitor(self):
        root = self._public_task_manager_selected_root()
        if not root:
            messagebox.showinfo("提示", "请先在任务列表中选择一个任务。", parent=self)
            return
        self._set_active_public_scraper_task(root, refresh=False)
        self._open_public_scraper_panel()
        self._refresh_scraper_monitor_panel()

    def _open_public_task_manager(self):
        win = self._public_task_manager_window
        if win is not None:
            try:
                if win.winfo_exists():
                    win.deiconify()
                    win.lift()
                    win.focus_force()
                    self._refresh_public_task_manager_list()
                    return
            except Exception:
                pass

        win = tk.Toplevel(self)
        win.title("抓取任务管理")
        win.geometry("1250x620")
        win.minsize(980, 500)
        self._public_task_manager_window = win

        top = ttk.Frame(win, padding=(10, 10, 10, 6))
        top.pack(fill=tk.X)
        ttk.Label(top, text="任务根目录").pack(side=tk.LEFT)
        base_default = self._default_public_tasks_root()
        if self._public_scraper_output_root:
            try:
                parent = os.path.dirname(os.path.abspath(self._public_scraper_output_root))
                if os.path.isdir(parent):
                    base_default = parent
            except Exception:
                pass
        self._public_task_manager_base_var = tk.StringVar(value=base_default)
        ttk.Entry(top, textvariable=self._public_task_manager_base_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 8))
        ttk.Button(
            top,
            text="浏览",
            command=lambda: self._choose_public_task_root_dir(),
            width=8,
        ).pack(side=tk.LEFT)
        ttk.Button(
            top,
            text="刷新",
            command=self._refresh_public_task_manager_list,
            width=8,
        ).pack(side=tk.LEFT, padx=(6, 0))

        body = ttk.Frame(win, padding=(10, 0, 10, 6))
        body.pack(fill=tk.BOTH, expand=True)
        columns = ("status", "task", "profiles", "images", "meta_ok", "pending", "review", "fail", "updated", "path")
        tree = ttk.Treeview(body, columns=columns, show="headings", height=18)
        self._public_task_manager_tree = tree
        col_cfg = {
            "status": ("状态", 130),
            "task": ("任务", 180),
            "profiles": ("详情", 70),
            "images": ("图片", 70),
            "meta_ok": ("元数据OK", 90),
            "pending": ("未完成", 80),
            "review": ("复核", 70),
            "fail": ("失败", 70),
            "updated": ("更新时间", 160),
            "path": ("目录", 360),
        }
        for key in columns:
            title, width = col_cfg[key]
            tree.heading(key, text=title)
            tree.column(key, width=width, stretch=(key == "path"), anchor=(tk.W if key in {"status", "task", "updated", "path"} else tk.CENTER))
        ybar = ttk.Scrollbar(body, orient=tk.VERTICAL, command=tree.yview)
        xbar = ttk.Scrollbar(body, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ybar.pack(side=tk.RIGHT, fill=tk.Y)
        xbar.pack(side=tk.BOTTOM, fill=tk.X)
        tree.bind("<Double-1>", lambda _e: self._open_selected_public_task_dir())

        actions = ttk.Frame(win, padding=(10, 2, 10, 10))
        actions.pack(fill=tk.X)
        ttk.Button(actions, text="继续任务", command=self._continue_selected_public_task, width=10).pack(side=tk.LEFT)
        ttk.Button(actions, text="重试失败", command=self._retry_selected_public_task_failures, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(actions, text="重写元数据", command=self._rewrite_selected_public_task_metadata, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(actions, text="打开目录", command=self._open_selected_public_task_dir, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(actions, text="打开日志", command=self._open_selected_public_task_log, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(actions, text="打开监控", command=self._open_selected_task_in_monitor, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(actions, text="关闭", command=self._on_public_task_manager_close, width=8).pack(side=tk.RIGHT)
        self._public_task_manager_status_var = tk.StringVar(value="任务数: 0")
        ttk.Label(actions, textvariable=self._public_task_manager_status_var, bootstyle="secondary").pack(
            side=tk.RIGHT, padx=(0, 12)
        )

        win.protocol("WM_DELETE_WINDOW", self._on_public_task_manager_close)
        win.lift()
        win.focus_force()
        self._refresh_public_task_manager_list()

    def _choose_public_task_root_dir(self):
        base_var = getattr(self, "_public_task_manager_base_var", None)
        if base_var is None:
            return
        current = str(base_var.get() or "").strip() or self._default_public_tasks_root()
        selected = filedialog.askdirectory(
            parent=self,
            title="选择任务根目录",
            initialdir=current,
            mustexist=True,
        )
        if not selected:
            return
        base_var.set(os.path.abspath(selected))
        self._refresh_public_task_manager_list()

    def _refresh_scraper_monitor_panel(self):
        if (not self._public_scraper_output_root) and self._public_scraper_tasks:
            chosen_root = ""
            for root, entry in self._public_scraper_tasks.items():
                if isinstance(entry, dict) and self._is_process_running(entry.get("proc")):
                    chosen_root = root
                    break
            if not chosen_root:
                try:
                    chosen_root = next(iter(self._public_scraper_tasks.keys()))
                except Exception:
                    chosen_root = ""
            if chosen_root:
                self._set_active_public_scraper_task(chosen_root, refresh=False)
        self._refresh_scraper_task_list_view()
        if self._scraper_monitor_state_var is not None:
            state_text = str(self._public_scraper_runtime_state or "空闲")
            active_root = str(self._public_scraper_output_root or "").strip()
            if active_root:
                _by_person, _by_detail, latest_action = self._extract_scraper_live_actions(active_root)
                if latest_action:
                    state_text = f"{state_text} · {latest_action}"
            self._scraper_monitor_state_var.set(f"状态: {state_text}")

        proc = self._public_scraper_proc
        pid_text = f"PID: {proc.pid}" if (proc and proc.poll() is None) else "PID: -"
        if self._scraper_monitor_pid_var is not None:
            self._scraper_monitor_pid_var.set(pid_text)

        elapsed_text = "运行时长: 00:00:00"
        if self._public_scraper_started_at:
            elapsed_text = f"运行时长: {self._format_elapsed(time.time() - self._public_scraper_started_at)}"
        if self._scraper_monitor_elapsed_var is not None:
            self._scraper_monitor_elapsed_var.set(elapsed_text)

        output_root = self._public_scraper_output_root
        if output_root:
            rows = self._collect_scraper_progress_rows(output_root)
            completed_rows = sum(1 for row in rows if self._is_scraper_row_completed(row))
            downloaded_rows = sum(1 for row in rows if self._is_scraper_row_image_downloaded(row))
            discovered_rows = len(rows)
            estimated_total = self._estimate_scraper_total_target(output_root)
            total_target = max(discovered_rows, estimated_total, self._scraper_monitor_total_hint)
            self._scraper_monitor_total_hint = total_target
            discovered_pct = (discovered_rows / total_target * 100.0) if total_target > 0 else 0.0
            download_target = max(discovered_rows, 0)
            download_pct = (downloaded_rows / download_target * 100.0) if download_target > 0 else 0.0
            if download_pct > 100.0:
                download_pct = 100.0

            list_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "list_records.jsonl"))
            profile_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "profiles.jsonl"))
            image_rows = self._count_jsonl_rows(os.path.join(output_root, "downloads", "image_downloads.jsonl"))
            metadata_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "metadata_write_results.jsonl"))
            if self._scraper_monitor_counts_var is not None:
                self._scraper_monitor_counts_var.set(
                    "进度: "
                    f"总目标 {total_target} / 已发现 {discovered_rows} ({discovered_pct:.1f}%) / "
                    f"已下载 {downloaded_rows} ({download_pct:.1f}%) / 已完成 {completed_rows} / "
                    f"列表 {list_rows} / 详情 {profile_rows} / 图片 {image_rows} / 元数据 {metadata_rows}"
                )
            if self._scraper_monitor_progress_var is not None:
                self._scraper_monitor_progress_var.set(
                    f"下载进度：{downloaded_rows} / {download_target} ({download_pct:.1f}%)"
                )
            if self._scraper_monitor_progress_bar is not None:
                try:
                    self._scraper_monitor_progress_bar["value"] = download_pct
                except Exception:
                    pass
            if self._scraper_monitor_paths_var is not None:
                self._scraper_monitor_paths_var.set(f"输出: {self._public_scraper_named_dir or output_root}")
            self._refresh_scraper_progress_table(output_root, rows=rows)
        else:
            if self._scraper_monitor_counts_var is not None:
                self._scraper_monitor_counts_var.set(
                    "进度: 总目标 0 / 已发现 0 (0.0%) / 已下载 0 (0.0%) / 已完成 0 / 列表 0 / 详情 0 / 图片 0 / 元数据 0"
                )
            if self._scraper_monitor_progress_var is not None:
                self._scraper_monitor_progress_var.set("下载进度：0 / 0 (0.0%)")
            if self._scraper_monitor_progress_bar is not None:
                try:
                    self._scraper_monitor_progress_bar["value"] = 0.0
                except Exception:
                    pass
            if self._scraper_monitor_paths_var is not None:
                self._scraper_monitor_paths_var.set("输出: -")
            for table in self._iter_scraper_progress_tables():
                try:
                    table.delete(*table.get_children())
                except Exception:
                    pass
            self._update_scraper_progress_group_titles(0, 0)
            self._scraper_monitor_last_progress_snapshot = ""
            self._scraper_monitor_last_opened_path = ""
            self._scraper_monitor_total_hint = 0

        tail = self._read_text_tail(
            self._public_scraper_log_path,
            max_lines=max(30, int(self._scraper_monitor_log_tail_lines or 30)),
        )
        if tail != self._scraper_monitor_last_log_snapshot:
            self._scraper_monitor_last_log_snapshot = tail
            if self._scraper_monitor_log_text is not None:
                try:
                    self._scraper_monitor_log_text.configure(state=tk.NORMAL)
                    self._scraper_monitor_log_text.delete("1.0", tk.END)
                    self._scraper_monitor_log_text.insert("1.0", tail or "暂无日志")
                    self._scraper_monitor_log_text.configure(state=tk.DISABLED)
                    self._scraper_monitor_log_text.see(tk.END)
                except Exception:
                    pass
        self._set_scraper_control_buttons(running=self._is_process_running(self._public_scraper_proc))

    def _build_json_editor(self, parent, title: str, kind: str) -> Dict[str, Any]:
        frame = ttk.Frame(parent)

        bar = ttk.Frame(frame, padding=(0, 0, 0, 6))
        bar.pack(fill=tk.X)
        ttk.Label(bar, text=title).pack(side=tk.LEFT)
        ttk.Button(bar, text="从当前重载", command=lambda: self._reload_raw_editors()).pack(side=tk.RIGHT)
        ttk.Button(bar, text="写入当前文件", command=lambda k=kind: self._apply_raw_editor(k)).pack(side=tk.RIGHT, padx=6)

        body = ttk.Frame(frame)
        body.pack(fill=tk.BOTH, expand=True)

        text = tk.Text(body, wrap=tk.NONE)
        y = ttk.Scrollbar(body, orient=tk.VERTICAL, command=text.yview)
        x = ttk.Scrollbar(body, orient=tk.HORIZONTAL, command=text.xview)
        text.configure(yscrollcommand=y.set, xscrollcommand=x.set)

        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y.pack(side=tk.RIGHT, fill=tk.Y)
        x.pack(side=tk.BOTTOM, fill=tk.X)

        return {"frame": frame, "text": text}

    def _set_status(self, text: str):
        self.status_var.set(str(text or ""))
        self.update_idletasks()

    def _set_llm_progress(self, running: bool, text: str = "") -> None:
        label = self._llm_progress_var
        bar = self._llm_progress_bar
        if label is not None:
            if text:
                label.set(str(text))
            else:
                label.set("AI: 运行中" if running else "AI: 空闲")
        if bar is not None:
            try:
                if running:
                    bar.start(10)
                else:
                    bar.stop()
            except Exception:
                pass

    def _schedule_llm_progress_idle_reset(self, delay_ms: int = 1800) -> None:
        if self._llm_progress_reset_after:
            try:
                self.after_cancel(self._llm_progress_reset_after)
            except Exception:
                pass

        def _reset() -> None:
            self._llm_progress_reset_after = None
            if not self._edit_llm_busy:
                self._set_llm_progress(False, "AI: 空闲")

        self._llm_progress_reset_after = self.after(max(100, int(delay_ms)), _reset)

    def _mark_all_tab_data_dirty(self):
        self._snapshot_dirty = True
        self._all_view_dirty = True
        self._xmp_view_dirty = True
        self._exif_view_dirty = True
        self._iptc_view_dirty = True
        self._png_view_dirty = True
        self._raw_editors_dirty = True

    def _build_all_metadata_map(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        basic = self._last_basic if isinstance(self._last_basic, dict) else {}
        for k, v in basic.items():
            if k == "png_text":
                continue
            out[f"basic.{k}"] = v

        png_text = basic.get("png_text") if isinstance(basic, dict) else {}
        if isinstance(png_text, dict):
            for k, v in png_text.items():
                out[f"png.{k}"] = v

        for k, v in (self._last_xmp or {}).items():
            out[f"xmp.{k}"] = v
        for k, v in (self._last_exif or {}).items():
            out[f"exif.{k}"] = v
        for k, v in (self._last_iptc or {}).items():
            out[f"iptc.{k}"] = v

        info = self._last_info
        if isinstance(info, ImageMetadataInfo):
            out["structured.filepath"] = info.filepath
            out["structured.filename"] = info.filename
            out["structured.filesize"] = info.filesize
            out["structured.modified_time"] = str(info.modified_time)
            out["structured.title"] = info.title
            out["structured.person"] = info.person
            out["structured.gender"] = info.gender
            out["structured.police_id"] = info.police_id
            out["structured.position"] = info.position
            out["structured.city"] = info.city
            out["structured.source"] = info.source
            out["structured.image_url"] = info.image_url
            out["structured.keywords"] = info.keywords
            out["structured.description"] = info.description
            out["structured.titi_asset_id"] = info.titi_asset_id
            out["structured.titi_world_id"] = info.titi_world_id
            out["structured.status"] = str(getattr(info.status, "value", info.status))

        return out

    def _refresh_visible_tab_data(self):
        if not getattr(self, "right_notebook", None):
            return
        tab_id = self.right_notebook.select()
        if not tab_id:
            return
        try:
            active = self.nametowidget(tab_id)
        except Exception:
            return

        if active is self.snapshot_tab:
            if self._snapshot_dirty and isinstance(self._last_info, ImageMetadataInfo):
                self._render_snapshot(self._last_info, self._last_basic or {})
                self._snapshot_dirty = False
            return

        if active is self.all_view:
            if self._all_view_dirty:
                self.all_view.set_data(self._build_all_metadata_map())
                self._all_view_dirty = False
            return

        if active is self.xmp_view:
            if self._xmp_view_dirty:
                self.xmp_view.set_data(self._last_xmp or {})
                self._xmp_view_dirty = False
            return

        if active is self.exif_view:
            if self._exif_view_dirty:
                self.exif_view.set_data(self._last_exif or {})
                self._exif_view_dirty = False
            return

        if active is self.iptc_view:
            if self._iptc_view_dirty:
                self.iptc_view.set_data(self._last_iptc or {})
                self._iptc_view_dirty = False
            return

        if active is self.png_view:
            if self._png_view_dirty:
                png_text = self._last_basic.get("png_text") if isinstance(self._last_basic, dict) else {}
                self.png_view.set_data(png_text if isinstance(png_text, dict) else {})
                self._png_view_dirty = False
            return

        if active is self.adv_tab:
            if self._raw_editors_dirty:
                self._reload_raw_editors()
            return

    def _on_tab_changed(self, _event=None):
        self._refresh_visible_tab_data()

    def _setup_edit_shortcuts_and_menu(self):
        self._ctx_menu = tk.Menu(self, tearoff=False)
        self._ctx_menu.add_command(label="复制", command=lambda: self._apply_edit_action("copy"))
        self._ctx_menu.add_command(label="剪切", command=lambda: self._apply_edit_action("cut"))
        self._ctx_menu.add_command(label="粘贴", command=lambda: self._apply_edit_action("paste"))
        self._ctx_menu.add_separator()
        self._ctx_menu.add_command(label="全选", command=lambda: self._apply_edit_action("select_all"))

        self.bind_all("<Control-c>", lambda e: self._on_edit_shortcut(e, "copy"), add="+")
        self.bind_all("<Control-x>", lambda e: self._on_edit_shortcut(e, "cut"), add="+")
        self.bind_all("<Control-v>", lambda e: self._on_edit_shortcut(e, "paste"), add="+")
        self.bind_all("<Control-a>", lambda e: self._on_edit_shortcut(e, "select_all"), add="+")
        self.bind_all("<Button-3>", self._on_context_menu, add="+")

    def _widget_kind(self, widget: Any) -> str:
        try:
            cls = str(widget.winfo_class() or "").lower()
        except Exception:
            return ""
        if "treeview" in cls:
            return "tree"
        if "text" in cls:
            return "text"
        if ("entry" in cls) or ("spinbox" in cls):
            return "entry"
        return ""

    def _is_editable_text_widget(self, widget: Any) -> bool:
        kind = self._widget_kind(widget)
        return kind in {"entry", "text"}

    def _copy_from_treeview(self, widget: Any) -> bool:
        try:
            selected = widget.selection()
            if not selected:
                return False
            item = widget.item(selected[0])
            key = str(item.get("text", "") or "").strip()
            values = item.get("values", [])
            val = ""
            if isinstance(values, (list, tuple)) and values:
                val = str(values[0] or "").strip()
            text = f"{key}\t{val}".strip()
            if not text:
                return False
            self.clipboard_clear()
            self.clipboard_append(text)
            return True
        except Exception:
            return False

    def _apply_edit_action(self, action: str, widget: Any = None) -> bool:
        w = widget or self._ctx_widget or self.focus_get()
        if w is None:
            return False

        kind = self._widget_kind(w)
        if kind == "tree":
            if action == "copy":
                return self._copy_from_treeview(w)
            return False

        if kind not in {"entry", "text"}:
            return False

        try:
            if action == "copy":
                w.event_generate("<<Copy>>")
                return True
            if action == "cut":
                w.event_generate("<<Cut>>")
                return True
            if action == "paste":
                w.event_generate("<<Paste>>")
                return True
            if action == "select_all":
                if kind == "entry":
                    w.select_range(0, tk.END)
                    w.icursor(tk.END)
                    return True
                if kind == "text":
                    w.tag_add(tk.SEL, "1.0", "end-1c")
                    w.mark_set(tk.INSERT, "end-1c")
                    return True
        except Exception:
            return False
        return False

    def _on_edit_shortcut(self, event: Any, action: str):
        widget = getattr(event, "widget", None)
        kind = self._widget_kind(widget)

        # Entry/Text 走 Tk 默认快捷键，避免与全局绑定叠加导致“粘贴两次”。
        if kind in {"entry", "text"}:
            if action == "select_all":
                if self._apply_edit_action(action, widget=widget):
                    return "break"
            return None

        # Treeview 等非文本控件由我们接管（主要是复制）。
        if self._apply_edit_action(action, widget=widget):
            return "break"
        return None

    def _on_context_menu(self, event: Any):
        widget = getattr(event, "widget", None)
        if widget is None:
            return

        kind = self._widget_kind(widget)
        if kind not in {"entry", "text", "tree"}:
            return

        self._ctx_widget = widget
        try:
            widget.focus_set()
        except Exception:
            pass

        if not self._ctx_menu:
            return

        try:
            self._ctx_menu.entryconfigure("复制", state="normal")
            self._ctx_menu.entryconfigure("全选", state="normal")
            if kind == "tree":
                self._ctx_menu.entryconfigure("剪切", state="disabled")
                self._ctx_menu.entryconfigure("粘贴", state="disabled")
                self._ctx_menu.entryconfigure("全选", state="disabled")
            else:
                self._ctx_menu.entryconfigure("剪切", state="normal")
                self._ctx_menu.entryconfigure("粘贴", state="normal")
            self._ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                self._ctx_menu.grab_release()
            except Exception:
                pass

    def _setup_drag_drop(self, *widgets: Any):
        if (not HAS_TK_DND) or (not DND_FILES):
            return
        for widget in widgets:
            if widget is None:
                continue
            try:
                widget.drop_target_register(DND_FILES)
                widget.dnd_bind("<<Drop>>", self._on_drop)
            except Exception:
                continue

    def _extract_drop_path(self, raw_data: Any) -> Optional[str]:
        text = str(raw_data or "").strip()
        if not text:
            return None

        try:
            candidates = list(self.tk.splitlist(text))
        except Exception:
            candidates = [text]

        for item in candidates:
            path = str(item or "").strip()
            if not path:
                continue
            if path.startswith("{") and path.endswith("}"):
                path = path[1:-1].strip()
            path = path.strip().strip('"')
            if not path:
                continue
            if os.path.exists(path):
                return os.path.abspath(path)
        return None

    def _on_drop(self, event: Any):
        target = self._extract_drop_path(getattr(event, "data", ""))
        if not target:
            self._set_status("拖拽失败：无有效路径")
            return
        self._load_target(target)

    def _browse_image(self):
        filetypes = [
            ("Image Files", "*.jpg *.jpeg *.png *.webp *.bmp *.tif *.tiff"),
            ("All Files", "*.*"),
        ]
        path = filedialog.askopenfilename(title="选择图片", filetypes=filetypes)
        if path:
            self._load_target(path)

    def _browse_folder(self):
        initial = self.current_folder or os.getcwd()
        folder = filedialog.askdirectory(title="选择文件夹", initialdir=initial)
        if folder:
            self._load_target(folder)

    def _load_target(self, target: str):
        target = os.path.abspath(str(target or "").strip().strip('"'))
        if not target:
            return

        if os.path.isdir(target):
            files = _list_images_in_folder(target)
            if not files:
                messagebox.showinfo("提示", "该文件夹没有可识别的图片")
                return
            self.current_folder = target
            self.folder_images = files
            self.current_index = 0
            self._folder_index_ready = True
            self._load_current()
            return

        if not os.path.isfile(target):
            messagebox.showerror("错误", f"路径不存在:\n{target}")
            return

        ext = os.path.splitext(target)[1].lower()
        if ext not in IMAGE_EXTS:
            messagebox.showerror("错误", "不是受支持的图片类型")
            return

        folder = os.path.dirname(target)
        if (
            self._folder_index_ready
            and (folder == self.current_folder)
            and self.folder_images
            and (target in self.folder_images)
        ):
            self.current_index = self.folder_images.index(target)
        else:
            # 拖拽单图时优先快速打开；目录索引在需要上一张/下一张时再懒加载。
            self.current_folder = folder
            self.folder_images = [target]
            self.current_index = 0
            self._folder_index_ready = False

        self._load_current()

    def _load_current(self):
        if (not self.folder_images) or (self.current_index < 0) or (self.current_index >= len(self.folder_images)):
            self.position_var.set("0 / 0")
            return

        path = self.folder_images[self.current_index]
        self.current_path = path
        self.path_var.set(path)
        self.position_var.set(f"{self.current_index + 1} / {len(self.folder_images)}")
        self._set_status("读取中...")

        # Clear stale UI state early to avoid showing previous metadata while loading.
        try:
            for var in (self.edit_vars or {}).values():
                try:
                    var.set("")
                except Exception:
                    pass
            try:
                self.desc_text.delete("1.0", tk.END)
            except Exception:
                pass
            try:
                self._clear_adaptive_profile_rows()
            except Exception:
                pass
            self._last_info = None
            self._last_basic = {}
            self._last_xmp = {}
            self._last_exif = {}
            self._last_iptc = {}
            self._mark_all_tab_data_dirty()
            self._refresh_visible_tab_data()
        except Exception:
            pass

        try:
            self.preview_label.configure(image="", text="(加载中...)")
        except Exception:
            pass

        self._load_current_token = int(self._load_current_token or 0) + 1
        token = self._load_current_token

        import threading

        def _worker(target_path: str, my_token: int) -> None:
            # Phase 1: preview (fastest) so user sees the image even if metadata is slow/hangs.
            preview_pil, preview_err = _svc_load_preview_image(target_path)

            def _apply_preview() -> None:
                if int(getattr(self, "_load_current_token", 0)) != my_token:
                    return
                if os.path.abspath(str(getattr(self, "current_path", "") or "")) != os.path.abspath(target_path):
                    return
                if preview_pil is not None:
                    self._preview_pil = preview_pil
                    self._refresh_preview_image()
                    return
                self._preview_pil = None
                self._preview_tk = None
                try:
                    self.preview_label.configure(image="", text="(无法预览)")
                except Exception:
                    pass

            try:
                self.after(0, _apply_preview)
            except Exception:
                _apply_preview()

            # Phase 2: metadata (may be slow on network/CJK paths).
            (
                basic,
                info,
                raw_xmp,
                raw_exif,
                raw_iptc,
                err,
            ) = _svc_load_metadata_snapshot(
                target_path,
                has_pyexiv2=HAS_PYEXIV2,
                read_basic_info_fn=_read_image_basic_info,
                read_image_metadata_fn=read_image_metadata,
                read_raw_with_pyexiv2_fn=_read_raw_with_pyexiv2,
            )

            def _apply_metadata() -> None:
                if int(getattr(self, "_load_current_token", 0)) != my_token:
                    return
                if os.path.abspath(str(getattr(self, "current_path", "") or "")) != os.path.abspath(target_path):
                    return
                if err or (info is None):
                    self._set_status("读取失败")
                    detail = err or preview_err or "未知错误"
                    messagebox.showerror("读取失败", detail, parent=self)
                    return

                self._last_info = info
                self._last_basic = basic
                self._last_xmp = raw_xmp or {}
                self._last_exif = raw_exif or {}
                self._last_iptc = raw_iptc or {}

                try:
                    self._fill_edit_form(info)
                except Exception:
                    pass
                self._mark_all_tab_data_dirty()
                self._refresh_visible_tab_data()
                self._set_status("完成")

            try:
                self.after(0, _apply_metadata)
            except Exception:
                _apply_metadata()

        threading.Thread(target=_worker, args=(path, token), daemon=True).start()

    def _refresh_current(self):
        if self.current_path:
            self._load_current()

    def _refresh_metadata_only(self):
        if not self.current_path:
            return
        try:
            info = read_image_metadata(self.current_path)
            self._last_info = info
            self._last_xmp = dict(getattr(info, "other_xmp", {}) or {})
            self._last_exif = dict(getattr(info, "other_exif", {}) or {})
            self._last_iptc = dict(getattr(info, "other_iptc", {}) or {})
            self._mark_all_tab_data_dirty()
            self._refresh_visible_tab_data()
        except Exception:
            # 保存成功后即使元数据重读失败，也不影响当前编辑内容。
            pass

    def _ensure_folder_index(self):
        if self._folder_index_ready:
            return
        if not self.current_path:
            return
        folder = os.path.dirname(self.current_path)
        if not folder or (not os.path.isdir(folder)):
            return

        files = _list_images_in_folder(folder)
        if not files:
            self.current_folder = folder
            self.folder_images = [self.current_path]
            self.current_index = 0
            self._folder_index_ready = False
            self.position_var.set("1 / 1")
            return

        self.current_folder = folder
        self.folder_images = files
        self._folder_index_ready = True
        try:
            self.current_index = self.folder_images.index(self.current_path)
        except ValueError:
            self.folder_images.insert(0, self.current_path)
            self.current_index = 0
        self.position_var.set(f"{self.current_index + 1} / {len(self.folder_images)}")

    def _goto_prev(self):
        self._ensure_folder_index()
        if not self.folder_images:
            return
        self.current_index = max(0, self.current_index - 1)
        self._load_current()

    def _goto_next(self):
        self._ensure_folder_index()
        if not self.folder_images:
            return
        self.current_index = min(len(self.folder_images) - 1, self.current_index + 1)
        self._load_current()

    def _open_current_file(self):
        if self.current_path and os.path.isfile(self.current_path):
            os.startfile(self.current_path)

    def _open_current_folder(self):
        if self.current_path and os.path.isfile(self.current_path):
            os.startfile(os.path.dirname(self.current_path))

    def _open_batch_downloader(self):
        script_path = os.path.join(os.path.dirname(__file__), "legacy_downloader_gui.py")
        if not os.path.exists(script_path):
            messagebox.showerror("启动失败", f"未找到批量下载器脚本:\n{script_path}")
            return
        try:
            subprocess.Popen(
                [sys.executable, script_path],
                cwd=os.path.dirname(script_path) or ".",
            )
            self._set_status("已启动批量下载器（旧版）")
        except Exception as e:
            messagebox.showerror("启动失败", f"无法启动批量下载器：\n{e}")

    @staticmethod
    def _resolve_python_cli_executable() -> str:
        return _svc_resolve_python_cli_executable()

    @staticmethod
    def _build_utf8_subprocess_env() -> Dict[str, str]:
        return _svc_build_utf8_subprocess_env()

    @staticmethod
    def _app_settings_path() -> str:
        return _svc_app_settings_path()

    @staticmethod
    def _default_app_settings() -> Dict[str, Any]:
        return _svc_default_app_settings()

    def _load_app_settings(self) -> Dict[str, Any]:
        return _svc_load_app_settings(self._app_settings_path())

    def _save_app_settings(self, payload: Dict[str, Any]) -> bool:
        data = payload if isinstance(payload, dict) else {}
        return _svc_save_app_settings(data, self._app_settings_path())

    def _get_global_llm_settings(self) -> Dict[str, Any]:
        llm = self._app_settings.get("llm") if isinstance(self._app_settings, dict) else {}
        return dict(llm) if isinstance(llm, dict) else {}

    @staticmethod
    def _apply_llm_env(
        env: Dict[str, str],
        *,
        api_base: str = "",
        api_key: str = "",
        model: str = "",
    ) -> Dict[str, str]:
        if not isinstance(env, dict):
            env = dict(os.environ)
        base = str(api_base or "").strip()
        key = str(api_key or "").strip()
        mdl = str(model or "").strip()
        if base:
            env["D2I_LLM_API_BASE"] = base
        if key:
            env["D2I_LLM_API_KEY"] = key
        if mdl:
            env["D2I_LLM_MODEL"] = mdl
        return env

    def _open_global_settings_dialog(self):
        existing = self._global_settings_window
        if existing is not None:
            try:
                if existing.winfo_exists():
                    existing.lift()
                    existing.focus_force()
                    return
            except Exception:
                pass

        dialog = tk.Toplevel(self)
        self._global_settings_window = dialog
        dialog.title("全局设置")
        dialog.transient(self)
        dialog.resizable(False, False)

        container = ttk.Frame(dialog, padding=12)
        container.pack(fill=tk.BOTH, expand=True)

        llm_cfg = self._get_global_llm_settings()

        enable_var = tk.BooleanVar(value=bool(llm_cfg.get("enabled_default", False)))
        api_base_var = tk.StringVar(value=str(llm_cfg.get("api_base", "")).strip())
        api_key_var = tk.StringVar(value=str(llm_cfg.get("api_key", "")).strip())
        model_var = tk.StringVar(value=str(llm_cfg.get("model", "")).strip())
        timeout_var = tk.StringVar(value=str(llm_cfg.get("timeout_seconds", 45)))
        retries_var = tk.StringVar(value=str(llm_cfg.get("max_retries", 2)))
        temp_var = tk.StringVar(value=str(llm_cfg.get("temperature", 0.1)))
        status_var = tk.StringVar(value=f"配置文件：{self._app_settings_path()}")

        box = ttk.Labelframe(container, text="在线大模型（OpenAI 兼容接口）", padding=10)
        box.pack(fill=tk.BOTH, expand=True)
        box.columnconfigure(1, weight=1)
        box.columnconfigure(3, weight=1)

        ttk.Checkbutton(box, text="默认启用 LLM 语义补全/小传", variable=enable_var).grid(
            row=0, column=0, columnspan=4, sticky="w"
        )

        ttk.Label(box, text="API Base:").grid(row=1, column=0, sticky="e", pady=(8, 0))
        ttk.Entry(box, textvariable=api_base_var, width=48).grid(row=1, column=1, sticky="we", pady=(8, 0), padx=(6, 18))

        ttk.Label(box, text="Model:").grid(row=1, column=2, sticky="e", pady=(8, 0))
        model_combo = ttk.Combobox(box, textvariable=model_var, width=34, values=())
        model_combo.grid(row=1, column=3, sticky="we", pady=(8, 0), padx=(6, 0))

        ttk.Label(box, text="API Key:").grid(row=2, column=0, sticky="e", pady=(6, 0))
        ttk.Entry(box, textvariable=api_key_var, width=48, show="*").grid(
            row=2, column=1, sticky="we", pady=(6, 0), padx=(6, 18)
        )

        ttk.Label(box, text="Timeout(s):").grid(row=2, column=2, sticky="e", pady=(6, 0))
        ttk.Entry(box, textvariable=timeout_var, width=8).grid(row=2, column=3, sticky="w", pady=(6, 0), padx=(6, 0))

        ttk.Label(box, text="Retries:").grid(row=3, column=2, sticky="e", pady=(6, 0))
        ttk.Entry(box, textvariable=retries_var, width=8).grid(row=3, column=3, sticky="w", pady=(6, 0), padx=(6, 0))

        ttk.Label(box, text="Temp:").grid(row=3, column=0, sticky="e", pady=(6, 0))
        ttk.Entry(box, textvariable=temp_var, width=8).grid(row=3, column=1, sticky="w", pady=(6, 0), padx=(6, 0))

        btns = ttk.Frame(box)
        btns.grid(row=4, column=0, columnspan=4, sticky="w", pady=(10, 0))

        def _collect_llm_config() -> Dict[str, Any]:
            api_base = str(api_base_var.get() or "").strip()
            if HAS_LLM_CLIENT and callable(normalize_api_base):
                try:
                    api_base = normalize_api_base(api_base)
                except Exception:
                    api_base = api_base.rstrip("/")
            else:
                api_base = api_base.rstrip("/")
            return {
                "enabled_default": bool(enable_var.get()),
                "api_base": api_base,
                "api_key": str(api_key_var.get() or "").strip(),
                "model": str(model_var.get() or "").strip(),
                "timeout_seconds": str(timeout_var.get() or "").strip(),
                "max_retries": str(retries_var.get() or "").strip(),
                "temperature": str(temp_var.get() or "").strip(),
            }

        def _fetch_models():
            if not HAS_LLM_CLIENT or OpenAICompatibleClient is None:
                messagebox.showerror("不可用", "当前环境缺少 LLM 客户端依赖（requests）。", parent=dialog)
                return
            cfg = _collect_llm_config()
            if not cfg.get("api_base"):
                messagebox.showerror("参数错误", "请先填写 API Base。", parent=dialog)
                return
            status_var.set("正在拉取模型列表...")
            try:
                box.update_idletasks()
            except Exception:
                pass

            import threading

            def _worker():
                err = ""
                models: List[str] = []
                try:
                    client = OpenAICompatibleClient(
                        api_base=str(cfg.get("api_base", "")),
                        api_key=str(cfg.get("api_key", "")),
                        timeout_seconds=int(cfg.get("timeout_seconds") or 45),
                        max_retries=int(cfg.get("max_retries") or 2),
                    )
                    models = client.list_models()
                except Exception as exc:
                    err = str(exc)

                def _done():
                    if err:
                        status_var.set(f"拉取失败: {err}")
                        messagebox.showerror("拉取失败", f"无法拉取模型列表：\n{err}", parent=dialog)
                        return
                    model_combo.configure(values=tuple(models))
                    status_var.set(f"已拉取 {len(models)} 个模型")
                    if models and (not str(model_var.get() or "").strip()):
                        model_var.set(models[0])

                try:
                    self.after(0, _done)
                except Exception:
                    _done()

            threading.Thread(target=_worker, daemon=True).start()

        def _test_connection():
            if not HAS_LLM_CLIENT or OpenAICompatibleClient is None:
                messagebox.showerror("不可用", "当前环境缺少 LLM 客户端依赖（requests）。", parent=dialog)
                return
            cfg = _collect_llm_config()
            if not cfg.get("api_base"):
                messagebox.showerror("参数错误", "请先填写 API Base。", parent=dialog)
                return
            model = str(cfg.get("model", "")).strip()
            if not model:
                messagebox.showerror("参数错误", "请先选择/填写 Model。", parent=dialog)
                return
            status_var.set("正在测试连接...")

            import threading

            def _worker():
                err = ""
                content = ""
                try:
                    client = OpenAICompatibleClient(
                        api_base=str(cfg.get("api_base", "")),
                        api_key=str(cfg.get("api_key", "")),
                        timeout_seconds=int(cfg.get("timeout_seconds") or 45),
                        max_retries=int(cfg.get("max_retries") or 2),
                    )
                    resp = client.chat_completions(
                        model=model,
                        temperature=0.0,
                        stream=False,
                        messages=[
                            {"role": "system", "content": "只输出 JSON，不要任何额外文字。"},
                            {"role": "user", "content": "输出：{\"ok\":true}"},
                        ],
                    )
                    content = client.extract_first_message_content(resp)
                except Exception as exc:
                    err = str(exc)

                def _done():
                    if err:
                        status_var.set(f"测试失败: {err}")
                        messagebox.showerror("测试失败", f"连接/调用失败：\n{err}", parent=dialog)
                        return
                    preview = (content or "").strip()
                    if len(preview) > 200:
                        preview = preview[:197] + "..."
                    status_var.set("测试成功")
                    messagebox.showinfo("测试成功", f"模型可用。\n\n返回内容预览：\n{preview}", parent=dialog)

                try:
                    self.after(0, _done)
                except Exception:
                    _done()

            threading.Thread(target=_worker, daemon=True).start()

        ttk.Button(btns, text="拉取模型列表", command=_fetch_models).pack(side=tk.LEFT)
        ttk.Button(btns, text="测试连接", command=_test_connection).pack(side=tk.LEFT, padx=6)

        tip = ttk.Label(box, textvariable=status_var, foreground="#666666")
        tip.grid(row=5, column=0, columnspan=4, sticky="w", pady=(10, 0))

        actions = ttk.Frame(container)
        actions.pack(fill=tk.X, pady=(10, 0))

        def _save():
            cfg = _collect_llm_config()
            try:
                cfg["timeout_seconds"] = max(5, int(cfg.get("timeout_seconds") or 45))
                cfg["max_retries"] = max(1, int(cfg.get("max_retries") or 2))
                cfg["temperature"] = float(cfg.get("temperature") or 0.1)
            except Exception:
                messagebox.showerror("参数错误", "Timeout / Retries / Temp 必须是合法数字。", parent=dialog)
                return

            payload = dict(self._app_settings or {})
            payload.setdefault("version", 1)
            payload["llm"] = cfg
            if not self._save_app_settings(payload):
                messagebox.showerror("保存失败", "无法写入全局设置文件。", parent=dialog)
                return
            self._app_settings = payload
            status_var.set("已保存")
            self._set_status("全局设置已保存")

        ttk.Button(actions, text="取消", width=10, command=lambda: dialog.destroy()).pack(side=tk.RIGHT)
        ttk.Button(actions, text="保存", width=10, command=_save).pack(side=tk.RIGHT, padx=(0, 8))

        def _on_close():
            try:
                dialog.destroy()
            finally:
                self._global_settings_window = None

        dialog.protocol("WM_DELETE_WINDOW", _on_close)
        dialog.bind("<Escape>", lambda _e: _on_close())

        dialog.update_idletasks()
        x = self.winfo_rootx() + max((self.winfo_width() - dialog.winfo_reqwidth()) // 2, 0)
        y = self.winfo_rooty() + max((self.winfo_height() - dialog.winfo_reqheight()) // 3, 0)
        dialog.geometry(f"+{x}+{y}")
        dialog.lift()

    @staticmethod
    def _normalize_public_task_root(output_root: str) -> str:
        return _svc_normalize_public_task_root(output_root)

    @staticmethod
    def _is_process_running(proc: Any) -> bool:
        return _svc_is_process_running(proc)

    def _is_any_public_scraper_running(self) -> bool:
        for entry in self._public_scraper_tasks.values():
            if self._is_process_running(entry.get("proc")):
                return True
        return False

    def _close_public_scraper_log_handle(self, handle: Optional[Any] = None):
        target = handle if handle is not None else self._public_scraper_log_handle
        if handle is None:
            self._public_scraper_log_handle = None
        if not target:
            return
        try:
            target.close()
        except Exception:
            pass

    def _sync_active_task_to_registry(self):
        root = self._normalize_public_task_root(self._public_scraper_active_task_root or self._public_scraper_output_root)
        if not root:
            return
        entry = self._public_scraper_tasks.get(root)
        if not isinstance(entry, dict):
            return
        entry["proc"] = self._public_scraper_proc
        entry["output_root"] = root
        entry["named_dir"] = self._public_scraper_named_dir
        entry["config_path"] = self._public_scraper_config_path
        entry["log_path"] = self._public_scraper_log_path
        entry["log_handle"] = self._public_scraper_log_handle
        entry["last_progress_text"] = self._public_scraper_last_progress_text
        entry["started_at"] = self._public_scraper_started_at
        entry["runtime_state"] = self._public_scraper_runtime_state
        entry["manual_paused"] = bool(self._public_scraper_manual_paused)
        entry["active_template_path"] = self._public_scraper_active_template_path

    def _set_active_public_scraper_task(self, output_root: str, *, refresh: bool = True):
        self._sync_active_task_to_registry()
        root = self._normalize_public_task_root(output_root)
        self._public_scraper_active_task_root = root

        if not root:
            self._public_scraper_proc = None
            self._public_scraper_output_root = ""
            self._public_scraper_named_dir = ""
            self._public_scraper_config_path = ""
            self._public_scraper_log_path = ""
            self._public_scraper_log_handle = None
            self._public_scraper_last_progress_text = ""
            self._public_scraper_started_at = None
            self._public_scraper_runtime_state = "空闲"
            self._public_scraper_manual_paused = False
            self._public_scraper_active_template_path = ""
        else:
            entry = self._public_scraper_tasks.get(root)
            if isinstance(entry, dict):
                self._reconcile_task_entry_runtime_state(root, entry)
                self._public_scraper_proc = entry.get("proc")
                self._public_scraper_output_root = root
                self._public_scraper_named_dir = str(entry.get("named_dir", "")).strip() or root
                self._public_scraper_config_path = str(entry.get("config_path", "")).strip()
                self._public_scraper_log_path = str(entry.get("log_path", "")).strip()
                self._public_scraper_log_handle = entry.get("log_handle")
                self._public_scraper_last_progress_text = str(entry.get("last_progress_text", "")).strip()
                self._public_scraper_started_at = entry.get("started_at")
                self._public_scraper_runtime_state = str(entry.get("runtime_state", "")).strip() or "任务浏览"
                self._public_scraper_manual_paused = bool(entry.get("manual_paused", False))
                self._public_scraper_active_template_path = str(entry.get("active_template_path", "")).strip()
            else:
                self._public_scraper_proc = None
                self._public_scraper_output_root = root
                self._public_scraper_named_dir = root
                self._public_scraper_config_path = os.path.join(root, "state", "runtime_config.json")
                self._public_scraper_log_path = os.path.join(root, "reports", "gui_public_scraper.log")
                self._public_scraper_log_handle = None
                self._public_scraper_last_progress_text = ""
                self._public_scraper_started_at = None
                self._public_scraper_runtime_state = "任务浏览"
                self._public_scraper_manual_paused = os.path.exists(self._public_scraper_pause_flag_path(root))
                self._public_scraper_active_template_path = ""

        running = self._is_process_running(self._public_scraper_proc)
        self._set_scraper_control_buttons(running=running)
        if refresh:
            self._refresh_scraper_monitor_panel()
            self._refresh_public_task_manager_list()

    def _register_public_scraper_task(
        self,
        *,
        output_root: str,
        proc: Any,
        named_dir: str,
        config_path: str,
        log_path: str,
        log_handle: Any,
        runtime_state: str,
        active_template_path: str,
    ) -> None:
        root = self._normalize_public_task_root(output_root)
        if not root:
            return
        old_entry = self._public_scraper_tasks.get(root)
        if isinstance(old_entry, dict):
            old_handle = old_entry.get("log_handle")
            if old_handle is not None and old_handle is not log_handle:
                self._close_public_scraper_log_handle(old_handle)
        self._public_scraper_tasks[root] = {
            "proc": proc,
            "output_root": root,
            "named_dir": str(named_dir or "").strip() or root,
            "config_path": str(config_path or "").strip(),
            "log_path": str(log_path or "").strip(),
            "log_handle": log_handle,
            "last_progress_text": "",
            "started_at": time.time(),
            "runtime_state": str(runtime_state or "").strip() or "运行中",
            "manual_paused": False,
            "active_template_path": str(active_template_path or "").strip(),
            "last_exit_code": None,
            "updated_at_ts": time.time(),
        }
        self._set_active_public_scraper_task(root, refresh=False)
        self._refresh_scraper_monitor_panel()
        self._refresh_public_task_manager_list()
        self._schedule_public_scraper_poll()

    def _task_entry_status_text(self, entry: Dict[str, Any]) -> str:
        return _svc_task_entry_status_text(entry, is_process_running_fn=self._is_process_running)

    def _reconcile_task_entry_runtime_state(self, root: str, entry: Dict[str, Any]) -> None:
        _ = root
        _svc_reconcile_task_entry_runtime_state(entry, is_process_running_fn=self._is_process_running)

    def _refresh_scraper_task_list_view(self):
        tree = self._scraper_task_tree
        status_var = self._scraper_task_status_var
        if tree is None:
            return
        selected_root = ""
        try:
            selected = tree.selection()
            if selected:
                values = tuple(tree.item(selected[0], "values") or ())
                if len(values) >= 4:
                    selected_root = self._normalize_public_task_root(values[3])
        except Exception:
            selected_root = ""

        active_root = self._normalize_public_task_root(self._public_scraper_active_task_root or self._public_scraper_output_root)
        rows, running_count = _svc_build_scraper_task_view_rows(
            self._public_scraper_tasks,
            active_root=active_root,
            active_entry_if_missing={
                "proc": self._public_scraper_proc,
                "runtime_state": self._public_scraper_runtime_state,
                "manual_paused": bool(self._public_scraper_manual_paused),
                "output_root": active_root,
            },
            normalize_root_fn=self._normalize_public_task_root,
            is_process_running_fn=self._is_process_running,
            reconcile_entry_fn=lambda e: self._reconcile_task_entry_runtime_state(str(e.get("output_root", "")), e),
            status_text_fn=self._task_entry_status_text,
        )

        try:
            tree.delete(*tree.get_children())
        except Exception:
            pass

        root_to_item: Dict[str, str] = {}
        for row in rows:
            root = str(row.get("root", "")).strip()
            item_id = tree.insert(
                "",
                tk.END,
                values=(
                    str(row.get("status_text", "")),
                    str(row.get("pid_text", "")),
                    str(row.get("task_name", "")),
                    root,
                ),
            )
            root_to_item[root] = item_id

        preferred_root = active_root or selected_root
        if preferred_root and preferred_root in root_to_item:
            try:
                tree.selection_set(root_to_item[preferred_root])
                tree.focus(root_to_item[preferred_root])
            except Exception:
                pass

        if status_var is not None:
            try:
                status_var.set(f"会话任务: {len(rows)}（运行中: {running_count}）")
            except Exception:
                pass

    def _on_scraper_task_selected(self, _event=None):
        tree = self._scraper_task_tree
        if tree is None:
            return
        try:
            selected = tree.selection()
            if not selected:
                return
            values = tuple(tree.item(selected[0], "values") or ())
            root = _svc_parse_task_root_from_values(
                values,
                root_index=3,
                normalize_root_fn=self._normalize_public_task_root,
            )
            if not root:
                return
            current = self._normalize_public_task_root(self._public_scraper_active_task_root or self._public_scraper_output_root)
            if current == root:
                return
            self._set_active_public_scraper_task(root, refresh=True)
        except Exception:
            return

    @staticmethod
    def _public_scraper_pause_flag_path(output_root: str) -> str:
        return _svc_public_scraper_pause_flag_path(output_root)

    def _set_public_scraper_manual_pause_flag(self, output_root: str, paused: bool) -> bool:
        ok = _svc_set_public_scraper_manual_pause_flag(output_root, paused)
        if not ok:
            return False
        root = self._normalize_public_task_root(output_root)
        entry = self._public_scraper_tasks.get(root)
        if isinstance(entry, dict):
            entry["manual_paused"] = bool(paused)
        return True

    def _clear_public_scraper_manual_pause_flag(self):
        output_root = str(self._public_scraper_output_root or "").strip()
        if output_root:
            self._set_public_scraper_manual_pause_flag(output_root, paused=False)
            entry = self._public_scraper_tasks.get(self._normalize_public_task_root(output_root))
            if isinstance(entry, dict):
                entry["manual_paused"] = False
        self._public_scraper_manual_paused = False

    def _count_jsonl_rows(self, path: str) -> int:
        return _svc_count_jsonl_rows(path, self._jsonl_count_cache)

    def _update_public_scraper_progress(self):
        output_root = self._public_scraper_output_root
        if not output_root:
            self._refresh_scraper_monitor_panel()
            return
        rows = self._collect_scraper_progress_rows(output_root)
        summary = _svc_summarize_scraper_progress_rows(
            rows,
            is_row_completed_fn=self._is_scraper_row_completed,
            is_row_image_downloaded_fn=self._is_scraper_row_image_downloaded,
        )
        completed_rows = int(summary.get("completed_rows", 0))
        downloaded_rows = int(summary.get("downloaded_rows", 0))
        discovered_rows = int(summary.get("discovered_rows", 0))
        total_target = max(len(rows), self._estimate_scraper_total_target(output_root), self._scraper_monitor_total_hint)
        self._scraper_monitor_total_hint = total_target
        list_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "list_records.jsonl"))
        profile_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "profiles.jsonl"))
        image_rows = self._count_jsonl_rows(os.path.join(output_root, "downloads", "image_downloads.jsonl"))
        metadata_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "metadata_write_results.jsonl"))
        text = _svc_build_public_scraper_progress_text(
            discovered_rows=discovered_rows,
            downloaded_rows=downloaded_rows,
            completed_rows=completed_rows,
            total_target=total_target,
            list_rows=list_rows,
            profile_rows=profile_rows,
            image_rows=image_rows,
            metadata_rows=metadata_rows,
        )
        if text != self._public_scraper_last_progress_text:
            self._public_scraper_last_progress_text = text
            if not self._public_scraper_manual_paused:
                self._set_status(text)
        self._sync_active_task_to_registry()
        self._refresh_scraper_monitor_panel()

    def _suggest_public_scraper_output_root(self, start_url: str) -> str:
        site_name = _svc_guess_public_site_name(start_url)
        return _svc_suggest_public_scraper_output_root(__file__, site_name)

    def _public_scraper_templates_dir(self) -> str:
        return _svc_public_scraper_templates_dir(__file__)

    def _public_scraper_template_state_path(self) -> str:
        return _svc_public_scraper_template_state_path(__file__)

    def _load_public_scraper_template_states(self) -> Dict[str, Dict[str, str]]:
        return _svc_load_public_scraper_template_states(__file__)

    def _save_public_scraper_template_states(self, states: Dict[str, Dict[str, str]]) -> None:
        _svc_save_public_scraper_template_states(__file__, states)

    def _set_public_scraper_template_state(self, template_path: str, status: str) -> None:
        _svc_set_public_scraper_template_state(__file__, template_path, status)

    def _list_public_scraper_templates(self) -> List[Tuple[str, str]]:
        return _svc_list_public_scraper_templates(__file__)

    def _show_public_scraper_setup_dialog(self, source_hint: str) -> Optional[Dict[str, Any]]:
        global_llm = self._get_global_llm_settings()
        initial_state = _svc_build_public_scraper_setup_initial_state(
            source_hint,
            __file__,
            global_llm_settings=global_llm,
        )

        dialog = tk.Toplevel(self)
        dialog.title("公共抓取(通用) 设置")
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)

        container = ttk.Frame(dialog, padding=12)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(1, weight=1)

        url_var = tk.StringVar(value=str(initial_state.get("start_url", "https://")))
        output_var = tk.StringVar(value=str(initial_state.get("output_root", "")))
        interval_min_var = tk.StringVar(value=str(initial_state.get("interval_min", "5.0")))
        interval_max_var = tk.StringVar(value=str(initial_state.get("interval_max", "8.0")))
        timeout_var = tk.StringVar(value=str(initial_state.get("timeout_seconds", "30")))
        suspect_failures_var = tk.StringVar(value=str(initial_state.get("suspect_block_consecutive_failures", "3")))
        jsl_var = tk.BooleanVar(value=bool(initial_state.get("jsl_enabled", True)))
        image_mode_var = tk.StringVar(value=str(initial_state.get("image_download_mode", "requests_jsl")))
        auto_fallback_var = tk.BooleanVar(value=bool(initial_state.get("auto_fallback_to_browser", True)))
        disable_page_images_var = tk.BooleanVar(value=bool(initial_state.get("disable_page_images_during_crawl", True)))
        output_minimal_var = tk.BooleanVar(value=bool(initial_state.get("output_minimal", True)))
        direct_write_images_var = tk.BooleanVar(value=bool(initial_state.get("direct_write_images", True)))
        llm_enable_var = tk.BooleanVar(value=bool(initial_state.get("llm_enrich_enabled", False)))
        llm_model_var = tk.StringVar(value=str(initial_state.get("llm_model", "")))
        llm_api_base_var = tk.StringVar(value=str(initial_state.get("llm_api_base", "")))
        llm_api_key_var = tk.StringVar(value=str(initial_state.get("llm_api_key", "")))
        template_pairs = self._list_public_scraper_templates()
        template_auto_label = "自动生成模板（按当前链接）"
        template_label_to_path: Dict[str, str] = {template_auto_label: ""}
        for label, path in template_pairs:
            template_label_to_path[label] = path
        template_var = tk.StringVar(value=template_auto_label)
        save_template_var = tk.BooleanVar(value=bool(initial_state.get("save_generated_template", True)))
        cleanup_template_var = tk.BooleanVar(value=bool(initial_state.get("cleanup_generated_template", True)))
        template_hint_var = tk.StringVar(value=str(initial_state.get("template_hint", "未选择模板时，需手动输入链接。")))
        template_start_url_cache: Dict[str, str] = {"url": str(initial_state.get("template_start_url", ""))}

        ttk.Label(container, text="列表页链接").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        url_entry = ttk.Entry(container, textvariable=url_var, width=80)
        url_entry.grid(row=0, column=1, columnspan=2, sticky="ew", pady=(0, 8))

        ttk.Label(container, text="模板").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        template_combo = ttk.Combobox(
            container,
            textvariable=template_var,
            values=list(template_label_to_path.keys()),
            state="readonly",
            width=78,
        )
        template_combo.grid(row=1, column=1, sticky="ew", pady=(0, 8))

        def _apply_template_to_form():
            selected_path = str(template_label_to_path.get(template_var.get(), "")).strip()
            patch = _svc_build_public_scraper_template_selection_state(
                selected_path,
                __file__,
                global_llm_settings=global_llm,
            )
            template_start_url_cache["url"] = str(patch.get("template_start_url", "")).strip()
            if "start_url" in patch:
                url_var.set(str(patch.get("start_url", "")))
            url_entry.configure(state=(tk.DISABLED if patch.get("url_locked") else tk.NORMAL))
            if "hint" in patch:
                template_hint_var.set(str(patch.get("hint", "")))
            if "interval_min" in patch:
                interval_min_var.set(str(patch.get("interval_min", "")))
            if "interval_max" in patch:
                interval_max_var.set(str(patch.get("interval_max", "")))
            if "timeout_seconds" in patch:
                timeout_var.set(str(patch.get("timeout_seconds", "")))
            if "suspect_block_consecutive_failures" in patch:
                suspect_failures_var.set(str(patch.get("suspect_block_consecutive_failures", "")))
            if "jsl_enabled" in patch:
                jsl_var.set(bool(patch.get("jsl_enabled", False)))
            if "image_download_mode" in patch:
                image_mode_var.set(str(patch.get("image_download_mode", "requests_jsl")))
            if "auto_fallback_to_browser" in patch:
                auto_fallback_var.set(bool(patch.get("auto_fallback_to_browser", True)))
            if "disable_page_images_during_crawl" in patch:
                disable_page_images_var.set(bool(patch.get("disable_page_images_during_crawl", True)))
            if "llm_enrich_enabled" in patch:
                llm_enable_var.set(bool(patch.get("llm_enrich_enabled", False)))
            if "llm_model" in patch:
                llm_model_var.set(str(patch.get("llm_model", "")))
            if "llm_api_base" in patch:
                llm_api_base_var.set(str(patch.get("llm_api_base", "")))
            if "llm_api_key" in patch:
                llm_api_key_var.set(str(patch.get("llm_api_key", "")))
            if "output_minimal" in patch:
                output_minimal_var.set(bool(patch.get("output_minimal", True)))
            if "direct_write_images" in patch:
                direct_write_images_var.set(bool(patch.get("direct_write_images", True)))
            if "output_root" in patch:
                output_var.set(str(patch.get("output_root", "")))
            if "save_generated_template" in patch:
                save_template_var.set(bool(patch.get("save_generated_template", True)))
            if "cleanup_generated_template" in patch:
                cleanup_template_var.set(bool(patch.get("cleanup_generated_template", True)))
            try:
                state = tk.DISABLED if patch.get("disable_template_persistence_controls") else tk.NORMAL
                save_tpl_cb.configure(state=state)
                cleanup_tpl_cb.configure(state=state)
            except Exception:
                pass

        def _refresh_templates():
            current_selected_path = str(template_label_to_path.get(template_var.get(), "")).strip()
            pairs = self._list_public_scraper_templates()
            mapping: Dict[str, str] = {template_auto_label: ""}
            for label, path in pairs:
                mapping[label] = path
            template_label_to_path.clear()
            template_label_to_path.update(mapping)
            template_combo.configure(values=list(template_label_to_path.keys()))
            selected_label = ""
            if current_selected_path:
                for label, path in template_label_to_path.items():
                    if os.path.abspath(str(path or "").strip()) == os.path.abspath(current_selected_path):
                        selected_label = label
                        break
            if selected_label:
                template_var.set(selected_label)
            elif template_var.get() not in template_label_to_path:
                template_var.set(template_auto_label)
            _apply_template_to_form()

        ttk.Button(container, text="刷新", command=_refresh_templates, width=10).grid(
            row=1, column=2, sticky="e", padx=(8, 0), pady=(0, 8)
        )
        ttk.Label(container, textvariable=template_hint_var, bootstyle="secondary").grid(
            row=2, column=1, columnspan=2, sticky="w", pady=(0, 8)
        )

        ttk.Label(container, text="输出目录").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        output_entry = ttk.Entry(container, textvariable=output_var, width=80)
        output_entry.grid(row=3, column=1, sticky="ew", pady=(0, 8))

        def _browse_output():
            selected = filedialog.askdirectory(
                parent=dialog,
                title="选择输出文件夹（最终图片和记录文件会放在该目录）",
                initialdir=output_var.get().strip() or self._suggest_public_scraper_output_root(url_var.get().strip() or "https://"),
                mustexist=False,
            )
            if selected:
                output_var.set(os.path.abspath(selected))
                dialog.lift()
                dialog.focus_force()

        browse_btn = ttk.Button(container, text="浏览...", command=_browse_output, width=10)
        browse_btn.grid(row=3, column=2, sticky="e", padx=(8, 0), pady=(0, 8))

        def _fill_output_by_url():
            output_var.set(self._suggest_public_scraper_output_root(url_var.get().strip() or "https://"))

        fill_output_btn = ttk.Button(container, text="按链接填充默认目录", command=_fill_output_by_url)
        fill_output_btn.grid(row=4, column=1, sticky="w", pady=(0, 10))
        ttk.Label(container, text="最终图片与 crawl_record.json 都输出到上方选择的目录（按姓名命名）").grid(
            row=5, column=0, columnspan=3, sticky="w", pady=(0, 4)
        )
        save_tpl_cb = ttk.Checkbutton(container, text="保存本次生成的模板（供下次选择）", variable=save_template_var)
        save_tpl_cb.grid(row=6, column=1, sticky="w", pady=(0, 2))
        cleanup_tpl_cb = ttk.Checkbutton(
            container,
            text="完成后清理本次生成模板（回溯已写入记录文档）",
            variable=cleanup_template_var,
        )
        cleanup_tpl_cb.grid(row=7, column=1, sticky="w", pady=(0, 10))

        opts = ttk.Labelframe(container, text="抓取参数", padding=10)
        opts.grid(row=8, column=0, columnspan=3, sticky="ew")
        opts.columnconfigure(1, weight=1)
        opts.columnconfigure(3, weight=1)

        ttk.Label(opts, text="统一间隔最小(秒)").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=(0, 8))
        ttk.Entry(opts, textvariable=interval_min_var, width=12).grid(row=0, column=1, sticky="w", pady=(0, 8))
        ttk.Label(opts, text="统一间隔最大(秒)").grid(row=0, column=2, sticky="w", padx=(18, 6), pady=(0, 8))
        ttk.Entry(opts, textvariable=interval_max_var, width=12).grid(row=0, column=3, sticky="w", pady=(0, 8))

        ttk.Label(opts, text="请求超时(秒)").grid(row=1, column=0, sticky="w", padx=(0, 6))
        ttk.Entry(opts, textvariable=timeout_var, width=12).grid(row=1, column=1, sticky="w")
        ttk.Label(opts, text="说明：每次请求随机停留于上述区间内").grid(
            row=1, column=2, columnspan=2, sticky="w", padx=(18, 6)
        )

        ttk.Label(opts, text="连续失败阈值").grid(row=2, column=0, sticky="w", padx=(0, 6), pady=(8, 0))
        ttk.Entry(opts, textvariable=suspect_failures_var, width=12).grid(row=2, column=1, sticky="w", pady=(8, 0))
        ttk.Label(opts, text="达到阈值将判定“疑似风控”并自动暂停").grid(
            row=2, column=2, columnspan=2, sticky="w", padx=(18, 6), pady=(8, 0)
        )

        ttk.Checkbutton(opts, text="启用 JSL 反爬挑战处理", variable=jsl_var).grid(
            row=3, column=0, columnspan=4, sticky="w", pady=(8, 0)
        )
        ttk.Label(opts, text="图片下载方式").grid(row=4, column=0, sticky="w", padx=(0, 6), pady=(6, 0))
        mode_box = ttk.Frame(opts)
        mode_box.grid(row=4, column=1, columnspan=3, sticky="w", pady=(6, 0))
        ttk.Radiobutton(
            mode_box,
            text="请求模式(快)",
            variable=image_mode_var,
            value="requests_jsl",
        ).pack(side=tk.LEFT)
        ttk.Radiobutton(
            mode_box,
            text="浏览器模式(慢稳)",
            variable=image_mode_var,
            value="browser",
        ).pack(side=tk.LEFT, padx=(12, 0))
        ttk.Label(
            opts,
            text="说明：请求模式=先抓详情再下载；浏览器模式=列表/详情/图片都走浏览器。",
            bootstyle="secondary",
        ).grid(row=5, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Checkbutton(
            opts,
            text="快速模式失败时自动回退浏览器模式",
            variable=auto_fallback_var,
        ).grid(row=6, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Label(
            opts,
            text="提示：回退仅在请求模式触发风控/连续失败时启用。",
            bootstyle="secondary",
        ).grid(row=7, column=0, columnspan=4, sticky="w", pady=(2, 0))
        ttk.Checkbutton(
            opts,
            text="浏览器抓取时禁用页面图片渲染（更省流量）",
            variable=disable_page_images_var,
        ).grid(row=8, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Label(
            opts,
            text="提示：只影响浏览器页面显示，不影响后续按 image_url 下载原图。",
            bootstyle="secondary",
        ).grid(row=9, column=0, columnspan=4, sticky="w", pady=(2, 0))
        ttk.Checkbutton(opts, text="仅保留最终图片 + 抓取记录文档", variable=output_minimal_var).grid(
            row=10, column=0, columnspan=4, sticky="w", pady=(4, 0)
        )
        ttk.Label(
            opts,
            text="提示：开启该项会在完成后清理中间文件；若需要“中断后继续”，请先关闭。",
            bootstyle="secondary",
        ).grid(row=11, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Checkbutton(
            opts,
            text="图片直写（不生成 downloads/images 缓存）",
            variable=direct_write_images_var,
        ).grid(row=12, column=0, columnspan=4, sticky="w", pady=(4, 0))
        ttk.Label(
            opts,
            text="提示：开启后图片将直接写入最终目录，减少中间产物（过程更干净）。",
            bootstyle="secondary",
        ).grid(row=13, column=0, columnspan=4, sticky="w", pady=(2, 0))
        ttk.Checkbutton(
            opts,
            text="启用 LLM 语义增强（补字段 + 生成小传）",
            variable=llm_enable_var,
        ).grid(row=14, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Label(opts, text="LLM 模型").grid(row=15, column=0, sticky="w", padx=(0, 6), pady=(4, 0))
        ttk.Entry(opts, textvariable=llm_model_var, width=28).grid(row=15, column=1, sticky="w", pady=(4, 0))
        ttk.Label(opts, text="API Base").grid(row=15, column=2, sticky="w", padx=(18, 6), pady=(4, 0))
        ttk.Entry(opts, textvariable=llm_api_base_var, width=36).grid(row=15, column=3, sticky="w", pady=(4, 0))
        ttk.Label(opts, text="API Key").grid(row=16, column=0, sticky="w", padx=(0, 6), pady=(4, 0))
        ttk.Entry(opts, textvariable=llm_api_key_var, width=66, show="*").grid(
            row=16, column=1, columnspan=3, sticky="ew", pady=(4, 0)
        )
        ttk.Label(
            opts,
            text="提示：兼容在线 OpenAI API（如 https://api.openai.com/v1）和本地 Ollama。",
            bootstyle="secondary",
        ).grid(row=17, column=0, columnspan=4, sticky="w", pady=(2, 0))

        actions = ttk.Frame(container)
        actions.grid(row=9, column=0, columnspan=3, sticky="e", pady=(12, 0))

        result: Dict[str, Any] = {}

        def _cancel():
            dialog.destroy()

        def _start():
            selected_template_path = str(template_label_to_path.get(template_var.get(), "")).strip()
            submit_plan = _svc_build_public_scraper_setup_submit_plan(
                {
                    "start_url": url_var.get(),
                    "output_root": output_var.get(),
                    "interval_min": interval_min_var.get(),
                    "interval_max": interval_max_var.get(),
                    "timeout_seconds": timeout_var.get(),
                    "suspect_block_consecutive_failures": suspect_failures_var.get(),
                    "jsl_enabled": bool(jsl_var.get()),
                    "image_download_mode": image_mode_var.get(),
                    "auto_fallback_to_browser": bool(auto_fallback_var.get()),
                    "disable_page_images_during_crawl": bool(disable_page_images_var.get()),
                    "llm_enrich_enabled": bool(llm_enable_var.get()),
                    "llm_model": llm_model_var.get(),
                    "llm_api_base": llm_api_base_var.get(),
                    "llm_api_key": llm_api_key_var.get(),
                    "output_minimal": bool(output_minimal_var.get()),
                    "direct_write_images": bool(direct_write_images_var.get()),
                    "save_generated_template": bool(save_template_var.get()),
                    "cleanup_generated_template": bool(cleanup_template_var.get()),
                },
                app_file=__file__,
                selected_template_path=selected_template_path,
                template_start_url=str(template_start_url_cache.get("url", "")),
            )
            if not submit_plan.get("ok"):
                messagebox.showerror(
                    str(submit_plan.get("title", "参数错误")),
                    str(submit_plan.get("message", "")),
                    parent=dialog,
                )
                return
            result.update(dict(submit_plan.get("result") or {}))
            dialog.destroy()

        ttk.Button(actions, text="取消", command=_cancel, width=10).pack(side=tk.RIGHT)
        ttk.Button(actions, text="开始抓取", command=_start, width=12).pack(side=tk.RIGHT, padx=(0, 8))

        def _on_template_changed(_event=None):
            _apply_template_to_form()
            selected_template_path = str(template_label_to_path.get(template_var.get(), "")).strip()
            try:
                fill_output_btn.configure(state=tk.DISABLED if selected_template_path else tk.NORMAL)
            except Exception:
                pass

        template_combo.bind("<<ComboboxSelected>>", _on_template_changed)
        _on_template_changed()

        dialog.protocol("WM_DELETE_WINDOW", _cancel)
        dialog.bind("<Escape>", lambda _e: _cancel())
        dialog.bind("<Return>", lambda _e: _start())

        dialog.update_idletasks()
        x = self.winfo_rootx() + max((self.winfo_width() - dialog.winfo_reqwidth()) // 2, 0)
        y = self.winfo_rooty() + max((self.winfo_height() - dialog.winfo_reqheight()) // 3, 0)
        dialog.geometry(f"+{x}+{y}")
        dialog.lift()
        try:
            dialog.attributes("-topmost", True)
            dialog.after(300, lambda: dialog.attributes("-topmost", False))
        except Exception:
            pass
        url_entry.focus_set()
        self.wait_window(dialog)

        return result if result else None

    def _start_public_scraper_from_gui(self):
        app_dir = os.path.dirname(__file__)
        script_path = os.path.join(app_dir, "scraper", "run_public_scraper.py")
        if not os.path.exists(script_path):
            messagebox.showerror("启动失败", f"未找到抓取脚本:\n{script_path}", parent=self)
            return

        source_hint = ""
        try:
            source_hint = _normalize_http_url(self.edit_vars.get("source").get() if self.edit_vars.get("source") else "")
        except Exception:
            source_hint = ""
        setup = self._show_public_scraper_setup_dialog(source_hint)
        if not setup:
            self._set_status("已取消公共抓取")
            return

        start_url = str(setup["start_url"])
        output_root = str(setup["output_root"])
        template_path = str(setup.get("template_path", "")).strip()

        try:
            config_path, runtime_config = _svc_build_public_scraper_runtime_config(
                __file__,
                start_url,
                output_root,
                template_path=template_path,
            )
        except Exception as e:
            messagebox.showerror("启动失败", f"无法生成抓取配置：\n{e}", parent=self)
            return

        output_root = os.path.abspath(str(runtime_config.get("output_root", output_root)))
        existing_task = self._public_scraper_tasks.get(output_root)
        if isinstance(existing_task, dict) and self._is_process_running(existing_task.get("proc")):
            messagebox.showinfo("提示", f"该任务已在运行中：\n{output_root}", parent=self)
            self._set_active_public_scraper_task(output_root, refresh=True)
            return

        global_llm = self._get_global_llm_settings()
        apply_plan = _svc_apply_public_scraper_setup_to_runtime_config(
            runtime_config,
            setup,
            global_llm_settings=global_llm,
            normalize_api_base_fn=(normalize_api_base if HAS_LLM_CLIENT and callable(normalize_api_base) else None),
        )
        runtime_config = dict(apply_plan.get("runtime_config") or {})
        llm_model = str(apply_plan.get("llm_model", "")).strip()
        llm_api_base = str(apply_plan.get("llm_api_base", "")).strip()
        llm_api_key = str(apply_plan.get("llm_api_key", "")).strip()

        if (not template_path) and bool(setup.get("save_generated_template", True)):
            try:
                generated_template_path = _svc_save_generated_public_scraper_template(
                    start_url,
                    runtime_config,
                    self._public_scraper_templates_dir(),
                )
                runtime_config = _svc_register_generated_template(
                    runtime_config,
                    generated_template_path,
                    cleanup_after_run=bool(setup.get("cleanup_generated_template", True)),
                )
            except Exception:
                pass

        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(runtime_config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            messagebox.showerror("启动失败", f"无法写入运行配置：\n{e}", parent=self)
            return

        named_dir = _svc_resolve_named_images_dir(output_root, runtime_config.get("rules"))
        log_path = os.path.join(output_root, "reports", "gui_public_scraper.log")

        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        self._set_public_scraper_manual_pause_flag(output_root, paused=False)
        try:
            log_handle = open(log_path, "a", encoding="utf-8")
            log_handle.write(_svc_build_public_scraper_log_banner("Run", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            log_handle.flush()
        except Exception as e:
            messagebox.showerror("启动失败", f"无法创建日志文件：\n{e}", parent=self)
            return

        python_exec = self._resolve_python_cli_executable()
        cmd = _svc_build_public_scraper_command(python_exec, script_path, config_path, output_root)
        env = self._build_utf8_subprocess_env()
        env = self._apply_llm_env(env, api_base=llm_api_base, api_key=llm_api_key, model=llm_model)
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(script_path) or ".",
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=env,
            )
        except Exception as e:
            try:
                log_handle.close()
            except Exception:
                pass
            messagebox.showerror("启动失败", f"无法启动抓取任务：\n{e}", parent=self)
            return

        runtime_rules = runtime_config.get("rules")
        if not isinstance(runtime_rules, dict):
            runtime_rules = {}
        active_template_path_abs = _svc_resolve_active_template_path(template_path, runtime_rules)
        if active_template_path_abs:
            self._set_public_scraper_template_state(active_template_path_abs, "pending")
        self._register_public_scraper_task(
            output_root=output_root,
            proc=proc,
            named_dir=named_dir,
            config_path=config_path,
            log_path=log_path,
            log_handle=log_handle,
            runtime_state="运行中",
            active_template_path=active_template_path_abs,
        )
        self._set_status("通用抓取已启动（后台运行）")
        messagebox.showinfo(
            "已启动",
            _svc_build_started_task_message(
                pid=proc.pid,
                named_dir=named_dir,
                log_path=log_path,
                runtime_rules=runtime_rules,
            ),
            parent=self,
        )

    def _start_public_scraper_from_existing_task(
        self,
        output_root: str,
        *,
        skip_crawl: bool = False,
        skip_images: bool = False,
        skip_metadata: bool = False,
        show_success_dialog: bool = True,
        success_title: str = "继续任务",
        runtime_state: str = "继续运行中",
        mode_override: str = "",
        auto_fallback_override: Optional[bool] = None,
        disable_page_images_override: Optional[bool] = None,
    ) -> bool:
        app_dir = os.path.dirname(__file__)
        script_path = os.path.join(app_dir, "scraper", "run_public_scraper.py")
        if not os.path.exists(script_path):
            messagebox.showerror("启动失败", f"未找到抓取脚本:\n{script_path}", parent=self)
            return False

        output_root_abs = os.path.abspath(str(output_root or "").strip())
        existing_task = self._public_scraper_tasks.get(output_root_abs)
        if isinstance(existing_task, dict) and self._is_process_running(existing_task.get("proc")):
            messagebox.showinfo("提示", f"该任务已在运行中：\n{output_root_abs}", parent=self)
            self._set_active_public_scraper_task(output_root_abs, refresh=True)
            return False
        config_path = os.path.join(output_root_abs, "state", "runtime_config.json")
        if not os.path.exists(config_path):
            messagebox.showerror(
                "继续失败",
                "未找到运行配置文件：\n"
                f"{config_path}\n\n"
                "请先从“公共抓取(通用)”启动过一次该任务。",
                parent=self,
            )
            return False

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                runtime_config = json.load(f)
            if not isinstance(runtime_config, dict):
                raise ValueError("配置内容不是 JSON 对象")
        except Exception as e:
            messagebox.showerror("继续失败", f"无法读取运行配置：\n{e}", parent=self)
            return False

        runtime_config = _svc_prepare_existing_task_runtime_config(
            runtime_config,
            output_root=output_root_abs,
            mode_override=mode_override,
            auto_fallback_override=auto_fallback_override,
            disable_page_images_override=disable_page_images_override,
        )
        rules = runtime_config.get("rules")
        if not isinstance(rules, dict):
            rules = {}
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(runtime_config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            messagebox.showerror("继续失败", f"无法更新运行配置：\n{e}", parent=self)
            return False

        named_dir = _svc_resolve_named_images_dir(output_root_abs, runtime_config.get("rules"))

        log_path = os.path.join(output_root_abs, "reports", "gui_public_scraper.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        self._set_public_scraper_manual_pause_flag(output_root_abs, paused=False)

        try:
            log_handle = open(log_path, "a", encoding="utf-8")
            run_label = "Retry" if skip_crawl else "Continue"
            log_handle.write(_svc_build_public_scraper_log_banner(run_label, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            log_handle.flush()
        except Exception as e:
            messagebox.showerror("继续失败", f"无法创建日志文件：\n{e}", parent=self)
            return False

        python_exec = self._resolve_python_cli_executable()
        cmd = _svc_build_public_scraper_command(
            python_exec,
            script_path,
            config_path,
            output_root_abs,
            skip_crawl=skip_crawl,
            skip_images=skip_images,
            skip_metadata=skip_metadata,
        )

        global_llm = self._get_global_llm_settings()
        llm_model = str(rules.get("llm_model", "")).strip() or str(global_llm.get("model", "")).strip()
        llm_api_base = str(rules.get("llm_api_base", "")).strip() or str(global_llm.get("api_base", "")).strip()
        llm_api_key = str(rules.get("llm_api_key", "")).strip() or str(global_llm.get("api_key", "")).strip()
        if llm_api_base and HAS_LLM_CLIENT and callable(normalize_api_base):
            try:
                llm_api_base = normalize_api_base(llm_api_base)
            except Exception:
                llm_api_base = llm_api_base.rstrip("/")
        env = self._build_utf8_subprocess_env()
        env = self._apply_llm_env(env, api_base=llm_api_base, api_key=llm_api_key, model=llm_model)

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(script_path) or ".",
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=env,
            )
        except Exception as e:
            try:
                log_handle.close()
            except Exception:
                pass
            messagebox.showerror("继续失败", f"无法启动抓取任务：\n{e}", parent=self)
            return False

        runtime_rules = runtime_config.get("rules")
        if not isinstance(runtime_rules, dict):
            runtime_rules = {}
        active_template_path_abs = _svc_resolve_active_template_path("", runtime_rules)
        if active_template_path_abs:
            self._set_public_scraper_template_state(active_template_path_abs, "pending")
        self._register_public_scraper_task(
            output_root=output_root_abs,
            proc=proc,
            named_dir=named_dir,
            config_path=config_path,
            log_path=log_path,
            log_handle=log_handle,
            runtime_state=runtime_state,
            active_template_path=active_template_path_abs,
        )
        self._set_status("抓取任务继续运行中（后台）")
        if show_success_dialog:
            messagebox.showinfo(
                success_title,
                _svc_build_existing_task_started_message(
                    pid=proc.pid,
                    output_root=output_root_abs,
                    named_dir=named_dir,
                    log_path=log_path,
                    skip_crawl=skip_crawl,
                ),
                parent=self,
            )
        return True

    @staticmethod
    def _read_public_task_runtime_rules(output_root: str) -> Dict[str, Any]:
        root = os.path.abspath(str(output_root or "").strip())
        if not root:
            return {}
        config_path = os.path.join(root, "state", "runtime_config.json")
        if not os.path.exists(config_path):
            return {}
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return {}
            rules = payload.get("rules")
            return dict(rules) if isinstance(rules, dict) else {}
        except Exception:
            return {}

    def _show_public_scraper_continue_options_dialog(self, output_root: str) -> Optional[Dict[str, Any]]:
        root = os.path.abspath(str(output_root or "").strip())
        if not root:
            return None
        rules = self._read_public_task_runtime_rules(root)
        mode_default = str(rules.get("image_download_mode", "requests_jsl")).strip().lower()
        if mode_default not in {"requests_jsl", "browser"}:
            mode_default = "requests_jsl"
        fallback_default = bool(rules.get("auto_fallback_to_browser", True))
        disable_page_images_default = bool(rules.get("disable_page_images_during_crawl", True))

        dialog = tk.Toplevel(self)
        dialog.title("继续任务：模式设置")
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)

        frame = ttk.Frame(dialog, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="任务目录").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Label(frame, text=root, bootstyle="secondary").grid(row=0, column=1, sticky="w", pady=(0, 8))

        mode_var = tk.StringVar(value=mode_default)
        ttk.Label(frame, text="抓取模式").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 6))
        mode_box = ttk.Frame(frame)
        mode_box.grid(row=1, column=1, sticky="w", pady=(0, 6))
        ttk.Radiobutton(mode_box, text="请求模式(快)", variable=mode_var, value="requests_jsl").pack(side=tk.LEFT)
        ttk.Radiobutton(mode_box, text="浏览器模式(慢稳)", variable=mode_var, value="browser").pack(
            side=tk.LEFT, padx=(12, 0)
        )

        auto_fallback_var = tk.BooleanVar(value=fallback_default)
        disable_page_images_var = tk.BooleanVar(value=disable_page_images_default)
        ttk.Checkbutton(
            frame,
            text="请求模式失败时自动回退浏览器模式",
            variable=auto_fallback_var,
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(2, 0))
        ttk.Checkbutton(
            frame,
            text="浏览器抓取时禁用页面图片渲染（更省流量）",
            variable=disable_page_images_var,
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(4, 0))

        ttk.Label(
            frame,
            text="提示：本次继续任务会按这里的设置覆盖运行配置。",
            bootstyle="secondary",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(6, 0))

        result: Dict[str, Any] = {}

        def _cancel():
            dialog.destroy()

        def _ok():
            mode = str(mode_var.get() or "requests_jsl").strip().lower()
            if mode not in {"requests_jsl", "browser"}:
                mode = "requests_jsl"
            result["mode"] = mode
            result["auto_fallback"] = bool(auto_fallback_var.get())
            result["disable_page_images"] = bool(disable_page_images_var.get())
            dialog.destroy()

        actions = ttk.Frame(frame)
        actions.grid(row=5, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(actions, text="取消", command=_cancel, width=10).pack(side=tk.RIGHT)
        ttk.Button(actions, text="继续运行", command=_ok, width=12).pack(side=tk.RIGHT, padx=(0, 8))

        dialog.protocol("WM_DELETE_WINDOW", _cancel)
        dialog.bind("<Escape>", lambda _e: _cancel())
        dialog.bind("<Return>", lambda _e: _ok())

        dialog.update_idletasks()
        x = self.winfo_rootx() + max((self.winfo_width() - dialog.winfo_reqwidth()) // 2, 0)
        y = self.winfo_rooty() + max((self.winfo_height() - dialog.winfo_reqheight()) // 3, 0)
        dialog.geometry(f"+{x}+{y}")
        dialog.lift()
        try:
            dialog.attributes("-topmost", True)
            dialog.after(300, lambda: dialog.attributes("-topmost", False))
        except Exception:
            pass
        self.wait_window(dialog)
        return result if result else None

    def _continue_public_scraper_from_gui(self):
        proc = self._public_scraper_proc
        selected = ""
        if proc and (proc.poll() is None):
            if self._public_scraper_manual_paused:
                output_root = str(self._public_scraper_output_root or "").strip()
                if not output_root:
                    messagebox.showerror("继续失败", "当前任务目录丢失，无法恢复运行。", parent=self)
                    return
                choice = messagebox.askyesnocancel(
                    "继续任务",
                    "当前任务处于手动暂停状态。\n\n"
                    "是：直接恢复当前进程（沿用当前模式）。\n"
                    "否：结束当前进程，先改模式再继续任务。",
                    parent=self,
                )
                if choice is None:
                    self._set_status("已取消继续任务")
                    return
                if choice:
                    ok = self._set_public_scraper_manual_pause_flag(output_root, paused=False)
                    if not ok:
                        messagebox.showerror("继续失败", "无法移除暂停标记文件，请检查目录写权限。", parent=self)
                        return
                    self._public_scraper_manual_paused = False
                    self._public_scraper_runtime_state = "运行中"
                    self._sync_active_task_to_registry()
                    self._set_scraper_control_buttons(running=True)
                    self._set_status("抓取任务已继续运行")
                    self._refresh_scraper_monitor_panel()
                    self._refresh_public_task_manager_list()
                    self._schedule_public_scraper_poll()
                    return
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                        proc.wait(timeout=2)
                    except Exception:
                        pass
                self._set_public_scraper_manual_pause_flag(output_root, paused=False)
                self._close_public_scraper_log_handle()
                self._public_scraper_proc = None
                self._public_scraper_manual_paused = False
                self._public_scraper_runtime_state = "已停止(待继续)"
                self._sync_active_task_to_registry()
                self._set_scraper_control_buttons(running=False)
                self._refresh_scraper_monitor_panel()
                self._refresh_public_task_manager_list()
                selected = output_root
            else:
                messagebox.showinfo("提示", "当前任务仍在运行，请先暂停任务后再继续。", parent=self)
                return

        if not selected:
            initial_dir = self._public_scraper_output_root or self._default_public_tasks_root()
            selected = filedialog.askdirectory(
                parent=self,
                title="选择要继续的任务目录（可在当前任务运行时并行启动）",
                initialdir=initial_dir,
                mustexist=True,
            )
        if not selected:
            self._set_status("已取消继续任务")
            return

        continue_opts = self._show_public_scraper_continue_options_dialog(selected)
        if not continue_opts:
            self._set_status("已取消继续任务")
            return

        start_kwargs = _svc_build_continue_start_existing_task_args(continue_opts)
        self._start_public_scraper_from_existing_task(
            output_root=selected,
            **start_kwargs,
        )

    def _retry_public_scraper_from_gui(self):
        if self._public_scraper_proc and (self._public_scraper_proc.poll() is None):
            messagebox.showinfo("提示", "当前任务正在运行，请先暂停后再重试失败项。", parent=self)
            return

        active_root = self._normalize_public_task_root(self._public_scraper_active_task_root or self._public_scraper_output_root)
        initial_dir = active_root or self._default_public_tasks_root()
        selected = active_root or filedialog.askdirectory(
            parent=self,
            title="选择要重试失败项的任务目录",
            initialdir=initial_dir,
            mustexist=True,
        )
        if not selected:
            self._set_status("已取消失败重试")
            return

        retry_opts = self._show_public_scraper_continue_options_dialog(selected)
        if not retry_opts:
            self._set_status("已取消失败重试")
            return

        self._set_active_public_scraper_task(selected, refresh=False)
        need_crawl = self._retry_requires_crawl_phase(selected)
        start_kwargs = _svc_build_retry_start_existing_task_args(need_crawl, retry_opts)
        self._start_public_scraper_from_existing_task(
            output_root=selected,
            **start_kwargs,
        )

    def _pause_public_scraper_from_gui(self):
        proc = self._public_scraper_proc
        if (proc is None) or (proc.poll() is not None):
            self._public_scraper_proc = None
            self._public_scraper_named_dir = ""
            self._public_scraper_last_progress_text = ""
            self._public_scraper_started_at = None
            self._public_scraper_runtime_state = "空闲"
            self._public_scraper_manual_paused = False
            self._public_scraper_active_template_path = ""
            self._close_public_scraper_log_handle()
            self._set_scraper_control_buttons(running=False)
            self._refresh_scraper_monitor_panel()
            messagebox.showinfo("提示", "当前没有运行中的抓取任务。", parent=self)
            return

        if self._public_scraper_manual_paused:
            messagebox.showinfo("提示", "当前任务已处于手动暂停状态。", parent=self)
            return

        output_root = str(self._public_scraper_output_root or "").strip()
        if not output_root:
            messagebox.showerror("暂停失败", "当前任务目录丢失，无法写入暂停标记。", parent=self)
            return

        ok = self._set_public_scraper_manual_pause_flag(output_root, paused=True)
        if not ok:
            messagebox.showerror("暂停失败", "无法写入暂停标记文件，请检查目录写权限。", parent=self)
            return

        self._public_scraper_manual_paused = True
        self._public_scraper_runtime_state = "已暂停(手动)"
        self._sync_active_task_to_registry()
        self._set_scraper_control_buttons(running=True)
        self._set_status("抓取任务已手动暂停，可点击继续运行")
        self._refresh_scraper_monitor_panel()
        self._refresh_public_task_manager_list()

    def _stop_public_scraper_from_gui(self):
        # backward compatibility: old callback name
        self._pause_public_scraper_from_gui()

    def _schedule_public_scraper_poll(self):
        if self._public_scraper_poll_after:
            try:
                self.after_cancel(self._public_scraper_poll_after)
            except Exception:
                pass
        self._public_scraper_poll_after = None
        if not self._is_any_public_scraper_running():
            return
        self._public_scraper_poll_after = self.after(1500, self._poll_public_scraper_proc)

    def _handle_public_scraper_task_exit(self, root: str, task: Dict[str, Any], code: int):
        exit_plan = _svc_build_public_scraper_task_exit_plan(
            root,
            task,
            int(code),
            current_active_root=self._normalize_public_task_root(
                self._public_scraper_active_task_root or self._public_scraper_output_root
            ),
            record_path=self._get_scraper_record_path(root),
            backoff_state=(self._read_scraper_backoff_state(root) if int(code) == 2 else {"blocked_until": "", "blocked_reason": ""}),
            updated_at_ts=time.time(),
        )
        root_abs = str(exit_plan.get("root", "") or "").strip()
        log_handle = exit_plan.get("log_handle")
        self._close_public_scraper_log_handle(log_handle)
        if self._public_scraper_log_handle is log_handle:
            self._public_scraper_log_handle = None
        self._set_public_scraper_manual_pause_flag(root_abs, paused=False)

        next_task = dict(exit_plan.get("task") or {})
        active_template_path = str(exit_plan.get("active_template_path", "") or "").strip()
        template_state = str(exit_plan.get("template_state", "")).strip()
        if active_template_path and template_state:
            self._set_public_scraper_template_state(active_template_path, template_state)

        is_active = bool(exit_plan.get("is_active"))
        status_text = str(exit_plan.get("status_text", "")).strip()
        if is_active and status_text:
            self._set_status(status_text)

        dialog_kind = str(exit_plan.get("dialog_kind", "")).strip().lower()
        dialog_title = str(exit_plan.get("dialog_title", "")).strip()
        dialog_message = str(exit_plan.get("dialog_message", "")).strip()
        if is_active and dialog_kind and dialog_title and dialog_message:
            if dialog_kind == "warning":
                messagebox.showwarning(dialog_title, dialog_message, parent=self)
            else:
                messagebox.showinfo(dialog_title, dialog_message, parent=self)

        self._public_scraper_tasks[root_abs] = next_task

    def _poll_public_scraper_proc(self):
        self._public_scraper_poll_after = None
        for root, entry in list(self._public_scraper_tasks.items()):
            if not isinstance(entry, dict):
                continue
            proc = entry.get("proc")
            if proc is None:
                continue
            try:
                code = proc.poll()
            except Exception:
                code = 1
            if code is None:
                continue
            self._handle_public_scraper_task_exit(root, entry, int(code))

        poll_plan = _svc_build_public_scraper_poll_plan(
            self._normalize_public_task_root(self._public_scraper_active_task_root or self._public_scraper_output_root),
            self._public_scraper_tasks,
            is_process_running_fn=self._is_process_running,
        )
        self._set_active_public_scraper_task(str(poll_plan.get("active_root", "") or "").strip(), refresh=False)

        if self._is_process_running(self._public_scraper_proc):
            self._update_public_scraper_progress()
        else:
            self._set_scraper_control_buttons(running=False)
            self._refresh_scraper_monitor_panel()

        self._refresh_public_task_manager_list()
        if bool(poll_plan.get("should_schedule_poll")):
            self._schedule_public_scraper_poll()

    def _on_app_close(self):
        if self._public_scraper_poll_after:
            try:
                self.after_cancel(self._public_scraper_poll_after)
            except Exception:
                pass
            self._public_scraper_poll_after = None

        close_plan = _svc_build_public_scraper_app_close_plan(
            self._public_scraper_tasks,
            is_process_running_fn=self._is_process_running,
        )

        if bool(close_plan.get("should_confirm")):
            should_exit = messagebox.askyesno(
                str(close_plan.get("confirm_title", "") or "关闭确认"),
                str(close_plan.get("confirm_message", "") or ""),
                parent=self,
            )
            if not should_exit:
                return
            for cleanup in list(close_plan.get("cleanup_plans") or []):
                root = str(cleanup.get("root", "") or "").strip()
                proc = cleanup.get("proc")
                if proc is None:
                    continue
                try:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()
                except Exception:
                    pass
                active_template_path = str(cleanup.get("active_template_path", "") or "").strip()
                if active_template_path:
                    self._set_public_scraper_template_state(active_template_path, "pending")
                self._set_public_scraper_manual_pause_flag(root, paused=False)
                self._close_public_scraper_log_handle(cleanup.get("log_handle"))

        for handle in list(close_plan.get("all_log_handles") or []):
            self._close_public_scraper_log_handle(handle)

        reset_state = _svc_build_public_scraper_close_reset_state()
        self._public_scraper_tasks = dict(reset_state.get("tasks") or {})
        self._public_scraper_active_task_root = str(reset_state.get("active_task_root", "") or "").strip()
        self._clear_public_scraper_manual_pause_flag()
        self._public_scraper_output_root = str(reset_state.get("output_root", "") or "").strip()
        self._public_scraper_proc = reset_state.get("proc")
        self._public_scraper_named_dir = str(reset_state.get("named_dir", "") or "").strip()
        self._public_scraper_last_progress_text = str(reset_state.get("last_progress_text", "") or "").strip()
        self._public_scraper_started_at = reset_state.get("started_at")
        self._public_scraper_runtime_state = str(reset_state.get("runtime_state", "") or "空闲").strip() or "空闲"
        self._public_scraper_active_template_path = str(reset_state.get("active_template_path", "") or "").strip()
        self._close_public_scraper_log_handle()
        self._refresh_scraper_monitor_panel()
        self.destroy()

    def _open_source_url(self):
        raw = (self.edit_vars.get("source").get() if self.edit_vars.get("source") else "").strip()
        url = _normalize_http_url(raw)
        if not url:
            messagebox.showinfo("提示", "来源字段为空")
            return
        if raw != url and self.edit_vars.get("source"):
            self.edit_vars["source"].set(url)
        webbrowser.open(url)

    def _open_image_url(self):
        raw = (self.edit_vars.get("image_url").get() if self.edit_vars.get("image_url") else "").strip()
        url = _normalize_http_url(raw)
        if not url:
            messagebox.showinfo("提示", "原图链接字段为空")
            return
        if raw != url and self.edit_vars.get("image_url"):
            self.edit_vars["image_url"].set(url)
        webbrowser.open(url)

    def _repair_from_image_url(self):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        if not os.path.isfile(self.current_path):
            messagebox.showerror("错误", f"当前文件不存在:\n{self.current_path}")
            return

        raw_url = (self.edit_vars.get("image_url").get() if self.edit_vars.get("image_url") else "").strip()
        url = _normalize_http_url(raw_url)
        if not url:
            messagebox.showinfo("提示", "原图链接为空，无法下载修复")
            return
        if not (url.lower().startswith("http://") or url.lower().startswith("https://")):
            messagebox.showerror("错误", "原图链接必须是 http/https URL")
            return
        if raw_url != url and self.edit_vars.get("image_url"):
            self.edit_vars["image_url"].set(url)

        source_url = _normalize_http_url(self.edit_vars.get("source").get() if self.edit_vars.get("source") else "")
        backup_path = f"{self.current_path}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        if not messagebox.askyesno(
            "确认下载修复",
            "将执行以下操作：\n"
            "1) 从原图链接下载图片\n"
            "2) 备份当前文件\n"
            "3) 用下载结果替换当前文件\n"
            "4) 尝试把当前表单元数据回写到新文件\n\n"
            f"备份路径：\n{backup_path}\n\n继续吗？",
        ):
            return

        curr_path = self.current_path
        meta_payload = self._collect_structured_payload()
        tmp_path = ""
        try:
            self._set_status("下载原图中...")

            fd, tmp_path = tempfile.mkstemp(
                prefix="d2i_repair_",
                suffix=os.path.splitext(curr_path)[1] or ".img",
                dir=os.path.dirname(curr_path),
            )
            os.close(fd)

            req = urllib.request.Request(
                url,
                headers=self._build_browser_style_headers(url, source_url),
            )
            with urllib.request.urlopen(req, timeout=45) as resp, open(tmp_path, "wb") as f:
                shutil.copyfileobj(resp, f)

            self._replace_current_with_file(
                replacement_path=tmp_path,
                backup_path=backup_path,
                meta_payload=meta_payload,
                success_prefix="直连下载修复完成",
                move_replacement=True,
            )
            tmp_path = ""
        except urllib.error.URLError as e:
            self._set_status("下载失败")
            messagebox.showerror("下载失败", f"无法下载原图：\n{e}")
        except Exception as e:
            self._set_status("修复失败")
            messagebox.showerror("修复失败", str(e))
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def _repair_from_image_url_via_browser(self):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        if not os.path.isfile(self.current_path):
            messagebox.showerror("错误", f"当前文件不存在:\n{self.current_path}")
            return

        raw_url = (self.edit_vars.get("image_url").get() if self.edit_vars.get("image_url") else "").strip()
        url = _normalize_http_url(raw_url)
        if not url:
            messagebox.showinfo("提示", "原图链接为空，无法下载修复")
            return
        if not (url.lower().startswith("http://") or url.lower().startswith("https://")):
            messagebox.showerror("错误", "原图链接必须是 http/https URL")
            return
        if raw_url != url and self.edit_vars.get("image_url"):
            self.edit_vars["image_url"].set(url)

        source_url = _normalize_http_url(self.edit_vars.get("source").get() if self.edit_vars.get("source") else "")
        backup_path = f"{self.current_path}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        meta_payload = self._collect_structured_payload()
        tmp_path = ""
        try:
            self._set_status("下载修复中（自动）...")
            tmp_path = self._download_image_auto_no_interaction(url=url, source_url=source_url)
            self._replace_current_with_file(
                replacement_path=tmp_path,
                backup_path=backup_path,
                meta_payload=meta_payload,
                success_prefix="下载修复完成",
                move_replacement=True,
                ask_delete_backup=True,
            )
            tmp_path = ""
        except urllib.error.URLError as e:
            self._set_status("下载失败")
            messagebox.showerror("下载失败", f"无法下载原图：\n{e}")
        except Exception as e:
            self._set_status("修复失败")
            messagebox.showerror("修复失败", str(e))
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def _download_image_auto_no_interaction(self, *, url: str, source_url: str = "") -> str:
        curr_path = str(self.current_path or "").strip()
        suffix = os.path.splitext(curr_path)[1] or ".img"
        fd, tmp_path = tempfile.mkstemp(
            prefix="d2i_repair_auto_",
            suffix=suffix,
            dir=os.path.dirname(curr_path) if curr_path else None,
        )
        os.close(fd)

        # 采用单一稳定链路：使用 d2ilite 内置实现，避免对 d2i 项目产生运行时依赖。
        try:
            self._set_status("下载修复中：内置稳定链路...")
            self._download_via_local_stable(url=url, source_url=source_url, output_path=tmp_path)
            self._validate_image_file(tmp_path)
            return tmp_path
        except Exception as e:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            raise RuntimeError(f"自动下载失败（内置稳定链路）：{self._short_error(e)}")

    def _short_error(self, err: Any, limit: int = 560) -> str:
        s = str(err or "").strip()
        if not s:
            return ""
        s = s.replace("\r", "\n")
        s = re.sub(r"\n{2,}", "\n", s)
        s = s.strip()
        if len(s) <= limit:
            return s
        return s[:limit].rstrip() + " ..."

    def _is_sensitive_domain(self, url: str) -> bool:
        try:
            domain = (urllib.parse.urlparse(str(url or "")).netloc or "").lower()
        except Exception:
            domain = ""
        if not domain:
            return False
        return any(marker in domain for marker in SENSITIVE_DOMAINS)

    def _extract_image_candidates_from_html(self, html: str, base_url: str) -> List[str]:
        text = str(html or "")
        if not text:
            return []

        patterns = [
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<img[^>]+src=["\']([^"\']+)["\']',
            r"https?://[^\s\"'<>]+?\.(?:jpg|jpeg|png|webp|bmp|tif|tiff)(?:\?[^\s\"'<>]*)?",
            r"/[^\s\"'<>]+?\.(?:jpg|jpeg|png|webp|bmp|tif|tiff)(?:\?[^\s\"'<>]*)?",
        ]

        out: List[str] = []
        seen = set()
        for pat in patterns:
            try:
                hits = re.findall(pat, text, flags=re.IGNORECASE)
            except Exception:
                hits = []
            for raw in hits:
                cand = _normalize_http_url(raw)
                if not cand:
                    continue
                full = urllib.parse.urljoin(base_url, cand)
                full = _normalize_http_url(full)
                if not full.lower().startswith(("http://", "https://")):
                    continue
                key = full.strip()
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(key)

        return out[:50]

    def _create_stealth_session(self, image_url: str, source_url: str, forced_user_agent: str = ""):
        if not HAS_REQUESTS:
            return None
        session = requests.Session()
        ua = str(forced_user_agent or "").strip() or random.choice(STEALTH_USER_AGENTS)
        headers = self._build_browser_style_headers(image_url=image_url, source_url=source_url)
        headers["User-Agent"] = ua
        headers["Accept-Encoding"] = "gzip, deflate, br"
        headers["Connection"] = "keep-alive"
        session.headers.update(headers)
        session.verify = False
        return session

    def _download_via_requests_session(
        self,
        *,
        url: str,
        source_url: str,
        output_path: str,
        seed_urls: Optional[List[str]] = None,
        warmup_urls: Optional[List[str]] = None,
        injected_cookies: Optional[Dict[str, str]] = None,
        forced_user_agent: str = "",
    ):
        if not HAS_REQUESTS:
            raise RuntimeError("requests 未安装")

        session = self._create_stealth_session(url, source_url, forced_user_agent=forced_user_agent)
        if session is None:
            raise RuntimeError("requests session 创建失败")

        if injected_cookies:
            for ck, cv in (injected_cookies or {}).items():
                k = str(ck or "").strip()
                if not k:
                    continue
                try:
                    session.cookies.set(k, str(cv or ""))
                except Exception:
                    continue

        parsed = urllib.parse.urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}/" if (parsed.scheme and parsed.netloc) else ""

        warm_list: List[str] = []
        if source_url.lower().startswith(("http://", "https://")):
            warm_list.append(source_url)
        elif base_url:
            warm_list.append(base_url)
        for extra in (warmup_urls or []):
            u = _normalize_http_url(extra)
            if u and (u not in warm_list):
                warm_list.append(u)

        warm_candidates: List[str] = []
        for warm in warm_list:
            try:
                h = self._build_browser_style_headers(url, warm)
                r_warm = session.get(
                    warm,
                    headers=h,
                    timeout=REPAIR_WARMUP_TIMEOUT_SEC,
                    allow_redirects=True,
                    verify=False,
                )
                try:
                    ctype_warm = str((r_warm.headers or {}).get("Content-Type", "")).lower()
                    final_warm = _normalize_http_url(getattr(r_warm, "url", "") or warm)
                    if ("text/html" in ctype_warm) or (not ctype_warm.startswith("image/")):
                        html = ""
                        try:
                            html = r_warm.text
                        except Exception:
                            html = ""
                        if html:
                            for cand in self._extract_image_candidates_from_html(html, final_warm or warm):
                                if cand not in warm_candidates:
                                    warm_candidates.append(cand)
                finally:
                    try:
                        r_warm.close()
                    except Exception:
                        pass
            except Exception:
                continue

        queue: List[str] = []
        for u in (seed_urls or []):
            n = _normalize_http_url(u)
            if n and (n not in queue):
                queue.append(n)
        if not queue:
            queue = [_normalize_http_url(url)]
        for cand in warm_candidates:
            if cand and (cand not in queue):
                queue.append(cand)
        visited = set()
        errors: List[str] = []

        while queue and (len(visited) < REPAIR_MAX_CANDIDATES):
            cand = _normalize_http_url(queue.pop(0))
            if not cand or cand in visited:
                continue
            visited.add(cand)

            referer = source_url if source_url.lower().startswith(("http://", "https://")) else base_url
            headers = self._build_browser_style_headers(cand, referer)
            headers["User-Agent"] = session.headers.get("User-Agent", headers.get("User-Agent", ""))

            try:
                resp = session.get(
                    cand,
                    headers=headers,
                    timeout=REPAIR_REQUEST_TIMEOUT_SEC,
                    stream=True,
                    allow_redirects=True,
                    verify=False,
                )
            except Exception as e:
                errors.append(f"{cand}: {e}")
                continue

            final_url = _normalize_http_url(getattr(resp, "url", "") or cand)
            status = int(getattr(resp, "status_code", 0) or 0)
            ctype = str((resp.headers or {}).get("Content-Type", "")).lower()

            if status >= 400:
                errors.append(f"HTTP {status}: {final_url}")
                # 失败页也可能包含真实图片地址（或重定向线索），继续提取。
                try:
                    ctype_fail = str((resp.headers or {}).get("Content-Type", "")).lower()
                    if "text/html" in ctype_fail:
                        html_fail = ""
                        try:
                            html_fail = resp.text
                        except Exception:
                            html_fail = ""
                        if html_fail:
                            for next_url in self._extract_image_candidates_from_html(html_fail, final_url or cand):
                                if (next_url not in visited) and (next_url not in queue):
                                    queue.append(next_url)
                except Exception:
                    pass
                try:
                    resp.close()
                except Exception:
                    pass
                continue

            looks_image = ctype.startswith("image/") or any(final_url.split("?")[0].lower().endswith(ext) for ext in IMAGE_EXTS)
            if looks_image:
                with open(output_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                try:
                    resp.close()
                except Exception:
                    pass
                return

            text = ""
            try:
                text = resp.text
            except Exception:
                text = ""
            finally:
                try:
                    resp.close()
                except Exception:
                    pass

            if text:
                for next_url in self._extract_image_candidates_from_html(text, final_url):
                    if (next_url not in visited) and (next_url not in queue):
                        queue.append(next_url)

        msg = "; ".join(errors[:6]) if errors else "未发现可下载图片地址"
        raise RuntimeError(f"requests链路失败: {msg}")

    def _download_via_direct_request(self, *, url: str, source_url: str, output_path: str):
        if HAS_REQUESTS:
            return self._download_via_requests_session(url=url, source_url=source_url, output_path=output_path)

        headers_main = self._build_browser_style_headers(url, source_url)
        headers_fallback = self._build_browser_style_headers(url, "")
        header_candidates = [headers_main]
        if headers_fallback != headers_main:
            header_candidates.append(headers_fallback)

        last_err: Optional[Exception] = None
        for headers in header_candidates:
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=45) as resp, open(output_path, "wb") as f:
                    shutil.copyfileobj(resp, f)
                return
            except Exception as e:
                last_err = e
                continue

        if last_err:
            raise last_err
        raise RuntimeError("直连下载失败（未知错误）")

    def _download_via_real_browser(self, *, url: str, source_url: str, output_path: str):
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as e:
            raise RuntimeError(f"未安装 playwright：{e}")

        headers = self._build_browser_style_headers(url, source_url)
        user_agent = headers.get("User-Agent", "")
        extra_headers = {k: v for k, v in headers.items() if k != "User-Agent"}

        try:
            with sync_playwright() as pw:
                headed = str(os.environ.get("D2I_PLAYWRIGHT_HEADED", "1")).strip().lower() in {"1", "true", "yes"}
                preferred = str(os.environ.get("D2I_PLAYWRIGHT_CHANNEL", "chrome")).strip().lower()

                launch_errors: List[str] = []
                browser = None

                channel_order: List[Optional[str]] = []
                if preferred in {"chrome", "msedge"}:
                    channel_order.append(preferred)
                for ch in ("chrome", "msedge"):
                    if ch not in channel_order:
                        channel_order.append(ch)
                channel_order.append(None)  # 回退 Playwright 自带 chromium

                for ch in channel_order:
                    try:
                        if ch:
                            browser = pw.chromium.launch(channel=ch, headless=(not headed))
                        else:
                            browser = pw.chromium.launch(headless=(not headed))
                        break
                    except Exception as e:
                        label = ch or "chromium"
                        launch_errors.append(f"{label}: {e}")
                        browser = None
                        continue

                if browser is None:
                    raise RuntimeError("无法启动 Chrome/Edge/Chromium。 " + " | ".join(launch_errors))

                parsed = urllib.parse.urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}/" if (parsed.scheme and parsed.netloc) else ""

                context = None
                try:
                    context = browser.new_context(
                        accept_downloads=True,
                        user_agent=user_agent,
                        locale="zh-CN",
                    )
                    if extra_headers:
                        context.set_extra_http_headers(extra_headers)
                    page = context.new_page()

                    image_resp_candidates: List[Any] = []

                    def _on_response(resp):
                        try:
                            ct = str(resp.header_value("content-type") or "").lower()
                            if ct.startswith("image/"):
                                image_resp_candidates.append(resp)
                        except Exception:
                            pass

                    page.on("response", _on_response)

                    # 先走一次来源页/站点首页，复用 d2i 的“先过防护再拿图”策略。
                    warm_targets: List[str] = []
                    if source_url.lower().startswith(("http://", "https://")):
                        warm_targets.append(source_url)
                    if base_url and (base_url not in warm_targets):
                        warm_targets.append(base_url)

                    for warm in warm_targets:
                        try:
                            page.goto(warm, wait_until="domcontentloaded", timeout=45000)
                            self._wait_browser_challenge_clear(page, max_rounds=6)
                        except Exception:
                            continue

                    resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    self._wait_browser_challenge_clear(page, max_rounds=4)
                    if self._save_playwright_response_if_image(resp, output_path):
                        return

                    try:
                        page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass

                    # 策略1：浏览器请求上下文直接 GET（保留浏览器会话特征）
                    req_urls = [url]
                    final_url = str(getattr(page, "url", "") or "").strip()
                    if final_url and final_url not in req_urls:
                        req_urls.append(final_url)
                    for req_url in req_urls:
                        try:
                            r = page.request.get(req_url, timeout=60000)
                            if self._save_playwright_response_if_image(r, output_path):
                                return
                        except Exception:
                            continue

                    # 策略2：尝试页面上出现的图片响应（含动态加载）
                    for r in image_resp_candidates:
                        if self._save_playwright_response_if_image(r, output_path):
                            return

                    # 策略3：抓取页面中的 img src 列表逐个尝试
                    try:
                        img_urls = page.eval_on_selector_all(
                            "img",
                            (
                                "els => Array.from(new Set(els.map(el => "
                                "el.currentSrc || el.src || (el.getAttribute('src') ? new URL(el.getAttribute('src'), location.href).href : '')"
                                ").filter(Boolean))).slice(0, 50)"
                            ),
                        )
                    except Exception:
                        img_urls = []

                    if not isinstance(img_urls, list):
                        img_urls = []

                    for img_u in img_urls:
                        img_url = str(img_u or "").strip()
                        if not img_url:
                            continue
                        try:
                            r = page.request.get(img_url, timeout=60000)
                            if self._save_playwright_response_if_image(r, output_path):
                                return
                        except Exception:
                            continue

                    # 策略4：把浏览器 Cookie/UA 回灌给 requests 再下载（D2I 成功路径）
                    handoff_err: Optional[Exception] = None
                    if HAS_REQUESTS:
                        try:
                            browser_ua = ""
                            try:
                                browser_ua = str(page.evaluate("() => navigator.userAgent") or "")
                            except Exception:
                                browser_ua = user_agent

                            cookie_map: Dict[str, str] = {}
                            try:
                                for ck in (context.cookies() or []):
                                    name = str((ck or {}).get("name") or "").strip()
                                    if not name:
                                        continue
                                    cookie_map[name] = str((ck or {}).get("value") or "")
                            except Exception:
                                cookie_map = {}

                            seed_urls = list(req_urls)
                            for iu in img_urls:
                                su = _normalize_http_url(iu)
                                if su and (su not in seed_urls):
                                    seed_urls.append(su)

                            warmups = list(warm_targets)
                            if final_url and (final_url not in warmups):
                                warmups.append(final_url)

                            self._download_via_requests_session(
                                url=url,
                                source_url=source_url or base_url,
                                output_path=output_path,
                                seed_urls=seed_urls,
                                warmup_urls=warmups,
                                injected_cookies=cookie_map,
                                forced_user_agent=browser_ua,
                            )
                            return
                        except Exception as e_req:
                            handoff_err = e_req

                    detail = ""
                    try:
                        status = resp.status if resp else "n/a"
                        ctype = (resp.header_value("content-type") if resp else "") or ""
                        final_u = str(getattr(page, "url", "") or "").strip()
                        detail = f"主请求 status={status}, content-type={ctype}, final_url={final_u}"
                    except Exception:
                        detail = "无法获取主请求详情"

                    if handoff_err:
                        detail = f"{detail}; cookie回灌失败: {handoff_err}"

                    raise RuntimeError(f"浏览器已访问目标，但未提取到可写入图片数据。{detail}")
                finally:
                    if context is not None:
                        try:
                            context.close()
                        except Exception:
                            pass
                    try:
                        browser.close()
                    except Exception:
                        pass
        except Exception as e:
            raise RuntimeError(f"浏览器自动下载失败：{e}")

    def _wait_browser_challenge_clear(self, page: Any, max_rounds: int = 6):
        indicators = [
            "checking your browser",
            "just a moment",
            "ddos protection",
            "ray id",
            "attention required",
            "cloudflare",
        ]
        for _ in range(max(1, int(max_rounds))):
            try:
                content = str(page.content() or "").lower()
            except Exception:
                content = ""
            if not content:
                break
            if not any(ind in content for ind in indicators):
                break
            time.sleep(2.5)

    def _download_via_uc_selenium(self, *, url: str, source_url: str, output_path: str):
        if not HAS_REQUESTS:
            raise RuntimeError("requests 未安装，无法执行隐身浏览器回灌下载")

        driver = None
        try:
            # 单浏览器引擎：避免先开一个再回退再开一个。
            from selenium import webdriver  # type: ignore
            from selenium.webdriver.chrome.options import Options  # type: ignore
            from selenium.webdriver.chrome.service import Service  # type: ignore
            from webdriver_manager.chrome import ChromeDriverManager  # type: ignore

            options2 = Options()
            options2.add_argument("--disable-gpu")
            options2.add_argument("--no-sandbox")
            options2.add_argument("--disable-dev-shm-usage")
            options2.add_argument("--window-size=1920,1080")
            options2.add_argument("--ignore-certificate-errors")
            options2.add_argument("--ignore-ssl-errors")
            options2.add_argument("--disable-blink-features=AutomationControlled")
            options2.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            options2.add_experimental_option("useAutomationExtension", False)

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options2)
            driver.set_page_load_timeout(REPAIR_BROWSER_PAGELOAD_TIMEOUT_SEC)
            try:
                driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"},
                )
            except Exception:
                pass

            parsed = urllib.parse.urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}/" if (parsed.scheme and parsed.netloc) else ""

            def wait_clear(max_rounds: int = 6):
                indicators = [
                    "checking your browser",
                    "just a moment",
                    "ddos protection",
                    "ray id",
                    "attention required",
                    "cloudflare",
                ]
                for _ in range(max(1, int(max_rounds))):
                    try:
                        src = str(driver.page_source or "").lower()
                    except Exception:
                        src = ""
                    if not src:
                        break
                    if not any(ind in src for ind in indicators):
                        break
                    time.sleep(REPAIR_CHALLENGE_WAIT_SEC)

            warm_targets: List[str] = []
            if source_url.lower().startswith(("http://", "https://")):
                warm_targets.append(source_url)
            elif base_url:
                warm_targets.append(base_url)

            for warm in warm_targets:
                try:
                    driver.get(warm)
                    time.sleep(0.6)
                    wait_clear(REPAIR_CHALLENGE_WARM_ROUNDS)
                except Exception:
                    continue

            driver.get(url)
            time.sleep(0.6)
            wait_clear(REPAIR_CHALLENGE_MAIN_ROUNDS)

            cookies: Dict[str, str] = {}
            try:
                for ck in (driver.get_cookies() or []):
                    name = str((ck or {}).get("name") or "").strip()
                    if not name:
                        continue
                    cookies[name] = str((ck or {}).get("value") or "")
            except Exception:
                cookies = {}

            ua = ""
            try:
                ua = str(driver.execute_script("return navigator.userAgent;") or "")
            except Exception:
                ua = ""

            seed_urls: List[str] = [url]
            try:
                current = _normalize_http_url(driver.current_url)
                if current and (current not in seed_urls):
                    seed_urls.append(current)
            except Exception:
                pass

            img_candidates: List[str] = []
            try:
                from selenium.webdriver.common.by import By  # type: ignore

                imgs = driver.find_elements(By.TAG_NAME, "img")
                for el in imgs[:50]:
                    src = _normalize_http_url(el.get_attribute("src") or "")
                    if src and (src not in img_candidates):
                        img_candidates.append(src)
            except Exception:
                img_candidates = []

            for cand in img_candidates:
                if cand not in seed_urls:
                    seed_urls.append(cand)

            warmups = list(warm_targets)
            try:
                cur = _normalize_http_url(driver.current_url)
                if cur and (cur not in warmups):
                    warmups.append(cur)
            except Exception:
                pass

            # 复用 D2I 核心路径：浏览器拿 cookie + UA，再回灌 requests 下载真实图片。
            self._download_via_requests_session(
                url=url,
                source_url=source_url or base_url,
                output_path=output_path,
                seed_urls=seed_urls,
                warmup_urls=warmups,
                injected_cookies=cookies,
                forced_user_agent=ua,
            )
        except Exception as e:
            err_main = self._short_error(e)
            raise RuntimeError(f"隐身浏览器链路失败：{err_main}")
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    def _download_via_local_stable(self, *, url: str, source_url: str, output_path: str):
        # d2ilite 内置稳定链路：优先浏览器拿 cookie/ua，再回灌 requests 下载。
        # 不依赖外部 d2i 代码目录，保持项目独立。
        src = _normalize_http_url(source_url or "")
        try:
            self._download_via_uc_selenium(url=url, source_url=src, output_path=output_path)
            return
        except Exception as e_uc:
            # 浏览器不可用时回退 requests 伪装链路。
            try:
                self._download_via_requests_session(
                    url=url,
                    source_url=src,
                    output_path=output_path,
                    seed_urls=[url],
                    warmup_urls=[src] if src else [],
                )
                return
            except Exception as e_req:
                raise RuntimeError(
                    f"本地稳定链路失败：uc/selenium={self._short_error(e_uc, 320)}; "
                    f"requests={self._short_error(e_req, 320)}"
                )

    def _save_playwright_response_if_image(self, resp: Any, output_path: str) -> bool:
        if resp is None:
            return False
        try:
            ctype = str(resp.header_value("content-type") or "").lower()
        except Exception:
            ctype = ""

        url_text = str(getattr(resp, "url", "") or "").lower()
        looks_like_image = ctype.startswith("image/") or any((url_text.split("?")[0]).endswith(ext) for ext in IMAGE_EXTS)
        if not looks_like_image:
            return False

        try:
            body = resp.body()
            if not body:
                return False
            self._write_bytes_to_file(output_path, body)
            return self._validate_image_file(output_path, raise_on_error=False)
        except Exception:
            return False

    def _write_bytes_to_file(self, path: str, data: bytes):
        with open(path, "wb") as f:
            f.write(data or b"")

    def _validate_image_file(self, path: str, raise_on_error: bool = True) -> bool:
        try:
            with Image.open(path) as check_img:
                check_img.load()
            return True
        except Exception as e:
            if raise_on_error:
                raise RuntimeError(f"下载结果不是有效图片：{e}")
            return False

    def _build_browser_style_headers(self, image_url: str, source_url: str = "") -> Dict[str, str]:
        headers: Dict[str, str] = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        referer = (source_url or "").strip()
        if not referer:
            parsed = urllib.parse.urlparse(image_url)
            if parsed.scheme and parsed.netloc:
                referer = f"{parsed.scheme}://{parsed.netloc}/"
        if referer.lower().startswith("http://") or referer.lower().startswith("https://"):
            headers["Referer"] = referer
            parsed_ref = urllib.parse.urlparse(referer)
            if parsed_ref.scheme and parsed_ref.netloc:
                headers["Origin"] = f"{parsed_ref.scheme}://{parsed_ref.netloc}"

        return headers

    def _replace_current_with_file(
        self,
        *,
        replacement_path: str,
        backup_path: str,
        meta_payload: Dict[str, Any],
        success_prefix: str,
        move_replacement: bool,
        ask_delete_backup: bool = True,
    ):
        curr_path = str(self.current_path or "").strip()
        if not curr_path:
            raise RuntimeError("当前图片路径为空")
        if not os.path.isfile(curr_path):
            raise FileNotFoundError(f"当前文件不存在: {curr_path}")

        src = os.path.abspath(str(replacement_path or "").strip())
        if not src:
            raise RuntimeError("修复源文件为空")
        if not os.path.isfile(src):
            raise FileNotFoundError(f"修复源文件不存在: {src}")
        if os.path.abspath(curr_path) == src:
            raise RuntimeError("修复源文件与当前文件相同，无法替换")

        # 验证下载结果是可解码图片，避免把 HTML/错误页覆盖本地文件。
        with Image.open(src) as check_img:
            check_img.load()

        shutil.copy2(curr_path, backup_path)
        if move_replacement:
            os.replace(src, curr_path)
        else:
            shutil.copy2(src, curr_path)

        restored = bool(update_metadata_preserve_others(curr_path, meta_payload, clean_format=True))
        self._refresh_current()

        if restored:
            self._set_status(f"{success_prefix}（已回写元数据）")
            if ask_delete_backup:
                self._offer_delete_backup(backup_path, repaired_with_metadata=True)
        else:
            self._set_status(f"{success_prefix}（未回写元数据）")
            if ask_delete_backup:
                self._offer_delete_backup(backup_path, repaired_with_metadata=False)

    def _offer_delete_backup(self, backup_path: str, *, repaired_with_metadata: bool):
        backup = str(backup_path or "").strip()
        if not backup:
            return

        if repaired_with_metadata:
            tip = "已下载并修复当前图片，且元数据已回写。"
            title = "修复完成"
        else:
            tip = "图片已替换成功，但元数据回写失败（可手动再保存一次元数据）。"
            title = "部分完成"

        if not os.path.exists(backup):
            messagebox.showinfo(title, f"{tip}\n\n备份文件未找到：\n{backup}")
            return

        should_delete = messagebox.askyesno(
            title,
            f"{tip}\n\n备份文件：\n{backup}\n\n是否删除这个备份原图？",
        )
        if not should_delete:
            return

        try:
            os.remove(backup)
            self._set_status("已删除备份原图")
            messagebox.showinfo("已删除备份", f"已删除：\n{backup}")
        except Exception as e:
            messagebox.showerror("删除失败", f"无法删除备份文件：\n{backup}\n\n{e}")

    def _apply_autofill_suggestion(self):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        info = self._last_info
        if not isinstance(info, ImageMetadataInfo):
            messagebox.showinfo("提示", "当前图片尚未加载完成")
            return

        suggestion = suggest_metadata_fill(info)
        if not suggestion:
            messagebox.showinfo("提示", "没有可自动填空的缺失字段")
            return

        applied: List[str] = []

        def set_if_empty(key: str, value: Any):
            if value in (None, "", [], {}):
                return
            var = self.edit_vars.get(key)
            if var is None:
                return
            if str(var.get() or "").strip():
                return
            var.set(str(value))
            applied.append(key)

        for key in ("title", "person", "gender", "position", "city"):
            set_if_empty(key, suggestion.get(key))

        if ("keywords" in suggestion) and (not str(self.edit_vars["keywords"].get() or "").strip()):
            kws = suggestion.get("keywords")
            if isinstance(kws, list):
                text = ", ".join([str(x).strip() for x in kws if str(x).strip()])
                if text:
                    self.edit_vars["keywords"].set(text)
                    applied.append("keywords")

        if not applied:
            messagebox.showinfo("提示", "检测到建议，但当前字段已有值，未覆盖。")
            return

        self._set_status("自动填空完成（仅填空，不覆盖现有值）")
        messagebox.showinfo("完成", f"已填充字段: {', '.join(applied)}")

    @staticmethod
    def _normalize_single_line_text(value: Any) -> str:
        return _svc_normalize_single_line_text(value)

    @classmethod
    def _normalize_multiline_editor_text(cls, value: Any) -> str:
        return _svc_normalize_multiline_editor_text(value)

    @classmethod
    def _normalize_gender_text(cls, value: Any) -> str:
        return _svc_normalize_gender_text(value)

    @classmethod
    def _normalize_profile_for_editor(cls, value: Any) -> Any:
        return _svc_normalize_profile_for_editor(value, url_normalizer=_normalize_http_url)

    @staticmethod
    def _extract_json_payload_from_llm(raw_text: Any) -> Dict[str, Any]:
        return _svc_extract_json_payload_from_llm(raw_text)

    def _collect_editor_llm_config(self) -> Dict[str, Any]:
        if (not HAS_LLM_CLIENT) or (OpenAICompatibleClient is None):
            raise RuntimeError("当前环境缺少 LLM 客户端依赖（requests）。")
        llm_cfg = self._get_global_llm_settings()
        api_base = str(llm_cfg.get("api_base", "")).strip()
        if callable(normalize_api_base):
            try:
                api_base = normalize_api_base(api_base)
            except Exception:
                api_base = api_base.rstrip("/")
        else:
            api_base = api_base.rstrip("/")
        model = str(llm_cfg.get("model", "")).strip()
        api_key = str(llm_cfg.get("api_key", "")).strip()
        try:
            timeout_seconds = max(5, int(llm_cfg.get("timeout_seconds", 45)))
        except Exception:
            timeout_seconds = 45
        try:
            max_retries = max(1, int(llm_cfg.get("max_retries", 2)))
        except Exception:
            max_retries = 2
        try:
            temperature = float(llm_cfg.get("temperature", 0.1))
        except Exception:
            temperature = 0.1

        if not api_base:
            raise RuntimeError("未配置 API Base。请先在【全局设置】里填写。")
        if not model:
            raise RuntimeError("未配置模型名称。请先在【全局设置】里填写 Model。")
        return {
            "api_base": api_base,
            "api_key": api_key,
            "model": model,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "temperature": temperature,
        }

    def _run_edit_llm_job(
        self,
        *,
        running_text: str,
        failed_title: str,
        worker,
        on_success,
    ) -> None:
        if self._edit_llm_busy:
            messagebox.showinfo("提示", "已有 LLM 任务在执行，请稍候。")
            return
        self._edit_llm_busy = True
        self._set_status(running_text)
        progress_text = re.sub(r"^(?:ai|AI)\s*[:：]?\s*", "", str(running_text or "").strip())
        self._set_llm_progress(True, f"AI: {progress_text or '运行中'}")

        import threading

        def _thread_worker() -> None:
            error = ""
            result: Any = None
            try:
                result = worker()
            except Exception as exc:
                error = str(exc) or exc.__class__.__name__

            def _done() -> None:
                self._edit_llm_busy = False
                self._set_llm_progress(False, "AI: 已结束")
                if error:
                    self._set_status("LLM 操作失败")
                    messagebox.showerror(failed_title, error)
                    self._schedule_llm_progress_idle_reset()
                    return
                try:
                    on_success(result)
                except Exception as exc:
                    self._set_status("LLM 结果应用失败")
                    messagebox.showerror(failed_title, str(exc))
                finally:
                    self._schedule_llm_progress_idle_reset()

            try:
                self.after(0, _done)
            except Exception:
                _done()

        threading.Thread(target=_thread_worker, daemon=True).start()

    def _build_editor_llm_input(self, adaptive_fields: Dict[str, Any]) -> Dict[str, Any]:
        info_profile: Dict[str, Any] = {}
        if isinstance(self._last_info, ImageMetadataInfo) and isinstance(getattr(self._last_info, "titi_json", None), dict):
            prof_raw = (self._last_info.titi_json or {}).get("d2i_profile")
            if isinstance(prof_raw, dict):
                info_profile = dict(prof_raw)
        return {
            "title": self._normalize_single_line_text(self.edit_vars["title"].get()),
            "person": self._normalize_single_line_text(self.edit_vars["person"].get()),
            "gender": self._normalize_gender_text(self.edit_vars["gender"].get()),
            "position": self._normalize_single_line_text(self.edit_vars["position"].get()),
            "city": self._normalize_single_line_text(self.edit_vars["city"].get()),
            "source": _normalize_http_url(self.edit_vars["source"].get()),
            "image_url": _normalize_http_url(self.edit_vars["image_url"].get()),
            "keywords": _parse_keywords(self.edit_vars["keywords"].get()),
            "description": self._normalize_multiline_editor_text(self.desc_text.get("1.0", tk.END)),
            "adaptive_fields": adaptive_fields if isinstance(adaptive_fields, dict) else {},
            "profile_from_file": info_profile,
        }

    def _ai_autofill_current_metadata(self):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        try:
            adaptive = self._collect_adaptive_profile_fields()
        except Exception as e:
            messagebox.showerror("格式错误", str(e))
            return

        try:
            llm_cfg = self._collect_editor_llm_config()
        except Exception as e:
            messagebox.showerror("AI自动补全", str(e))
            return

        input_payload = self._build_editor_llm_input(self._normalize_profile_for_editor(adaptive))

        def _worker() -> Dict[str, Any]:
            client = OpenAICompatibleClient(
                api_base=str(llm_cfg["api_base"]),
                api_key=str(llm_cfg["api_key"]),
                timeout_seconds=int(llm_cfg["timeout_seconds"]),
                max_retries=int(llm_cfg["max_retries"]),
            )
            messages = [
                {
                    "role": "system",
                    "content": (
                        "你是图片元数据补全助手。只能基于输入内容，不得编造。"
                        "输出必须是 JSON 对象，不要额外文字。"
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": "补全缺失字段（尽量留空不确定项），并返回可合并的扩展字段。",
                            "constraints": {
                                "no_guess": True,
                                "gender_only": ["男", "女", ""],
                                "keywords_max": 8,
                                "leave_unknown_empty": True,
                            },
                            "output_schema": {
                                "title": "string",
                                "person": "string",
                                "gender": "string",
                                "position": "string",
                                "city": "string",
                                "source": "string",
                                "image_url": "string",
                                "description": "string",
                                "keywords": ["string"],
                                "extra_fields": {"key": "value"},
                            },
                            "input": input_payload,
                        },
                        ensure_ascii=False,
                    ),
                },
            ]
            response = client.chat_completions(
                model=str(llm_cfg["model"]),
                messages=messages,
                temperature=float(llm_cfg["temperature"]),
                stream=False,
            )
            content = client.extract_first_message_content(response)
            payload = self._extract_json_payload_from_llm(content)
            if not payload:
                raise RuntimeError("模型返回不是有效 JSON。")
            return payload

        self._run_edit_llm_job(
            running_text="AI 自动补全中...",
            failed_title="AI自动补全",
            worker=_worker,
            on_success=self._apply_ai_autofill_result,
        )

    def _apply_ai_autofill_result(self, payload: Any) -> None:
        result = payload if isinstance(payload, dict) else {}
        if not result:
            messagebox.showinfo("AI自动补全", "模型未返回可用结果。")
            self._set_status("AI 自动补全无变更")
            return

        applied: List[str] = []

        def _set_if_empty(key: str, value: Any, *, is_url: bool = False, gender: bool = False) -> None:
            var = self.edit_vars.get(key)
            if var is None:
                return
            if str(var.get() or "").strip():
                return
            text = (
                self._normalize_gender_text(value)
                if gender
                else self._normalize_single_line_text(value)
            )
            if is_url:
                text = _normalize_http_url(text)
            if not text:
                return
            var.set(text)
            applied.append(key)

        _set_if_empty("title", result.get("title"))
        _set_if_empty("person", result.get("person"))
        _set_if_empty("gender", result.get("gender"), gender=True)
        _set_if_empty("position", result.get("position"))
        _set_if_empty("city", result.get("city"))
        _set_if_empty("source", result.get("source"), is_url=True)
        _set_if_empty("image_url", result.get("image_url"), is_url=True)

        if not str(self.edit_vars["keywords"].get() or "").strip():
            keywords = result.get("keywords")
            if isinstance(keywords, list):
                normalized = clean_keywords([self._normalize_single_line_text(x) for x in keywords if str(x or "").strip()])
                if normalized:
                    self.edit_vars["keywords"].set(", ".join(normalized))
                    applied.append("keywords")

        if not str(self.desc_text.get("1.0", tk.END).strip() or "").strip():
            desc_text = self._normalize_multiline_editor_text(result.get("description", ""))
            if desc_text:
                self.desc_text.delete("1.0", tk.END)
                self.desc_text.insert("1.0", desc_text)
                applied.append("description")

        extra_fields = result.get("extra_fields")
        if isinstance(extra_fields, dict):
            current = self._collect_adaptive_profile_fields()
            merged = self._normalize_profile_for_editor(current)
            if not isinstance(merged, dict):
                merged = {}
            extra_added = 0
            for raw_k, raw_v in extra_fields.items():
                key = self._normalize_single_line_text(raw_k)
                if not key:
                    continue
                if key in merged and merged.get(key) not in (None, "", [], {}):
                    continue
                value = self._normalize_profile_for_editor(raw_v)
                if value in (None, "", [], {}):
                    continue
                merged[key] = value
                extra_added += 1
            if extra_added:
                self._render_adaptive_profile_rows(merged)
                applied.append(f"extra_fields({extra_added})")

        if not applied:
            self._set_status("AI 自动补全完成（无可应用字段）")
            messagebox.showinfo("AI自动补全", "模型返回了结果，但当前字段已有值或结果为空。")
            return

        self._set_status("AI 自动补全完成")
        messagebox.showinfo("AI自动补全", f"已更新：{', '.join(applied)}")

    def _ai_generate_biography_for_current(self):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        try:
            adaptive = self._collect_adaptive_profile_fields()
        except Exception as e:
            messagebox.showerror("格式错误", str(e))
            return

        try:
            llm_cfg = self._collect_editor_llm_config()
        except Exception as e:
            messagebox.showerror("AI自动小传", str(e))
            return

        input_payload = self._build_editor_llm_input(self._normalize_profile_for_editor(adaptive))

        def _worker() -> Dict[str, Any]:
            client = OpenAICompatibleClient(
                api_base=str(llm_cfg["api_base"]),
                api_key=str(llm_cfg["api_key"]),
                timeout_seconds=int(llm_cfg["timeout_seconds"]),
                max_retries=int(llm_cfg["max_retries"]),
            )
            messages = [
                {
                    "role": "system",
                    "content": (
                        "你是人物小传生成助手。只能基于输入文本，不得编造。"
                        "输出必须是 JSON 对象，不要额外文字。"
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": "生成一段简洁中文人物小传，适合写入元数据字段 biography_short。",
                            "constraints": {
                                "no_guess": True,
                                "max_chars": 220,
                                "style": "客观简洁",
                            },
                            "output_schema": {
                                "biography_short": "string",
                            },
                            "input": input_payload,
                        },
                        ensure_ascii=False,
                    ),
                },
            ]
            response = client.chat_completions(
                model=str(llm_cfg["model"]),
                messages=messages,
                temperature=float(llm_cfg["temperature"]),
                stream=False,
            )
            content = client.extract_first_message_content(response)
            payload = self._extract_json_payload_from_llm(content)
            if not payload:
                raise RuntimeError("模型返回不是有效 JSON。")
            return payload

        self._run_edit_llm_job(
            running_text="AI 小传生成中...",
            failed_title="AI自动小传",
            worker=_worker,
            on_success=self._apply_ai_biography_result,
        )

    def _apply_ai_biography_result(self, payload: Any) -> None:
        result = payload if isinstance(payload, dict) else {}
        bio = self._normalize_single_line_text(result.get("biography_short", ""))
        if not bio:
            self._set_status("AI 小传生成完成（无结果）")
            messagebox.showinfo("AI自动小传", "模型未返回有效小传。")
            return
        if len(bio) > 260:
            bio = bio[:257] + "..."

        current = self._collect_adaptive_profile_fields()
        merged = self._normalize_profile_for_editor(current)
        if not isinstance(merged, dict):
            merged = {}
        merged["biography_short"] = bio
        self._render_adaptive_profile_rows(merged)

        old_desc = str(self.desc_text.get("1.0", tk.END) or "").strip()
        trimmed = re.sub(r"\n?小传：[\s\S]*$", "", old_desc).strip()
        new_desc = (f"{trimmed}\n小传：\n{bio}" if trimmed else f"小传：\n{bio}").strip()
        self.desc_text.delete("1.0", tk.END)
        self.desc_text.insert("1.0", new_desc)

        self._set_status("AI 小传生成完成")
        messagebox.showinfo("AI自动小传", "已写入 biography_short，并同步到描述中的“小传”段。")

    def _normalize_current_keys_content(self):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        try:
            current_profile = self._collect_adaptive_profile_fields()
        except Exception as e:
            messagebox.showerror("整理失败", str(e))
            return

        changed: List[str] = []

        def _set_var_if_changed(key: str, value: str) -> None:
            var = self.edit_vars.get(key)
            if var is None:
                return
            old = str(var.get() or "")
            if old == value:
                return
            var.set(value)
            changed.append(key)

        _set_var_if_changed("title", self._normalize_single_line_text(self.edit_vars["title"].get()))
        _set_var_if_changed("person", self._normalize_single_line_text(self.edit_vars["person"].get()))
        _set_var_if_changed("gender", self._normalize_gender_text(self.edit_vars["gender"].get()))
        _set_var_if_changed("position", self._normalize_single_line_text(self.edit_vars["position"].get()))
        _set_var_if_changed("city", self._normalize_single_line_text(self.edit_vars["city"].get()))
        _set_var_if_changed("source", _normalize_http_url(self.edit_vars["source"].get()))
        _set_var_if_changed("image_url", _normalize_http_url(self.edit_vars["image_url"].get()))
        _set_var_if_changed("titi_asset_id", self._normalize_single_line_text(self.edit_vars["titi_asset_id"].get()))
        _set_var_if_changed("titi_world_id", self._normalize_single_line_text(self.edit_vars["titi_world_id"].get()))

        old_kw = str(self.edit_vars["keywords"].get() or "")
        new_kw = ", ".join(_parse_keywords(old_kw))
        if old_kw != new_kw:
            self.edit_vars["keywords"].set(new_kw)
            changed.append("keywords")

        old_desc = str(self.desc_text.get("1.0", tk.END) or "").strip()
        new_desc = self._normalize_multiline_editor_text(old_desc)
        if old_desc != new_desc:
            self.desc_text.delete("1.0", tk.END)
            self.desc_text.insert("1.0", new_desc)
            changed.append("description")

        normalized_profile = self._normalize_profile_for_editor(current_profile)
        normalized_profile = self._prune_empty_profile_values(normalized_profile)
        if not isinstance(normalized_profile, dict):
            normalized_profile = {}
        current_keys = [str(k).strip() for k in current_profile.keys() if str(k).strip()] if isinstance(current_profile, dict) else []
        target_keys: List[str] = []
        if normalized_profile:
            if "police_id" in normalized_profile:
                target_keys.append("police_id")
            for key in sorted([str(k).strip() for k in normalized_profile.keys() if str(k).strip()]):
                if key == "police_id":
                    continue
                target_keys.append(key)
        before_json = _safe_json_dumps(current_profile)
        after_json = _safe_json_dumps(normalized_profile)
        if (before_json != after_json) or (current_keys != target_keys):
            self._render_adaptive_profile_rows(normalized_profile)
            changed.append("extra_fields")

        if not changed:
            self._set_status("一键整理完成（无变更）")
            messagebox.showinfo("一键整理", "当前内容已经是规范状态。")
            return

        self._set_status("一键整理完成")
        messagebox.showinfo("一键整理", f"已整理：{', '.join(changed)}")

    def _render_preview(self, path: str):
        try:
            with Image.open(path) as img:
                self._preview_pil = img.copy()
            self._refresh_preview_image()
        except Exception:
            self._preview_pil = None
            self._preview_tk = None
            self.preview_label.configure(image="", text="(无法预览)")

    def _on_preview_resize(self, _event=None):
        if self._preview_resize_after:
            try:
                self.after_cancel(self._preview_resize_after)
            except Exception:
                pass
        self._preview_resize_after = self.after(80, self._refresh_preview_image)

    def _refresh_preview_image(self):
        self._preview_resize_after = None
        if self._preview_pil is None:
            self.preview_label.configure(image="", text="打开图片后显示预览")
            return

        w = max(160, int(self.preview_label.winfo_width() or 0) - 12)
        h = max(160, int(self.preview_label.winfo_height() or 0) - 12)

        img = self._preview_pil.copy()
        img.thumbnail((w, h))

        photo = ImageTk.PhotoImage(img)
        self._preview_tk = photo
        self.preview_label.configure(image=photo, text="")

    def _fill_edit_form(self, info: ImageMetadataInfo):
        self.edit_vars["title"].set(str(info.title or ""))
        self.edit_vars["person"].set(str(info.person or ""))
        self.edit_vars["gender"].set(str(info.gender or ""))
        self.edit_vars["position"].set(str(info.position or ""))
        self.edit_vars["city"].set(str(info.city or ""))
        self.edit_vars["source"].set(_normalize_http_url(str(info.source or "")))
        self.edit_vars["image_url"].set(_normalize_http_url(str(getattr(info, "image_url", "") or "")))
        self.edit_vars["keywords"].set(", ".join([str(x).strip() for x in (info.keywords or []) if str(x).strip()]))
        self.edit_vars["titi_asset_id"].set(str(info.titi_asset_id or ""))
        self.edit_vars["titi_world_id"].set(str(info.titi_world_id or ""))

        self.desc_text.delete("1.0", tk.END)
        self.desc_text.insert("1.0", str(info.description or ""))
        extra_payload = self._extract_adaptive_profile_fields(info)
        if self._should_show_police_id_field(info, extra_payload):
            extra_payload = dict(extra_payload)
            extra_payload.setdefault("police_id", "")
        self._render_adaptive_profile_rows(extra_payload)

    def _refresh_adaptive_profile_scrollregion(self):
        canvas = getattr(self, "extra_profile_rows_canvas", None)
        holder = getattr(self, "extra_profile_rows_frame", None)
        if canvas is None or holder is None:
            return
        try:
            holder.update_idletasks()
            bbox = canvas.bbox("all")
            if bbox:
                canvas.configure(scrollregion=bbox)
        except Exception:
            pass

    def _scroll_adaptive_profile_rows_to_end(self):
        canvas = getattr(self, "extra_profile_rows_canvas", None)
        if canvas is None:
            return
        self._refresh_adaptive_profile_scrollregion()
        try:
            canvas.yview_moveto(1.0)
        except Exception:
            pass

    def _on_add_adaptive_field_clicked(self):
        self._add_adaptive_profile_row("", "")
        self._scroll_adaptive_profile_rows_to_end()

    def _clear_adaptive_profile_rows(self):
        rows = list(getattr(self, "extra_profile_rows", []))
        for row in rows:
            frame = row.get("frame")
            if frame is not None:
                try:
                    frame.destroy()
                except Exception:
                    pass
        self.extra_profile_rows = []
        self._refresh_adaptive_profile_scrollregion()

    def _remove_adaptive_profile_row(self, row_token: Dict[str, Any]):
        frame = row_token.get("frame")
        if frame is not None:
            try:
                frame.destroy()
            except Exception:
                pass
        rows = list(getattr(self, "extra_profile_rows", []))
        self.extra_profile_rows = [item for item in rows if item is not row_token]
        self._refresh_adaptive_profile_scrollregion()

    @staticmethod
    def _adaptive_value_to_text(value: Any) -> str:
        if value in (None, ""):
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    @staticmethod
    def _adaptive_text_to_value(text: str) -> Any:
        raw = str(text or "").strip()
        if not raw:
            return ""
        if raw.startswith("{") or raw.startswith("["):
            try:
                return json.loads(raw)
            except Exception as e:
                raise ValueError(f"JSON 解析失败：{e}")
        return raw

    def _add_adaptive_profile_row(self, key: Any, value: Any):
        holder = getattr(self, "extra_profile_rows_frame", None)
        if holder is None:
            return
        row_frame = ttk.Frame(holder)
        row_frame.pack(fill=tk.X, pady=2)
        key_var = tk.StringVar(value=str(key or "").strip())
        value_var = tk.StringVar(value=self._adaptive_value_to_text(value))
        key_entry = ttk.Entry(row_frame, textvariable=key_var, width=24)
        key_entry.pack(side=tk.LEFT)
        value_entry = ttk.Entry(row_frame, textvariable=value_var)
        value_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 0))
        remove_btn = ttk.Button(row_frame, text="删除", width=7)
        remove_btn.pack(side=tk.LEFT, padx=(6, 0))
        row_token: Dict[str, Any] = {
            "frame": row_frame,
            "key_var": key_var,
            "value_var": value_var,
            "key_entry": key_entry,
            "value_entry": value_entry,
            "remove_btn": remove_btn,
        }
        remove_btn.configure(command=lambda token=row_token: self._remove_adaptive_profile_row(token))
        self.extra_profile_rows.append(row_token)
        self._refresh_adaptive_profile_scrollregion()

    def _render_adaptive_profile_rows(self, profile: Dict[str, Any]):
        self._clear_adaptive_profile_rows()
        if not isinstance(profile, dict):
            return
        keys = [str(k).strip() for k in profile.keys() if str(k).strip()]
        if not keys:
            return
        ordered_keys: List[str] = []
        if "police_id" in keys:
            ordered_keys.append("police_id")
        for key in sorted(keys):
            if key == "police_id":
                continue
            ordered_keys.append(key)
        for key in ordered_keys:
            self._add_adaptive_profile_row(key, profile.get(key))

    @staticmethod
    def _is_police_context_text(text: str) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        lowered = raw.lower()
        tokens = (
            "公安",
            "警察",
            "警官",
            "民警",
            "交警",
            "刑警",
            "派出所",
            "警督",
            "警衔",
            "英烈",
            "因公",
            "mps.gov.cn",
            "police",
        )
        return any(token in lowered for token in tokens)

    def _should_show_police_id_field(self, info: ImageMetadataInfo, profile: Dict[str, Any]) -> bool:
        if isinstance(profile, dict):
            if "police_id" in profile:
                return True
            for key in ("police_no", "police_number", "badge_no", "badge_id", "badge_number", "officer_id", "警号"):
                if key in profile and str(profile.get(key, "") or "").strip():
                    return True
        if str(getattr(info, "police_id", "") or "").strip():
            return True
        text_parts = [
            str(getattr(info, "source", "") or ""),
            str(getattr(info, "position", "") or ""),
            str(getattr(info, "description", "") or ""),
            str(getattr(info, "person", "") or ""),
            str(getattr(info, "title", "") or ""),
        ]
        for kw in getattr(info, "keywords", []) or []:
            text_parts.append(str(kw or ""))
        blob = " ".join([part for part in text_parts if part])
        return self._is_police_context_text(blob)

    @staticmethod
    def _prune_empty_profile_values(value: Any) -> Any:
        return _svc_prune_empty_profile_values(value)

    def _extract_adaptive_profile_fields(self, info: ImageMetadataInfo) -> Dict[str, Any]:
        profile: Dict[str, Any] = {}
        try:
            titi_json = info.titi_json if isinstance(info.titi_json, dict) else {}
            d2i_profile = titi_json.get("d2i_profile", {}) if isinstance(titi_json, dict) else {}
            if not isinstance(d2i_profile, dict):
                d2i_profile = {}

            extras = d2i_profile.get("extra_fields")
            if isinstance(extras, dict):
                for key, value in extras.items():
                    if value in (None, "", [], {}):
                        continue
                    profile[str(key)] = value

            mapped = d2i_profile.get("mapped_fields")
            if isinstance(mapped, dict):
                for key, value in mapped.items():
                    if str(key) in profile:
                        continue
                    if value in (None, "", [], {}):
                        continue
                    profile[str(key)] = value

            hidden_keys = {
                "name",
                "person",
                "description",
                "keywords",
                "source",
                "image_url",
                "city",
                "gender",
                "position",
                "title",
                "location",
                "extracted_at",
                "extra_fields",
                "mapped_fields",
            }
            for key, value in d2i_profile.items():
                k = str(key)
                if k in hidden_keys or (k in profile):
                    continue
                if value in (None, "", [], {}):
                    continue
                profile[k] = value

            police_id = str(getattr(info, "police_id", "") or "").strip()
            if police_id and ("police_id" not in profile):
                profile["police_id"] = police_id
        except Exception:
            return {}
        cleaned = self._prune_empty_profile_values(profile)
        return cleaned if isinstance(cleaned, dict) else {}

    def _collect_adaptive_profile_fields(self) -> Dict[str, Any]:
        rows = list(getattr(self, "extra_profile_rows", []))
        parsed: Dict[str, Any] = {}
        for idx, row in enumerate(rows, start=1):
            key_var = row.get("key_var")
            value_var = row.get("value_var")
            key = str(key_var.get() if key_var is not None else "").strip()
            value_text = str(value_var.get() if value_var is not None else "").strip()
            if (not key) and (not value_text):
                continue
            if (not key) and value_text:
                raise ValueError(f"扩展字段第 {idx} 行缺少字段名")
            if key in parsed:
                raise ValueError(f"扩展字段存在重复字段名：{key}")
            if not value_text:
                continue
            try:
                parsed[key] = self._adaptive_text_to_value(value_text)
            except Exception as e:
                raise ValueError(f"扩展字段 {key} 值格式错误：{e}")
        cleaned = self._prune_empty_profile_values(parsed)
        return cleaned if isinstance(cleaned, dict) else {}

    def _render_snapshot(self, info: ImageMetadataInfo, basic: Dict[str, Any]):
        payload = {
            "loaded_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
            "path": info.filepath,
            "basic": basic,
            "structured": asdict(info),
        }
        self.snapshot_text.delete("1.0", tk.END)
        self.snapshot_text.insert("1.0", _safe_json_dumps(payload))
        self._snapshot_dirty = False

    def _collect_structured_payload(self) -> Dict[str, Any]:
        adaptive = self._collect_adaptive_profile_fields()
        return _svc_build_structured_payload(
            title=self.edit_vars["title"].get(),
            person=self.edit_vars["person"].get(),
            gender=self.edit_vars["gender"].get(),
            position=self.edit_vars["position"].get(),
            city=self.edit_vars["city"].get(),
            source=self.edit_vars["source"].get(),
            image_url=self.edit_vars["image_url"].get(),
            keywords_text=self.edit_vars["keywords"].get(),
            titi_asset_id=self.edit_vars["titi_asset_id"].get(),
            titi_world_id=self.edit_vars["titi_world_id"].get(),
            description=self.desc_text.get("1.0", tk.END),
            adaptive_profile=adaptive,
        )

    def _save_structured(self) -> bool:
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return False

        try:
            payload = self._collect_structured_payload()
        except Exception as e:
            messagebox.showerror("格式错误", str(e))
            return False
        self._set_status("保存中...")

        try:
            ok = bool(update_metadata_preserve_others(self.current_path, payload, clean_format=True))
            if not ok:
                raise RuntimeError("写入失败，请确认 pyexiv2 已正确安装")
        except Exception as e:
            self._set_status("保存失败")
            messagebox.showerror("保存失败", str(e))
            return False

        self._set_status("保存成功")
        self._refresh_metadata_only()
        return True

    def _save_structured_and_next(self):
        if not self.current_path:
            return
        ok = self._save_structured()
        if ok and self.folder_images and (self.current_index < len(self.folder_images) - 1):
            self.current_index += 1
            self._load_current()

    def _reload_raw_editors(self):
        self.xmp_editor["text"].delete("1.0", tk.END)
        self.exif_editor["text"].delete("1.0", tk.END)
        self.iptc_editor["text"].delete("1.0", tk.END)

        self.xmp_editor["text"].insert("1.0", _safe_json_dumps(self._last_xmp or {}))
        self.exif_editor["text"].insert("1.0", _safe_json_dumps(self._last_exif or {}))
        self.iptc_editor["text"].insert("1.0", _safe_json_dumps(self._last_iptc or {}))
        self._raw_editors_dirty = False

    def _apply_raw_editor(self, kind: str):
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return
        if not HAS_PYEXIV2:
            messagebox.showerror("错误", "pyexiv2 未安装，无法执行高级写入")
            return

        text_widget = None
        if kind == "xmp":
            text_widget = self.xmp_editor["text"]
        elif kind == "exif":
            text_widget = self.exif_editor["text"]
        elif kind == "iptc":
            text_widget = self.iptc_editor["text"]

        if text_widget is None:
            return

        raw = text_widget.get("1.0", tk.END).strip()
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception as e:
            messagebox.showerror("JSON 错误", f"解析失败:\n{e}")
            return

        if not isinstance(parsed, dict):
            messagebox.showerror("格式错误", "必须是 JSON 对象（key-value）")
            return

        if not messagebox.askyesno("确认写入", f"将覆盖当前文件的 {kind.upper()} 命名空间，继续吗？"):
            return

        kwargs: Dict[str, Any] = {"xmp_data": None, "exif_data": None, "iptc_data": None}
        if kind == "xmp":
            kwargs["xmp_data"] = parsed
        elif kind == "exif":
            kwargs["exif_data"] = parsed
        elif kind == "iptc":
            kwargs["iptc_data"] = parsed

        self._set_status(f"写入 {kind.upper()} 中...")
        try:
            _write_raw_with_pyexiv2(self.current_path, **kwargs)
        except Exception as e:
            self._set_status("高级写入失败")
            messagebox.showerror("写入失败", str(e))
            return

        self._set_status(f"{kind.upper()} 写入成功")
        self._refresh_current()


def main():
    start_target: Optional[str] = None
    if len(sys.argv) > 1:
        candidate = " ".join(sys.argv[1:]).strip()
        if candidate:
            start_target = candidate

    app = D2ILiteApp(start_target=start_target)
    app.mainloop()


if __name__ == "__main__":
    main()
