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
KEYWORD_SPLIT_RE = re.compile(r"[;,，、\n]+")
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
    folder_abs = os.path.abspath(folder)
    if not os.path.isdir(folder_abs):
        return []
    results: List[str] = []
    for name in sorted(os.listdir(folder_abs), key=lambda x: x.lower()):
        path = os.path.join(folder_abs, name)
        ext = os.path.splitext(name)[1].lower()
        if os.path.isfile(path) and ext in IMAGE_EXTS:
            results.append(path)
    return results


def _parse_keywords(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    values = [x.strip() for x in KEYWORD_SPLIT_RE.split(raw) if x.strip()]
    uniq: List[str] = []
    seen = set()
    for item in values:
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(item)
    return clean_keywords(uniq)


def _normalize_http_url(text: Any) -> str:
    s = str(text or "").replace("\x00", "").strip()
    if not s:
        return ""

    m = re.search(r"https?://", s, flags=re.IGNORECASE)
    if not m:
        return s

    tail = s[m.start():]
    m2 = re.search(r"https?://", tail[len(m.group(0)):], flags=re.IGNORECASE)
    if m2:
        tail = tail[: len(m.group(0)) + m2.start()]

    for sep in ('"', "'", "<", ">", " ", "\r", "\n", "\t"):
        idx = tail.find(sep)
        if idx >= 0:
            tail = tail[:idx]
            break

    return tail.strip().rstrip("，,。.;；）)]}>")


def _read_image_basic_info(filepath: str) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    try:
        with Image.open(filepath) as img:
            info["format"] = img.format
            info["mode"] = img.mode
            info["width"] = int(img.size[0])
            info["height"] = int(img.size[1])
            if filepath.lower().endswith(".png") and isinstance(getattr(img, "info", None), dict):
                png_text: Dict[str, Any] = {}
                for k, v in img.info.items():
                    if isinstance(v, bytes):
                        png_text[str(k)] = v.decode("utf-8", errors="ignore")
                    else:
                        png_text[str(k)] = v
                info["png_text"] = png_text
    except Exception:
        pass
    return info


def _read_raw_with_pyexiv2(filepath: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    if not HAS_PYEXIV2:
        return {}, {}, {}

    def _read(path: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        with pyexiv2.Image(path) as img:
            xmp = img.read_xmp() or {}
            exif = img.read_exif() or {}
            try:
                iptc = img.read_iptc() or {}
            except Exception:
                iptc = {}
            return dict(xmp), dict(exif), dict(iptc)

    try:
        return _read(filepath)
    except Exception as e:
        err = str(e)
        if (
            ("Illegal byte sequence" in err)
            or ("errno = 42" in err)
            or ("errno = 2" in err)
            or ("No such file or directory" in err)
            or ("Failed to open the data source" in err and os.path.exists(filepath))
        ):
            fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(filepath)[1])
            os.close(fd)
            try:
                shutil.copy2(filepath, tmp_path)
                return _read(tmp_path)
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        raise


def _write_raw_with_pyexiv2(
    filepath: str,
    *,
    xmp_data: Optional[Dict[str, Any]] = None,
    exif_data: Optional[Dict[str, Any]] = None,
    iptc_data: Optional[Dict[str, Any]] = None,
) -> None:
    if not HAS_PYEXIV2:
        raise RuntimeError("pyexiv2 未安装，无法执行高级写入")

    def _write(path: str):
        with pyexiv2.Image(path) as img:
            if xmp_data is not None:
                img.modify_xmp(xmp_data)
            if exif_data is not None:
                img.modify_exif(exif_data)
            if iptc_data is not None:
                img.modify_iptc(iptc_data)

    try:
        _write(filepath)
    except Exception as e:
        err = str(e)
        if ("Illegal byte sequence" in err) or ("errno = 42" in err):
            fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(filepath)[1])
            os.close(fd)
            try:
                shutil.copy2(filepath, tmp_path)
                _write(tmp_path)
                shutil.copy2(tmp_path, filepath)
                return
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        raise


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
        self.minsize(1200, 760)

        self.current_path: Optional[str] = None
        self.current_folder: str = ""
        self.folder_images: List[str] = []
        self.current_index: int = -1

        self._preview_pil: Optional[Image.Image] = None
        self._preview_tk = None
        self._preview_resize_after: Optional[str] = None

        self._last_info: Optional[ImageMetadataInfo] = None
        self._last_basic: Dict[str, Any] = {}
        self._last_xmp: Dict[str, Any] = {}
        self._last_exif: Dict[str, Any] = {}
        self._last_iptc: Dict[str, Any] = {}
        self._folder_index_ready: bool = False

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
        self._public_scraper_active_template_path: str = ""
        self._public_scraper_panel: Optional[tk.Toplevel] = None
        self._scraper_start_btn: Optional[ttk.Button] = None
        self._scraper_stop_btn: Optional[ttk.Button] = None
        self._scraper_resume_btn: Optional[ttk.Button] = None
        self._scraper_monitor_state_var: Optional[tk.StringVar] = None
        self._scraper_monitor_pid_var: Optional[tk.StringVar] = None
        self._scraper_monitor_elapsed_var: Optional[tk.StringVar] = None
        self._scraper_monitor_counts_var: Optional[tk.StringVar] = None
        self._scraper_monitor_progress_var: Optional[tk.StringVar] = None
        self._scraper_monitor_progress_bar: Optional[ttk.Progressbar] = None
        self._scraper_monitor_paths_var: Optional[tk.StringVar] = None
        self._scraper_monitor_log_text: Optional[tk.Text] = None
        self._scraper_monitor_progress_table: Optional[ttk.Treeview] = None
        self._scraper_monitor_progress_done_table: Optional[ttk.Treeview] = None
        self._scraper_monitor_last_log_snapshot: str = ""
        self._scraper_monitor_last_progress_snapshot: str = ""
        self._scraper_monitor_last_opened_path: str = ""
        self._scraper_monitor_total_hint: int = 0

        self._build_ui()
        self._setup_edit_shortcuts_and_menu()
        self.protocol("WM_DELETE_WINDOW", self._on_app_close)

        if start_target:
            self._load_target(start_target)

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill=tk.X)

        ttk.Label(top, text="路径:").pack(side=tk.LEFT)
        self.path_var = tk.StringVar()
        path_entry = ttk.Entry(top, textvariable=self.path_var, width=96)
        path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        path_entry.bind("<Return>", lambda _e: self._load_target(self.path_var.get().strip()))

        ttk.Button(top, text="打开图片", command=self._browse_image).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(top, text="打开文件夹", command=self._browse_folder).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="上一张", command=self._goto_prev).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(top, text="下一张", command=self._goto_next).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="刷新", command=self._refresh_current).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="批量下载器(旧版)", command=self._open_batch_downloader).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            top,
            text="公共抓取面板",
            command=self._open_public_scraper_panel,
        ).pack(side=tk.LEFT, padx=(6, 0))

        info_bar = ttk.Frame(self, padding=(10, 0, 10, 8))
        info_bar.pack(fill=tk.X)

        self.position_var = tk.StringVar(value="0 / 0")
        ttk.Label(info_bar, textvariable=self.position_var, width=16).pack(side=tk.LEFT)

        backend_text = "pyexiv2: 已启用" if HAS_PYEXIV2 else "pyexiv2: 未安装（写入不可用）"
        ttk.Label(info_bar, text=backend_text).pack(side=tk.LEFT, padx=(4, 14))

        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(info_bar, textvariable=self.status_var).pack(side=tk.LEFT)

        dnd_text = "拖拽打开: 已启用" if HAS_TK_DND else "拖拽打开: 未启用（安装 tkinterdnd2）"
        ttk.Label(info_bar, text=dnd_text).pack(side=tk.RIGHT)

        main_pane = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
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

    def _build_edit_tab(self, parent):
        wrap = ttk.Frame(parent, padding=10)
        wrap.pack(fill=tk.BOTH, expand=True)

        form = ttk.Frame(wrap)
        form.pack(fill=tk.X)

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

        desc_box = ttk.Labelframe(wrap, text="描述", padding=6)
        desc_box.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        self.desc_text = tk.Text(desc_box, height=8, wrap=tk.WORD)
        desc_scroll = ttk.Scrollbar(desc_box, orient=tk.VERTICAL, command=self.desc_text.yview)
        self.desc_text.configure(yscrollcommand=desc_scroll.set)
        self.desc_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        desc_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        extra_box = ttk.Labelframe(wrap, text="扩展字段（自适应）", padding=6)
        extra_box.pack(fill=tk.BOTH, expand=False, pady=(8, 0))
        self.extra_profile_rows: List[Dict[str, Any]] = []
        self.extra_profile_rows_frame: Optional[Any] = None

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

        rows_holder = ttk.Frame(extra_box)
        rows_holder.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
        self.extra_profile_rows_frame = rows_holder

        ttk.Label(
            extra_box,
            text='值支持普通文本；如需结构化可填 JSON（例如 {"rank":"三级警督"}）',
            bootstyle="secondary",
        ).pack(fill=tk.X, pady=(6, 0))

        btns = ttk.Frame(wrap)
        btns.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(btns, text="保存元数据", command=self._save_structured).pack(side=tk.RIGHT)
        ttk.Button(btns, text="保存并下一张", command=self._save_structured_and_next).pack(side=tk.RIGHT, padx=6)
        ttk.Button(btns, text="自动填空(手动)", command=self._apply_autofill_suggestion).pack(side=tk.LEFT, padx=6)

        hint = "提示：结构化保存会更新 XMP + titi:meta，并保留未知字段。"
        ttk.Label(wrap, text=hint).pack(fill=tk.X, pady=(8, 0))

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
        start_state = tk.DISABLED if running else tk.NORMAL
        stop_state = tk.NORMAL if running else tk.DISABLED
        resume_state = tk.DISABLED if running else tk.NORMAL
        for btn, state in [
            (self._scraper_start_btn, start_state),
            (self._scraper_stop_btn, stop_state),
            (self._scraper_resume_btn, resume_state),
        ]:
            if btn is None:
                continue
            try:
                btn.configure(state=state)
            except Exception:
                pass

    def _on_public_scraper_panel_close(self):
        panel = self._public_scraper_panel
        self._public_scraper_panel = None
        self._scraper_start_btn = None
        self._scraper_stop_btn = None
        self._scraper_resume_btn = None
        self._scraper_monitor_state_var = None
        self._scraper_monitor_pid_var = None
        self._scraper_monitor_elapsed_var = None
        self._scraper_monitor_counts_var = None
        self._scraper_monitor_progress_var = None
        self._scraper_monitor_progress_bar = None
        self._scraper_monitor_paths_var = None
        self._scraper_monitor_log_text = None
        self._scraper_monitor_progress_table = None
        self._scraper_monitor_progress_done_table = None
        self._scraper_monitor_last_log_snapshot = ""
        self._scraper_monitor_last_progress_snapshot = ""
        self._scraper_monitor_last_opened_path = ""
        self._scraper_monitor_total_hint = 0
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
            text="中止任务",
            command=self._stop_public_scraper_from_gui,
        )
        self._scraper_stop_btn.pack(side=tk.LEFT, padx=(8, 0))
        self._scraper_resume_btn = ttk.Button(
            top,
            text="继续任务",
            command=self._continue_public_scraper_from_gui,
        )
        self._scraper_resume_btn.pack(side=tk.LEFT, padx=(8, 0))
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
            value="进度: 总目标 0 / 已完成 0 (0.0%) / 详情 0 / 图片 0 / 元数据 0"
        )
        ttk.Label(line2, textvariable=self._scraper_monitor_counts_var).pack(side=tk.LEFT)

        line3 = ttk.Frame(panel)
        line3.pack(fill=tk.X, pady=(4, 0))
        self._scraper_monitor_progress_var = tk.StringVar(value="完成进度：0 / 0 (0.0%)")
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
        ttk.Button(line4, text="打开日志", command=self._open_scraper_log_path, width=10).pack(side=tk.RIGHT, padx=(0, 6))

        split = ttk.Panedwindow(panel, orient=tk.VERTICAL)
        split.pack(fill=tk.BOTH, expand=True, pady=(6, 0))

        progress_box = ttk.Labelframe(split, text="任务明细（按发现顺序）", padding=4)
        logs_box = ttk.Labelframe(split, text="运行日志(最近30行)", padding=4)
        split.add(progress_box, weight=3)
        split.add(logs_box, weight=1)

        progress_split = ttk.Panedwindow(progress_box, orient=tk.VERTICAL)
        progress_split.pack(fill=tk.BOTH, expand=True)
        pending_box = ttk.Labelframe(progress_split, text="待处理条目（上）", padding=4)
        done_box = ttk.Labelframe(progress_split, text="已完成条目（下）", padding=4)
        progress_split.add(pending_box, weight=3)
        progress_split.add(done_box, weight=2)
        self._scraper_monitor_progress_table = self._build_scraper_progress_tree(pending_box, height=9)
        self._scraper_monitor_progress_done_table = self._build_scraper_progress_tree(done_box, height=6)

        log_wrap = ttk.Frame(logs_box)
        log_wrap.pack(fill=tk.BOTH, expand=True)
        self._scraper_monitor_log_text = tk.Text(log_wrap, height=6, wrap=tk.NONE)
        log_scroll = ttk.Scrollbar(log_wrap, orient=tk.VERTICAL, command=self._scraper_monitor_log_text.yview)
        self._scraper_monitor_log_text.configure(yscrollcommand=log_scroll.set, state=tk.DISABLED)
        self._scraper_monitor_log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    def _build_scraper_progress_tree(self, parent: Any, height: int = 8) -> ttk.Treeview:
        table_wrap = ttk.Frame(parent)
        table_wrap.pack(fill=tk.BOTH, expand=True)
        columns = ("idx", "name", "detail", "image", "meta", "reason", "detail_url", "image_path")
        table = ttk.Treeview(table_wrap, columns=columns, show="headings", height=max(4, int(height or 8)))
        table.heading("idx", text="#")
        table.heading("name", text="姓名")
        table.heading("detail", text="详情")
        table.heading("image", text="图片")
        table.heading("meta", text="元数据")
        table.heading("reason", text="说明/错误")
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
        y_table.pack(side=tk.RIGHT, fill=tk.Y)
        x_table.pack(side=tk.BOTTOM, fill=tk.X)
        return table

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        s = max(0, int(seconds))
        h = s // 3600
        m = (s % 3600) // 60
        sec = s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"

    @staticmethod
    def _read_text_tail(path: str, max_lines: int = 30) -> str:
        if not path or (not os.path.exists(path)):
            return ""
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                lines = list(deque(f, max_lines))
            return "".join(lines).strip()
        except Exception:
            return ""

    @staticmethod
    def _read_jsonl_rows(path: str, max_rows: int = 0) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if (not path) or (not os.path.exists(path)):
            return rows
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(obj, dict):
                        rows.append(obj)
                        if max_rows > 0 and len(rows) >= max_rows:
                            break
        except Exception:
            return []
        return rows

    @staticmethod
    def _merge_status_reason(entry: Dict[str, Any], msg: str):
        text = str(msg or "").strip()
        if not text:
            return
        old = str(entry.get("reason", "")).strip()
        if not old:
            entry["reason"] = text
            return
        if text in old:
            return
        entry["reason"] = f"{old} | {text}"

    @staticmethod
    def _normalize_existing_path(path_value: Any) -> str:
        path = str(path_value or "").strip()
        if not path:
            return ""
        try:
            normalized = os.path.abspath(path)
        except Exception:
            normalized = path
        return normalized if os.path.isfile(normalized) else ""

    @staticmethod
    def _read_json_file(path: str) -> Dict[str, Any]:
        if (not path) or (not os.path.exists(path)):
            return {}
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            num = int(value)
            return num if num > 0 else 0
        except Exception:
            return 0

    def _estimate_scraper_total_target(self, output_root: str) -> int:
        if not output_root:
            return 0
        candidates: List[int] = []

        crawl_report = self._read_json_file(os.path.join(output_root, "reports", "crawl_report.json"))
        if crawl_report:
            metrics = crawl_report.get("metrics_this_run")
            if isinstance(metrics, dict):
                candidates.append(self._safe_int(metrics.get("detail_requests_enqueued")))
            totals = crawl_report.get("totals_on_disk")
            if isinstance(totals, dict):
                candidates.append(self._safe_int(totals.get("profiles")))

        seen_detail_urls: set[str] = set()
        list_path = os.path.join(output_root, "raw", "list_records.jsonl")
        if os.path.exists(list_path):
            try:
                with open(list_path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        raw = line.strip()
                        if not raw:
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(obj, dict):
                            continue
                        detail_url = str(obj.get("detail_url", "")).strip()
                        if detail_url:
                            seen_detail_urls.add(detail_url)
            except Exception:
                pass
        candidates.append(len(seen_detail_urls))
        candidates.append(self._count_jsonl_rows(os.path.join(output_root, "raw", "profiles.jsonl")))
        candidates.append(self._count_jsonl_rows(os.path.join(output_root, "downloads", "image_downloads.jsonl")))
        return max(candidates) if candidates else 0

    def _collect_scraper_progress_rows(self, output_root: str, max_rows: int = 3000) -> List[Dict[str, str]]:
        list_path = os.path.join(output_root, "raw", "list_records.jsonl")
        profile_path = os.path.join(output_root, "raw", "profiles.jsonl")
        manifest_path = os.path.join(output_root, "downloads", "image_downloads.jsonl")
        metadata_queue_path = os.path.join(output_root, "raw", "metadata_queue.jsonl")
        metadata_result_path = os.path.join(output_root, "raw", "metadata_write_results.jsonl")
        review_path = os.path.join(output_root, "raw", "review_queue.jsonl")
        failures_path = os.path.join(output_root, "raw", "failures.jsonl")

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
            }
            rows.append(row)
            if row["detail_url"]:
                detail_index[row["detail_url"]] = len(rows) - 1
            return len(rows) - 1

        for item in self._read_jsonl_rows(list_path, max_rows=max_rows * 2):
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
                self._merge_status_reason(rows[row_pos], "列表缺少详情链接")
            if len(rows) >= max_rows:
                break

        for item in self._read_jsonl_rows(profile_path, max_rows=max_rows * 2):
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
            if not image_url and row["image"] != "√":
                row["image"] = "×"
                self._merge_status_reason(row, "详情缺少图片链接")
            if len(rows) >= max_rows and detail_url not in detail_index:
                break

        for item in self._read_jsonl_rows(manifest_path, max_rows=max_rows * 3):
            detail_url = str(item.get("detail_url", "")).strip()
            if not detail_url:
                continue
            row_pos = detail_index.get(detail_url)
            if row_pos is None:
                row_pos = _append_row(str(item.get("name", "")).strip(), detail_url)
            row = rows[row_pos]
            row["image"] = "√"
            candidate = (
                self._normalize_existing_path(item.get("named_path"))
                or self._normalize_existing_path(item.get("saved_path"))
            )
            if candidate:
                row["image_path"] = candidate

        for item in self._read_jsonl_rows(metadata_queue_path, max_rows=max_rows * 3):
            detail_url = str(item.get("detail_url", "")).strip()
            if not detail_url:
                continue
            row_pos = detail_index.get(detail_url)
            if row_pos is None:
                row_pos = _append_row(str(item.get("name", "")).strip(), detail_url)
            candidate = self._normalize_existing_path(item.get("local_image_path"))
            if candidate:
                rows[row_pos]["image_path"] = candidate

        for item in self._read_jsonl_rows(metadata_result_path, max_rows=max_rows * 3):
            detail_url = str(item.get("detail_url", "")).strip()
            if not detail_url:
                continue
            row_pos = detail_index.get(detail_url)
            if row_pos is None:
                row_pos = _append_row("", detail_url)
            row = rows[row_pos]
            status = str(item.get("status", "")).strip().lower()
            if status == "ok":
                row["meta"] = "√"
                candidate = self._normalize_existing_path(item.get("output_path"))
                if candidate:
                    row["image_path"] = candidate
            elif status:
                row["meta"] = "×"
                self._merge_status_reason(row, str(item.get("error", "")).strip() or f"元数据失败({status})")

        for item in self._read_jsonl_rows(review_path, max_rows=max_rows * 3):
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
            if lower_reason.startswith("metadata_"):
                if row["meta"] != "√":
                    row["meta"] = "×"
            if "missing_required_fields" in lower_reason and row["detail"] != "√":
                row["detail"] = "×"
            self._merge_status_reason(row, reason)

        for item in self._read_jsonl_rows(failures_path, max_rows=max_rows * 3):
            url = str(item.get("url", "")).strip()
            if not url:
                continue
            row_pos = detail_index.get(url)
            if row_pos is None:
                continue
            row = rows[row_pos]
            phase = str((item.get("context") or {}).get("phase", "")).strip().lower() if isinstance(item.get("context"), dict) else ""
            if phase == "detail":
                row["detail"] = "×"
            self._merge_status_reason(row, str(item.get("reason", "")).strip())

        output: List[Dict[str, str]] = []
        for row in rows[:max_rows]:
            detail_status = row["detail"]
            image_status = row["image"]
            meta_status = row["meta"]
            if detail_status == "√" and row.get("_has_image_url") and image_status == "…":
                image_status = "⌛"
            if image_status == "√" and meta_status == "…":
                meta_status = "⌛"
            output.append(
                {
                    "idx": str(row.get("idx", "")),
                    "name": str(row.get("name", "")).strip(),
                    "detail": detail_status,
                    "image": image_status,
                    "meta": meta_status,
                    "reason": str(row.get("reason", "")).strip(),
                    "detail_url": str(row.get("detail_url", "")).strip(),
                    "image_path": str(row.get("image_path", "")).strip(),
                }
            )
        return output

    @staticmethod
    def _is_scraper_row_completed(row: Dict[str, Any]) -> bool:
        if not isinstance(row, dict):
            return False
        ok_tokens = {"√", "✓"}
        detail_ok = str(row.get("detail", "")).strip() in ok_tokens
        image_ok = str(row.get("image", "")).strip() in ok_tokens
        meta_ok = str(row.get("meta", "")).strip() in ok_tokens
        return detail_ok and image_ok and meta_ok

    def _refresh_scraper_progress_table(self, output_root: str, rows: Optional[List[Dict[str, Any]]] = None):
        pending_table = self._scraper_monitor_progress_table
        done_table = self._scraper_monitor_progress_done_table
        if (pending_table is None) and (done_table is None):
            return
        if rows is None:
            rows = self._collect_scraper_progress_rows(output_root)
        pending_rows: List[Dict[str, Any]] = []
        done_rows: List[Dict[str, Any]] = []
        for row in rows:
            if self._is_scraper_row_completed(row):
                done_rows.append(row)
            else:
                pending_rows.append(row)
        snapshot = json.dumps({"pending": pending_rows, "done": done_rows}, ensure_ascii=False)
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
                        values=(
                            row.get("idx", ""),
                            row.get("name", ""),
                            row.get("detail", ""),
                            row.get("image", ""),
                            row.get("meta", ""),
                            row.get("reason", ""),
                            row.get("detail_url", ""),
                            row.get("image_path", ""),
                        ),
                    )
            if done_table is not None:
                done_table.delete(*done_table.get_children())
                for row in done_rows:
                    done_table.insert(
                        "",
                        tk.END,
                        values=(
                            row.get("idx", ""),
                            row.get("name", ""),
                            row.get("detail", ""),
                            row.get("image", ""),
                            row.get("meta", ""),
                            row.get("reason", ""),
                            row.get("detail_url", ""),
                            row.get("image_path", ""),
                        ),
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

        for table in prioritized:
            try:
                selected = table.selection()
                if not selected:
                    continue
                values = table.item(selected[0], "values")
                if isinstance(values, (list, tuple)):
                    return tuple(values)
            except Exception:
                continue
        return tuple()

    def _resolve_scraper_selected_image_path(self) -> str:
        values = self._get_selected_scraper_progress_values()
        if not isinstance(values, (list, tuple)) or len(values) < 8:
            return ""
        image_status = str(values[3] or "").strip()
        image_path = str(values[7] or "").strip()
        if image_status not in {"√", "✓"}:
            return ""
        if not image_path:
            return ""
        try:
            normalized = os.path.abspath(image_path)
        except Exception:
            normalized = image_path
        return normalized if os.path.isfile(normalized) else ""

    def _open_selected_scraper_result(self):
        target = self._resolve_scraper_selected_image_path()
        if not target:
            messagebox.showinfo("提示", "当前选中项还没有可打开的本地图片。", parent=self)
            return
        if os.path.abspath(str(self.current_path or "")) == os.path.abspath(target):
            return
        self._load_target(target)
        self._scraper_monitor_last_opened_path = target
        self._set_status(f"已打开：{os.path.basename(target)}")
        self._focus_main_preview_from_scraper()

    def _on_scraper_progress_row_selected(self, _event=None):
        try:
            event_widget = getattr(_event, "widget", None)
            if event_widget is not None:
                for table in self._iter_scraper_progress_tables():
                    if table is event_widget:
                        continue
                    table.selection_remove(table.selection())
        except Exception:
            pass
        target = self._resolve_scraper_selected_image_path()
        if not target:
            return
        if target == self._scraper_monitor_last_opened_path:
            return
        if os.path.abspath(str(self.current_path or "")) == os.path.abspath(target):
            self._scraper_monitor_last_opened_path = target
            return
        try:
            self._load_target(target)
            self._scraper_monitor_last_opened_path = target
            self._set_status(f"已打开：{os.path.basename(target)}")
            self._focus_main_preview_from_scraper()
        except Exception:
            pass

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
        target = self._public_scraper_named_dir
        if target and os.path.isdir(target):
            os.startfile(target)
            return
        messagebox.showinfo("提示", "当前暂无可打开的图片目录。", parent=self)

    def _open_scraper_log_path(self):
        target = self._public_scraper_log_path
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
        if not output_root:
            return ""
        path = os.path.join(output_root, "crawl_record.json")
        return path if os.path.exists(path) else ""

    @staticmethod
    def _read_scraper_backoff_state(output_root: str) -> Dict[str, str]:
        if not output_root:
            return {"blocked_until": "", "blocked_reason": ""}
        path = os.path.join(output_root, "state", "backoff_state.json")
        if not os.path.exists(path):
            return {"blocked_until": "", "blocked_reason": ""}
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return {"blocked_until": "", "blocked_reason": ""}
            return {
                "blocked_until": str(payload.get("blocked_until", "")).strip(),
                "blocked_reason": str(payload.get("blocked_reason", "")).strip(),
            }
        except Exception:
            return {"blocked_until": "", "blocked_reason": ""}

    def _refresh_scraper_monitor_panel(self):
        if self._scraper_monitor_state_var is not None:
            self._scraper_monitor_state_var.set(f"状态: {self._public_scraper_runtime_state}")

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
            known_rows = len(rows)
            estimated_total = self._estimate_scraper_total_target(output_root)
            total_target = max(known_rows, estimated_total, self._scraper_monitor_total_hint)
            self._scraper_monitor_total_hint = total_target
            progress_pct = (completed_rows / total_target * 100.0) if total_target > 0 else 0.0

            list_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "list_records.jsonl"))
            profile_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "profiles.jsonl"))
            image_rows = self._count_jsonl_rows(os.path.join(output_root, "downloads", "image_downloads.jsonl"))
            metadata_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "metadata_write_results.jsonl"))
            if self._scraper_monitor_counts_var is not None:
                self._scraper_monitor_counts_var.set(
                    "进度: "
                    f"总目标 {total_target} / 已完成 {completed_rows} ({progress_pct:.1f}%) / "
                    f"列表 {list_rows} / 详情 {profile_rows} / 图片 {image_rows} / 元数据 {metadata_rows}"
                )
            if self._scraper_monitor_progress_var is not None:
                self._scraper_monitor_progress_var.set(
                    f"完成进度：{completed_rows} / {total_target} ({progress_pct:.1f}%)"
                )
            if self._scraper_monitor_progress_bar is not None:
                try:
                    self._scraper_monitor_progress_bar["value"] = progress_pct
                except Exception:
                    pass
            if self._scraper_monitor_paths_var is not None:
                self._scraper_monitor_paths_var.set(f"输出: {self._public_scraper_named_dir or output_root}")
            self._refresh_scraper_progress_table(output_root, rows=rows)
        else:
            if self._scraper_monitor_counts_var is not None:
                self._scraper_monitor_counts_var.set("进度: 总目标 0 / 已完成 0 (0.0%) / 详情 0 / 图片 0 / 元数据 0")
            if self._scraper_monitor_progress_var is not None:
                self._scraper_monitor_progress_var.set("完成进度：0 / 0 (0.0%)")
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
            self._scraper_monitor_last_progress_snapshot = ""
            self._scraper_monitor_last_opened_path = ""
            self._scraper_monitor_total_hint = 0

        tail = self._read_text_tail(self._public_scraper_log_path, max_lines=30)
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

        try:
            basic = _read_image_basic_info(path)
            info = read_image_metadata(path)
            raw_xmp = dict(getattr(info, "other_xmp", {}) or {})
            raw_exif = dict(getattr(info, "other_exif", {}) or {})
            raw_iptc = dict(getattr(info, "other_iptc", {}) or {})

            # 回退：极少数情况下结构化读取未带出全量命名空间，再补一次原始读取。
            if HAS_PYEXIV2 and (not raw_xmp) and (not raw_exif) and (not raw_iptc):
                try:
                    raw_xmp, raw_exif, raw_iptc = _read_raw_with_pyexiv2(path)
                except Exception:
                    raw_xmp, raw_exif, raw_iptc = {}, {}, {}

            self._last_info = info
            self._last_basic = basic
            self._last_xmp = raw_xmp or {}
            self._last_exif = raw_exif or {}
            self._last_iptc = raw_iptc or {}

            self._render_preview(path)
            self._fill_edit_form(info)
            self._mark_all_tab_data_dirty()
            self._refresh_visible_tab_data()
            self._set_status("完成")
        except Exception as e:
            self._set_status("读取失败")
            messagebox.showerror("读取失败", str(e))

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
    def _guess_public_site_name(start_url: str) -> str:
        parsed = urllib.parse.urlparse(str(start_url or "").strip())
        host = (parsed.hostname or "site").strip().lower()
        first_path = parsed.path.strip("/").split("/", 1)[0].strip().lower()
        seed = f"{host}_{first_path or 'index'}"
        normalized = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
        if normalized:
            return normalized
        return f"site_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    @staticmethod
    def _sanitize_public_subdir_name(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
        text = re.sub(r"\s+", " ", text).strip().strip(". ")
        text = re.sub(r"_+", "_", text).strip("_")
        return text

    @staticmethod
    def _extract_public_year_token(*texts: Any) -> str:
        for raw in texts:
            text = str(raw or "")
            m = re.search(r"(?<!\d)(19|20)\d{2}(?!\d)", text)
            if m:
                return m.group(0)
        return ""

    @staticmethod
    def _guess_public_unit_name(start_url: str, payload: Dict[str, Any], template_path: str = "") -> str:
        rules = payload.get("rules")
        if not isinstance(rules, dict):
            rules = {}
        for key in ("unit_name", "organization_name", "org_name", "unit"):
            candidate = str(rules.get(key, "")).strip()
            if candidate:
                return candidate

        host = (urllib.parse.urlparse(str(start_url or "")).hostname or "").strip().lower()
        if "tiantonglaw.com" in host:
            return "天同律师事务所"
        if host.endswith("mps.gov.cn") or ("mps.gov.cn" in host):
            return "公安部"

        site_name = str(payload.get("site_name", "")).strip()
        if site_name:
            cleaned = re.sub(r"[_\-]+", " ", site_name).strip()
            if cleaned:
                return cleaned

        template_name = os.path.splitext(os.path.basename(str(template_path or "").strip()))[0]
        if template_name:
            return template_name

        if host:
            parts = [p for p in host.split(".") if p]
            if len(parts) >= 2:
                return parts[-2]
            return host
        return "单位"

    def _resolve_public_task_output_root(
        self,
        base_output_root: str,
        start_url: str,
        payload: Dict[str, Any],
        template_path: str = "",
    ) -> str:
        base_root = os.path.abspath(str(base_output_root or "").strip() or self._suggest_public_scraper_output_root(start_url))
        rules = payload.get("rules")
        if not isinstance(rules, dict):
            rules = {}
            payload["rules"] = rules

        auto_unit_subdir = bool(rules.get("auto_unit_subdir", False))
        if not auto_unit_subdir:
            rules.pop("output_root_parent", None)
            rules.pop("resolved_output_subdir", None)
            rules.pop("resolved_unit_name", None)
            rules.pop("resolved_year", None)
            return base_root

        unit_name = self._sanitize_public_subdir_name(
            str(self._guess_public_unit_name(start_url, payload, template_path) or "")
        )
        site_name = self._sanitize_public_subdir_name(str(payload.get("site_name", "") or ""))
        host = self._sanitize_public_subdir_name(
            str((urllib.parse.urlparse(str(start_url or "")).hostname or "").strip().lower())
        )
        year = self._sanitize_public_subdir_name(
            str(
                rules.get("year_hint")
                or self._extract_public_year_token(
                    start_url,
                    payload.get("site_name", ""),
                    os.path.basename(str(template_path or "")),
                )
            )
        )
        year_suffix = f"_{year}" if year else ""

        pattern = str(rules.get("output_subdir_pattern", "{unit}{year_suffix}") or "").strip()
        if not pattern:
            pattern = "{unit}{year_suffix}"

        format_ctx = {
            "unit": unit_name,
            "year": year,
            "year_suffix": year_suffix,
            "site_name": site_name,
            "host": host,
        }

        class _SafeDict(dict):
            def __missing__(self, key: str) -> str:
                return ""

        try:
            subdir_raw = pattern.format_map(_SafeDict(format_ctx))
        except Exception:
            subdir_raw = f"{unit_name}{year_suffix}".strip()
        subdir_name = self._sanitize_public_subdir_name(subdir_raw)
        if not subdir_name:
            subdir_name = self._sanitize_public_subdir_name(unit_name or site_name or host)
        if not subdir_name:
            rules.pop("output_root_parent", None)
            rules.pop("resolved_output_subdir", None)
            rules.pop("resolved_unit_name", None)
            rules.pop("resolved_year", None)
            return base_root

        resolved_root = os.path.abspath(os.path.join(base_root, subdir_name))
        rules["output_root_parent"] = base_root
        rules["resolved_output_subdir"] = subdir_name
        rules["resolved_unit_name"] = unit_name or site_name or host
        if year:
            rules["resolved_year"] = year
        else:
            rules.pop("resolved_year", None)
        return resolved_root

    @staticmethod
    def _default_public_scraper_template() -> Dict[str, Any]:
        return {
            "site_name": "generic_profiles",
            "start_urls": ["https://example.org/list"],
            "allowed_domains": ["example.org"],
            "user_agent": "D2ILiteArchiveBot/1.0 (+local archival use)",
            "default_headers": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            },
            "image_headers": {},
            "output_root": "data/public_archive/generic_profiles",
            "selectors": {
                "list_item": "article a[href], .list a[href], .news a[href], .item a[href], li a[href], dl dd a[href], a[href]",
                "name": ["::text", "img::attr(alt)", "img::attr(title)"],
                "detail_link": "::attr(href)",
                "list_fields": {},
                "next_page": [
                    "a.next::attr(href)",
                    "a[rel='next']::attr(href)",
                    "xpath://a[contains(@class,'next')]/@href",
                    "xpath://a[contains(normalize-space(),'下一页')]/@href",
                    "xpath://a[contains(normalize-space(),'下页')]/@href",
                ],
                "detail_name": [
                    "h1::text",
                    "h2::text",
                    ".title::text",
                    ".name::text",
                    "meta[property='og:title']::attr(content)",
                    "title::text",
                ],
                "detail_image": [
                    "meta[property='og:image']::attr(content)",
                    ".article img::attr(src)",
                    ".content img::attr(src)",
                    ".detail img::attr(src)",
                    ".main img::attr(src)",
                    "img::attr(src)",
                ],
                "detail_gender": [
                    ".gender::text",
                    "xpath:string(//*[contains(normalize-space(),'性别')][1])",
                ],
                "detail_summary": [
                    ".article p::text",
                    ".content p::text",
                    ".detail p::text",
                    ".main p::text",
                    "article p::text",
                    "p::text",
                ],
                "detail_full_text": [],
                "detail_fields": {},
                "detail_field_labels": {},
            },
            "rules": {
                "obey_robots_txt": False,
                "snapshot_html": True,
                "extract_images": True,
                "write_metadata": True,
                "named_images_dir": "",
                "image_referer_from_detail_url": True,
                "required_fields": ["name", "detail_url", "image_url"],
                "default_gender": "",
                "gender_map": {"男": "male", "女": "female"},
                "field_map": {},
                "detail_field_labels": {},
                "auto_unit_subdir": False,
                "unit_name": "",
                "output_subdir_pattern": "{unit}{year_suffix}",
                "year_hint": "",
                "jsl_clearance_enabled": True,
                "jsl_max_retries": 3,
                "image_download_mode": "requests_jsl",
                "auto_fallback_to_browser": True,
                "browser_engine": "edge",
                "output_mode": "images_only_with_record",
                "keep_record_file": True,
            },
            "crawl": {
                "concurrent_requests": 1,
                "download_delay": 5,
                "autothrottle_start_delay": 5,
                "autothrottle_max_delay": 8,
                "retry_times": 3,
                "timeout_seconds": 30,
                "blocked_statuses": [403, 429],
                "blocked_backoff_hours": 6,
                "suspect_block_consecutive_failures": 3,
                "interval_min_seconds": 5,
                "interval_max_seconds": 8,
                "image_interval_min_seconds": 5,
                "image_interval_max_seconds": 8,
            },
        }

    def _build_public_scraper_runtime_config(
        self,
        start_url: str,
        output_root: str,
        template_path: str = "",
    ) -> Tuple[str, Dict[str, Any]]:
        app_dir = os.path.dirname(__file__)
        template_candidates = [
            template_path,
            os.path.join(app_dir, "scraper", "config.template.generic.json"),
            os.path.join(app_dir, "scraper", "config.example.json"),
        ]
        payload: Dict[str, Any] = {}
        for candidate in template_candidates:
            if not os.path.exists(candidate):
                continue
            try:
                with open(candidate, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    payload = loaded
                    break
            except Exception:
                continue
        if not payload:
            payload = self._default_public_scraper_template()

        payload = json.loads(json.dumps(payload, ensure_ascii=False))

        parsed = urllib.parse.urlparse(start_url)
        host = (parsed.hostname or "").strip().lower()
        if not host:
            raise ValueError("无效链接：无法解析域名")

        allowed_domains: List[str] = []
        for domain in [host, host[4:] if host.startswith("www.") else f"www.{host}"]:
            d = str(domain or "").strip().lower()
            if d and (d not in allowed_domains):
                allowed_domains.append(d)

        site_name = self._guess_public_site_name(start_url)
        referer = f"{parsed.scheme}://{parsed.netloc}/"
        base_output_root = os.path.abspath(str(output_root or "").strip() or self._suggest_public_scraper_output_root(start_url))

        payload["site_name"] = site_name
        payload["start_urls"] = [start_url]
        payload["allowed_domains"] = allowed_domains
        payload["output_root"] = base_output_root

        default_headers = payload.get("default_headers")
        if not isinstance(default_headers, dict):
            default_headers = {}
        if not str(default_headers.get("Referer", "")).strip():
            default_headers["Referer"] = referer
        payload["default_headers"] = default_headers

        image_headers = payload.get("image_headers")
        if not isinstance(image_headers, dict):
            image_headers = {}
        if not str(image_headers.get("Referer", "")).strip():
            image_headers["Referer"] = referer
        payload["image_headers"] = image_headers

        defaults = self._default_public_scraper_template()

        selectors = payload.get("selectors")
        if not isinstance(selectors, dict):
            selectors = {}
        for key, value in defaults["selectors"].items():
            if key not in selectors:
                selectors[key] = value
        payload["selectors"] = selectors

        rules = payload.get("rules")
        if not isinstance(rules, dict):
            rules = {}
        for key, value in defaults["rules"].items():
            if key not in rules:
                rules[key] = value
        # Final images should be written directly into the selected output folder.
        rules["named_images_dir"] = ""
        rules["final_output_root"] = ""
        rules["record_root"] = ""
        rules["default_gender"] = ""
        rules["template_source_path"] = os.path.abspath(template_path) if template_path else ""
        payload["rules"] = rules

        resolved_output_root = self._resolve_public_task_output_root(
            base_output_root,
            start_url,
            payload,
            template_path=template_path,
        )
        payload["output_root"] = resolved_output_root

        crawl = payload.get("crawl")
        if not isinstance(crawl, dict):
            crawl = {}
        for key, value in defaults["crawl"].items():
            if key not in crawl:
                crawl[key] = value
        payload["crawl"] = crawl

        runtime_config_path = os.path.join(resolved_output_root, "state", "runtime_config.json")
        os.makedirs(os.path.dirname(runtime_config_path), exist_ok=True)
        with open(runtime_config_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return runtime_config_path, payload

    def _close_public_scraper_log_handle(self):
        handle = self._public_scraper_log_handle
        self._public_scraper_log_handle = None
        if not handle:
            return
        try:
            handle.close()
        except Exception:
            pass

    @staticmethod
    def _count_jsonl_rows(path: str) -> int:
        if not path or (not os.path.exists(path)):
            return 0
        count = 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        count += 1
        except Exception:
            return 0
        return count

    def _update_public_scraper_progress(self):
        output_root = self._public_scraper_output_root
        if not output_root:
            self._refresh_scraper_monitor_panel()
            return
        rows = self._collect_scraper_progress_rows(output_root)
        completed_rows = sum(1 for row in rows if self._is_scraper_row_completed(row))
        total_target = max(len(rows), self._estimate_scraper_total_target(output_root), self._scraper_monitor_total_hint)
        self._scraper_monitor_total_hint = total_target
        progress_pct = (completed_rows / total_target * 100.0) if total_target > 0 else 0.0
        list_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "list_records.jsonl"))
        profile_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "profiles.jsonl"))
        image_rows = self._count_jsonl_rows(os.path.join(output_root, "downloads", "image_downloads.jsonl"))
        metadata_rows = self._count_jsonl_rows(os.path.join(output_root, "raw", "metadata_write_results.jsonl"))
        text = (
            "抓取中 "
            f"完成:{completed_rows}/{total_target}({progress_pct:.1f}%) "
            f"列表:{list_rows} "
            f"详情:{profile_rows} "
            f"图片:{image_rows} "
            f"元数据:{metadata_rows}"
        )
        if text != self._public_scraper_last_progress_text:
            self._public_scraper_last_progress_text = text
            self._set_status(text)
        self._refresh_scraper_monitor_panel()

    def _suggest_public_scraper_output_root(self, start_url: str) -> str:
        app_dir = os.path.dirname(__file__)
        site_name = self._guess_public_site_name(start_url)
        return os.path.abspath(os.path.join(app_dir, "data", "public_archive", site_name))

    def _public_scraper_templates_dir(self) -> str:
        path = os.path.join(os.path.dirname(__file__), "scraper", "templates")
        os.makedirs(path, exist_ok=True)
        return path

    def _public_scraper_template_state_path(self) -> str:
        state_dir = os.path.join(os.path.dirname(__file__), "scraper", "state")
        os.makedirs(state_dir, exist_ok=True)
        return os.path.join(state_dir, "template_run_state.json")

    def _load_public_scraper_template_states(self) -> Dict[str, Dict[str, str]]:
        path = self._public_scraper_template_state_path()
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return {}
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
        except Exception:
            return {}

    def _save_public_scraper_template_states(self, states: Dict[str, Dict[str, str]]) -> None:
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
        path = self._public_scraper_template_state_path()
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _set_public_scraper_template_state(self, template_path: str, status: str) -> None:
        path = os.path.abspath(str(template_path or "").strip())
        status_text = str(status or "").strip().lower()
        if (not path) or (not status_text):
            return
        states = self._load_public_scraper_template_states()
        states[path] = {
            "status": status_text,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        self._save_public_scraper_template_states(states)

    def _list_public_scraper_templates(self) -> List[Tuple[str, str]]:
        unfinished_pairs: List[Tuple[str, str]] = []
        done_pairs: List[Tuple[str, str]] = []
        templates_dir = self._public_scraper_templates_dir()
        root_dir = os.path.dirname(__file__)
        template_states = self._load_public_scraper_template_states()

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

    def _save_generated_template(self, start_url: str, runtime_config: Dict[str, Any]) -> str:
        payload = json.loads(json.dumps(runtime_config, ensure_ascii=False))
        site_name = self._guess_public_site_name(start_url)
        payload["site_name"] = site_name
        payload["output_root"] = f"data/public_archive/{site_name}"
        rules = payload.get("rules")
        if not isinstance(rules, dict):
            rules = {}
        rules.pop("cleanup_paths", None)
        rules.pop("template_source_path", None)
        rules.pop("generated_template_path", None)
        rules.pop("output_root_parent", None)
        rules.pop("resolved_output_subdir", None)
        rules.pop("resolved_unit_name", None)
        rules.pop("resolved_year", None)
        payload["rules"] = rules

        templates_dir = self._public_scraper_templates_dir()
        base = os.path.join(templates_dir, f"{site_name}.json")
        target = base
        if os.path.exists(target):
            target = os.path.join(
                templates_dir,
                f"{site_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            )
        with open(target, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return target

    def _show_public_scraper_setup_dialog(self, source_hint: str) -> Optional[Dict[str, Any]]:
        defaults = self._default_public_scraper_template()
        crawl_defaults = defaults.get("crawl", {})
        rules_defaults = defaults.get("rules", {})

        initial_url = str(source_hint or "").strip() or "https://"
        initial_output = self._suggest_public_scraper_output_root(initial_url)

        dialog = tk.Toplevel(self)
        dialog.title("公共抓取(通用) 设置")
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)

        container = ttk.Frame(dialog, padding=12)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(1, weight=1)

        url_var = tk.StringVar(value=initial_url)
        output_var = tk.StringVar(value=initial_output)
        interval_min_default = crawl_defaults.get(
            "interval_min_seconds",
            crawl_defaults.get("image_interval_min_seconds", crawl_defaults.get("download_delay", 5)),
        )
        interval_max_default = crawl_defaults.get(
            "interval_max_seconds",
            crawl_defaults.get("image_interval_max_seconds", max(float(interval_min_default), 8.0)),
        )
        try:
            interval_min_default = float(interval_min_default)
        except Exception:
            interval_min_default = 5.0
        try:
            interval_max_default = float(interval_max_default)
        except Exception:
            interval_max_default = max(interval_min_default, 8.0)
        if interval_max_default < interval_min_default:
            interval_max_default = interval_min_default

        interval_min_var = tk.StringVar(value=str(interval_min_default))
        interval_max_var = tk.StringVar(value=str(interval_max_default))
        timeout_var = tk.StringVar(value=str(crawl_defaults.get("timeout_seconds", 30)))
        suspect_failures_default = crawl_defaults.get("suspect_block_consecutive_failures", 3)
        try:
            suspect_failures_default = int(suspect_failures_default)
        except Exception:
            suspect_failures_default = 3
        if suspect_failures_default < 2:
            suspect_failures_default = 2
        suspect_failures_var = tk.StringVar(value=str(suspect_failures_default))
        jsl_var = tk.BooleanVar(value=bool(rules_defaults.get("jsl_clearance_enabled", True)))
        image_mode_raw = str(rules_defaults.get("image_download_mode", "requests_jsl")).strip().lower()
        if image_mode_raw not in {"requests_jsl", "browser"}:
            image_mode_raw = "requests_jsl"
        image_mode_var = tk.StringVar(value=image_mode_raw)
        auto_fallback_var = tk.BooleanVar(value=bool(rules_defaults.get("auto_fallback_to_browser", True)))
        output_minimal_var = tk.BooleanVar(
            value=str(rules_defaults.get("output_mode", "images_only_with_record")).strip().lower()
            in {"images_only", "images_only_with_record"}
        )
        template_pairs = self._list_public_scraper_templates()
        template_auto_label = "自动生成模板（按当前链接）"
        template_label_to_path: Dict[str, str] = {template_auto_label: ""}
        for label, path in template_pairs:
            template_label_to_path[label] = path
        template_var = tk.StringVar(value=template_auto_label)
        save_template_var = tk.BooleanVar(value=True)
        cleanup_template_var = tk.BooleanVar(value=True)
        template_hint_var = tk.StringVar(value="未选择模板时，需手动输入链接。")
        template_start_url_cache: Dict[str, str] = {"url": ""}

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

        def _normalize_template_output_root(path_text: str) -> str:
            raw = str(path_text or "").strip()
            if not raw:
                return ""
            path_obj = Path(raw)
            if not path_obj.is_absolute():
                path_obj = (Path(os.path.dirname(__file__)) / raw).resolve()
            return str(path_obj.resolve())

        def _apply_template_to_form():
            selected_path = str(template_label_to_path.get(template_var.get(), "")).strip()
            if not selected_path:
                template_start_url_cache["url"] = ""
                url_entry.configure(state=tk.NORMAL)
                template_hint_var.set("未选择模板时，需手动输入链接。")
                try:
                    save_tpl_cb.configure(state=tk.NORMAL)
                    cleanup_tpl_cb.configure(state=tk.NORMAL)
                except Exception:
                    pass
                return

            try:
                with open(selected_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if not isinstance(payload, dict):
                    raise ValueError("模板不是 JSON 对象")
            except Exception as e:
                template_start_url_cache["url"] = ""
                template_hint_var.set(f"模板读取失败：{e}")
                url_entry.configure(state=tk.NORMAL)
                return

            start_url = ""
            start_urls = payload.get("start_urls")
            if isinstance(start_urls, list) and start_urls:
                start_url = _normalize_http_url(start_urls[0])
            if not start_url.lower().startswith(("http://", "https://")):
                template_start_url_cache["url"] = ""
                template_hint_var.set("模板缺少有效 start_urls，无法直接启动。")
                url_entry.configure(state=tk.NORMAL)
                return

            template_start_url_cache["url"] = start_url
            url_var.set(start_url)
            url_entry.configure(state=tk.DISABLED)
            template_hint_var.set("已使用模板内置链接，可直接开始任务。")

            crawl_cfg = payload.get("crawl")
            if isinstance(crawl_cfg, dict):
                min_val = crawl_cfg.get(
                    "interval_min_seconds",
                    crawl_cfg.get("image_interval_min_seconds", crawl_cfg.get("download_delay", "")),
                )
                max_val = crawl_cfg.get(
                    "interval_max_seconds",
                    crawl_cfg.get("image_interval_max_seconds", min_val),
                )
                interval_min_var.set(str(min_val))
                interval_max_var.set(str(max_val))
                if "timeout_seconds" in crawl_cfg:
                    timeout_var.set(str(crawl_cfg.get("timeout_seconds", "")))
                suspect_val = crawl_cfg.get(
                    "suspect_block_consecutive_failures",
                    crawl_defaults.get("suspect_block_consecutive_failures", 3),
                )
                suspect_failures_var.set(str(suspect_val))

            rules_cfg = payload.get("rules")
            if isinstance(rules_cfg, dict):
                jsl_var.set(bool(rules_cfg.get("jsl_clearance_enabled", True)))
                mode = str(rules_cfg.get("image_download_mode", "requests_jsl")).strip().lower()
                image_mode_var.set(mode if mode in {"requests_jsl", "browser"} else "requests_jsl")
                auto_fallback_var.set(bool(rules_cfg.get("auto_fallback_to_browser", True)))
                output_minimal_var.set(
                    str(rules_cfg.get("output_mode", "images_only_with_record")).strip().lower()
                    in {"images_only", "images_only_with_record"}
                )

            output_cfg = _normalize_template_output_root(str(payload.get("output_root", "")))
            if output_cfg:
                output_var.set(output_cfg)
            else:
                output_var.set(self._suggest_public_scraper_output_root(start_url))

            save_template_var.set(False)
            cleanup_template_var.set(False)
            try:
                save_tpl_cb.configure(state=tk.DISABLED)
                cleanup_tpl_cb.configure(state=tk.DISABLED)
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
        ttk.Checkbutton(opts, text="仅保留最终图片 + 抓取记录文档", variable=output_minimal_var).grid(
            row=8, column=0, columnspan=4, sticky="w", pady=(4, 0)
        )
        ttk.Label(
            opts,
            text="提示：开启该项会在完成后清理中间文件；若需要“中断后继续”，请先关闭。",
            bootstyle="secondary",
        ).grid(row=9, column=0, columnspan=4, sticky="w", pady=(4, 0))

        actions = ttk.Frame(container)
        actions.grid(row=9, column=0, columnspan=3, sticky="e", pady=(12, 0))

        result: Dict[str, Any] = {}

        def _cancel():
            dialog.destroy()

        def _start():
            selected_template_path = str(template_label_to_path.get(template_var.get(), "")).strip()
            if selected_template_path:
                start_url = str(template_start_url_cache.get("url", "")).strip() or _normalize_http_url(url_var.get())
                if not start_url.lower().startswith(("http://", "https://")):
                    messagebox.showerror("模板错误", "所选模板缺少有效 start_urls，无法直接启动。", parent=dialog)
                    return
            else:
                start_url = _normalize_http_url(url_var.get())
                if not start_url.lower().startswith(("http://", "https://")):
                    messagebox.showerror("链接无效", "请输入有效的 http/https 链接。", parent=dialog)
                    return

            output_root_raw = str(output_var.get() or "").strip()
            output_root = os.path.abspath(
                output_root_raw if output_root_raw else self._suggest_public_scraper_output_root(start_url)
            )

            try:
                interval_min = float(str(interval_min_var.get()).strip())
                interval_max = float(str(interval_max_var.get()).strip())
                timeout_seconds = int(str(timeout_var.get()).strip())
                suspect_failures = int(str(suspect_failures_var.get()).strip())
            except Exception:
                messagebox.showerror("参数错误", "间隔、超时、连续失败阈值必须是数字。", parent=dialog)
                return

            if interval_min < 0.1:
                messagebox.showerror("参数错误", "统一间隔最小值必须 >= 0.1 秒。", parent=dialog)
                return
            if interval_max < interval_min:
                interval_max = interval_min
            if timeout_seconds < 5:
                messagebox.showerror("参数错误", "请求超时必须 >= 5 秒。", parent=dialog)
                return
            if suspect_failures < 2:
                messagebox.showerror("参数错误", "连续失败阈值必须 >= 2。", parent=dialog)
                return

            result["start_url"] = start_url
            result["output_root"] = output_root
            result["interval_min"] = round(interval_min, 3)
            result["interval_max"] = round(interval_max, 3)
            result["timeout_seconds"] = int(timeout_seconds)
            result["suspect_block_consecutive_failures"] = int(suspect_failures)
            result["jsl_enabled"] = bool(jsl_var.get())
            result["image_download_mode"] = str(image_mode_var.get() or "requests_jsl").strip().lower()
            result["auto_fallback_to_browser"] = bool(auto_fallback_var.get())
            result["output_minimal"] = bool(output_minimal_var.get())
            result["template_path"] = selected_template_path
            result["save_generated_template"] = bool(save_template_var.get()) and (not selected_template_path)
            result["cleanup_generated_template"] = bool(cleanup_template_var.get()) and (not selected_template_path)
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
        if self._public_scraper_proc and (self._public_scraper_proc.poll() is None):
            messagebox.showinfo("提示", "抓取任务已在运行中。", parent=self)
            return

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
            config_path, runtime_config = self._build_public_scraper_runtime_config(
                start_url,
                output_root,
                template_path=template_path,
            )
        except Exception as e:
            messagebox.showerror("启动失败", f"无法生成抓取配置：\n{e}", parent=self)
            return

        output_root = os.path.abspath(str(runtime_config.get("output_root", output_root)))

        crawl = runtime_config.get("crawl")
        if not isinstance(crawl, dict):
            crawl = {}
        rules = runtime_config.get("rules")
        if not isinstance(rules, dict):
            rules = {}

        interval_min = float(setup["interval_min"])
        interval_max = float(setup["interval_max"])
        crawl["interval_min_seconds"] = interval_min
        crawl["interval_max_seconds"] = interval_max
        crawl["download_delay"] = interval_min
        crawl["autothrottle_start_delay"] = interval_min
        crawl["autothrottle_max_delay"] = interval_max
        crawl["image_interval_min_seconds"] = interval_min
        crawl["image_interval_max_seconds"] = interval_max
        crawl["timeout_seconds"] = int(setup["timeout_seconds"])
        crawl["suspect_block_consecutive_failures"] = max(
            2,
            int(setup.get("suspect_block_consecutive_failures", crawl.get("suspect_block_consecutive_failures", 3))),
        )
        rules["jsl_clearance_enabled"] = bool(setup["jsl_enabled"])
        mode = str(setup.get("image_download_mode", "requests_jsl")).strip().lower()
        rules["image_download_mode"] = mode if mode in {"requests_jsl", "browser"} else "requests_jsl"
        rules["auto_fallback_to_browser"] = bool(setup.get("auto_fallback_to_browser", True))
        if mode == "browser":
            rules["browser_engine"] = str(rules.get("browser_engine", "edge")).strip().lower() or "edge"
        if bool(setup.get("output_minimal", True)):
            rules["output_mode"] = "images_only_with_record"
            rules["keep_record_file"] = True
        else:
            rules["output_mode"] = "full"
            rules["keep_record_file"] = True

        generated_template_path = ""
        if (not template_path) and bool(setup.get("save_generated_template", True)):
            try:
                generated_template_path = self._save_generated_template(start_url, runtime_config)
                rules["generated_template_path"] = generated_template_path
                if bool(setup.get("cleanup_generated_template", True)):
                    cleanup_paths = rules.get("cleanup_paths", [])
                    if not isinstance(cleanup_paths, list):
                        cleanup_paths = []
                    if generated_template_path not in cleanup_paths:
                        cleanup_paths.append(generated_template_path)
                    rules["cleanup_paths"] = cleanup_paths
            except Exception:
                generated_template_path = ""

        runtime_config["crawl"] = crawl
        runtime_config["rules"] = rules

        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(runtime_config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            messagebox.showerror("启动失败", f"无法写入运行配置：\n{e}", parent=self)
            return

        named_dir_cfg = (
            runtime_config.get("rules", {}).get("named_images_dir", "")
            if isinstance(runtime_config.get("rules"), dict)
            else ""
        )
        named_dir_raw = str(named_dir_cfg or "").strip()
        if not named_dir_raw:
            named_dir = os.path.abspath(output_root)
        else:
            named_dir = named_dir_raw if os.path.isabs(named_dir_raw) else os.path.join(output_root, named_dir_raw)
            named_dir = os.path.abspath(named_dir)
        log_path = os.path.join(output_root, "reports", "gui_public_scraper.log")

        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        try:
            log_handle = open(log_path, "a", encoding="utf-8")
            log_handle.write(
                "\n\n=== D2I Public Scraper Run "
                + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " ===\n"
            )
            log_handle.flush()
        except Exception as e:
            messagebox.showerror("启动失败", f"无法创建日志文件：\n{e}", parent=self)
            return

        cmd = [
            sys.executable,
            script_path,
            "--config",
            config_path,
            "--output-root",
            output_root,
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(script_path) or ".",
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:
            try:
                log_handle.close()
            except Exception:
                pass
            messagebox.showerror("启动失败", f"无法启动抓取任务：\n{e}", parent=self)
            return

        self._public_scraper_proc = proc
        self._public_scraper_output_root = output_root
        self._public_scraper_named_dir = named_dir
        self._public_scraper_config_path = config_path
        self._public_scraper_log_path = log_path
        self._public_scraper_log_handle = log_handle
        self._public_scraper_last_progress_text = ""
        self._public_scraper_started_at = time.time()
        self._public_scraper_runtime_state = "运行中"
        runtime_rules = runtime_config.get("rules")
        if not isinstance(runtime_rules, dict):
            runtime_rules = {}
        active_template_path = (
            str(template_path or "").strip()
            or str(runtime_rules.get("template_source_path", "")).strip()
            or str(runtime_rules.get("generated_template_path", "")).strip()
        )
        self._public_scraper_active_template_path = os.path.abspath(active_template_path) if active_template_path else ""
        if self._public_scraper_active_template_path:
            self._set_public_scraper_template_state(self._public_scraper_active_template_path, "pending")
        self._set_scraper_control_buttons(running=True)
        self._set_status("通用抓取已启动（后台运行）")
        self._refresh_scraper_monitor_panel()
        self._schedule_public_scraper_poll()
        template_msg = ""
        used_template_path = str(runtime_config.get("rules", {}).get("template_source_path", "")).strip()
        generated_template_path = str(runtime_config.get("rules", {}).get("generated_template_path", "")).strip()
        image_mode = str(runtime_config.get("rules", {}).get("image_download_mode", "requests_jsl")).strip().lower()
        image_mode_text = "浏览器模式(慢稳)" if image_mode == "browser" else "请求模式(快)"
        folder_msg = ""
        resolved_subdir = str(runtime_config.get("rules", {}).get("resolved_output_subdir", "")).strip()
        if resolved_subdir:
            folder_msg = f"\n任务子目录：{resolved_subdir}\n"
        if used_template_path:
            template_msg = f"\n模板：\n{used_template_path}"
        elif generated_template_path:
            template_msg = f"\n模板（本次生成）：\n{generated_template_path}"
        messagebox.showinfo(
            "已启动",
            "抓取任务已在后台启动。\n"
            f"任务进程 PID: {proc.pid}\n\n"
            f"图片下载方式：{image_mode_text}\n\n"
            f"{folder_msg}"
            f"最终图片会输出到：\n{named_dir}\n\n"
            f"运行日志：\n{log_path}{template_msg}",
            parent=self,
        )

    def _continue_public_scraper_from_gui(self):
        if self._public_scraper_proc and (self._public_scraper_proc.poll() is None):
            messagebox.showinfo("提示", "抓取任务已在运行中。", parent=self)
            return

        app_dir = os.path.dirname(__file__)
        script_path = os.path.join(app_dir, "scraper", "run_public_scraper.py")
        if not os.path.exists(script_path):
            messagebox.showerror("启动失败", f"未找到抓取脚本:\n{script_path}", parent=self)
            return

        initial_dir = self._public_scraper_output_root or os.path.join(app_dir, "data", "public_archive")
        selected = filedialog.askdirectory(
            parent=self,
            title="选择要继续的任务目录（包含 state/runtime_config.json）",
            initialdir=initial_dir,
            mustexist=True,
        )
        if not selected:
            self._set_status("已取消继续任务")
            return

        output_root = os.path.abspath(selected)
        config_path = os.path.join(output_root, "state", "runtime_config.json")
        if not os.path.exists(config_path):
            messagebox.showerror(
                "继续失败",
                "未找到运行配置文件：\n"
                f"{config_path}\n\n"
                "请先从“公共抓取(通用)”启动过一次该任务。",
                parent=self,
            )
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                runtime_config = json.load(f)
            if not isinstance(runtime_config, dict):
                raise ValueError("配置内容不是 JSON 对象")
        except Exception as e:
            messagebox.showerror("继续失败", f"无法读取运行配置：\n{e}", parent=self)
            return

        runtime_config["output_root"] = output_root
        rules = runtime_config.get("rules")
        if not isinstance(rules, dict):
            rules = {}
        rules["named_images_dir"] = ""
        rules["final_output_root"] = ""
        rules["record_root"] = ""
        runtime_config["rules"] = rules
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(runtime_config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            messagebox.showerror("继续失败", f"无法更新运行配置：\n{e}", parent=self)
            return

        named_dir_cfg = (
            runtime_config.get("rules", {}).get("named_images_dir", "")
            if isinstance(runtime_config.get("rules"), dict)
            else ""
        )
        named_dir_raw = str(named_dir_cfg or "").strip()
        if not named_dir_raw:
            named_dir = os.path.abspath(output_root)
        else:
            named_dir = named_dir_raw if os.path.isabs(named_dir_raw) else os.path.join(output_root, named_dir_raw)
            named_dir = os.path.abspath(named_dir)
        log_path = os.path.join(output_root, "reports", "gui_public_scraper.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        try:
            log_handle = open(log_path, "a", encoding="utf-8")
            log_handle.write(
                "\n\n=== D2I Public Scraper Continue "
                + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " ===\n"
            )
            log_handle.flush()
        except Exception as e:
            messagebox.showerror("继续失败", f"无法创建日志文件：\n{e}", parent=self)
            return

        cmd = [
            sys.executable,
            script_path,
            "--config",
            config_path,
            "--output-root",
            output_root,
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(script_path) or ".",
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:
            try:
                log_handle.close()
            except Exception:
                pass
            messagebox.showerror("继续失败", f"无法启动抓取任务：\n{e}", parent=self)
            return

        self._public_scraper_proc = proc
        self._public_scraper_output_root = output_root
        self._public_scraper_named_dir = named_dir
        self._public_scraper_config_path = config_path
        self._public_scraper_log_path = log_path
        self._public_scraper_log_handle = log_handle
        self._public_scraper_last_progress_text = ""
        self._public_scraper_started_at = time.time()
        self._public_scraper_runtime_state = "继续运行中"
        runtime_rules = runtime_config.get("rules")
        if not isinstance(runtime_rules, dict):
            runtime_rules = {}
        active_template_path = (
            str(runtime_rules.get("template_source_path", "")).strip()
            or str(runtime_rules.get("generated_template_path", "")).strip()
        )
        self._public_scraper_active_template_path = os.path.abspath(active_template_path) if active_template_path else ""
        if self._public_scraper_active_template_path:
            self._set_public_scraper_template_state(self._public_scraper_active_template_path, "pending")
        self._set_scraper_control_buttons(running=True)
        self._set_status("抓取任务继续运行中（后台）")
        self._refresh_scraper_monitor_panel()
        self._schedule_public_scraper_poll()
        messagebox.showinfo(
            "继续任务",
            "已按已有配置继续抓取任务。\n\n"
            f"任务进程 PID: {proc.pid}\n\n"
            f"任务目录：\n{output_root}\n\n"
            f"最终图片目录：\n{named_dir}\n\n"
            f"运行日志：\n{log_path}",
            parent=self,
        )

    def _stop_public_scraper_from_gui(self):
        proc = self._public_scraper_proc
        if (proc is None) or (proc.poll() is not None):
            self._public_scraper_proc = None
            self._public_scraper_named_dir = ""
            self._public_scraper_last_progress_text = ""
            self._public_scraper_started_at = None
            self._public_scraper_runtime_state = "空闲"
            self._public_scraper_active_template_path = ""
            self._close_public_scraper_log_handle()
            self._set_scraper_control_buttons(running=False)
            self._refresh_scraper_monitor_panel()
            messagebox.showinfo("提示", "当前没有运行中的抓取任务。", parent=self)
            return

        if not messagebox.askyesno("确认停止", "确定要停止当前抓取任务吗？", parent=self):
            return

        try:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        except Exception as e:
            messagebox.showerror("停止失败", f"无法停止抓取任务：\n{e}", parent=self)
            return

        self._public_scraper_proc = None
        self._public_scraper_named_dir = ""
        self._public_scraper_last_progress_text = ""
        self._public_scraper_started_at = None
        self._public_scraper_runtime_state = "已停止"
        active_template_path = self._public_scraper_active_template_path
        if active_template_path:
            self._set_public_scraper_template_state(active_template_path, "pending")
        self._public_scraper_active_template_path = ""
        self._close_public_scraper_log_handle()
        self._set_scraper_control_buttons(running=False)
        self._set_status("抓取任务已停止")
        self._refresh_scraper_monitor_panel()

    def _schedule_public_scraper_poll(self):
        if self._public_scraper_poll_after:
            try:
                self.after_cancel(self._public_scraper_poll_after)
            except Exception:
                pass
        self._public_scraper_poll_after = self.after(1500, self._poll_public_scraper_proc)

    def _poll_public_scraper_proc(self):
        self._public_scraper_poll_after = None
        proc = self._public_scraper_proc
        if proc is None:
            self._set_scraper_control_buttons(running=False)
            self._refresh_scraper_monitor_panel()
            return

        code = proc.poll()
        if code is None:
            self._update_public_scraper_progress()
            self._schedule_public_scraper_poll()
            return

        named_dir = self._public_scraper_named_dir
        active_template_path = self._public_scraper_active_template_path
        self._public_scraper_proc = None
        self._public_scraper_named_dir = ""
        self._public_scraper_last_progress_text = ""
        self._public_scraper_started_at = None
        self._public_scraper_active_template_path = ""
        self._close_public_scraper_log_handle()
        self._set_scraper_control_buttons(running=False)
        if code == 0:
            self._public_scraper_runtime_state = "已完成"
            self._set_status("抓取任务完成")
            if active_template_path:
                self._set_public_scraper_template_state(active_template_path, "done")
            if named_dir:
                record_path = self._get_scraper_record_path(self._public_scraper_output_root)
                tail_msg = f"\n\n抓取记录：\n{record_path}" if record_path else ""
                messagebox.showinfo(
                    "完成",
                    "抓取任务已完成。\n\n"
                    f"最终图片目录：\n{named_dir}{tail_msg}",
                    parent=self,
                )
        elif code == 2:
            backoff = self._read_scraper_backoff_state(self._public_scraper_output_root)
            blocked_until = backoff.get("blocked_until", "")
            blocked_reason = backoff.get("blocked_reason", "")
            self._public_scraper_runtime_state = "已暂停(风控等待)"
            self._set_status("抓取任务已暂停，等待 backoff 后继续")
            if active_template_path:
                self._set_public_scraper_template_state(active_template_path, "pending")
            detail_lines = ["抓取任务已自动暂停（风控 backoff）。"]
            if blocked_until:
                detail_lines.append(f"恢复时间：{blocked_until}")
            if blocked_reason:
                detail_lines.append(f"原因：{blocked_reason}")
            reason_lower = blocked_reason.lower()
            if "suspected_block_consecutive" in reason_lower:
                detail_lines.append("提示：检测到连续提取失败，建议先手动打开目标网页检查是否触发风控或页面结构变化。")
            detail_lines.append("")
            detail_lines.append("当前进度已归档，可在稍后点击“继续任务”。")
            messagebox.showinfo("任务已暂停", "\n".join(detail_lines), parent=self)
        else:
            self._public_scraper_runtime_state = f"异常结束({code})"
            self._set_status("抓取任务异常结束")
            if active_template_path:
                self._set_public_scraper_template_state(active_template_path, "pending")
            record_path = self._get_scraper_record_path(self._public_scraper_output_root)
            detail = (
                f"抓取任务异常结束，退出码：{code}\n\n抓取记录：\n{record_path}"
                if record_path
                else f"抓取任务异常结束，退出码：{code}\n\n运行日志：\n{self._public_scraper_log_path}"
            )
            messagebox.showwarning(
                "任务结束",
                detail,
                parent=self,
            )
        self._refresh_scraper_monitor_panel()

    def _on_app_close(self):
        if self._public_scraper_poll_after:
            try:
                self.after_cancel(self._public_scraper_poll_after)
            except Exception:
                pass
            self._public_scraper_poll_after = None

        proc = self._public_scraper_proc
        if proc and (proc.poll() is None):
            should_exit = messagebox.askyesno(
                "关闭确认",
                "抓取任务仍在运行。\n\n关闭软件将停止该任务。\n是否继续关闭？",
                parent=self,
            )
            if not should_exit:
                return
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
            except Exception:
                pass
            active_template_path = self._public_scraper_active_template_path
            if active_template_path:
                self._set_public_scraper_template_state(active_template_path, "pending")

        self._public_scraper_proc = None
        self._public_scraper_named_dir = ""
        self._public_scraper_last_progress_text = ""
        self._public_scraper_started_at = None
        self._public_scraper_runtime_state = "空闲"
        self._public_scraper_active_template_path = ""
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

    def _on_add_adaptive_field_clicked(self):
        self._add_adaptive_profile_row("", "")

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

    def _remove_adaptive_profile_row(self, row_token: Dict[str, Any]):
        frame = row_token.get("frame")
        if frame is not None:
            try:
                frame.destroy()
            except Exception:
                pass
        rows = list(getattr(self, "extra_profile_rows", []))
        self.extra_profile_rows = [item for item in rows if item is not row_token]

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
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for k, v in value.items():
                cleaned = D2ILiteApp._prune_empty_profile_values(v)
                if cleaned in (None, "", [], {}):
                    continue
                out[str(k)] = cleaned
            return out
        if isinstance(value, list):
            out_list = []
            for item in value:
                cleaned = D2ILiteApp._prune_empty_profile_values(item)
                if cleaned in (None, "", [], {}):
                    continue
                out_list.append(cleaned)
            return out_list
        return value

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
        payload = {
            "title": self.edit_vars["title"].get().strip(),
            "person": self.edit_vars["person"].get().strip(),
            "gender": self.edit_vars["gender"].get().strip(),
            "position": self.edit_vars["position"].get().strip(),
            "city": self.edit_vars["city"].get().strip(),
            "source": self.edit_vars["source"].get().strip(),
            "image_url": self.edit_vars["image_url"].get().strip(),
            "keywords": _parse_keywords(self.edit_vars["keywords"].get()),
            "titi_asset_id": self.edit_vars["titi_asset_id"].get().strip(),
            "titi_world_id": self.edit_vars["titi_world_id"].get().strip(),
            "description": self.desc_text.get("1.0", tk.END).strip(),
        }
        adaptive = self._collect_adaptive_profile_fields()
        if adaptive:
            payload["d2i_profile"] = adaptive
            police_id_val = str(adaptive.get("police_id", "")).strip()
            if police_id_val:
                payload["police_id"] = police_id_val
        return payload

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
