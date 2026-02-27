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
    return uniq


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

        self._build_ui()
        self._setup_edit_shortcuts_and_menu()

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
        return {
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

    def _save_structured(self) -> bool:
        if not self.current_path:
            messagebox.showinfo("提示", "请先打开一张图片")
            return False

        payload = self._collect_structured_payload()
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
