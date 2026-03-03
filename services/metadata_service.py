# -*- coding: utf-8 -*-
"""Metadata-related domain services (UI-independent)."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from metadata_manager import clean_keywords

try:
    import pyexiv2  # type: ignore

    HAS_PYEXIV2 = True
except Exception:
    pyexiv2 = None  # type: ignore
    HAS_PYEXIV2 = False


KEYWORD_SPLIT_RE = re.compile(r"[;,，、\n]+")


def parse_keywords(text: str) -> List[str]:
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


def normalize_http_url(text: Any) -> str:
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


def read_raw_with_pyexiv2(filepath: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
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

    def _should_force_temp(path: str) -> bool:
        p = str(path or "").strip()
        if not p:
            return False
        # pyexiv2 on Windows may hang or fail on non-ASCII/long/UNC paths; prefer a temp ASCII copy.
        if p.startswith("\\\\"):
            return True
        try:
            p.encode("ascii")
        except UnicodeEncodeError:
            return True
        return len(p) >= 240

    try:
        if _should_force_temp(filepath):
            raise RuntimeError("force_temp")
        return _read(filepath)
    except Exception as e:
        err = str(e)
        if (
            ("Illegal byte sequence" in err)
            or ("errno = 42" in err)
            or ("errno = 2" in err)
            or ("No such file or directory" in err)
            or ("Failed to open the data source" in err and os.path.exists(filepath))
            or (err == "force_temp")
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


def write_raw_with_pyexiv2(
    filepath: str,
    *,
    xmp_data: Optional[Dict[str, Any]] = None,
    exif_data: Optional[Dict[str, Any]] = None,
    iptc_data: Optional[Dict[str, Any]] = None,
) -> None:
    if not HAS_PYEXIV2:
        raise RuntimeError("pyexiv2 未安装，无法执行高级写入")

    def _write(path: str) -> None:
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

