# -*- coding: utf-8 -*-
"""Viewer loading workflow services (UI-independent)."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from PIL import Image


def load_preview_image(target_path: str) -> Tuple[Optional[Image.Image], str]:
    preview_pil: Optional[Image.Image] = None
    preview_err = ""
    try:
        with Image.open(target_path) as img:
            preview_pil = img.copy()
    except Exception as exc:
        preview_pil = None
        preview_err = str(exc)
    return preview_pil, preview_err


def load_metadata_snapshot(
    target_path: str,
    *,
    has_pyexiv2: bool,
    read_basic_info_fn: Callable[[str], Dict[str, Any]],
    read_image_metadata_fn: Callable[[str], Any],
    read_raw_with_pyexiv2_fn: Callable[[str], Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]],
) -> Tuple[Dict[str, Any], Any, Dict[str, Any], Dict[str, Any], Dict[str, Any], str]:
    err = ""
    basic: Dict[str, Any] = {}
    info: Any = None
    raw_xmp: Dict[str, Any] = {}
    raw_exif: Dict[str, Any] = {}
    raw_iptc: Dict[str, Any] = {}

    try:
        basic = read_basic_info_fn(target_path)
        info = read_image_metadata_fn(target_path)
        raw_xmp = dict(getattr(info, "other_xmp", {}) or {})
        raw_exif = dict(getattr(info, "other_exif", {}) or {})
        raw_iptc = dict(getattr(info, "other_iptc", {}) or {})

        # Fallback: supplement raw namespaces if structured read didn't include them.
        if has_pyexiv2 and (not raw_xmp) and (not raw_exif) and (not raw_iptc):
            try:
                raw_xmp, raw_exif, raw_iptc = read_raw_with_pyexiv2_fn(target_path)
            except Exception:
                raw_xmp, raw_exif, raw_iptc = {}, {}, {}
    except Exception as exc:
        err = str(exc)

    return basic, info, raw_xmp, raw_exif, raw_iptc, err

