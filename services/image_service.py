# -*- coding: utf-8 -*-
"""Image-related domain services (UI-independent)."""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional

from PIL import Image


def list_images_in_folder(folder: str, image_exts: Optional[Iterable[str]] = None) -> List[str]:
    """Return sorted image file paths in the given folder."""
    folder_abs = os.path.abspath(folder)
    if not os.path.isdir(folder_abs):
        return []

    ext_set = set(image_exts or {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"})
    results: List[str] = []
    for name in sorted(os.listdir(folder_abs), key=lambda x: x.lower()):
        path = os.path.join(folder_abs, name)
        ext = os.path.splitext(name)[1].lower()
        if os.path.isfile(path) and ext in ext_set:
            results.append(path)
    return results


def read_image_basic_info(filepath: str) -> Dict[str, Any]:
    """Read lightweight image attributes used by the viewer."""
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

