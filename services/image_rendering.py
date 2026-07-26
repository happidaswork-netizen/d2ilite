# -*- coding: utf-8 -*-
"""Image rendering helpers used by current-image actions."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from PIL import Image, ImageDraw, ImageFont, ImageOps

INVALID_FILENAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


@dataclass
class NameBarRenderOptions:
    label: str
    output_format: str = "png"
    output_name_mode: str = "suffix"
    suffix: str = "_named"
    output_dir: str = ""
    bar_height_mode: str = "auto"
    bar_height_ratio: float = 0.14
    min_bar_height: int = 48
    align: str = "center"
    bar_color: str = "white"
    text_color: str = "black"
    jpg_quality: int = 100
    webp_lossless: bool = True


def sanitize_filename_label(label: str, fallback: str = "image") -> str:
    cleaned = INVALID_FILENAME_RE.sub("_", str(label or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        cleaned = fallback
    if cleaned.upper() in WINDOWS_RESERVED_NAMES:
        cleaned = f"{cleaned}_"
    return cleaned


def unique_output_path(directory: str, stem: str, extension: str) -> str:
    ext = extension if extension.startswith(".") else f".{extension}"
    safe_stem = sanitize_filename_label(stem)
    candidate = os.path.join(directory, f"{safe_stem}{ext}")
    if not os.path.exists(candidate):
        return candidate
    for idx in range(2, 10000):
        candidate = os.path.join(directory, f"{safe_stem}_{idx}{ext}")
        if not os.path.exists(candidate):
            return candidate
    raise RuntimeError("无法生成不重名的输出文件名")


def normalize_output_format(value: str, source_path: str = "") -> str:
    fmt = str(value or "original").strip().lower().lstrip(".")
    if fmt in {"original", "keep", "same"}:
        fmt = os.path.splitext(source_path)[1].lower().lstrip(".") or "png"
    if fmt == "jpeg":
        fmt = "jpg"
    if fmt not in {"png", "jpg", "webp", "bmp", "tif", "tiff"}:
        fmt = "png"
    return fmt


def _font_candidates() -> Iterable[str]:
    windir = os.environ.get("WINDIR") or r"C:\Windows"
    yield os.path.join(windir, "Fonts", "msyh.ttc")
    yield os.path.join(windir, "Fonts", "simhei.ttf")
    yield os.path.join(windir, "Fonts", "Deng.ttf")
    yield "/System/Library/Fonts/PingFang.ttc"
    yield "/System/Library/Fonts/STHeiti Light.ttc"
    yield "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
    yield "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"
    yield "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"


def _load_font(size: int) -> ImageFont.ImageFont:
    for path in _font_candidates():
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size=size)
            except Exception:
                continue
    try:
        return ImageFont.truetype("arial.ttf", size=size)
    except Exception:
        return ImageFont.load_default()


def _text_bbox(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int, int, int]:
    try:
        return draw.textbbox((0, 0), text, font=font)
    except Exception:
        width, height = draw.textsize(text, font=font)
        return (0, 0, width, height)


def _fit_font(draw: ImageDraw.ImageDraw, text: str, max_width: int, max_height: int) -> ImageFont.ImageFont:
    start = max(10, min(max_height, int(max_height * 0.72)))
    for size in range(start, 9, -1):
        font = _load_font(size)
        left, top, right, bottom = _text_bbox(draw, text, font)
        if (right - left) <= max_width and (bottom - top) <= max_height:
            return font
    return _load_font(10)


def _composite_on_white(image: Image.Image) -> Image.Image:
    if image.mode in {"RGBA", "LA"} or ("transparency" in image.info):
        rgba = image.convert("RGBA")
        background = Image.new("RGBA", rgba.size, "white")
        background.alpha_composite(rgba)
        return background.convert("RGB")
    if image.mode not in {"RGB", "L"}:
        return image.convert("RGB")
    return image.convert("RGB")


def render_name_bar(source_path: str, options: NameBarRenderOptions) -> str:
    source = os.path.abspath(str(source_path or "").strip())
    if not os.path.isfile(source):
        raise FileNotFoundError(f"图片不存在: {source}")

    label = str(options.label or "").strip()
    if not label:
        raise ValueError("角色名不能为空")

    output_format = normalize_output_format(options.output_format, source)
    output_dir = os.path.abspath(str(options.output_dir or "").strip() or os.path.dirname(source))
    os.makedirs(output_dir, exist_ok=True)

    source_stem = os.path.splitext(os.path.basename(source))[0]
    if str(options.output_name_mode or "").strip().lower() == "label":
        output_stem = sanitize_filename_label(label, fallback=source_stem)
    else:
        output_stem = sanitize_filename_label(f"{source_stem}{options.suffix or '_named'}", fallback=f"{source_stem}_named")
    output_path = unique_output_path(output_dir, output_stem, output_format)
    if os.path.abspath(output_path) == os.path.abspath(source):
        raise RuntimeError("输出路径不能覆盖原图")

    with Image.open(source) as raw:
        icc_profile: Optional[bytes] = raw.info.get("icc_profile")
        image = ImageOps.exif_transpose(raw)
        base = _composite_on_white(image)

    width, height = base.size
    if str(options.bar_height_mode or "auto").lower() == "auto":
        bar_height = max(int(height * float(options.bar_height_ratio or 0.14)), int(options.min_bar_height or 48))
    else:
        try:
            bar_height = int(options.bar_height_mode)
        except Exception:
            bar_height = max(int(height * 0.14), 48)
    bar_height = max(24, bar_height)

    canvas = Image.new("RGB", (width, height + bar_height), options.bar_color or "white")
    canvas.paste(base, (0, 0))

    draw = ImageDraw.Draw(canvas)
    padding_x = max(12, int(width * 0.04))
    padding_y = max(6, int(bar_height * 0.18))
    max_text_width = max(1, width - padding_x * 2)
    max_text_height = max(1, bar_height - padding_y * 2)
    font = _fit_font(draw, label, max_text_width, max_text_height)
    left, top, right, bottom = _text_bbox(draw, label, font)
    text_width = right - left
    text_height = bottom - top

    align = str(options.align or "center").lower()
    if align == "left":
        x = padding_x
    elif align == "right":
        x = width - padding_x - text_width
    else:
        x = (width - text_width) / 2
    y = height + (bar_height - text_height) / 2 - top
    draw.text((x, y), label, fill=options.text_color or "black", font=font)

    save_kwargs: dict[str, Any] = {}
    if icc_profile:
        save_kwargs["icc_profile"] = icc_profile
    if output_format == "jpg":
        save_kwargs.update({"quality": int(options.jpg_quality or 100), "subsampling": 0, "optimize": True})
        canvas.save(output_path, format="JPEG", **save_kwargs)
    elif output_format == "webp":
        save_kwargs.update({"lossless": bool(options.webp_lossless), "quality": int(options.jpg_quality or 100)})
        canvas.save(output_path, format="WEBP", **save_kwargs)
    else:
        canvas.save(output_path, format=output_format.upper(), **save_kwargs)
    return output_path
