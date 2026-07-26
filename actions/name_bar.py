# -*- coding: utf-8 -*-
"""Add a white name bar under the current image."""

from __future__ import annotations

import os
import re
from typing import Any

from actions.base import ImageActionContext, ImageActionResult, register_image_action
from services.image_rendering import NameBarRenderOptions, render_name_bar

NAME_BAR_ACTION_ID = "name-bar"


def _first_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        for item in value:
            text = _first_text(item)
            if text:
                return text
    if isinstance(value, dict):
        for item in value.values():
            text = _first_text(item)
            if text:
                return text
    return ""


def _title_to_name(title: str) -> str:
    text = str(title or "").strip()
    if not text:
        return ""
    for sep in (" - ", " — ", " – ", "：", ":"):
        if sep in text:
            head = text.split(sep, 1)[0].strip()
            if head:
                return head
    return text


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def derive_name_bar_label(image_path: str, metadata: Any | None = None) -> str:
    if metadata is not None:
        person = _first_text(getattr(metadata, "person", ""))
        if person:
            return person

        other_xmp = getattr(metadata, "other_xmp", {}) or {}
        person_in_image = _first_text(other_xmp.get("Xmp.iptcExt.PersonInImage"))
        if person_in_image:
            return person_in_image

        titi_json = getattr(metadata, "titi_json", {}) or {}
        if isinstance(titi_json, dict):
            profile = titi_json.get("d2i_profile") or {}
            if isinstance(profile, dict):
                name = _first_text(profile.get("name"))
                if name:
                    return name

        title_name = _title_to_name(getattr(metadata, "title", ""))
        if title_name:
            return title_name

    stem = os.path.splitext(os.path.basename(str(image_path or "")))[0]
    stem = re.sub(r"[_-]+named$", "", stem, flags=re.IGNORECASE).strip()
    return stem


class NameBarAction:
    id = NAME_BAR_ACTION_ID
    label = "加名字"

    def run(self, context: ImageActionContext, **options: Any) -> ImageActionResult:
        label = str(options.get("name") or "").strip() or derive_name_bar_label(context.image_path, context.metadata)
        action_config = {}
        settings = context.app_config or {}
        image_actions = settings.get("image_actions") if isinstance(settings, dict) else None
        if isinstance(image_actions, dict):
            maybe_config = image_actions.get("name_bar")
            if isinstance(maybe_config, dict):
                action_config = dict(maybe_config)

        render_options = NameBarRenderOptions(
            label=label,
            output_format=str(options.get("format") or action_config.get("output_format") or "png"),
            output_name_mode=str(options.get("output_name") or action_config.get("output_name_mode") or "suffix"),
            output_dir=str(options.get("output_dir") or action_config.get("output_dir") or ""),
            suffix=str(options.get("suffix") or action_config.get("suffix") or "_named"),
            bar_height_mode=str(action_config.get("bar_height_mode") or "auto"),
            bar_height_ratio=_safe_float(action_config.get("bar_height_ratio"), 0.14),
            min_bar_height=_safe_int(action_config.get("min_bar_height"), 48),
            align=str(action_config.get("align") or "center"),
            bar_color=str(action_config.get("bar_color") or "white"),
            text_color=str(action_config.get("text_color") or "black"),
            jpg_quality=_safe_int(action_config.get("jpg_quality"), 100),
            webp_lossless=_safe_bool(action_config.get("webp_lossless"), True),
        )
        output_path = render_name_bar(context.image_path, render_options)
        return ImageActionResult(
            ok=True,
            message=f"已生成加名字图片: {output_path}",
            output_path=output_path,
            reveal_path=output_path,
        )


register_image_action(NameBarAction())
