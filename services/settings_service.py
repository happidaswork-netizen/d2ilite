# -*- coding: utf-8 -*-
"""App settings IO services."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict


def app_settings_path() -> str:
    # Keep secrets (API keys) outside the repo/workspace to avoid accidental commits.
    root = os.path.join(os.path.expanduser("~"), ".d2ilite")
    return os.path.join(root, "settings.json")


def default_app_settings() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "llm": {
            "enabled_default": False,
            "api_base": "",
            "api_key": "",
            "model": "",
            "timeout_seconds": 45,
            "max_retries": 2,
            "temperature": 0.1,
        },
        "image_actions": {
            "name_bar": {
                "output_format": "original",
                "jpg_quality": 100,
                "output_name_mode": "suffix",
                "output_dir": "",
                "suffix": "_named",
                "bar_height_mode": "auto",
                "bar_height_ratio": 0.14,
                "min_bar_height": 48,
                "align": "center",
                "bar_color": "white",
                "text_color": "black",
                "webp_lossless": True,
            },
            "model_gateway": {
                "provider": "openai-compatible",
                "base_url": "",
                "api_key_env": "D2I_MODEL_API_KEY",
                "image_model": "",
                "vision_model": "",
            },
        },
    }


def _merge_dict_section(base: Dict[str, Any], payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    merged_section = dict(base.get(key, {}) if isinstance(base.get(key), dict) else {})
    payload_section = payload.get(key, {})
    if isinstance(payload_section, dict):
        for section_key, section_value in payload_section.items():
            if isinstance(section_value, dict) and isinstance(merged_section.get(section_key), dict):
                nested = dict(merged_section.get(section_key, {}))
                nested.update(section_value)
                merged_section[section_key] = nested
            else:
                merged_section[section_key] = section_value
    return merged_section


def load_app_settings(path: str = "") -> Dict[str, Any]:
    settings_path = path or app_settings_path()
    base = default_app_settings()
    if not os.path.exists(settings_path):
        return base
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return base
    except Exception:
        return base

    merged = dict(base)
    merged.update(payload)
    merged["llm"] = _merge_dict_section(base, payload, "llm")
    merged["image_actions"] = _merge_dict_section(base, payload, "image_actions")
    return merged


def save_app_settings(payload: Dict[str, Any], path: str = "") -> bool:
    settings_path = path or app_settings_path()
    try:
        base = default_app_settings()
        data = dict(payload or {})
        data["llm"] = _merge_dict_section(base, data, "llm")
        data["image_actions"] = _merge_dict_section(base, data, "image_actions")
        data["version"] = int(data.get("version") or 1)
        data["updated_at"] = datetime.now().isoformat(timespec="seconds")

        os.makedirs(os.path.dirname(settings_path), exist_ok=True)
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False
