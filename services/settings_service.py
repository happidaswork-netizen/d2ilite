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
    }


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
    llm_default = dict(base.get("llm", {}) if isinstance(base.get("llm"), dict) else {})
    llm_payload = payload.get("llm", {})
    if isinstance(llm_payload, dict):
        llm_default.update(llm_payload)
    merged["llm"] = llm_default
    return merged


def save_app_settings(payload: Dict[str, Any], path: str = "") -> bool:
    settings_path = path or app_settings_path()
    try:
        base = default_app_settings()
        data = dict(payload or {})
        llm_default = dict(base.get("llm", {}))
        llm_payload = data.get("llm", {})
        if isinstance(llm_payload, dict):
            llm_default.update(llm_payload)
        data["llm"] = llm_default
        data["version"] = int(data.get("version") or 1)
        data["updated_at"] = datetime.now().isoformat(timespec="seconds")

        os.makedirs(os.path.dirname(settings_path), exist_ok=True)
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

