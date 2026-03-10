# -*- coding: utf-8 -*-
"""Desktop metadata backend service for desktop-next runtime adapters."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

from metadata_manager import read_image_metadata, update_metadata_preserve_others
from services.metadata_service import build_structured_payload


BACKEND_PROVIDER = "python-cli"
BACKEND_VERSION = "metadata-backend-v1"


def build_ping_payload() -> Dict[str, Any]:
    return {
        "provider": BACKEND_PROVIDER,
        "version": BACKEND_VERSION,
    }


def serialize_metadata_info(path: str) -> Dict[str, Any]:
    info = read_image_metadata(path)
    data = asdict(info)

    modified = data.get("modified_time")
    try:
        data["modified_time"] = modified.isoformat() if modified is not None else ""
    except Exception:
        data["modified_time"] = str(modified or "")

    status = data.get("status")
    try:
        data["status"] = getattr(status, "value", str(status))
    except Exception:
        data["status"] = str(status or "")

    return data


def build_read_payload(path: str) -> Dict[str, Any]:
    return {
        "item": serialize_metadata_info(path),
    }


def build_save_payload(path: str, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = build_structured_payload(
        title=raw_payload.get("title", ""),
        person=raw_payload.get("person", ""),
        gender=raw_payload.get("gender", ""),
        position=raw_payload.get("position", ""),
        city=raw_payload.get("city", ""),
        source=raw_payload.get("source", ""),
        image_url=raw_payload.get("image_url", ""),
        keywords_text=", ".join(raw_payload.get("keywords", []))
        if isinstance(raw_payload.get("keywords"), list)
        else raw_payload.get("keywords", ""),
        titi_asset_id=raw_payload.get("titi_asset_id", ""),
        titi_world_id=raw_payload.get("titi_world_id", ""),
        description=raw_payload.get("description", ""),
        adaptive_profile=raw_payload.get("d2i_profile")
        if isinstance(raw_payload.get("d2i_profile"), dict)
        else None,
        role_aliases=raw_payload.get("role_aliases")
        if isinstance(raw_payload.get("role_aliases"), list)
        else None,
    )
    ok = bool(update_metadata_preserve_others(path, payload, clean_format=True))
    if not ok:
        raise RuntimeError("update_metadata_preserve_others returned False")
    return {
        "saved": True,
        "path": path,
    }
