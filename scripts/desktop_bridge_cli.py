#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Desktop bridge CLI for D2I Lite Next (Phase 1)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from metadata_manager import (  # type: ignore
    read_image_metadata,
    update_metadata_preserve_others,
)
from services.image_service import list_images_in_folder
from services.metadata_service import build_structured_payload


def _json_print(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def _ok(data: Dict[str, Any]) -> int:
    _json_print({"ok": True, **data})
    return 0


def _fail(message: str, *, code: int = 1, detail: str = "") -> int:
    payload = {"ok": False, "error": str(message or "unknown error")}
    if detail:
        payload["detail"] = detail
    _json_print(payload)
    return int(code)


def _serialize_info(path: str) -> Dict[str, Any]:
    info = read_image_metadata(path)
    data = asdict(info)
    # datetime -> string
    modified = data.get("modified_time")
    try:
        data["modified_time"] = modified.isoformat() if modified is not None else ""
    except Exception:
        data["modified_time"] = str(modified or "")
    # enum -> value
    status = data.get("status")
    try:
        data["status"] = getattr(status, "value", str(status))
    except Exception:
        data["status"] = str(status or "")
    return data


def _load_payload(args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if getattr(args, "payload_file", ""):
        with open(str(args.payload_file), "r", encoding="utf-8") as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                payload = obj
    elif getattr(args, "payload_json", ""):
        obj = json.loads(str(args.payload_json))
        if isinstance(obj, dict):
            payload = obj
    return payload


def cmd_ping(_args: argparse.Namespace) -> int:
    return _ok({"provider": "python-cli", "version": "phase1-bridge-v1"})


def cmd_list(args: argparse.Namespace) -> int:
    folder = os.path.abspath(str(args.folder or "").strip())
    if not folder:
        return _fail("folder is required", code=2)
    if not os.path.isdir(folder):
        return _fail("folder not found", code=2, detail=folder)
    files = list_images_in_folder(folder)
    limit = max(0, int(args.limit or 0))
    if limit > 0:
        files = files[:limit]
    return _ok({"folder": folder, "count": len(files), "items": files})


def cmd_read(args: argparse.Namespace) -> int:
    path = os.path.abspath(str(args.path or "").strip())
    if not path:
        return _fail("path is required", code=2)
    if not os.path.isfile(path):
        return _fail("file not found", code=2, detail=path)
    try:
        data = _serialize_info(path)
        return _ok({"item": data})
    except Exception as e:
        return _fail("read failed", detail=str(e))


def cmd_save(args: argparse.Namespace) -> int:
    path = os.path.abspath(str(args.path or "").strip())
    if not path:
        return _fail("path is required", code=2)
    if not os.path.isfile(path):
        return _fail("file not found", code=2, detail=path)
    try:
        raw_payload = _load_payload(args)
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
            adaptive_profile=raw_payload.get("d2i_profile", {})
            if isinstance(raw_payload.get("d2i_profile"), dict)
            else {},
        )
        ok = bool(update_metadata_preserve_others(path, payload, clean_format=True))
        if not ok:
            return _fail("save failed", detail="update_metadata_preserve_others returned False")
        return _ok({"saved": True, "path": path})
    except Exception as e:
        return _fail("save failed", detail=str(e))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="D2I Lite desktop bridge CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    ping = sub.add_parser("ping", help="health check")
    ping.set_defaults(func=cmd_ping)

    list_cmd = sub.add_parser("list", help="list image files in folder")
    list_cmd.add_argument("--folder", required=True)
    list_cmd.add_argument("--limit", type=int, default=0)
    list_cmd.set_defaults(func=cmd_list)

    read = sub.add_parser("read", help="read metadata of one image")
    read.add_argument("--path", required=True)
    read.set_defaults(func=cmd_read)

    save = sub.add_parser("save", help="save structured metadata to one image")
    save.add_argument("--path", required=True)
    save.add_argument("--payload-json", default="")
    save.add_argument("--payload-file", default="")
    save.set_defaults(func=cmd_save)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    func = getattr(args, "func", None)
    if not callable(func):
        return _fail("invalid command", code=2)
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())

