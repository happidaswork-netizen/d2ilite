#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Desktop metadata backend for D2I Lite Next runtime adapters."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.desktop_metadata_backend_service import (  # type: ignore
    build_ping_payload,
    build_read_payload,
    build_save_payload,
)


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


def _load_payload(args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if getattr(args, "payload_file", ""):
        with open(str(args.payload_file), "r", encoding="utf-8") as handle:
            raw = json.load(handle)
            if isinstance(raw, dict):
                payload = raw
    elif getattr(args, "payload_json", ""):
        raw = json.loads(str(args.payload_json))
        if isinstance(raw, dict):
            payload = raw
    return payload


def cmd_ping(_args: argparse.Namespace) -> int:
    return _ok(build_ping_payload())


def cmd_read(args: argparse.Namespace) -> int:
    path = os.path.abspath(str(args.path or "").strip())
    if not path:
        return _fail("path is required", code=2)
    if not os.path.isfile(path):
        return _fail("file not found", code=2, detail=path)
    try:
        return _ok(build_read_payload(path))
    except Exception as error:
        return _fail("read failed", detail=str(error))


def cmd_save(args: argparse.Namespace) -> int:
    path = os.path.abspath(str(args.path or "").strip())
    if not path:
        return _fail("path is required", code=2)
    if not os.path.isfile(path):
        return _fail("file not found", code=2, detail=path)
    try:
        return _ok(build_save_payload(path, _load_payload(args)))
    except Exception as error:
        return _fail("save failed", detail=str(error))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="D2I Lite desktop metadata backend")
    sub = parser.add_subparsers(dest="command", required=True)

    ping = sub.add_parser("ping", help="health check")
    ping.set_defaults(func=cmd_ping)

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
