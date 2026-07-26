# -*- coding: utf-8 -*-
"""Headless app settings CLI used by D2I Lite Next."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from services.settings_service import app_settings_path, load_app_settings, save_app_settings


def _json_line(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def _run_get(_args: argparse.Namespace) -> int:
    _json_line({"ok": True, "settings": load_app_settings(), "path": app_settings_path()})
    return 0


def _run_save(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(str(args.payload_json or "{}"))
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        ok = save_app_settings(payload)
    except Exception as exc:
        _json_line({"ok": False, "error": "save settings failed", "detail": str(exc)})
        return 1
    _json_line({"ok": bool(ok), "settings": load_app_settings(), "path": app_settings_path()})
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="D2I Lite settings CLI")
    subparsers = parser.add_subparsers(dest="action", required=True)

    get_cmd = subparsers.add_parser("get", help="Read app settings")
    get_cmd.set_defaults(func=_run_get)

    save_cmd = subparsers.add_parser("save", help="Save app settings")
    save_cmd.add_argument("--payload-json", required=True)
    save_cmd.set_defaults(func=_run_save)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
