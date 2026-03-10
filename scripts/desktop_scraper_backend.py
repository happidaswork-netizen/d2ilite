#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Desktop scraper backend for D2I Lite Next runtime adapters."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.desktop_scraper_backend_service import (  # type: ignore
    build_default_base_root_payload,
    build_ping_payload,
    build_scraper_workspace_payload,
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


def cmd_ping(_args: argparse.Namespace) -> int:
    return _ok(build_ping_payload())


def cmd_default_root(_args: argparse.Namespace) -> int:
    return _ok(build_default_base_root_payload())


def cmd_workspace(args: argparse.Namespace) -> int:
    try:
        payload = build_scraper_workspace_payload(
            str(args.base_root or "").strip(),
            selected_root=str(args.selected_root or "").strip(),
            progress_limit=max(20, int(args.progress_limit or 300)),
            log_lines=max(20, int(args.log_lines or 80)),
        )
    except Exception as error:
        return _fail("workspace read failed", detail=str(error))
    return _ok(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="D2I Lite desktop scraper backend")
    sub = parser.add_subparsers(dest="command", required=True)

    ping = sub.add_parser("ping", help="health check")
    ping.set_defaults(func=cmd_ping)

    default_root = sub.add_parser("default-root", help="get default scraper task root")
    default_root.set_defaults(func=cmd_default_root)

    workspace = sub.add_parser("workspace", help="read scraper workspace snapshot")
    workspace.add_argument("--base-root", default="")
    workspace.add_argument("--selected-root", default="")
    workspace.add_argument("--progress-limit", type=int, default=300)
    workspace.add_argument("--log-lines", type=int, default=80)
    workspace.set_defaults(func=cmd_workspace)
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
