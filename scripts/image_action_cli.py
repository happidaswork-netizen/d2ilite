# -*- coding: utf-8 -*-
"""Headless image actions used by D2I Lite Next."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from actions.base import ImageActionContext, get_image_action
from actions.name_bar import NAME_BAR_ACTION_ID
from metadata_manager import read_image_metadata
from services.settings_service import load_app_settings


def _json_line(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=True), flush=True)


def _run_name_bar(args: argparse.Namespace) -> int:
    image_path = os.path.abspath(str(args.image or "").strip().strip('"'))
    if not image_path or not os.path.isfile(image_path):
        _json_line({"ok": False, "error": "image not found", "detail": image_path})
        return 2

    name = str(args.name or "").strip()
    if not name:
        _json_line({"ok": False, "error": "name is required"})
        return 2

    action = get_image_action(NAME_BAR_ACTION_ID)
    if action is None:
        _json_line({"ok": False, "error": "name-bar action is not registered"})
        return 2

    metadata = None
    try:
        metadata = read_image_metadata(image_path)
    except Exception:
        metadata = None

    options: Dict[str, Any] = {"name": name}
    if args.output_dir is not None:
        options["output_dir"] = args.output_dir
    if args.output_format is not None:
        options["format"] = args.output_format
    if args.output_name is not None:
        options["output_name"] = args.output_name

    try:
        result = action.run(
            ImageActionContext(
                image_path=image_path,
                metadata=metadata,
                app_config=load_app_settings(),
                ui_parent=None,
            ),
            **options,
        )
    except Exception as exc:
        _json_line({"ok": False, "error": "name-bar failed", "detail": str(exc)})
        return 1

    _json_line(
        {
            "ok": bool(result.ok),
            "message": result.message,
            "output_path": result.output_path or "",
            "reveal_path": result.reveal_path or result.output_path or "",
        }
    )
    return 0 if result.ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="D2I Lite image action CLI")
    subparsers = parser.add_subparsers(dest="action", required=True)

    name_bar = subparsers.add_parser("name-bar", help="Add a white name bar under an image")
    name_bar.add_argument("--image", required=True)
    name_bar.add_argument("--name", required=True)
    name_bar.add_argument("--output-dir", default=None)
    name_bar.add_argument("--format", dest="output_format", default=None)
    name_bar.add_argument("--output-name", default=None, choices=("suffix", "label"))
    name_bar.set_defaults(func=_run_name_bar)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
