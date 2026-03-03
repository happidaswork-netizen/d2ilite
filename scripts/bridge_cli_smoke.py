#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for desktop bridge CLI contract."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image


def _run_cli(args: list[str], cwd: Path) -> dict:
    cmd = [sys.executable, str(cwd / "scripts" / "desktop_bridge_cli.py")] + args
    cp = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True)
    if cp.returncode != 0:
        raise RuntimeError(f"CLI failed: args={args} rc={cp.returncode} out={cp.stdout} err={cp.stderr}")
    try:
        payload = json.loads(cp.stdout.strip())
    except Exception as e:
        raise RuntimeError(f"CLI output is not JSON: {cp.stdout}") from e
    if not isinstance(payload, dict) or (payload.get("ok") is not True):
        raise RuntimeError(f"CLI returned failure envelope: {payload}")
    return payload


def main() -> int:
    repo = Path(__file__).resolve().parent.parent

    with tempfile.TemporaryDirectory(prefix="d2i_bridge_smoke_") as td:
        root = Path(td)
        image_path = root / "样例.jpg"
        Image.new("RGB", (48, 48), (120, 88, 220)).save(image_path)

        ping = _run_cli(["ping"], repo)
        if str(ping.get("provider", "")) != "python-cli":
            raise RuntimeError(f"unexpected ping provider: {ping}")

        listed = _run_cli(["list", "--folder", str(root)], repo)
        items = listed.get("items")
        if not isinstance(items, list) or (not items):
            raise RuntimeError(f"list items invalid: {listed}")

        read = _run_cli(["read", "--path", str(image_path)], repo)
        item = read.get("item")
        if not isinstance(item, dict):
            raise RuntimeError(f"read item invalid: {read}")
        if str(item.get("filename", "")) != image_path.name:
            raise RuntimeError(f"filename mismatch: {item}")

    print("[OK] bridge cli smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

