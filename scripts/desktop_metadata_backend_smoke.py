#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for desktop metadata backend contract."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image


def _run_backend(args: list[str], cwd: Path) -> dict:
    cmd = [sys.executable, str(cwd / "scripts" / "desktop_metadata_backend.py")] + args
    cp = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True)
    if cp.returncode != 0:
        raise RuntimeError(f"backend failed: args={args} rc={cp.returncode} out={cp.stdout} err={cp.stderr}")
    try:
        payload = json.loads(cp.stdout.strip())
    except Exception as error:
        raise RuntimeError(f"backend output is not JSON: {cp.stdout}") from error
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RuntimeError(f"backend returned failure envelope: {payload}")
    return payload


def main() -> int:
    repo = Path(__file__).resolve().parent.parent

    with tempfile.TemporaryDirectory(prefix="d2i_metadata_backend_") as td:
        root = Path(td)
        image_path = root / "样例.jpg"
        Image.new("RGB", (48, 48), (90, 120, 200)).save(image_path)

        ping = _run_backend(["ping"], repo)
        if str(ping.get("provider", "")) != "python-cli":
            raise RuntimeError(f"unexpected ping provider: {ping}")

        read_before = _run_backend(["read", "--path", str(image_path)], repo)
        item_before = read_before.get("item")
        if not isinstance(item_before, dict) or str(item_before.get("filename", "")) != image_path.name:
            raise RuntimeError(f"read response invalid: {read_before}")

        payload = {
            "title": "metadata backend smoke",
            "person": "",
            "gender": "",
            "position": "",
            "city": "",
            "source": "",
            "image_url": "",
            "keywords": [],
            "titi_asset_id": "",
            "titi_world_id": "",
            "description": "",
        }
        save = _run_backend(
            ["save", "--path", str(image_path), "--payload-json", json.dumps(payload, ensure_ascii=False)],
            repo,
        )
        if save.get("saved") is not True:
            raise RuntimeError(f"save response invalid: {save}")

        read_after = _run_backend(["read", "--path", str(image_path)], repo)
        item_after = read_after.get("item")
        if not isinstance(item_after, dict) or str(item_after.get("title", "")) != "metadata backend smoke":
            raise RuntimeError(f"save not persisted: {read_after}")

    print("[OK] desktop metadata backend smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
