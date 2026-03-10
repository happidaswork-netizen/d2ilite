#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for the desktop scraper backend."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON_EXE = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
BACKEND_SCRIPT = REPO_ROOT / "scripts" / "desktop_scraper_backend.py"
TMP_ROOT = REPO_ROOT / ".tmp" / "desktop-scraper-backend-smoke"


def _safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(str(text).encode("utf-8", errors="replace") + b"\n")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    path.write_text(body, encoding="utf-8")


def _run_backend(*args: str) -> dict:
    result = subprocess.run(
        [str(PYTHON_EXE if PYTHON_EXE.is_file() else sys.executable), str(BACKEND_SCRIPT), *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stdout or result.stderr or f"backend exited with {result.returncode}")
    payload = json.loads(result.stdout)
    if payload.get("ok") is not True:
        raise RuntimeError(str(payload))
    return payload


def _prepare_sample_task(base_root: Path) -> Path:
    task_root = base_root / "sample_scraper_task"
    image_path = task_root / "downloads" / "named_images" / "done.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"fake-image")

    _write_json(task_root / "state" / "runtime_config.json", {"rules": {}})
    _write_jsonl(
        task_root / "raw" / "list_records.jsonl",
        [
          {"name": "已完成人物", "detail_url": "https://example.com/detail/done"},
          {"name": "缺失详情人物", "detail_url": ""},
        ],
    )
    _write_jsonl(
        task_root / "raw" / "profiles.jsonl",
        [
          {
            "name": "已完成人物",
            "detail_url": "https://example.com/detail/done",
            "image_url": "https://example.com/image/done.jpg",
          }
        ],
    )
    _write_jsonl(
        task_root / "downloads" / "image_downloads.jsonl",
        [
          {
            "name": "已完成人物",
            "detail_url": "https://example.com/detail/done",
            "named_path": str(image_path),
          }
        ],
    )
    _write_jsonl(
        task_root / "raw" / "metadata_write_results.jsonl",
        [{"detail_url": "https://example.com/detail/done", "status": "ok", "output_path": str(image_path)}],
    )
    _write_jsonl(task_root / "raw" / "review_queue.jsonl", [{"detail_url": "https://example.com/detail/review"}])
    _write_jsonl(task_root / "raw" / "failures.jsonl", [{"detail_url": "https://example.com/detail/failure"}])
    (task_root / "reports").mkdir(parents=True, exist_ok=True)
    (task_root / "reports" / "gui_public_scraper.log").write_text(
        "[2026-03-10 12:00:00] 开始抓取\n[2026-03-10 12:01:00] 详情页=https://example.com/detail/done\n",
        encoding="utf-8",
    )
    return task_root


def main() -> int:
    if TMP_ROOT.exists():
        shutil.rmtree(TMP_ROOT)
    base_root = TMP_ROOT / "public_archive"
    task_root = _prepare_sample_task(base_root)

    payload = _run_backend(
        "workspace",
        "--base-root",
        str(base_root),
        "--progress-limit",
        "50",
        "--log-lines",
        "20",
    )

    if str(payload.get("base_root", "")) != str(base_root.resolve()):
        raise RuntimeError(f"unexpected base root: {payload.get('base_root')}")
    if int(payload.get("task_count", 0)) != 1:
        raise RuntimeError(f"unexpected task count: {payload.get('task_count')}")
    if str(payload.get("selected_root", "")) != str(task_root.resolve()):
        raise RuntimeError(f"unexpected selected root: {payload.get('selected_root')}")

    detail = payload.get("detail") or {}
    if str(detail.get("task", "")) != "sample_scraper_task":
        raise RuntimeError(f"unexpected task detail: {detail}")
    if int(detail.get("discovered_rows", 0)) < 2:
        raise RuntimeError(f"unexpected discovered rows: {detail}")
    if len(detail.get("pending_rows") or []) < 1:
        raise RuntimeError(f"expected pending rows: {detail}")
    if len(detail.get("done_rows") or []) < 1:
        raise RuntimeError(f"expected done rows: {detail}")
    if "抓取中" not in str(detail.get("progress_text", "")):
        raise RuntimeError(f"unexpected progress text: {detail}")
    if "开始抓取" not in str(detail.get("log_tail", "")):
        raise RuntimeError(f"unexpected log tail: {detail}")

    _safe_print("desktop scraper backend smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
