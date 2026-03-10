#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for desktop scraper control actions."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict


REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON_EXE = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
BACKEND_SCRIPT = REPO_ROOT / "scripts" / "desktop_scraper_backend.py"
REGISTRY_PATH = REPO_ROOT / ".tmp" / "desktop-next" / "scraper-runtime-registry.json"


def _safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(str(text).encode("utf-8", errors="replace") + b"\n")


def _python_executable() -> str:
    return str(PYTHON_EXE if PYTHON_EXE.is_file() else Path(sys.executable))


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    path.write_text(body, encoding="utf-8")


def _write_fake_runner(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--skip-crawl", action="store_true")
    parser.add_argument("--skip-images", action="store_true")
    parser.add_argument("--skip-metadata", action="store_true")
    args = parser.parse_args()

    root = Path(args.output_root).resolve()
    pause_flag = root / "state" / "manual_pause.flag"
    mode = "rewrite" if args.skip_crawl and args.skip_images else "retry" if args.skip_crawl else "continue"
    print(f"[fake-runner] mode={mode} root={root}", flush=True)
    for index in range(80):
        print(f"[fake-runner] tick={index}", flush=True)
        if pause_flag.exists():
            print("[fake-runner] manual pause detected", flush=True)
            return 0
        time.sleep(0.2)
    print("[fake-runner] completed timeout loop", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""",
        encoding="utf-8",
    )


def _run_backend(*args: str, env: Dict[str, str] | None = None) -> Dict[str, Any]:
    result = subprocess.run(
        [_python_executable(), str(BACKEND_SCRIPT), *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        env=env,
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
    (task_root / "reports" / "gui_public_scraper.log").write_text("[2026-03-10 12:00:00] 开始抓取\n", encoding="utf-8")
    return task_root


def _workspace(base_root: Path, task_root: Path, env: Dict[str, str]) -> Dict[str, Any]:
    return _run_backend(
        "workspace",
        "--base-root",
        str(base_root),
        "--selected-root",
        str(task_root),
        "--progress-limit",
        "50",
        "--log-lines",
        "20",
        env=env,
    )


def _action(base_root: Path, task_root: Path, action_name: str, control: Dict[str, Any] | None, env: Dict[str, str]) -> Dict[str, Any]:
    payload_file = task_root / "state" / f"{action_name}-options.json"
    if control is not None:
        _write_json(payload_file, control)
    args = [
        "action",
        "--action",
        action_name,
        "--output-root",
        str(task_root),
        "--base-root",
        str(base_root),
    ]
    if control is not None:
        args.extend(["--options-file", str(payload_file)])
    return _run_backend(*args, env=env)


def _read_runtime_config(task_root: Path) -> Dict[str, Any]:
    return json.loads((task_root / "state" / "runtime_config.json").read_text(encoding="utf-8"))


def _wait_for_workspace(
    base_root: Path,
    task_root: Path,
    env: Dict[str, str],
    predicate: Callable[[Dict[str, Any]], bool],
    timeout_seconds: float = 15.0,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_payload: Dict[str, Any] = {}
    while time.time() < deadline:
        last_payload = _workspace(base_root, task_root, env)
        detail = last_payload.get("detail") or {}
        if predicate(detail if isinstance(detail, dict) else {}):
            return last_payload
        time.sleep(0.4)
    raise RuntimeError(f"workspace wait timeout: {last_payload}")


def _kill_pid(pid: int) -> None:
    target = max(0, int(pid or 0))
    if target <= 0:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(target), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        os.kill(target, 9)
    except Exception:
        pass


def _restore_registry(backup: bytes | None) -> None:
    if backup is None:
        try:
            REGISTRY_PATH.unlink()
        except FileNotFoundError:
            pass
        return
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_PATH.write_bytes(backup)


def main() -> int:
    backup = REGISTRY_PATH.read_bytes() if REGISTRY_PATH.is_file() else None
    if REGISTRY_PATH.exists():
        REGISTRY_PATH.unlink()

    try:
        with tempfile.TemporaryDirectory(prefix="d2i_scraper_control_") as td:
            temp_root = Path(td)
            base_root = temp_root / "public_archive"
            task_root = _prepare_sample_task(base_root)
            runner_path = temp_root / "fake_scraper_runner.py"
            _write_fake_runner(runner_path)

            env = dict(os.environ)
            env["D2I_DESKTOP_SCRAPER_RUNNER"] = str(runner_path)
            env["PYTHONUTF8"] = "1"

            current_pid = 0
            try:
                initial = _workspace(base_root, task_root, env)
                if int(initial.get("task_count", 0)) != 1:
                    raise RuntimeError(f"unexpected initial workspace: {initial}")

                continue_options = {
                    "mode": "browser",
                    "auto_fallback": False,
                    "disable_page_images": False,
                }
                continued = _action(base_root, task_root, "continue", continue_options, env)
                continue_detail = (continued.get("workspace") or {}).get("detail") or {}
                current_pid = int(continue_detail.get("pid", 0) or 0)
                if not continue_detail.get("session_running") or current_pid <= 0:
                    raise RuntimeError(f"continue did not start task: {continued}")
                rules = (_read_runtime_config(task_root).get("rules") or {})
                if rules.get("image_download_mode") != "browser" or rules.get("auto_fallback_to_browser") is not False:
                    raise RuntimeError(f"continue options were not applied: {rules}")

                paused = _action(base_root, task_root, "pause", None, env)
                pause_detail = (paused.get("workspace") or {}).get("detail") or {}
                if not pause_detail.get("manual_paused"):
                    raise RuntimeError(f"pause did not mark task paused: {paused}")
                _wait_for_workspace(base_root, task_root, env, lambda detail: not bool(detail.get("session_running")))

                resumed = _action(base_root, task_root, "continue", continue_options, env)
                resume_detail = (resumed.get("workspace") or {}).get("detail") or {}
                current_pid = int(resume_detail.get("pid", 0) or 0)
                if resume_detail.get("manual_paused") or not resume_detail.get("session_running"):
                    raise RuntimeError(f"continue did not resume/start task: {resumed}")

                _action(base_root, task_root, "pause", None, env)
                _wait_for_workspace(base_root, task_root, env, lambda detail: not bool(detail.get("session_running")))

                retry_options = {
                    "mode": "requests_jsl",
                    "auto_fallback": True,
                    "disable_page_images": True,
                }
                retried = _action(base_root, task_root, "retry", retry_options, env)
                retry_detail = (retried.get("workspace") or {}).get("detail") or {}
                current_pid = int(retry_detail.get("pid", 0) or 0)
                if not retry_detail.get("session_running"):
                    raise RuntimeError(f"retry did not start task: {retried}")
                rules = (_read_runtime_config(task_root).get("rules") or {})
                if rules.get("image_download_mode") != "requests_jsl" or rules.get("disable_page_images_during_crawl") is not True:
                    raise RuntimeError(f"retry options were not applied: {rules}")

                _action(base_root, task_root, "pause", None, env)
                _wait_for_workspace(base_root, task_root, env, lambda detail: not bool(detail.get("session_running")))

                rewritten = _action(base_root, task_root, "rewrite", None, env)
                rewrite_detail = (rewritten.get("workspace") or {}).get("detail") or {}
                current_pid = int(rewrite_detail.get("pid", 0) or 0)
                if not rewrite_detail.get("session_running") or str(rewrite_detail.get("runtime_state", "")) != "元数据重写中":
                    raise RuntimeError(f"rewrite did not start task: {rewritten}")

                _action(base_root, task_root, "pause", None, env)
                _wait_for_workspace(base_root, task_root, env, lambda detail: not bool(detail.get("session_running")))
            finally:
                _kill_pid(current_pid)
    finally:
        _restore_registry(backup)

    _safe_print("desktop scraper control smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
