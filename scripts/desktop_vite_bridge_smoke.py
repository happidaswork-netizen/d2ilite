#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for desktop-next Vite bridge routes."""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

from PIL import Image


REPO_ROOT = Path(__file__).resolve().parent.parent
DESKTOP_ROOT = REPO_ROOT / "desktop-next"
REGISTRY_PATH = REPO_ROOT / ".tmp" / "desktop-next" / "scraper-runtime-registry.json"
TIMEOUT_SECONDS = 90
BASE_URL = "http://127.0.0.1:1420"
READY_PATTERN = "http://127.0.0.1:1420/"


def _safe_print(text: str) -> None:
    message = str(text or "")
    try:
        print(message)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(message.encode("utf-8", errors="replace") + b"\n")


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", str(text or ""))


def _kill_process_tree(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        proc.terminate()


def _request_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RuntimeError(f"bridge json invalid: {payload}")
    return payload


def _request_json_post(url: str, payload: object) -> dict:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict) or data.get("ok") is not True:
        raise RuntimeError(f"bridge json invalid: {data}")
    return data


def _request_preview(url: str) -> bool:
    with urllib.request.urlopen(url, timeout=15) as response:
        return response.status == 200 and str(response.headers.get("Content-Type", "")).startswith("image/")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    path.write_text(body, encoding="utf-8")


def _prepare_scraper_task(base_root: Path) -> Path:
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
    for index in range(80):
        print(f"[fake-runner] tick={index}", flush=True)
        if pause_flag.exists():
            print("[fake-runner] manual pause detected", flush=True)
            return 0
        time.sleep(0.2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""",
        encoding="utf-8",
    )


def main() -> int:
    if not DESKTOP_ROOT.is_dir():
        _safe_print(f"[ERROR] desktop-next not found: {DESKTOP_ROOT}")
        return 2

    backup = REGISTRY_PATH.read_bytes() if REGISTRY_PATH.is_file() else None
    if REGISTRY_PATH.exists():
        REGISTRY_PATH.unlink()

    try:
        with tempfile.TemporaryDirectory(prefix="d2i_vite_bridge_") as td:
            temp_root = Path(td)
            image_path = temp_root / "sample.jpg"
            Image.new("RGB", (32, 32), (200, 200, 200)).save(image_path, format="JPEG", quality=90)
            scraper_base_root = temp_root / "public_archive"
            scraper_task_root = _prepare_scraper_task(scraper_base_root)
            fake_runner_path = temp_root / "fake_scraper_runner.py"
            _write_fake_runner(fake_runner_path)

            cmd = ["npm.cmd", "run", "dev"] if os.name == "nt" else ["npm", "run", "dev"]
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            preexec_fn = None if os.name == "nt" else os.setsid
            env = dict(os.environ)
            env["D2I_DESKTOP_SCRAPER_RUNNER"] = str(fake_runner_path)
            env["PYTHONUTF8"] = "1"

            proc = subprocess.Popen(
                cmd,
                cwd=str(DESKTOP_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=creationflags,
                preexec_fn=preexec_fn,
                env=env,
            )

            lines: list[str] = []
            stdout_queue: "queue.SimpleQueue[str]" = queue.SimpleQueue()

            def _pump_stdout() -> None:
                if proc.stdout is None:
                    return
                try:
                    for line in proc.stdout:
                        stdout_queue.put(line.rstrip())
                except Exception:
                    return

            threading.Thread(target=_pump_stdout, daemon=True).start()

            deadline = time.time() + TIMEOUT_SECONDS
            ping_ok = False
            preview_ok = False
            scraper_ok = False
            control_ok = False
            last_ping: dict = {}
            last_list: dict = {}
            last_read: dict = {}
            last_scraper: dict = {}
            last_action: dict = {}
            try:
                while time.time() < deadline:
                    while True:
                        try:
                            lines.append(stdout_queue.get_nowait())
                        except queue.Empty:
                            break

                    text = _strip_ansi("\n".join(lines))
                    if READY_PATTERN not in text:
                        if proc.poll() is not None:
                            break
                        time.sleep(1)
                        continue

                    try:
                        ping = _request_json(f"{BASE_URL}/api/bridge/ping")
                        last_ping = ping
                        ping_ok = str(ping.get("provider", "")) == "native-exiftool"
                        folder_qs = urllib.parse.urlencode({"folder": str(temp_root), "limit": "10"})
                        listed = _request_json(f"{BASE_URL}/api/bridge/list?{folder_qs}")
                        last_list = listed
                        if not isinstance(listed.get("items"), list) or str(image_path) not in listed.get("items", []):
                            raise RuntimeError(f"list response invalid: {listed}")
                        read_qs = urllib.parse.urlencode({"path": str(image_path)})
                        read = _request_json(f"{BASE_URL}/api/bridge/read?{read_qs}")
                        last_read = read
                        if str(read.get("item", {}).get("filename", "")) != image_path.name:
                            raise RuntimeError(f"read response invalid: {read}")
                        preview_ok = _request_preview(f"{BASE_URL}/api/bridge/preview?{read_qs}")
                        scraper_qs = urllib.parse.urlencode(
                            {
                                "baseRoot": str(scraper_base_root),
                                "progressLimit": "50",
                                "logLines": "20",
                            }
                        )
                        scraper = _request_json(f"{BASE_URL}/api/bridge/scraper/workspace?{scraper_qs}")
                        last_scraper = scraper
                        scraper_ok = (
                            str(scraper.get("selected_root", "")) == str(scraper_task_root)
                            and int(scraper.get("task_count", 0)) == 1
                            and len((scraper.get("detail") or {}).get("pending_rows") or []) >= 1
                        )
                        if scraper_ok and not control_ok:
                            action_payload = _request_json_post(
                                f"{BASE_URL}/api/bridge/scraper/action",
                                {
                                    "action": "continue",
                                    "outputRoot": str(scraper_task_root),
                                    "baseRoot": str(scraper_base_root),
                                    "control": {
                                        "mode": "browser",
                                        "auto_fallback": False,
                                        "disable_page_images": False,
                                    },
                                },
                            )
                            last_action = action_payload
                            detail = (action_payload.get("workspace") or {}).get("detail") or {}
                            control_ok = bool(detail.get("session_running")) and int(detail.get("pid", 0) or 0) > 0
                            if control_ok:
                                _request_json_post(
                                    f"{BASE_URL}/api/bridge/scraper/action",
                                    {
                                        "action": "pause",
                                        "outputRoot": str(scraper_task_root),
                                        "baseRoot": str(scraper_base_root),
                                        "control": {},
                                    },
                                )
                        if ping_ok and preview_ok and scraper_ok and control_ok:
                            break
                    except Exception:
                        if proc.poll() is not None:
                            break
                        time.sleep(1)
                else:
                    text = _strip_ansi("\n".join(lines))
            finally:
                _kill_process_tree(proc)
                try:
                    proc.wait(timeout=10)
                except Exception:
                    pass

            if not ping_ok or not preview_ok or not scraper_ok or not control_ok:
                _safe_print("[ERROR] desktop vite bridge smoke failed")
                _safe_print(
                    json.dumps(
                        {
                            "ping_ok": ping_ok,
                            "preview_ok": preview_ok,
                            "scraper_ok": scraper_ok,
                            "control_ok": control_ok,
                            "ping": last_ping,
                            "list": last_list,
                            "read": last_read,
                            "scraper": last_scraper,
                            "action": last_action,
                        },
                        ensure_ascii=False,
                    )
                )
                if lines:
                    _safe_print(_strip_ansi("\n".join(lines)))
                return 1
    finally:
        if backup is None:
            try:
                REGISTRY_PATH.unlink()
            except FileNotFoundError:
                pass
        else:
            REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
            REGISTRY_PATH.write_bytes(backup)

    _safe_print("[OK] desktop vite bridge smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
