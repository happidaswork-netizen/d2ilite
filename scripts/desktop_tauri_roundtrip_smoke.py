#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""End-to-end smoke for desktop-next tauri bridge roundtrip."""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from PIL import Image


REPO_ROOT = Path(__file__).resolve().parent.parent
DESKTOP_ROOT = REPO_ROOT / "desktop-next"
TEMP_ROOT = REPO_ROOT / ".tmp" / "desktop-next"
REGISTRY_PATH = REPO_ROOT / ".tmp" / "desktop-next" / "scraper-runtime-registry.json"
SMOKE_REQUEST_PATH = TEMP_ROOT / "smoke-request.json"
SMOKE_REPORT_PATH = TEMP_ROOT / "smoke-report.json"
FRONTEND_STATUS_PATH = TEMP_ROOT / "frontend-status.json"
TIMEOUT_SECONDS = 120
SLEEP_SECONDS = 2

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


def _write_sample_image(target_dir: Path) -> tuple[Path, str]:
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = "smoke-sample.jpg"
    target_path = target_dir / filename
    image = Image.new("RGB", (24, 24), (245, 245, 245))
    image.save(target_path, format="JPEG", quality=92)
    return target_path, filename


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


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
    _write_jsonl(task_root / "raw" / "failures.jsonl", [{"url": "https://example.com/detail/failure", "reason": "image_download_http_error"}])
    (task_root / "reports").mkdir(parents=True, exist_ok=True)
    (task_root / "reports" / "gui_public_scraper.log").write_text("[2026-03-10 12:00:00] 开始抓取\n", encoding="utf-8")
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


def _prepare_request() -> tuple[dict[str, object], Path, Path, Path]:
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    for path in (SMOKE_REQUEST_PATH, SMOKE_REPORT_PATH, FRONTEND_STATUS_PATH):
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    session_dir = TEMP_ROOT / f"roundtrip-{uuid.uuid4().hex}"
    images_dir = session_dir / "images"
    image_path, filename = _write_sample_image(images_dir)
    scraper_base_root = session_dir / "public_archive"
    scraper_task_root = _prepare_scraper_task(scraper_base_root)
    fake_runner_path = session_dir / "fake_scraper_runner.py"
    _write_fake_runner(fake_runner_path)
    marker = f"tauri-smoke-{uuid.uuid4().hex[:8]}"
    request = {
        "smoke_id": uuid.uuid4().hex,
        "folder": str(images_dir),
        "filename": filename,
        "marker": marker,
        "scraper_base_root": str(scraper_base_root),
        "scraper_task_root": str(scraper_task_root),
    }
    SMOKE_REQUEST_PATH.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")
    return request, image_path, scraper_task_root, fake_runner_path


def _cleanup_request(image_path: Path) -> None:
    for path in (SMOKE_REQUEST_PATH, SMOKE_REPORT_PATH, FRONTEND_STATUS_PATH):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    try:
        shutil.rmtree(image_path.parent.parent, ignore_errors=True)
    except Exception:
        pass


def main() -> int:
    if not DESKTOP_ROOT.is_dir():
        _safe_print(f"[ERROR] desktop-next not found: {DESKTOP_ROOT}")
        return 2

    backup = REGISTRY_PATH.read_bytes() if REGISTRY_PATH.is_file() else None
    if REGISTRY_PATH.exists():
        REGISTRY_PATH.unlink()

    request, image_path, _scraper_task_root, fake_runner_path = _prepare_request()
    cmd = ["npm.cmd", "run", "tauri:dev"] if os.name == "nt" else ["npm", "run", "tauri:dev"]
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

    reader = threading.Thread(target=_pump_stdout, daemon=True)
    reader.start()

    report: dict[str, object] = {}
    deadline = time.time() + TIMEOUT_SECONDS
    ok = False
    try:
        while time.time() < deadline:
            drained = False
            while True:
                try:
                    line = stdout_queue.get_nowait()
                except queue.Empty:
                    break
                drained = True
                lines.append(line)

            if SMOKE_REPORT_PATH.is_file():
                try:
                    report = json.loads(SMOKE_REPORT_PATH.read_text(encoding="utf-8"))
                except Exception:
                    report = {}

            ok = (
                report.get("smoke_id") == request["smoke_id"]
                and report.get("provider") == "tauri"
                and report.get("ping_ok") is True
                and report.get("bridge_provider") == "native-exiftool"
                and report.get("ok") is True
                and report.get("list_ok") is True
                and report.get("read_ok") is True
                and report.get("save_ok") is True
                and report.get("preview_ok") is True
                and report.get("scraper_ok") is True
                and report.get("scraper_control_ok") is True
                and report.get("filename") == request["filename"]
                and report.get("title_after") == request["marker"]
            )
            if ok:
                break
            if proc.poll() is not None:
                break
            if not drained:
                time.sleep(SLEEP_SECONDS)
    finally:
        _kill_process_tree(proc)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        while True:
            try:
                lines.append(stdout_queue.get_nowait())
            except queue.Empty:
                break

    output = _strip_ansi("\n".join(lines))
    if not ok:
        _safe_print("[ERROR] desktop tauri roundtrip smoke failed")
        if output.strip():
            _safe_print(output)
        if report:
            _safe_print(json.dumps(report, ensure_ascii=False, indent=2))
        _cleanup_request(image_path)
        if backup is None:
            try:
                REGISTRY_PATH.unlink()
            except FileNotFoundError:
                pass
        else:
            REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
            REGISTRY_PATH.write_bytes(backup)
        return 1

    _safe_print("[OK] desktop tauri roundtrip smoke passed")
    _cleanup_request(image_path)
    if backup is None:
        try:
            REGISTRY_PATH.unlink()
        except FileNotFoundError:
            pass
    else:
        REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        REGISTRY_PATH.write_bytes(backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
