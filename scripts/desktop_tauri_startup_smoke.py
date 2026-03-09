#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for desktop-next tauri dev startup."""

from __future__ import annotations

import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DESKTOP_ROOT = REPO_ROOT / "desktop-next"
FRONTEND_STATUS_REPORT = REPO_ROOT / ".tmp" / "desktop-next" / "frontend-status.json"
TIMEOUT_SECONDS = 120
SLEEP_SECONDS = 2

READY_PATTERNS = [
    "http://127.0.0.1:1420/",
    "Running DevCommand (`cargo",
    "Watching",
    "Running `target\\debug\\d2i-lite-next.exe`",
]


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


def main() -> int:
    if not DESKTOP_ROOT.is_dir():
        print(f"[ERROR] desktop-next not found: {DESKTOP_ROOT}")
        return 2
    try:
        FRONTEND_STATUS_REPORT.unlink()
    except FileNotFoundError:
        pass

    cmd = ["npm.cmd", "run", "tauri:dev"] if os.name == "nt" else ["npm", "run", "tauri:dev"]
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    preexec_fn = None if os.name == "nt" else os.setsid

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

    deadline = time.time() + TIMEOUT_SECONDS
    ready = False
    status_report: dict[str, object] = {}
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
            text = _strip_ansi("\n".join(lines))
            log_ready = all(token in text for token in READY_PATTERNS)
            if FRONTEND_STATUS_REPORT.is_file():
                try:
                    status_report = json.loads(FRONTEND_STATUS_REPORT.read_text(encoding="utf-8"))
                except Exception:
                    status_report = {}
            status_ready = (
                status_report.get("provider") == "tauri"
                and status_report.get("ping_ok") is True
                and status_report.get("bridge_provider") == "python-cli"
            )
            if log_ready and status_ready:
                ready = True
                break
            if ready:
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
    if not ready:
        _safe_print("[ERROR] desktop tauri startup smoke failed")
        if output.strip():
            _safe_print(output)
        if status_report:
            _safe_print(json.dumps(status_report, ensure_ascii=False, indent=2))
        return 1

    _safe_print("[OK] desktop tauri startup smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
