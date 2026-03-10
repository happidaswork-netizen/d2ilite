#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the desktop-next release gate and emit a machine-readable report."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DESKTOP_ROOT = REPO_ROOT / "desktop-next"
REPORT_PATH = REPO_ROOT / ".tmp" / "desktop-next" / "release-gate-report.json"


@dataclass
class StepResult:
    name: str
    command: list[str]
    cwd: str
    duration_seconds: float


def _safe_print(text: str) -> None:
    message = str(text or "")
    try:
        print(message)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(message.encode("utf-8", errors="replace") + b"\n")


def _python_executable() -> Path:
    if os.name == "nt":
        candidate = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = REPO_ROOT / ".venv" / "bin" / "python"
    return candidate if candidate.is_file() else Path(sys.executable)


def _npm_command() -> str:
    return "npm.cmd" if os.name == "nt" else "npm"


def _artifact_path() -> Path:
    if os.name == "nt":
        return DESKTOP_ROOT / "src-tauri" / "target" / "debug" / "d2i-lite-next.exe"
    return DESKTOP_ROOT / "src-tauri" / "target" / "debug" / "d2i-lite-next"


def _run_step(name: str, command: list[str], cwd: Path) -> StepResult:
    _safe_print(f">> {name}")
    started_at = time.time()
    result = subprocess.run(
        command,
        cwd=str(cwd),
        check=False,
    )
    duration = time.time() - started_at
    if result.returncode != 0:
        raise RuntimeError(f"{name} failed with exit code {result.returncode}")
    return StepResult(
        name=name,
        command=command,
        cwd=str(cwd),
        duration_seconds=round(duration, 2),
    )


def main() -> int:
    if not DESKTOP_ROOT.is_dir():
        _safe_print(f"[ERROR] desktop-next not found: {DESKTOP_ROOT}")
        return 2

    python_exe = _python_executable()
    npm_cmd = _npm_command()
    artifact_path = _artifact_path()
    artifact_before = artifact_path.stat().st_mtime if artifact_path.is_file() else None
    steps: list[StepResult] = []

    try:
        steps.append(
            _run_step(
                "py_compile",
                [
                    str(python_exe),
                    "-m",
                    "py_compile",
                    "app.py",
                    "services/metadata_service.py",
                    "services/desktop_metadata_backend_service.py",
                    "services/desktop_scraper_backend_service.py",
                    "metadata_manager.py",
                    "metadata_writer.py",
                    "scripts/desktop_bridge_cli.py",
                    "scripts/desktop_metadata_backend.py",
                    "scripts/desktop_metadata_backend_smoke.py",
                    "scripts/desktop_scraper_backend.py",
                    "scripts/desktop_scraper_backend_smoke.py",
                    "scripts/desktop_scraper_control_smoke.py",
                    "scripts/desktop_tauri_startup_smoke.py",
                    "scripts/desktop_tauri_roundtrip_smoke.py",
                    "scripts/desktop_vite_bridge_smoke.py",
                    "scripts/desktop_next_release_gate.py",
                ],
                REPO_ROOT,
            )
        )
        steps.append(_run_step("phase0_contract_smoke", [str(python_exe), "scripts/phase0_contract_smoke.py"], REPO_ROOT))
        steps.append(_run_step("bridge_cli_smoke", [str(python_exe), "scripts/bridge_cli_smoke.py"], REPO_ROOT))
        steps.append(_run_step("metadata_backend_smoke", [str(python_exe), "scripts/desktop_metadata_backend_smoke.py"], REPO_ROOT))
        steps.append(_run_step("scraper_backend_smoke", [str(python_exe), "scripts/desktop_scraper_backend_smoke.py"], REPO_ROOT))
        steps.append(_run_step("scraper_control_smoke", [str(python_exe), "scripts/desktop_scraper_control_smoke.py"], REPO_ROOT))
        steps.append(_run_step("cargo_check", ["cargo", "check", "--manifest-path", "desktop-next/src-tauri/Cargo.toml"], REPO_ROOT))
        steps.append(_run_step("npm_lint", [npm_cmd, "run", "lint"], DESKTOP_ROOT))
        steps.append(_run_step("npm_build", [npm_cmd, "run", "build"], DESKTOP_ROOT))
        steps.append(_run_step("vite_bridge_smoke", [str(python_exe), "scripts/desktop_vite_bridge_smoke.py"], REPO_ROOT))
        steps.append(_run_step("smoke_provider", [npm_cmd, "run", "smoke:provider"], DESKTOP_ROOT))
        steps.append(_run_step("smoke_roles", [npm_cmd, "run", "smoke:roles"], DESKTOP_ROOT))
        steps.append(_run_step("tauri_startup_smoke", [str(python_exe), "scripts/desktop_tauri_startup_smoke.py"], REPO_ROOT))
        steps.append(_run_step("tauri_roundtrip_smoke", [str(python_exe), "scripts/desktop_tauri_roundtrip_smoke.py"], REPO_ROOT))
        steps.append(_run_step("tauri_build_debug", [npm_cmd, "run", "tauri:build:debug"], DESKTOP_ROOT))
    except Exception as error:
        _safe_print(f"[ERROR] {error}")
        return 1

    if not artifact_path.is_file():
        _safe_print(f"[ERROR] desktop-next artifact not found: {artifact_path}")
        return 1

    artifact_after = artifact_path.stat().st_mtime
    report = {
        "ok": True,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "python": str(python_exe),
        "artifact_path": str(artifact_path),
        "artifact_updated": artifact_before is None or artifact_after >= artifact_before,
        "steps": [asdict(step) for step in steps],
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _safe_print(f"[OK] desktop next release gate passed")
    _safe_print(f"[OK] artifact: {artifact_path}")
    _safe_print(f"[OK] report: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
