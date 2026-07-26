# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_FILE = str(PROJECT_ROOT / "app.py")
SCRAPER_SCRIPT = PROJECT_ROOT / "scraper" / "run_public_scraper.py"
TEMPLATES_DIR = PROJECT_ROOT / "scraper" / "templates"
CONFIG_TEMPLATES_DIR = PROJECT_ROOT / "scraper"


def cloud_data_root() -> Path:
    override = str(os.environ.get("D2I_CLOUD_DATA_ROOT", "") or "").strip()
    if override:
        path = Path(override).expanduser().resolve()
    else:
        path = PROJECT_ROOT / "data" / "cloud"
    path.mkdir(parents=True, exist_ok=True)
    return path


def jobs_db_path() -> Path:
    override = str(os.environ.get("D2I_CLOUD_JOBS_DB", "") or "").strip()
    if override:
        path = Path(override).expanduser().resolve()
    else:
        path = cloud_data_root() / "jobs.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def default_tasks_base_root() -> str:
    override = str(os.environ.get("D2I_CLOUD_TASKS_ROOT", "") or "").strip()
    if override:
        return str(Path(override).expanduser().resolve())
    from services.task_service import default_public_tasks_root

    return default_public_tasks_root(APP_FILE)


def people_db_path() -> Path:
    """Person registry (Hermes/D-layer). NAS default can be overridden."""
    override = str(os.environ.get("D2I_PEOPLE_DB", "") or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    candidates = [
        Path("/vol4/1001/hermes-runtime/db/people.sqlite"),
        Path("/runtime/people/people.sqlite"),
        cloud_data_root() / "people.sqlite",
        PROJECT_ROOT / "data" / "people.sqlite",
    ]
    for nas in candidates:
        try:
            if nas.is_file():
                return nas.resolve()
        except OSError:
            continue
    return candidates[-1]


def coverage_snapshot_path() -> Path:
    override = str(os.environ.get("D2I_COVERAGE_SNAPSHOT", "") or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    candidates = [
        cloud_data_root() / "coverage" / "rollup_snapshot_20260726.json",
        Path("/runtime/d2i-cloud-data/coverage/rollup_snapshot_20260726.json"),
        PROJECT_ROOT / "data" / "coverage" / "rollup_snapshot_20260726.json",
    ]
    for path in candidates:
        try:
            if path.is_file():
                return path.resolve()
        except OSError:
            continue
    return candidates[-1]
