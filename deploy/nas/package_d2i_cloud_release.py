#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build a slim D2I Cloud release tarball for NAS deploy."""

from __future__ import annotations

import tarfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "deploy" / "nas" / "dist"

INCLUDE_DIRS = [
    "cloud",
    "services",
    "scraper",
    "scripts",
    "deploy/nas",
    "data/coverage",
]
INCLUDE_FILES = [
    "app.py",
    "config.py",
    "llm_client.py",
    "metadata_manager.py",
    "metadata_writer.py",
    "text_parser.py",
    "queue_manager.py",
    "downloader.py",
    "excel_reader.py",
    "image_asset_safety.py",
    "image_source_meta.py",
    "titi_metadata_schema.py",
    "visual_classifier.py",
    "requirements.txt",
    "cloud/README.md",
]
EXCLUDE_PARTS = {
    "__pycache__",
    ".pyc",
    ".git",
    "node_modules",
    "desktop-next",
    ".venv",
    "_tmp_browser",
    "dist",
}


def _skip(path: Path) -> bool:
    parts = set(path.parts)
    if parts & EXCLUDE_PARTS:
        return True
    name = path.name
    if name.endswith(".pyc") or name == ".DS_Store":
        return True
    if "_tmp_browser" in path.as_posix():
        return True
    return False


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"d2i-cloud-{stamp}.tar.gz"
    count = 0
    with tarfile.open(out, "w:gz") as tar:
        for rel in INCLUDE_FILES:
            path = ROOT / rel
            if path.is_file() and not _skip(path):
                tar.add(path, arcname=rel.replace("\\", "/"))
                count += 1
        for d in INCLUDE_DIRS:
            base = ROOT / d
            if not base.exists():
                continue
            for path in base.rglob("*"):
                if path.is_file() and not _skip(path):
                    arc = path.relative_to(ROOT).as_posix()
                    tar.add(path, arcname=arc)
                    count += 1
        manifest = OUT_DIR / f"MANIFEST-{stamp}.txt"
        # write release marker inside tar
        marker = OUT_DIR / f"RELEASE-{stamp}.txt"
        marker.write_text(
            f"product=d2i-cloud\nbuilt={stamp}\nroot={ROOT}\nfiles~{count}\n",
            encoding="utf-8",
        )
        tar.add(marker, arcname="RELEASE.txt")
    print(out)
    print(f"files~{count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
