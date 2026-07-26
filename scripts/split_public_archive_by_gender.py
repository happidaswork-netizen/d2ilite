"""Split a public scraper output folder into gender subfolders.

The scraper keeps final images in the task root. This utility moves those
images into ``男`` / ``女`` / ``未知`` and updates JSONL path fields that point
to the moved files. Multi-image article names such as ``张三-资料图-1.jpg`` use
the base person name for gender lookup.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


PATH_KEYS = {
    "path",
    "saved_path",
    "named_path",
    "output_path",
    "input_path",
    "local_image_path",
}

MATERIAL_IMAGE_RE = re.compile(r"-资料图-\d+$")
UNIQUE_SUFFIX_RE = re.compile(r"_\d+$")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
    tmp.replace(path)


def normalized_gender(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"男", "male", "m", "man", "男性"}:
        return "男"
    if text in {"女", "female", "f", "woman", "女性"}:
        return "女"
    return "未知"


def gender_from_row(row: Dict[str, Any]) -> str:
    gender = normalized_gender(row.get("gender"))
    if gender != "未知":
        return gender
    audit = row.get("audit")
    if isinstance(audit, dict):
        gender = normalized_gender(audit.get("gender"))
        if gender != "未知":
            return gender
    values = row.get("audit_values")
    if isinstance(values, dict):
        gender = normalized_gender(values.get("gender"))
        if gender != "未知":
            return gender
    record = row.get("record")
    if isinstance(record, dict):
        return gender_from_row(record)
    return "未知"


def person_name_from_row(row: Dict[str, Any]) -> str:
    for key in ("name", "person"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    record = row.get("record")
    if isinstance(record, dict):
        return person_name_from_row(record)
    audit = row.get("audit")
    if isinstance(audit, dict):
        value = str(audit.get("name") or "").strip()
        if value:
            return value
    return ""


def gender_lookup_key_from_stem(stem: str, known_names: Dict[str, str]) -> str:
    key = MATERIAL_IMAGE_RE.sub("", stem)
    if key in known_names:
        return key
    return UNIQUE_SUFFIX_RE.sub("", key)


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    index = 2
    while True:
        candidate = parent / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def move_file(src: Path, dest_dir: Path) -> Tuple[Path, bool]:
    if not src.exists() or not src.is_file():
        return src, False
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = unique_path(dest_dir / src.name)
    if src.resolve() == dest.resolve():
        return dest, False
    shutil.move(str(src), str(dest))
    return dest, True


def update_row_paths(row: Dict[str, Any], moved: Dict[str, str]) -> Dict[str, Any]:
    updated = dict(row)
    for key, value in list(updated.items()):
        if key in PATH_KEYS and isinstance(value, str):
            replacement = moved.get(str(Path(value)))
            if replacement:
                updated[key] = replacement
        elif isinstance(value, dict):
            updated[key] = update_row_paths(value, moved)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", help="Public scraper output root, e.g. data/public_archive/公安部英烈_2022")
    parser.add_argument("--apply", action="store_true", help="Move files and update JSONL files.")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"root not found: {root}")

    profiles = read_jsonl(root / "raw" / "profiles.jsonl")
    metadata_results = read_jsonl(root / "raw" / "metadata_write_results.jsonl")
    gender_skips = read_jsonl(root / "raw" / "gender_skips.jsonl")
    rows_for_gender = (
        profiles
        + metadata_results
        + [row.get("record", row) for row in gender_skips]
    )

    by_name: Dict[str, str] = {}
    for row in rows_for_gender:
        if not isinstance(row, dict):
            continue
        name = person_name_from_row(row)
        if name:
            by_name[name] = gender_from_row(row)

    moved: Dict[str, str] = {}
    planned: List[Dict[str, str]] = []
    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

    candidate_dirs = [root, root / "_skipped_gender"]
    for candidate_dir in candidate_dirs:
        if not candidate_dir.exists():
            continue
        for src in sorted(candidate_dir.iterdir()):
            if not src.is_file() or src.suffix.lower() not in image_exts:
                continue
            lookup_key = gender_lookup_key_from_stem(src.stem, by_name)
            if lookup_key not in by_name:
                continue
            gender = by_name.get(lookup_key, "未知")
            dest_dir = root / gender
            planned.append({"from": str(src), "to_dir": str(dest_dir), "gender": gender})
            if args.apply:
                dest, did_move = move_file(src, dest_dir)
                if did_move:
                    moved[str(src)] = str(dest)

    if args.apply and moved:
        for rel in [
            "downloads/image_downloads.jsonl",
            "raw/metadata_write_results.jsonl",
            "raw/metadata_queue.jsonl",
            "raw/review_queue.jsonl",
            "raw/failures.jsonl",
            "raw/gender_skips.jsonl",
        ]:
            path = root / rel
            rows = read_jsonl(path)
            if not rows:
                continue
            write_jsonl(path, [update_row_paths(row, moved) for row in rows])

    summary = {
        "root": str(root),
        "apply": bool(args.apply),
        "planned": len(planned),
        "moved": len(moved),
        "by_gender": {
            "男": sum(1 for item in planned if item["gender"] == "男"),
            "女": sum(1 for item in planned if item["gender"] == "女"),
            "未知": sum(1 for item in planned if item["gender"] == "未知"),
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
