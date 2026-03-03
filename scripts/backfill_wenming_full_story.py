#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill per-person full stories for Wenming candidate images.

Usage:
  py -3 ./scripts/backfill_wenming_full_story.py \
    --profiles ./data/public_archive/wenming_9thmd_candidates_safe_run_20260302_171151/raw/profiles.jsonl \
    --images-dir "C:/Users/rpy/OneDrive/图片/角色肖像/榜样2025"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from metadata_manager import read_image_metadata  # type: ignore
from metadata_writer import write_xmp_metadata  # type: ignore


INDEX_LINE_RE = re.compile(r"^\d{2,4}(?:\s+[A-Za-z\u4e00-\u9fff·•\s]{1,40})?$")


def normalize_multiline(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return ""
    lines = [" ".join(line.split()).strip() for line in text.split("\n")]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


def normalize_name_key(value: Any) -> str:
    text = str(value or "").strip()
    text = text.replace("·", "").replace("•", "")
    text = re.sub(r"\s+", "", text)
    return text


def filename_to_name_key(stem: str) -> str:
    base = re.sub(r"_\d+$", "", str(stem or "").strip())
    return normalize_name_key(base)


def _find_non_space_offset(text: str, target_non_space_idx: int) -> int:
    seen = 0
    for i, ch in enumerate(text):
        if ch.isspace():
            continue
        if seen == target_non_space_idx:
            return i
        seen += 1
    return -1


def find_summary_pos(full_text: str, summary: str) -> int:
    if not full_text or not summary:
        return -1
    pos = full_text.find(summary)
    if pos >= 0:
        return pos

    compact_summary = re.sub(r"\s+", "", summary)
    if not compact_summary:
        return -1
    probe = compact_summary[:18]
    if not probe:
        return -1

    compact_full = re.sub(r"\s+", "", full_text)
    compact_pos = compact_full.find(probe)
    if compact_pos < 0:
        return -1
    return _find_non_space_offset(full_text, compact_pos)


def expand_start_to_index_line(full_text: str, start_pos: int) -> int:
    if start_pos <= 0:
        return 0
    line_start = full_text.rfind("\n", 0, start_pos)
    if line_start < 0:
        return 0
    prev_end = line_start
    prev_start = full_text.rfind("\n", 0, prev_end)
    prev_line = full_text[(prev_start + 1 if prev_start >= 0 else 0) : prev_end].strip()
    if INDEX_LINE_RE.match(prev_line):
        return prev_start + 1 if prev_start >= 0 else 0
    return start_pos


def pick_summary(row: Dict[str, Any]) -> str:
    fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
    summary = fields.get("person_summary") or row.get("summary") or ""
    return normalize_multiline(summary)


def extract_story_by_detail(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    by_group: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        group_key = str(row.get("list_url") or "")
        by_group[group_key].append(row)

    result: Dict[str, str] = {}
    for group_rows in by_group.values():
        full_text = ""
        for row in group_rows:
            candidate = normalize_multiline(row.get("full_content", ""))
            if len(candidate) > len(full_text):
                full_text = candidate

        entries: List[Tuple[Dict[str, Any], int, str]] = []
        for row in group_rows:
            summary = pick_summary(row)
            pos = find_summary_pos(full_text, summary)
            entries.append((row, pos, summary))

        indexed_entries = [item for item in entries if item[1] >= 0]
        indexed_entries.sort(key=lambda item: item[1])

        for i, (row, start_pos, summary) in enumerate(indexed_entries):
            end_pos = len(full_text)
            if i + 1 < len(indexed_entries):
                end_pos = indexed_entries[i + 1][1]
            start_pos = expand_start_to_index_line(full_text, start_pos)
            story = normalize_multiline(full_text[start_pos:end_pos])
            if (not story) and summary:
                story = summary
            result[str(row.get("detail_url") or "")] = story

        for row, _, summary in entries:
            detail_url = str(row.get("detail_url") or "")
            if detail_url in result:
                continue
            fallback = summary or normalize_multiline(row.get("full_content") or "")
            if fallback:
                result[detail_url] = fallback

    return result


def build_description(existing_description: str, summary: str, full_story: str) -> str:
    desc = normalize_multiline(existing_description)
    for marker in ("个人事迹：", "完整事迹：", "事迹："):
        idx = desc.find(marker)
        if idx >= 0:
            desc = desc[:idx].rstrip()
            break

    if summary and ("简介：" not in desc):
        if desc:
            desc = f"{desc}\n简介：\n{summary}"
        else:
            desc = f"简介：\n{summary}"

    if full_story:
        if desc:
            desc = f"{desc}\n个人事迹：\n{full_story}"
        else:
            desc = full_story
    return desc.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill full story metadata for Wenming candidate images.")
    parser.add_argument("--profiles", required=True, help="Path to raw/profiles.jsonl")
    parser.add_argument("--images-dir", required=True, help="Target image directory")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; do not write metadata")
    args = parser.parse_args()

    profiles_path = Path(args.profiles).resolve()
    images_dir = Path(args.images_dir).resolve()
    if not profiles_path.exists():
        raise SystemExit(f"profiles not found: {profiles_path}")
    if not images_dir.exists():
        raise SystemExit(f"images dir not found: {images_dir}")

    rows: List[Dict[str, Any]] = []
    for line in profiles_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))

    story_by_detail = extract_story_by_detail(rows)
    row_by_name: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = normalize_name_key(row.get("name", ""))
        if key and (key not in row_by_name):
            row_by_name[key] = row

    image_files = sorted(
        [p for p in images_dir.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png"}]
    )

    written = 0
    missing_row = 0
    failed = 0
    samples: List[Dict[str, Any]] = []

    for image_path in image_files:
        name_key = filename_to_name_key(image_path.stem)
        row = row_by_name.get(name_key)
        if row is None:
            missing_row += 1
            continue

        detail_url = str(row.get("detail_url") or "")
        list_url = str(row.get("list_url") or "")
        fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
        mapped = row.get("mapped") if isinstance(row.get("mapped"), dict) else {}
        summary = pick_summary(row)
        full_story = normalize_multiline(story_by_detail.get(detail_url) or "")
        if not full_story:
            full_story = summary or normalize_multiline(row.get("full_content") or "")

        info = read_image_metadata(str(image_path))
        person = str(row.get("name") or info.person or image_path.stem).strip()
        image_url = str(row.get("image_url") or info.image_url or "").strip()
        birth_date = ""
        if isinstance(getattr(info, "titi_json", None), dict):
            prof = (info.titi_json or {}).get("d2i_profile")
            if isinstance(prof, dict):
                birth_date = str(prof.get("birth_date") or "").strip()
        person_index = str(fields.get("person_index") or "").strip()
        person_image_url = str(fields.get("person_image_url") or "").strip()

        description = build_description(info.description or "", summary, full_story)
        profile_patch: Dict[str, Any] = {
            "summary": summary,
            "full_content": full_story,
            "source_detail_url": detail_url,
            "source_list_url": list_url,
            "person_index": person_index,
            "person_summary": summary,
        }
        if person_image_url:
            profile_patch["person_image_url"] = person_image_url
        if birth_date:
            profile_patch["birth_date"] = birth_date

        payload: Dict[str, Any] = {
            "title": person,
            "person": person,
            "name": person,
            "description": description,
            "source": detail_url or list_url,
            "image_url": image_url,
            "url": image_url,
            "gender": info.gender,
            "city": info.city,
            "position": info.position,
            "police_id": info.police_id,
            "d2i_profile": profile_patch,
        }

        try:
            if not args.dry_run:
                write_xmp_metadata(str(image_path), payload)
            written += 1
            if len(samples) < 5:
                samples.append(
                    {
                        "file": image_path.name,
                        "person": person,
                        "summary_len": len(summary),
                        "full_story_len": len(full_story),
                    }
                )
        except Exception as exc:  # pragma: no cover - operational safety path
            failed += 1
            print(f"[fail] {image_path.name}: {exc}")

    result = {
        "profiles_rows": len(rows),
        "images_total": len(image_files),
        "written": written,
        "missing_row": missing_row,
        "failed": failed,
        "dry_run": bool(args.dry_run),
        "sample": samples,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
