#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Audit and repair image metadata fields in bulk.

Usage:
  python scripts/audit_fix_metadata_fields.py --root data/public_archive --recursive --dry-run
  python scripts/audit_fix_metadata_fields.py --root "Z:\\your\\final_images" --recursive
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from metadata_manager import (  # type: ignore
    clean_keywords,
    read_image_metadata,
    update_metadata_preserve_others,
)


SUPPORTED_EXTS = {".jpg", ".jpeg"}


@dataclass
class AuditIssue:
    path: str
    missing_person: bool = False
    missing_source: bool = False
    missing_image_url: bool = False
    missing_description: bool = False
    fragmented_description: bool = False
    invalid_gender: bool = False


def _iter_images(root: Path, recursive: bool) -> Iterable[Path]:
    if recursive:
        for ext in SUPPORTED_EXTS:
            yield from root.rglob(f"*{ext}")
        return
    for p in root.iterdir():
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
            yield p


def _normalize_gender(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in {"male", "m", "man", "男性", "男"}:
        return "男"
    if lowered in {"female", "f", "woman", "女性", "女"}:
        return "女"
    return ""


def _guess_name_from_filename(path: Path) -> str:
    stem = path.stem.strip()
    if not stem:
        return ""
    # Remove duplicate suffixes like "_2" / "-3"
    stem = re.sub(r"[_\-]\d+$", "", stem).strip()
    # Do not treat hash-like filenames as person names.
    if re.fullmatch(r"[a-fA-F0-9]{16,}", stem):
        return ""
    if re.fullmatch(r"[a-zA-Z0-9_\-]{24,}", stem):
        return ""
    return stem


def _profile_from_titi(info: Any) -> Dict[str, Any]:
    titi = getattr(info, "titi_json", None)
    if not isinstance(titi, dict):
        return {}
    profile = titi.get("d2i_profile")
    return dict(profile) if isinstance(profile, dict) else {}


def _first_non_empty(*values: Any) -> str:
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _description_looks_fragmented(text: str) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    if re.search(r"\n\s*年\s*\n|\n\s*月\s*\n|\n\s*日\s*\n", s):
        return True
    lines = [x.strip() for x in s.replace("\r", "").split("\n") if x.strip()]
    short_lines = sum(1 for x in lines if len(x) <= 2)
    return short_lines >= 6


def _normalize_description(text: Any) -> str:
    s = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    s = s.strip()
    if not s:
        return ""

    # Drop isolated quote artifacts at line start.
    s = re.sub(r"^\s*[\"'`“”‘’]+\s*\n?", "", s)

    # Normalize common date fragments split by line breaks.
    s = re.sub(
        r"(\d{4})\s*\n\s*年\s*\n\s*(\d{1,2})\s*\n\s*月\s*\n\s*(\d{1,2})\s*\n\s*日",
        r"\1年\2月\3日",
        s,
    )
    s = re.sub(r"(\d{4})\s*\n\s*年", r"\1年", s)
    s = re.sub(r"(\d{1,2})\s*\n\s*月", r"\1月", s)
    s = re.sub(r"(\d{1,2})\s*\n\s*日", r"\1日", s)
    s = re.sub(r"(\d{1,2})\s*\n\s*号", r"\1号", s)
    s = re.sub(r"(\d{1,2})\s*\n\s*次", r"\1次", s)

    # Remove blank lines and trim each line.
    lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
    if not lines:
        return ""

    def _is_fragment_line(line: str) -> bool:
        token = str(line or "").strip()
        if not token:
            return False
        if re.fullmatch(r"\d{1,4}(?:\.\d+)?%?", token):
            return True
        if re.fullmatch(r"\d{1,4}年|\d{1,2}月|\d{1,2}日|\d{1,2}时|\d{1,2}分|\d{1,2}次", token):
            return True
        if token in {".", "。", ",", "，", ":", "：", ";", "；", "、", "余名", "余次", "余人", "余辆", "余起"}:
            return True
        return False

    def _should_attach_to_previous(prev: str, cur: str) -> bool:
        prev_t = str(prev or "").strip()
        cur_t = str(cur or "").strip()
        if (not prev_t) or (not cur_t):
            return False
        if _is_fragment_line(cur_t):
            return True
        if re.fullmatch(r"\d{2,4}年", cur_t):
            return True
        if prev_t.endswith(("自", "于", "从", "在", "至", "达", "约", "近", "共", "累计", "超过", "仅", "当日")):
            return True
        return False

    merged_lines: List[str] = []
    for line in lines:
        if merged_lines and _should_attach_to_previous(merged_lines[-1], line):
            merged_lines[-1] = f"{merged_lines[-1]}{line}"
        else:
            merged_lines.append(line)

    s = "\n".join(merged_lines)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def _collect_keywords(info: Any, profile: Dict[str, Any], person: str, gender: str, position: str, city: str) -> List[str]:
    existing = list(getattr(info, "keywords", []) or [])
    if not existing and isinstance(profile.get("keywords"), list):
        existing = [str(x or "").strip() for x in profile.get("keywords", []) if str(x or "").strip()]
    if not existing:
        return []
    return clean_keywords(existing)


def _propose_patch(path: Path, info: Any) -> Tuple[Dict[str, Any], AuditIssue]:
    profile = _profile_from_titi(info)
    guessed_name = _guess_name_from_filename(path)
    title = str(getattr(info, "title", "") or "").strip()
    person = _first_non_empty(
        getattr(info, "person", ""),
        profile.get("name", ""),
        title.split(" - ", 1)[0] if " - " in title else "",
        guessed_name,
    )

    desc_existing = str(getattr(info, "description", "") or "").strip()
    description = _first_non_empty(
        _normalize_description(desc_existing),
        _normalize_description(profile.get("description", "")),
        _normalize_description(profile.get("summary", "")),
        _normalize_description(profile.get("full_content", "")),
    )

    image_url = _first_non_empty(
        getattr(info, "image_url", ""),
        profile.get("image_url", ""),
    )
    source = _first_non_empty(
        getattr(info, "source", ""),
        profile.get("source_detail_url", ""),
        profile.get("source_profile_url", ""),
        profile.get("source_url", ""),
        profile.get("source", ""),
        profile.get("detail_url", ""),
        image_url,
    )
    gender = _normalize_gender(_first_non_empty(getattr(info, "gender", ""), profile.get("gender", "")))
    position = _first_non_empty(
        getattr(info, "position", ""),
        profile.get("position", ""),
        profile.get("title", ""),
    )
    city = _first_non_empty(
        getattr(info, "city", ""),
        profile.get("city", ""),
        profile.get("location", ""),
        profile.get("location_text", ""),
    )

    if (not title) and person:
        first_line = (description.splitlines()[0] if description else "").strip()
        title = f"{person} - {first_line}" if first_line else person

    keywords = _collect_keywords(info, profile, person, gender, position, city)

    issue = AuditIssue(
        path=str(path),
        missing_person=(not str(getattr(info, "person", "") or "").strip()),
        missing_source=(not str(getattr(info, "source", "") or "").strip()),
        missing_image_url=(not str(getattr(info, "image_url", "") or "").strip()),
        missing_description=(not desc_existing),
        fragmented_description=_description_looks_fragmented(desc_existing),
        invalid_gender=bool(str(getattr(info, "gender", "") or "").strip()) and (_normalize_gender(getattr(info, "gender", "")) == ""),
    )

    patch: Dict[str, Any] = {}
    old_title = str(getattr(info, "title", "") or "").strip()
    old_person = str(getattr(info, "person", "") or "").strip()
    old_desc = str(getattr(info, "description", "") or "").strip()
    old_source = str(getattr(info, "source", "") or "").strip()
    old_image_url = str(getattr(info, "image_url", "") or "").strip()
    old_gender = _normalize_gender(getattr(info, "gender", ""))
    old_position = str(getattr(info, "position", "") or "").strip()
    old_city = str(getattr(info, "city", "") or "").strip()
    old_keywords = list(getattr(info, "keywords", []) or [])

    # If there is no useful metadata source at all, do not synthesize garbage values.
    has_any_signal = any(
        [
            profile,
            old_title,
            old_person,
            old_desc,
            old_source,
            old_image_url,
            old_position,
            old_city,
            old_keywords,
            guessed_name,
        ]
    )
    if not has_any_signal:
        return {}, issue

    if title and title != old_title:
        patch["title"] = title
    if person and person != old_person:
        patch["person"] = person
    if description and description != old_desc:
        patch["description"] = description
    if source and source != old_source:
        patch["source"] = source
    if image_url and image_url != old_image_url:
        patch["image_url"] = image_url
        patch["url"] = image_url
    if gender and gender != old_gender:
        patch["gender"] = gender
    if position and position != old_position:
        patch["position"] = position
    if city and city != old_city:
        patch["city"] = city
    if keywords and keywords != old_keywords:
        patch["keywords"] = keywords

    d2i_profile: Dict[str, Any] = {}
    if person:
        d2i_profile["name"] = person
    if description:
        d2i_profile["description"] = description
    if source:
        d2i_profile["source"] = source
    if image_url:
        d2i_profile["image_url"] = image_url
    if gender:
        d2i_profile["gender"] = gender
    if position:
        d2i_profile["position"] = position
        d2i_profile["title"] = position
    if city:
        d2i_profile["city"] = city
    if keywords:
        d2i_profile["keywords"] = keywords
    if d2i_profile:
        patch["d2i_profile"] = d2i_profile

    return patch, issue


def _summarize_issues(issues: List[AuditIssue]) -> Dict[str, int]:
    out = {
        "total": len(issues),
        "missing_person": 0,
        "missing_source": 0,
        "missing_image_url": 0,
        "missing_description": 0,
        "fragmented_description": 0,
        "invalid_gender": 0,
    }
    for x in issues:
        out["missing_person"] += int(x.missing_person)
        out["missing_source"] += int(x.missing_source)
        out["missing_image_url"] += int(x.missing_image_url)
        out["missing_description"] += int(x.missing_description)
        out["fragmented_description"] += int(x.fragmented_description)
        out["invalid_gender"] += int(x.invalid_gender)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and repair image metadata fields in bulk.")
    parser.add_argument("--root", required=True, help="Image root directory.")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan sub-folders.")
    parser.add_argument("--dry-run", action="store_true", help="Only audit and preview fixes, do not write.")
    parser.add_argument("--limit", type=int, default=0, help="Max images to process (0 means all).")
    parser.add_argument("--report", default="", help="Optional output report path (json).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    if not root.exists():
        print(f"[ERROR] root not found: {root}")
        return 2

    images = [p for p in _iter_images(root, recursive=bool(args.recursive))]
    images = sorted({p.resolve() for p in images}, key=lambda p: str(p))
    if args.limit and args.limit > 0:
        images = images[: args.limit]

    issues: List[AuditIssue] = []
    changed_rows: List[Dict[str, Any]] = []
    fixed = 0
    failed = 0

    for idx, path in enumerate(images, start=1):
        try:
            info = read_image_metadata(str(path))
            patch, issue = _propose_patch(path, info)
            issues.append(issue)
            if patch:
                changed_rows.append(
                    {
                        "path": str(path),
                        "changes": sorted([k for k in patch.keys() if k != "d2i_profile"]),
                    }
                )
                if not args.dry_run:
                    ok = bool(update_metadata_preserve_others(str(path), patch, clean_format=True))
                    if ok:
                        fixed += 1
                    else:
                        failed += 1
        except Exception as exc:  # pragma: no cover
            failed += 1
            changed_rows.append({"path": str(path), "error": str(exc)})

        if idx % 50 == 0:
            print(f"[INFO] scanned {idx}/{len(images)}")

    issue_summary = _summarize_issues(issues)
    report = {
        "root": str(root),
        "recursive": bool(args.recursive),
        "dry_run": bool(args.dry_run),
        "images_scanned": len(images),
        "issue_summary": issue_summary,
        "rows_with_proposed_changes": len(changed_rows),
        "fixed": fixed,
        "failed": failed,
        "sample_changes": changed_rows[:50],
    }

    if args.report:
        report_path = Path(args.report).resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] report saved: {report_path}")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
