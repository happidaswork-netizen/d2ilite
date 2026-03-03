#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Phase 0 contract smoke tests for service layer."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from PIL import Image

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.editor_text_service import (
    extract_json_payload_from_llm,
    normalize_gender_text,
    normalize_profile_for_editor,
)
from services.image_service import read_image_basic_info
from services.metadata_service import (
    build_structured_payload,
    normalize_http_url,
    parse_keywords,
)
from services.scraper_monitor_service import (
    humanize_scraper_reason,
    read_jsonl_rows,
    write_jsonl_rows,
)
from services.settings_service import load_app_settings, save_app_settings


def _assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"[{label}] expected={expected!r} actual={actual!r}")


def _assert_true(cond: bool, label: str) -> None:
    if not cond:
        raise AssertionError(f"[{label}] expected True")


def test_normalize_http_url() -> None:
    src = 'text https://example.com/a.jpg" trailing'
    _assert_equal(normalize_http_url(src), "https://example.com/a.jpg", "normalize_http_url")


def test_parse_keywords() -> None:
    kws = parse_keywords(" 警察, 医生,警察; 教师 ")
    _assert_equal(kws, ["警察", "医生", "教师"], "parse_keywords")


def test_scraper_jsonl_rw() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "rows.jsonl")
        write_jsonl_rows(path, [{"a": 1}, {"b": 2}])
        rows = read_jsonl_rows(path)
        _assert_equal(rows, [{"a": 1}, {"b": 2}], "jsonl_rw")


def test_humanize_reason() -> None:
    msg = humanize_scraper_reason("image_download_http_error")
    _assert_equal(msg, "图片下载失败：HTTP 错误", "humanize_reason")


def test_extract_json_payload_from_llm() -> None:
    payload = extract_json_payload_from_llm("```json\n{\"a\":1,\"b\":\"x\"}\n```")
    _assert_equal(payload, {"a": 1, "b": "x"}, "extract_json_payload")


def test_normalize_profile_for_editor() -> None:
    prof = {" a ": "  x  ", "url": " https://a.com/p.jpg  ", "empty": ""}
    out = normalize_profile_for_editor(prof)
    _assert_equal(out, {"a": "x", "url": "https://a.com/p.jpg"}, "normalize_profile")
    _assert_equal(normalize_gender_text("female"), "女", "normalize_gender")


def test_build_structured_payload() -> None:
    payload = build_structured_payload(
        title=" 张三 ",
        person=" 张三 ",
        keywords_text="警察, 民警,警察",
        description=" 简介 ",
        adaptive_profile={"police_id": "1101", "unit": "某局"},
    )
    _assert_equal(payload["title"], "张三", "payload_title")
    _assert_equal(payload["keywords"], ["警察", "民警"], "payload_keywords")
    _assert_equal(payload["police_id"], "1101", "payload_police_id")
    _assert_true(isinstance(payload.get("d2i_profile"), dict), "payload_profile")


def test_read_image_basic_info() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.png"
        Image.new("RGB", (120, 80), (255, 0, 0)).save(path)
        info = read_image_basic_info(str(path))
        _assert_equal(info.get("width"), 120, "image_width")
        _assert_equal(info.get("height"), 80, "image_height")


def test_settings_rw() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "settings.json")
        ok = save_app_settings({"llm": {"model": "gpt-x"}}, path)
        _assert_true(ok, "settings_save_ok")
        data = load_app_settings(path)
        _assert_equal(str(data.get("llm", {}).get("model", "")), "gpt-x", "settings_model")


def main() -> int:
    tests = [
        test_normalize_http_url,
        test_parse_keywords,
        test_scraper_jsonl_rw,
        test_humanize_reason,
        test_extract_json_payload_from_llm,
        test_normalize_profile_for_editor,
        test_build_structured_payload,
        test_read_image_basic_info,
        test_settings_rw,
    ]
    for fn in tests:
        fn()
    print(f"[OK] phase0 contract smoke passed: {len(tests)} tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
