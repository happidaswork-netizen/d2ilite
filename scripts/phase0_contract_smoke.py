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
from services.task_service import (
    build_scraper_task_view_rows,
    collect_detail_urls_from_progress_values,
    collect_scraper_progress_rows,
    count_jsonl_rows,
    count_latest_metadata_status,
    dedupe_progress_values,
    default_public_tasks_root,
    derive_public_task_status,
    discover_public_task_roots,
    estimate_scraper_total_target,
    is_scraper_row_completed,
    is_scraper_row_image_downloaded,
    list_public_scraper_templates,
    load_public_scraper_template_states,
    public_scraper_template_state_path,
    public_scraper_templates_dir,
    retry_requires_crawl_phase,
    reconcile_task_entry_runtime_state,
    scraper_progress_row_to_table_values,
    scraper_progress_snapshot,
    scraper_progress_values_has_error,
    save_public_scraper_template_states,
    set_public_scraper_template_state,
    split_scraper_progress_rows,
    sort_public_task_summaries,
    summarize_public_task,
    suggest_public_scraper_output_root,
    task_entry_status_text,
)


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


def test_task_discovery_and_root_defaults() -> None:
    base = default_public_tasks_root(__file__)
    _assert_true(base.endswith(os.path.join("data", "public_archive")), "default_task_root_suffix")
    with tempfile.TemporaryDirectory() as td:
        task_root = os.path.join(td, "task_a")
        os.makedirs(os.path.join(task_root, "state"), exist_ok=True)
        with open(os.path.join(task_root, "state", "runtime_config.json"), "w", encoding="utf-8") as f:
            f.write("{}")
        roots = discover_public_task_roots(td)
        _assert_equal(roots, [os.path.abspath(task_root)], "discover_task_roots")


def test_estimate_total_target_and_metadata_status() -> None:
    with tempfile.TemporaryDirectory() as td:
        os.makedirs(os.path.join(td, "reports"), exist_ok=True)
        os.makedirs(os.path.join(td, "raw"), exist_ok=True)
        os.makedirs(os.path.join(td, "downloads"), exist_ok=True)
        with open(os.path.join(td, "reports", "crawl_report.json"), "w", encoding="utf-8") as f:
            f.write('{"metrics_this_run":{"detail_requests_enqueued":12},"totals_on_disk":{"profiles":9}}')
        with open(os.path.join(td, "raw", "list_records.jsonl"), "w", encoding="utf-8") as f:
            f.write('{"detail_url":"https://a/1"}\n')
            f.write('{"detail_url":"https://a/2"}\n')
            f.write('{"detail_url":"https://a/2"}\n')
        with open(os.path.join(td, "raw", "profiles.jsonl"), "w", encoding="utf-8") as f:
            f.write("{}\n{}\n{}\n")
        with open(os.path.join(td, "downloads", "image_downloads.jsonl"), "w", encoding="utf-8") as f:
            f.write("{}\n{}\n{}\n{}\n{}\n")
        with open(os.path.join(td, "raw", "metadata_write_results.jsonl"), "w", encoding="utf-8") as f:
            f.write('{"detail_url":"u1","status":"failed"}\n')
            f.write('{"detail_url":"u1","status":"ok"}\n')
            f.write('{"detail_url":"u2","status":"error"}\n')
        _assert_equal(estimate_scraper_total_target(td), 12, "estimate_total_target")
        _assert_equal(count_latest_metadata_status(td), (1, 1), "latest_metadata_status")


def test_retry_requires_crawl_phase_and_status() -> None:
    rows = [
        {"detail": "√", "meta": "√"},
        {"detail": "x", "meta": "…"},
    ]
    _assert_true(retry_requires_crawl_phase(rows), "retry_requires_crawl")
    done_rows = [{"detail": "√", "meta": "√"}]
    _assert_equal(retry_requires_crawl_phase(done_rows), False, "retry_not_required")
    class _RunningProc:
        def poll(self):
            return None

    status = derive_public_task_status(
        root="r1",
        entry={"manual_paused": True, "proc": _RunningProc()},
        current_active_root="r1",
        pause_flag_exists=False,
        backoff_state={"blocked_until": "", "blocked_reason": ""},
        profile_rows=3,
        pending_rows=3,
        metadata_failed=0,
        activity_total=5,
    )
    _assert_equal(status, "手动暂停(当前)", "derive_task_status")


def test_template_state_services() -> None:
    with tempfile.TemporaryDirectory() as td:
        app_file = os.path.join(td, "app.py")
        Path(app_file).write_text("# smoke\n", encoding="utf-8")
        templates_dir = public_scraper_templates_dir(app_file)
        state_path = public_scraper_template_state_path(app_file)
        _assert_true(templates_dir.endswith(os.path.join("scraper", "templates")), "templates_dir_suffix")
        _assert_true(state_path.endswith(os.path.join("scraper", "state", "template_run_state.json")), "state_path_suffix")
        custom_tpl = os.path.join(templates_dir, "demo.json")
        Path(custom_tpl).write_text("{}", encoding="utf-8")
        scraper_tpl = os.path.join(td, "scraper", "config.sample.json")
        os.makedirs(os.path.dirname(scraper_tpl), exist_ok=True)
        Path(scraper_tpl).write_text("{}", encoding="utf-8")
        save_public_scraper_template_states(app_file, {custom_tpl: {"status": "done", "updated_at": ""}})
        set_public_scraper_template_state(app_file, scraper_tpl, "pending")
        states = load_public_scraper_template_states(app_file)
        _assert_equal(states[os.path.abspath(custom_tpl)]["status"], "done", "template_state_done")
        _assert_equal(states[os.path.abspath(scraper_tpl)]["status"], "pending", "template_state_pending")
        pairs = list_public_scraper_templates(app_file)
        found = {os.path.abspath(path) for _label, path in pairs}
        _assert_true(os.path.abspath(custom_tpl) in found, "list_templates_custom")
        _assert_true(os.path.abspath(scraper_tpl) in found, "list_templates_builtin")
        out_root = suggest_public_scraper_output_root(app_file, "site_demo")
        _assert_true(out_root.endswith(os.path.join("data", "public_archive", "site_demo")), "suggest_output_root")


def test_count_jsonl_rows_cache() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "rows.jsonl")
        Path(path).write_text('{"a":1}\n{"b":2}\n', encoding="utf-8")
        cache = {}
        first = count_jsonl_rows(path, cache)
        second = count_jsonl_rows(path, cache)
        _assert_equal(first, 2, "count_jsonl_rows_first")
        _assert_equal(second, 2, "count_jsonl_rows_cached")


def test_collect_scraper_progress_rows() -> None:
    with tempfile.TemporaryDirectory() as td:
        os.makedirs(os.path.join(td, "raw"), exist_ok=True)
        os.makedirs(os.path.join(td, "downloads"), exist_ok=True)
        os.makedirs(os.path.join(td, "state"), exist_ok=True)
        Path(os.path.join(td, "raw", "list_records.jsonl")).write_text(
            '{"name":"张三","detail_url":"https://d/1"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "raw", "profiles.jsonl")).write_text(
            '{"name":"张三","detail_url":"https://d/1","image_url":"https://img/1.jpg"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "downloads", "image_downloads.jsonl")).write_text(
            '{"name":"张三","detail_url":"https://d/1","saved_path":"X:/not-exists.jpg"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "raw", "metadata_write_results.jsonl")).write_text(
            '{"detail_url":"https://d/1","status":"ok"}\n',
            encoding="utf-8",
        )
        rows = collect_scraper_progress_rows(td, max_rows=20)
        _assert_equal(len(rows), 1, "collect_rows_count")
        row = rows[0]
        _assert_equal(row.get("name"), "张三", "collect_row_name")
        _assert_equal(row.get("detail"), "√", "collect_row_detail")
        _assert_equal(row.get("meta"), "√", "collect_row_meta")


def test_scraper_row_status_helpers() -> None:
    row = {"detail": "√", "image": "√", "meta": "√"}
    _assert_true(is_scraper_row_completed(row), "row_completed_true")
    _assert_true(is_scraper_row_image_downloaded(row), "row_image_downloaded_true")
    values = ("1", "张三", "×", "√", "…", "详情缺失")
    _assert_true(scraper_progress_values_has_error(values), "progress_values_error")


def test_scraper_progress_view_helpers() -> None:
    rows = [
        {"idx": "1", "name": "甲", "detail": "√", "image": "√", "meta": "√", "reason": "", "detail_url": "u1", "image_path": "p1"},
        {"idx": "2", "name": "乙", "detail": "√", "image": "×", "meta": "…", "reason": "图片缺失", "detail_url": "u2", "image_path": ""},
    ]
    pending, done = split_scraper_progress_rows(rows)
    _assert_equal(len(pending), 1, "split_pending")
    _assert_equal(len(done), 1, "split_done")
    snapshot = scraper_progress_snapshot(pending, done)
    _assert_true(bool(snapshot), "snapshot_not_empty")
    values = scraper_progress_row_to_table_values(rows[0])
    _assert_equal(values[1], "甲", "row_to_values_name")
    urls = collect_detail_urls_from_progress_values(
        [
            ("1", "甲", "√", "√", "√", "", "u1", "p1"),
            ("2", "乙", "×", "×", "…", "失败", "u2", "p2"),
            ("2", "乙", "×", "×", "…", "失败", "u2", "p2"),
        ]
    )
    _assert_equal(urls, ["u1", "u2"], "collect_detail_urls")
    uniq = dedupe_progress_values(
        [
            ("1", "甲"),
            ("1", "甲"),
            tuple(),
            ("2", "乙"),
        ]
    )
    _assert_equal(uniq, [("1", "甲"), ("2", "乙")], "dedupe_progress_values")


def test_public_task_summary_sort() -> None:
    with tempfile.TemporaryDirectory() as td:
        os.makedirs(os.path.join(td, "raw"), exist_ok=True)
        os.makedirs(os.path.join(td, "downloads"), exist_ok=True)
        os.makedirs(os.path.join(td, "state"), exist_ok=True)
        Path(os.path.join(td, "state", "runtime_config.json")).write_text("{}", encoding="utf-8")
        Path(os.path.join(td, "raw", "profiles.jsonl")).write_text("{}\n{}\n", encoding="utf-8")
        Path(os.path.join(td, "raw", "metadata_write_results.jsonl")).write_text(
            '{"detail_url":"u1","status":"ok"}\n',
            encoding="utf-8",
        )
        summary = summarize_public_task(td, count_jsonl_rows_fn=lambda p: count_jsonl_rows(p, {}))
        _assert_equal(summary.get("profiles"), 2, "task_summary_profiles")
        _assert_equal(summary.get("metadata_ok"), 1, "task_summary_metadata_ok")
        rows = sort_public_task_summaries(
            [
                {"status": "未完成", "updated_at": "2025-01-01 00:00:00"},
                {"status": "运行中", "updated_at": "2024-01-01 00:00:00"},
            ]
        )
        _assert_equal(rows[0]["status"], "未完成", "task_summary_sort_current_behavior")


def test_task_view_model_helpers() -> None:
    class _RunningProc:
        pid = 12345

        def poll(self):
            return None

    class _StoppedProc:
        pid = 999

        def poll(self):
            return 0

    running_entry = {"proc": _RunningProc(), "manual_paused": False, "runtime_state": "运行中"}
    _assert_equal(task_entry_status_text(running_entry), "运行中", "task_entry_status_running")

    paused_entry = {"proc": _StoppedProc(), "manual_paused": True, "runtime_state": "运行中"}
    reconcile_task_entry_runtime_state(paused_entry)
    _assert_equal(str(paused_entry.get("runtime_state", "")), "已暂停(手动)", "task_entry_reconcile_paused")

    finished_entry = {"proc": _StoppedProc(), "manual_paused": False, "runtime_state": "运行中", "last_exit_code": 0}
    reconcile_task_entry_runtime_state(finished_entry)
    _assert_equal(str(finished_entry.get("runtime_state", "")), "已完成", "task_entry_reconcile_finished")

    tasks = {
        "C:/task_a": {"proc": _RunningProc(), "manual_paused": False, "runtime_state": "运行中", "started_at": 10},
        "C:/task_b": {"proc": None, "manual_paused": False, "runtime_state": "已完成", "started_at": 20},
    }
    rows, running_count = build_scraper_task_view_rows(
        tasks,
        active_root="C:/task_c",
        active_entry_if_missing={"proc": None, "runtime_state": "任务浏览", "manual_paused": False},
    )
    _assert_equal(running_count, 1, "task_view_running_count")
    _assert_equal(len(rows), 3, "task_view_row_count")
    _assert_true(bool(rows[0].get("running")), "task_view_running_first")


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
        test_task_discovery_and_root_defaults,
        test_estimate_total_target_and_metadata_status,
        test_retry_requires_crawl_phase_and_status,
        test_template_state_services,
        test_count_jsonl_rows_cache,
        test_collect_scraper_progress_rows,
        test_scraper_row_status_helpers,
        test_scraper_progress_view_helpers,
        test_public_task_summary_sort,
        test_task_view_model_helpers,
    ]
    for fn in tests:
        fn()
    print(f"[OK] phase0 contract smoke passed: {len(tests)} tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
