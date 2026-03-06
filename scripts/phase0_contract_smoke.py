#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Phase 0 contract smoke tests for service layer."""

from __future__ import annotations

import os
import json
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
from services.public_scraper_config_service import (
    apply_public_scraper_setup_to_runtime_config,
    build_public_scraper_runtime_config,
    build_public_scraper_setup_initial_state,
    build_public_scraper_setup_submit_plan,
    build_public_scraper_template_selection_state,
    register_generated_template,
    resolve_public_task_output_root,
    save_generated_public_scraper_template,
)
from services.scraper_monitor_service import (
    extract_scraper_live_actions,
    humanize_scraper_reason,
    read_jsonl_rows,
    write_jsonl_rows,
)
from services.settings_service import load_app_settings, save_app_settings
from services.task_orchestration_service import (
    build_app_close_confirmation,
    build_public_scraper_app_close_plan,
    build_public_scraper_close_reset_state,
    build_public_scraper_task_close_cleanup_plan,
    collect_public_scraper_log_handles,
    collect_running_public_scraper_tasks,
    build_public_scraper_poll_plan,
    build_public_scraper_task_exit_plan,
    build_existing_task_started_message,
    build_continue_start_existing_task_args,
    build_public_scraper_command,
    build_public_scraper_log_banner,
    build_retry_start_existing_task_args,
    build_rewrite_metadata_start_existing_task_args,
    build_started_task_message,
    continue_action_for_active_entry,
    decide_task_exit_outcome,
    pick_next_active_root,
    prepare_existing_task_runtime_config,
    resolve_active_template_path,
    resolve_named_images_dir,
    retry_started_status_text,
)
from services.task_service import (
    build_progress_context_menu_plan,
    build_retry_detail_plan,
    build_public_task_manager_list_view,
    build_progress_selection_sync_plan,
    build_scraper_task_view_rows,
    build_public_scraper_progress_text,
    collect_progress_values,
    collect_detail_urls_from_progress_values,
    collect_other_progress_table_refs,
    collect_scraper_error_row_ids,
    collect_scraper_progress_rows,
    count_jsonl_rows,
    count_latest_metadata_status,
    dedupe_progress_values,
    default_public_tasks_root,
    derive_public_task_status,
    discover_public_task_roots,
    execute_retry_detail_cleanup,
    estimate_scraper_total_target,
    extract_detail_url_from_progress_values,
    is_scraper_row_completed,
    is_scraper_row_image_downloaded,
    list_public_scraper_templates,
    load_public_scraper_template_states,
    parse_task_root_from_values,
    pick_first_progress_values,
    public_task_manager_status_text,
    public_task_summary_to_tree_values,
    resolve_public_task_directory,
    resolve_public_task_log_path,
    resolve_openable_image_path_from_progress_values,
    normalize_progress_values,
    normalize_retry_detail_urls,
    public_scraper_template_state_path,
    public_scraper_templates_dir,
    retry_requires_crawl_phase,
    missing_audit_fields_from_metadata_snapshot,
    reconcile_task_entry_runtime_state,
    scraper_progress_row_to_table_values,
    scraper_progress_snapshot,
    scraper_progress_values_has_error,
    scraper_error_selection_status_text,
    save_public_scraper_template_states,
    set_public_scraper_manual_pause_flag,
    set_public_scraper_template_state,
    split_scraper_progress_rows,
    sort_public_task_summaries,
    summarize_scraper_progress_rows,
    summarize_public_task,
    suggest_public_scraper_output_root,
    sync_audit_review_queue_rows,
    sync_scraper_audit_review_queue,
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


def test_scraper_audit_review_helpers() -> None:
    with tempfile.TemporaryDirectory() as td:
        os.makedirs(os.path.join(td, "reports"), exist_ok=True)
        os.makedirs(os.path.join(td, "raw"), exist_ok=True)
        Path(os.path.join(td, "reports", "gui_public_scraper.log")).write_text(
            "INFO | 人物: 张三 | 详情页: https://d/1 | 正在抓取张三的详情页\n"
            "INFO | 人物: 张三 | 详情页: https://d/1 | 正在下载张三的图片\n"
            "WARN | 人物: 李四 | 详情页: https://d/2 | 元数据写入失败，准备延迟重试\n",
            encoding="utf-8",
        )
        by_person, by_detail, latest_action = extract_scraper_live_actions(td)
        _assert_equal(latest_action, "元数据重试中", "live_actions_latest_action")
        _assert_equal(by_person.get("李四"), "元数据重试中", "live_actions_person_li_si")
        _assert_equal(by_person.get("张三"), "正在下载图片", "live_actions_person_zhang_san")
        _assert_equal(by_detail.get("https://d/2"), "元数据重试中", "live_actions_detail_action")
        _assert_equal(by_detail.get("https://d/1"), "正在下载图片", "live_actions_detail_download")

        missing = missing_audit_fields_from_metadata_snapshot(
            gender="",
            profile={
                "birth_date": "1990-01-01",
                "photo_taken_at": "未知",
                "age_at_photo": "-",
            },
        )
        _assert_equal(
            missing,
            ["gender", "photo_taken_at", "age_at_photo"],
            "audit_missing_fields_from_snapshot",
        )

        row_plan = sync_audit_review_queue_rows(
            [
                {
                    "scraped_at": "2026-03-06T12:00:00",
                    "reason": "audit_missing_metadata_fields:gender",
                    "detail_url": "https://d/1",
                    "name": "",
                    "missing_fields": ["gender"],
                },
                {
                    "scraped_at": "2026-03-06T12:00:00",
                    "reason": "other_reason",
                    "detail_url": "https://d/9",
                },
            ],
            "https://d/1",
            missing_fields=["gender", "photo_taken_at", "gender"],
            name_hint="张三",
            scraped_at="2026-03-06T13:00:00",
        )
        _assert_true(bool(row_plan.get("changed")), "audit_queue_rows_changed")
        planned_rows = list(row_plan.get("rows") or [])
        _assert_equal(len(planned_rows), 2, "audit_queue_rows_count")
        updated_row = planned_rows[0]
        _assert_equal(
            updated_row.get("reason"),
            "audit_missing_metadata_fields:gender,photo_taken_at",
            "audit_queue_rows_reason",
        )
        _assert_equal(updated_row.get("name"), "张三", "audit_queue_rows_name_hint")

        review_path = os.path.join(td, "raw", "review_queue.jsonl")
        write_jsonl_rows(review_path, [])
        changed_add = sync_scraper_audit_review_queue(
            td,
            "https://d/3",
            missing_fields=["gender", "birth_date"],
            name_hint="王五",
            scraped_at="2026-03-06T14:00:00",
        )
        _assert_true(changed_add, "audit_queue_file_add_changed")
        review_rows = read_jsonl_rows(review_path, max_rows=0)
        _assert_equal(len(review_rows), 1, "audit_queue_file_add_count")
        _assert_equal(
            review_rows[0].get("reason"),
            "audit_missing_metadata_fields:gender,birth_date",
            "audit_queue_file_add_reason",
        )

        changed_remove = sync_scraper_audit_review_queue(
            td,
            "https://d/3",
            missing_fields=[],
            name_hint="王五",
            scraped_at="2026-03-06T15:00:00",
        )
        _assert_true(changed_remove, "audit_queue_file_remove_changed")
        _assert_equal(read_jsonl_rows(review_path, max_rows=0), [], "audit_queue_file_remove_empty")


def test_public_scraper_config_helpers() -> None:
    with tempfile.TemporaryDirectory() as td:
        app_file = os.path.join(td, "app.py")
        Path(app_file).write_text("# smoke\n", encoding="utf-8")
        global_llm = {
            "enabled_default": True,
            "model": "global-model",
            "api_base": "http://global/v1",
            "api_key": "global-key",
        }
        initial = build_public_scraper_setup_initial_state(
            "https://example.org/list",
            app_file,
            global_llm_settings=global_llm,
        )
        _assert_equal(initial["start_url"], "https://example.org/list", "setup_initial_url")
        _assert_true(
            str(initial["output_root"]).endswith(os.path.join("data", "public_archive", "example_org_list")),
            "setup_initial_output_root",
        )
        _assert_equal(initial["llm_model"], "global-model", "setup_initial_llm_model")

        template_path = os.path.join(td, "scraper", "templates", "unit_demo.json")
        os.makedirs(os.path.dirname(template_path), exist_ok=True)
        template_payload = {
            "site_name": "tpl_demo",
            "start_urls": [" https://example.org/unit/2024/list "],
            "allowed_domains": ["example.org"],
            "output_root": "relative/out",
            "crawl": {
                "interval_min_seconds": 1.5,
                "interval_max_seconds": 2.5,
                "timeout_seconds": 40,
                "suspect_block_consecutive_failures": 4,
            },
            "rules": {
                "auto_unit_subdir": True,
                "unit_name": "示例单位",
                "output_subdir_pattern": "{unit}{year_suffix}",
                "jsl_clearance_enabled": False,
                "image_download_mode": "browser",
                "auto_fallback_to_browser": False,
                "disable_page_images_during_crawl": False,
                "llm_enrich_enabled": False,
                "llm_model": "template-model",
                "llm_api_base": "http://template/v1",
                "llm_api_key": "template-key",
                "output_mode": "full",
                "direct_write_images": False,
            },
        }
        Path(template_path).write_text(json.dumps(template_payload, ensure_ascii=False), encoding="utf-8")

        selected = build_public_scraper_template_selection_state(
            template_path,
            app_file,
            global_llm_settings=global_llm,
        )
        _assert_true(bool(selected.get("url_locked")), "template_selection_url_locked")
        _assert_equal(
            selected["template_start_url"],
            "https://example.org/unit/2024/list",
            "template_selection_start_url",
        )
        _assert_equal(
            selected["output_root"],
            os.path.abspath(os.path.join(td, "relative", "out")),
            "template_selection_output_root",
        )
        _assert_equal(selected["llm_model"], "global-model", "template_selection_llm_model")
        _assert_equal(selected["llm_api_base"], "http://global/v1", "template_selection_llm_base")
        _assert_equal(selected["llm_api_key"], "global-key", "template_selection_llm_key")
        _assert_true(not bool(selected.get("save_generated_template", True)), "template_selection_save_disabled")

        submit = build_public_scraper_setup_submit_plan(
            {
                "start_url": "",
                "output_root": selected["output_root"],
                "interval_min": selected["interval_min"],
                "interval_max": selected["interval_max"],
                "timeout_seconds": selected["timeout_seconds"],
                "suspect_block_consecutive_failures": selected["suspect_block_consecutive_failures"],
                "jsl_enabled": selected["jsl_enabled"],
                "image_download_mode": selected["image_download_mode"],
                "auto_fallback_to_browser": selected["auto_fallback_to_browser"],
                "disable_page_images_during_crawl": selected["disable_page_images_during_crawl"],
                "llm_enrich_enabled": selected["llm_enrich_enabled"],
                "llm_model": "",
                "llm_api_base": "http://custom/v1/",
                "llm_api_key": "secret-key",
                "output_minimal": False,
                "direct_write_images": selected["direct_write_images"],
                "save_generated_template": False,
                "cleanup_generated_template": False,
            },
            app_file=app_file,
            selected_template_path=template_path,
            template_start_url=str(selected.get("template_start_url", "")),
        )
        _assert_true(bool(submit.get("ok")), "setup_submit_ok")
        setup_result = dict(submit.get("result") or {})
        _assert_equal(setup_result["start_url"], "https://example.org/unit/2024/list", "setup_submit_start_url")

        manual_root, _manual_payload = resolve_public_task_output_root(
            os.path.join(td, "manual"),
            setup_result["start_url"],
            template_payload,
            template_path=template_path,
        )
        _assert_equal(
            manual_root,
            os.path.abspath(os.path.join(td, "manual", "示例单位_2024")),
            "resolve_public_task_output_root",
        )

        runtime_path, runtime_config = build_public_scraper_runtime_config(
            app_file,
            setup_result["start_url"],
            os.path.join(td, "tasks"),
            template_path=template_path,
        )
        expected_task_root = os.path.abspath(os.path.join(td, "tasks", "示例单位_2024"))
        _assert_equal(
            os.path.abspath(Path(runtime_path).parent.parent),
            expected_task_root,
            "runtime_config_output_root_path",
        )
        _assert_equal(runtime_config["output_root"], expected_task_root, "runtime_config_output_root")
        runtime_rules = dict(runtime_config.get("rules") or {})
        _assert_equal(runtime_rules.get("resolved_output_subdir"), "示例单位_2024", "runtime_config_subdir")
        _assert_equal(runtime_rules.get("resolved_year"), "2024", "runtime_config_year")

        apply_plan = apply_public_scraper_setup_to_runtime_config(
            runtime_config,
            setup_result,
            global_llm_settings=global_llm,
            normalize_api_base_fn=lambda base: str(base).rstrip("/"),
        )
        updated = dict(apply_plan.get("runtime_config") or {})
        updated_rules = dict(updated.get("rules") or {})
        updated_crawl = dict(updated.get("crawl") or {})
        _assert_equal(apply_plan.get("llm_model"), "global-model", "setup_apply_llm_model")
        _assert_equal(apply_plan.get("llm_api_base"), "http://custom/v1", "setup_apply_llm_base")
        _assert_equal(apply_plan.get("llm_api_key"), "secret-key", "setup_apply_llm_key")
        _assert_true("llm_api_key" not in updated_rules, "setup_apply_no_api_key_on_disk")
        _assert_equal(updated_rules.get("output_mode"), "full", "setup_apply_output_mode")
        _assert_equal(updated_rules.get("image_download_mode"), "browser", "setup_apply_image_mode")
        _assert_equal(updated_crawl.get("interval_min_seconds"), 1.5, "setup_apply_interval_min")

        generated_path = save_generated_public_scraper_template(
            setup_result["start_url"],
            updated,
            os.path.join(td, "generated_templates"),
        )
        generated_payload = json.loads(Path(generated_path).read_text(encoding="utf-8"))
        generated_rules = dict(generated_payload.get("rules") or {})
        _assert_equal(generated_payload.get("site_name"), "example_org_unit", "generated_template_site_name")
        _assert_equal(
            generated_payload.get("output_root"),
            "data/public_archive/example_org_unit",
            "generated_template_output_root",
        )
        _assert_true("template_source_path" not in generated_rules, "generated_template_strip_template_source")
        _assert_true("resolved_output_subdir" not in generated_rules, "generated_template_strip_resolved_subdir")

        registered = register_generated_template(updated, generated_path, cleanup_after_run=True)
        registered_rules = dict(registered.get("rules") or {})
        _assert_equal(registered_rules.get("generated_template_path"), generated_path, "register_generated_template_path")
        _assert_true(
            generated_path in list(registered_rules.get("cleanup_paths") or []),
            "register_generated_template_cleanup",
        )


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


def test_scraper_progress_selection_helpers() -> None:
    with tempfile.TemporaryDirectory() as td:
        image_path = os.path.abspath(os.path.join(td, "x.jpg"))
        Path(image_path).write_bytes(b"x")

        values = ("1", "甲", "√", "√", "…", "", "https://d/1", image_path)
        values_bad = ("2", "乙", "×", "√", "…", "详情缺失", "https://d/2", "")
        _assert_equal(
            normalize_progress_values(list(values)),
            values,
            "progress_normalize_values",
        )
        _assert_equal(
            pick_first_progress_values([tuple(), list(values_bad), values]),
            values_bad,
            "progress_pick_first_values",
        )
        _assert_equal(
            extract_detail_url_from_progress_values(values),
            "https://d/1",
            "progress_extract_detail_url",
        )
        resolved = resolve_openable_image_path_from_progress_values(values)
        _assert_equal(resolved, image_path, "progress_resolve_openable_image_path")

        bad_ids = collect_scraper_error_row_ids(
            [
                ("row_ok", values),
                ("row_bad", values_bad),
            ]
        )
        _assert_equal(bad_ids, ["row_bad"], "progress_collect_error_row_ids")
        _assert_equal(
            collect_progress_values([list(values), tuple(), values_bad, list(values)]),
            [values, values_bad],
            "progress_collect_values",
        )
        _assert_equal(
            collect_other_progress_table_refs(["pending", "done"], "pending"),
            ["done"],
            "progress_collect_other_table_refs",
        )
        sync_plan = build_progress_selection_sync_plan(
            ["pending", "done"],
            "pending",
            values,
        )
        _assert_equal(bool(sync_plan.get("has_active_table")), True, "progress_sync_plan_has_active")
        _assert_equal(sync_plan.get("detail_url"), "https://d/1", "progress_sync_plan_detail_url")
        _assert_equal(sync_plan.get("clear_other_refs"), ["done"], "progress_sync_plan_clear_others")
        _assert_equal(bool(sync_plan.get("should_queue_open")), True, "progress_sync_plan_queue_open")
        ctx_plan_reset = build_progress_context_menu_plan(
            "row_bad",
            ["row_ok"],
            values_bad,
            [values],
        )
        _assert_equal(bool(ctx_plan_reset.get("should_reset_selection")), True, "progress_ctx_plan_reset")
        _assert_equal(ctx_plan_reset.get("focus_row_id"), "row_bad", "progress_ctx_plan_focus")
        _assert_equal(ctx_plan_reset.get("detail_urls"), ["https://d/2"], "progress_ctx_plan_detail_urls_reset")
        ctx_plan_keep = build_progress_context_menu_plan(
            "row_ok",
            ["row_ok", "row_bad"],
            values,
            [values, values_bad],
        )
        _assert_equal(bool(ctx_plan_keep.get("should_reset_selection")), False, "progress_ctx_plan_keep_selection")
        _assert_equal(
            ctx_plan_keep.get("detail_urls"),
            ["https://d/1", "https://d/2"],
            "progress_ctx_plan_detail_urls_keep",
        )
        _assert_equal(int(ctx_plan_keep.get("retry_count", 0)), 2, "progress_ctx_plan_retry_count")
        _assert_equal(
            normalize_retry_detail_urls([" https://d/1 ", "", "https://d/2", "https://d/1"]),
            ["https://d/1", "https://d/2"],
            "progress_normalize_retry_detail_urls",
        )
        retry_plan = build_retry_detail_plan([" https://d/1 ", "https://d/2", "https://d/1", "https://d/3"])
        _assert_equal(int(retry_plan.get("detail_count", 0)), 3, "progress_retry_plan_count")
        _assert_equal(
            retry_plan.get("detail_preview"),
            "https://d/1\nhttps://d/2\nhttps://d/3",
            "progress_retry_plan_preview",
        )
        _assert_true(
            "选中条目：3" in str(retry_plan.get("confirm_message", "")),
            "progress_retry_plan_confirm_message",
        )
        _assert_equal(
            scraper_error_selection_status_text(2),
            "已选中错误项 2 条",
            "progress_error_selection_status_positive",
        )
        _assert_equal(
            scraper_error_selection_status_text(0),
            "当前列表没有可选中的错误项",
            "progress_error_selection_status_zero",
        )


def test_retry_detail_cleanup_helpers() -> None:
    with tempfile.TemporaryDirectory() as td:
        os.makedirs(os.path.join(td, "raw"), exist_ok=True)
        os.makedirs(os.path.join(td, "downloads"), exist_ok=True)
        os.makedirs(os.path.join(td, "state"), exist_ok=True)

        Path(os.path.join(td, "raw", "profiles.jsonl")).write_text(
            '{"detail_url":"https://d/1","image_url":"https://img/1.jpg"}\n'
            '{"detail_url":"https://d/2","image_url":"https://img/2.jpg"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "downloads", "image_downloads.jsonl")).write_text(
            '{"detail_url":"https://d/1","image_url":"https://img/1.jpg"}\n'
            '{"detail_url":"https://d/2","image_url":"https://img/2.jpg"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "raw", "metadata_queue.jsonl")).write_text(
            '{"detail_url":"https://d/1","image_url":"https://img/1.jpg"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "raw", "metadata_write_results.jsonl")).write_text(
            '{"detail_url":"https://d/1","status":"failed"}\n'
            '{"detail_url":"https://d/2","status":"ok"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "raw", "review_queue.jsonl")).write_text(
            '{"record":{"detail_url":"https://d/1"}}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "raw", "failures.jsonl")).write_text(
            '{"url":"https://d/1"}\n'
            '{"url":"https://d/3"}\n',
            encoding="utf-8",
        )
        Path(os.path.join(td, "state", "image_url_index.json")).write_text(
            '{"https://img/1.jpg":"a.jpg","https://img/2.jpg":"b.jpg"}',
            encoding="utf-8",
        )

        cleanup = execute_retry_detail_cleanup(td, ["https://d/1", "https://d/1"])
        _assert_true(bool(cleanup.get("has_touched")), "retry_cleanup_has_touched")
        _assert_equal(int(cleanup.get("removed_profile_count", 0)), 1, "retry_cleanup_removed_profiles")
        _assert_equal(int(cleanup.get("removed_manifest_count", 0)), 1, "retry_cleanup_removed_manifest")
        _assert_equal(int(cleanup.get("removed_queue_count", 0)), 1, "retry_cleanup_removed_queue")
        _assert_equal(int(cleanup.get("removed_meta_count", 0)), 1, "retry_cleanup_removed_meta")
        _assert_equal(int(cleanup.get("removed_review_count", 0)), 1, "retry_cleanup_removed_review")
        _assert_equal(int(cleanup.get("removed_failure_count", 0)), 1, "retry_cleanup_removed_failure")
        _assert_equal(int(cleanup.get("dropped_url_index", 0)), 1, "retry_cleanup_dropped_url_index")
        _assert_true(
            "详情1/图片1/元数据队列1/元数据结果1" in str(cleanup.get("status_text", "")),
            "retry_cleanup_status_text",
        )
        _assert_true(
            "下一步请点击“继续任务”。" in str(cleanup.get("dialog_message", "")),
            "retry_cleanup_dialog_message",
        )

        remaining_profiles = read_jsonl_rows(os.path.join(td, "raw", "profiles.jsonl"))
        _assert_equal(len(remaining_profiles), 1, "retry_cleanup_profiles_remaining")
        _assert_equal(
            str(remaining_profiles[0].get("detail_url", "")),
            "https://d/2",
            "retry_cleanup_profiles_remaining_detail",
        )
        remaining_index = json.loads(Path(os.path.join(td, "state", "image_url_index.json")).read_text(encoding="utf-8"))
        _assert_true("https://img/1.jpg" not in remaining_index, "retry_cleanup_index_removed")
        _assert_true("https://img/2.jpg" in remaining_index, "retry_cleanup_index_kept")


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

    root = parse_task_root_from_values(("运行中", "123", "task", "C:/task_a"), root_index=3)
    _assert_true(root.endswith("task_a"), "parse_task_root")

    with tempfile.TemporaryDirectory() as td:
        ok1 = set_public_scraper_manual_pause_flag(td, True)
        _assert_true(ok1, "set_pause_flag_true")
        flag_path = os.path.join(td, "state", "manual_pause.flag")
        _assert_true(os.path.exists(flag_path), "pause_flag_exists")
        ok2 = set_public_scraper_manual_pause_flag(td, False)
        _assert_true(ok2, "set_pause_flag_false")
        _assert_equal(os.path.exists(flag_path), False, "pause_flag_removed")


def test_public_scraper_progress_text_helpers() -> None:
    rows = [
        {"detail": "√", "image": "√", "meta": "√"},
        {"detail": "√", "image": "×", "meta": "…"},
    ]
    summary = summarize_scraper_progress_rows(rows)
    _assert_equal(summary["discovered_rows"], 2, "progress_summary_discovered")
    _assert_equal(summary["completed_rows"], 1, "progress_summary_completed")
    _assert_equal(summary["downloaded_rows"], 1, "progress_summary_downloaded")
    text = build_public_scraper_progress_text(
        discovered_rows=2,
        downloaded_rows=1,
        completed_rows=1,
        total_target=4,
        list_rows=5,
        profile_rows=3,
        image_rows=2,
        metadata_rows=1,
    )
    _assert_true("下载:1/2" in text, "progress_text_download_part")
    _assert_true("发现:2/4" in text, "progress_text_discover_part")


def test_task_orchestration_argument_helpers() -> None:
    continue_args = build_continue_start_existing_task_args(
        {"mode": "browser", "auto_fallback": False, "disable_page_images": False}
    )
    _assert_equal(continue_args["skip_crawl"], False, "continue_args_skip_crawl")
    _assert_equal(continue_args["mode_override"], "browser", "continue_args_mode")

    retry_args = build_retry_start_existing_task_args(True, {"mode": "requests_jsl"})
    _assert_equal(retry_args["skip_crawl"], False, "retry_args_need_crawl")
    _assert_equal(retry_args["runtime_state"], "继续运行中", "retry_args_runtime_need_crawl")
    _assert_equal(retry_started_status_text(True), "重试任务已启动（自动包含详情重抓）", "retry_status_need_crawl")

    retry_args2 = build_retry_start_existing_task_args(False, {"mode": "requests_jsl"})
    _assert_equal(retry_args2["skip_crawl"], True, "retry_args_skip_crawl")
    _assert_equal(retry_args2["runtime_state"], "失败重试中", "retry_args_runtime_skip_crawl")

    rewrite_args = build_rewrite_metadata_start_existing_task_args()
    _assert_equal(rewrite_args["skip_images"], True, "rewrite_args_skip_images")
    _assert_equal(rewrite_args["runtime_state"], "元数据重写中", "rewrite_args_runtime")

    prepared = prepare_existing_task_runtime_config(
        {"rules": {"llm_api_key": "secret", "named_images_dir": "named"}},
        output_root="D:/tasks/demo",
        mode_override="browser",
        auto_fallback_override=False,
        disable_page_images_override=True,
    )
    prepared_rules = prepared.get("rules", {})
    _assert_equal(prepared.get("output_root"), os.path.abspath("D:/tasks/demo"), "prepare_existing_output_root")
    _assert_equal(prepared_rules.get("named_images_dir"), "", "prepare_existing_named_dir_reset")
    _assert_equal(prepared_rules.get("image_download_mode"), "browser", "prepare_existing_mode_override")
    _assert_equal(prepared_rules.get("auto_fallback_to_browser"), False, "prepare_existing_auto_fallback")
    _assert_equal(
        prepared_rules.get("disable_page_images_during_crawl"),
        True,
        "prepare_existing_disable_page_images",
    )
    _assert_equal("llm_api_key" in prepared_rules, False, "prepare_existing_drop_api_key")
    _assert_equal(
        resolve_named_images_dir("D:/tasks/demo", {"named_images_dir": "named"}),
        os.path.abspath("D:/tasks/demo/named"),
        "resolve_named_images_dir_relative",
    )
    _assert_equal(
        resolve_named_images_dir("D:/tasks/demo", {"named_images_dir": ""}),
        os.path.abspath("D:/tasks/demo"),
        "resolve_named_images_dir_default",
    )
    _assert_equal(
        build_public_scraper_log_banner("Retry", "2026-03-06 10:00:00"),
        "\n\n=== D2I Public Scraper Retry 2026-03-06 10:00:00 ===\n",
        "build_public_scraper_log_banner",
    )
    _assert_equal(
        build_public_scraper_command(
            "python",
            "D:/repo/scraper/run_public_scraper.py",
            "D:/tasks/demo/state/runtime_config.json",
            "D:/tasks/demo",
            skip_crawl=True,
            skip_images=True,
        ),
        [
            "python",
            "-X",
            "utf8",
            "D:/repo/scraper/run_public_scraper.py",
            "--config",
            "D:/tasks/demo/state/runtime_config.json",
            "--output-root",
            os.path.abspath("D:/tasks/demo"),
            "--skip-crawl",
            "--skip-images",
        ],
        "build_public_scraper_command",
    )
    _assert_equal(
        resolve_active_template_path(
            "",
            {"template_source_path": "D:/templates/demo.json"},
        ),
        os.path.abspath("D:/templates/demo.json"),
        "resolve_active_template_path_rules",
    )
    _assert_true(
        "任务进程 PID: 123" in build_started_task_message(
            pid=123,
            named_dir="D:/out",
            log_path="D:/out/log.txt",
            runtime_rules={"image_download_mode": "browser", "template_source_path": "D:/tpl.json"},
        ),
        "build_started_task_message",
    )
    _assert_true(
        "任务目录：\nD:/tasks/demo" in build_existing_task_started_message(
            pid=456,
            output_root="D:/tasks/demo",
            named_dir="D:/tasks/demo/out",
            log_path="D:/tasks/demo/log.txt",
            skip_crawl=True,
        ),
        "build_existing_task_started_message",
    )

    class _RunningProc:
        def poll(self):
            return None

    action1 = continue_action_for_active_entry(
        {"proc": _RunningProc(), "manual_paused": True},
        is_process_running_fn=lambda p: bool(p and p.poll() is None),
    )
    action2 = continue_action_for_active_entry(
        {"proc": _RunningProc(), "manual_paused": False},
        is_process_running_fn=lambda p: bool(p and p.poll() is None),
    )
    action3 = continue_action_for_active_entry(
        {"proc": None},
        is_process_running_fn=lambda p: bool(p and p.poll() is None),
    )
    _assert_equal(action1, "resume_paused", "continue_action_paused")
    _assert_equal(action2, "already_running", "continue_action_running")
    _assert_equal(action3, "start_new", "continue_action_start")


def test_task_exit_and_active_root_helpers() -> None:
    done = decide_task_exit_outcome(
        0,
        is_active=True,
        named_dir="D:/out",
        active_template_path="D:/tpl.json",
        record_path="D:/out/crawl_record.json",
    )
    _assert_equal(done["runtime_state"], "已完成", "exit_done_runtime")
    _assert_equal(done["template_state"], "done", "exit_done_template_state")
    _assert_equal(done["dialog_kind"], "info", "exit_done_dialog_kind")

    backoff = decide_task_exit_outcome(
        2,
        is_active=True,
        active_template_path="D:/tpl.json",
        blocked_until="2026-03-05T12:00:00",
        blocked_reason="suspected_block_consecutive",
    )
    _assert_equal(backoff["runtime_state"], "已暂停(风控等待)", "exit_backoff_runtime")
    _assert_equal(backoff["template_state"], "pending", "exit_backoff_template_state")

    failed = decide_task_exit_outcome(
        3,
        is_active=True,
        active_template_path="D:/tpl.json",
        log_path="D:/out/gui_public_scraper.log",
    )
    _assert_equal(failed["runtime_state"], "异常结束(3)", "exit_failed_runtime")
    _assert_equal(failed["dialog_kind"], "warning", "exit_failed_dialog_kind")

    class _RunningProc:
        def poll(self):
            return None

    exit_plan = build_public_scraper_task_exit_plan(
        "D:/tasks/demo",
        {
            "proc": _RunningProc(),
            "log_handle": object(),
            "manual_paused": True,
            "runtime_state": "运行中",
            "named_dir": "D:/out",
            "active_template_path": "D:/tpl.json",
            "log_path": "D:/out/gui_public_scraper.log",
        },
        2,
        current_active_root="D:/tasks/demo",
        record_path="D:/out/crawl_record.json",
        backoff_state={"blocked_until": "2026-03-06T12:00:00", "blocked_reason": "suspected_block_consecutive"},
        updated_at_ts=123.5,
    )
    _assert_equal(exit_plan["root"], os.path.abspath("D:/tasks/demo"), "task_exit_plan_root")
    _assert_equal(bool(exit_plan["is_active"]), True, "task_exit_plan_is_active")
    _assert_equal(exit_plan["template_state"], "pending", "task_exit_plan_template_state")
    _assert_equal(exit_plan["task"]["proc"], None, "task_exit_plan_proc_cleared")
    _assert_equal(exit_plan["task"]["manual_paused"], False, "task_exit_plan_manual_pause_cleared")
    _assert_equal(exit_plan["task"]["last_exit_code"], 2, "task_exit_plan_exit_code")
    _assert_equal(exit_plan["task"]["updated_at_ts"], 123.5, "task_exit_plan_updated_at")

    tasks = {
        "A": {"proc": None},
        "B": {"proc": _RunningProc()},
    }
    next_root = pick_next_active_root("", tasks, is_process_running_fn=lambda p: bool(p and p.poll() is None))
    _assert_equal(next_root, "B", "pick_next_active_running")
    next_root2 = pick_next_active_root("C", tasks, is_process_running_fn=lambda p: bool(p and p.poll() is None))
    _assert_equal(next_root2, "C", "pick_next_active_keep_current")
    poll_plan = build_public_scraper_poll_plan(
        "",
        tasks,
        is_process_running_fn=lambda p: bool(p and p.poll() is None),
    )
    _assert_equal(poll_plan["active_root"], "B", "task_poll_plan_active_root")
    _assert_equal(bool(poll_plan["running_any"]), True, "task_poll_plan_running_any")
    _assert_equal(bool(poll_plan["should_schedule_poll"]), True, "task_poll_plan_should_schedule")

    h1 = object()
    close_tasks = {
        "A": {"proc": None, "log_handle": h1},
        "B": {"proc": _RunningProc(), "log_handle": h1, "active_template_path": "D:/tpl.json"},
    }
    running = collect_running_public_scraper_tasks(
        close_tasks,
        is_process_running_fn=lambda p: bool(p and p.poll() is None),
    )
    _assert_equal(len(running), 1, "task_close_running_count")
    close_confirm = build_app_close_confirmation(1)
    _assert_true("仍有 1 个抓取任务在运行" in close_confirm["message"], "task_close_confirmation_message")
    cleanup = build_public_scraper_task_close_cleanup_plan("B", close_tasks["B"])
    _assert_equal(cleanup["root"], os.path.abspath("B"), "task_close_cleanup_root")
    _assert_equal(cleanup["active_template_path"], "D:/tpl.json", "task_close_cleanup_template")
    _assert_equal(collect_public_scraper_log_handles(close_tasks), [h1], "task_close_log_handles")
    close_plan = build_public_scraper_app_close_plan(
        close_tasks,
        is_process_running_fn=lambda p: bool(p and p.poll() is None),
    )
    _assert_equal(int(close_plan["running_task_count"]), 1, "task_close_plan_running_count")
    _assert_equal(bool(close_plan["should_confirm"]), True, "task_close_plan_should_confirm")
    _assert_equal(len(close_plan["cleanup_plans"]), 1, "task_close_plan_cleanup_count")
    reset_state = build_public_scraper_close_reset_state()
    _assert_equal(reset_state["runtime_state"], "空闲", "task_close_reset_runtime")
    _assert_equal(reset_state["active_task_root"], "", "task_close_reset_active_root")


def test_public_task_manager_view_helpers() -> None:
    values = public_task_summary_to_tree_values(
        {
            "status": "运行中",
            "task": "任务A",
            "profiles": 10,
            "images": 8,
            "metadata_ok": 7,
            "pending": 3,
            "review": 1,
            "failures": 2,
            "updated_at": "2026-03-04 10:00:00",
            "root": "D:/x",
        }
    )
    _assert_equal(values[0], "运行中", "task_manager_values_status")
    _assert_equal(values[9], "D:/x", "task_manager_values_root")
    _assert_equal(public_task_manager_status_text(12), "任务数: 12", "task_manager_status_text")

    with tempfile.TemporaryDirectory() as td:
        root = resolve_public_task_directory(td)
        _assert_true(bool(root), "resolve_public_task_directory_exists")
        _assert_equal(resolve_public_task_log_path(td), "", "resolve_public_task_log_missing")
        log_dir = os.path.join(td, "reports")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "gui_public_scraper.log")
        Path(log_path).write_text("x", encoding="utf-8")
        _assert_equal(resolve_public_task_log_path(td), log_path, "resolve_public_task_log_exists")


def test_public_task_manager_list_view_builder() -> None:
    summaries = {
        "task_b": {
            "status": "未完成",
            "task": "任务B",
            "profiles": 5,
            "images": 3,
            "metadata_ok": 2,
            "pending": 3,
            "review": 1,
            "failures": 1,
            "updated_at": "2026-03-05 09:00:00",
        },
        "task_a": {
            "status": "运行中",
            "task": "任务A",
            "profiles": 10,
            "images": 9,
            "metadata_ok": 8,
            "pending": 2,
            "review": 0,
            "failures": 0,
            "updated_at": "2026-03-05 10:00:00",
        },
    }

    def _summarize(root: str) -> Dict[str, Any]:
        task_key = str(root or "").replace("\\", "/").rsplit("/", 1)[-1]
        row = dict(summaries[task_key])
        row["root"] = root
        return row

    view = build_public_task_manager_list_view(
        "D:/tasks",
        discover_task_roots_fn=lambda _base: ["D:/tasks/task_b", "D:/tasks/task_a"],
        summarize_task_fn=_summarize,
        sort_rows_fn=lambda rows: sorted(list(rows), key=lambda row: str(row.get("task", ""))),
    )
    _assert_equal(int(view.get("task_count", 0)), 2, "task_manager_view_task_count")
    values = list(view.get("tree_values") or [])
    _assert_equal(len(values), 2, "task_manager_view_values_count")
    _assert_equal(values[0][1], "任务A", "task_manager_view_sorted_first_task")
    _assert_equal(values[0][9], os.path.abspath("D:/tasks/task_a"), "task_manager_view_first_root")


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
        test_scraper_audit_review_helpers,
        test_public_scraper_config_helpers,
        test_count_jsonl_rows_cache,
        test_collect_scraper_progress_rows,
        test_scraper_row_status_helpers,
        test_scraper_progress_selection_helpers,
        test_retry_detail_cleanup_helpers,
        test_scraper_progress_view_helpers,
        test_public_task_summary_sort,
        test_task_view_model_helpers,
        test_public_scraper_progress_text_helpers,
        test_task_orchestration_argument_helpers,
        test_task_exit_and_active_root_helpers,
        test_public_task_manager_view_helpers,
        test_public_task_manager_list_view_builder,
    ]
    for fn in tests:
        fn()
    print(f"[OK] phase0 contract smoke passed: {len(tests)} tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
