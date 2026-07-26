# -*- coding: utf-8 -*-
"""A1-3 M4 速度档语义(总纲 §2 M4):继承矩阵 + 模板 crawl 数字保留。

校验点(铁则:最终生效档默认 safe,turbo 必须显式 allow_turbo):
- apply_speed_tier_to_crawl:空串继承 crawl.speed_tier;显式档覆盖;
  overwrite_existing 默认 False 保留模板自定义数字,只补缺失键;
- create_queue:模板 standard + 请求空串 → standard;请求显式 safe → safe;
  模板/请求 turbo 无 allow_turbo → 拒;模板无档 + 空串 → safe。

纯 python assert,可被 pytest 收集,也可直接 `python tests/test_a1_speed_tiers.py`。
"""

import atexit
import json
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_TMP = tempfile.mkdtemp(prefix="d2i_a1_tiers_")
os.environ["D2I_SCRAPER_REGISTRY_PATH"] = os.path.join(_TMP, "registry.json")
os.environ["D2I_CLOUD_DATA_ROOT"] = os.path.join(_TMP, "cloud")
os.environ["D2I_CLOUD_JOBS_DB"] = os.path.join(_TMP, "jobs.sqlite")
os.environ["D2I_CLOUD_TASKS_ROOT"] = os.path.join(_TMP, "tasks")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)

from cloud import queue_service as qs  # noqa: E402
from cloud.speed_tiers import SPEED_TIER_CRAWL, apply_speed_tier_to_crawl  # noqa: E402


def test_apply_inherits_template_tier_on_empty():
    crawl = {"speed_tier": "standard", "download_delay": 99}
    out = apply_speed_tier_to_crawl(crawl, "")
    assert out["speed_tier"] == "standard"
    # overwrite_existing 默认 False:模板自定义数字保留,缺失键按档补齐
    assert out["download_delay"] == 99
    assert out["interval_min_seconds"] == SPEED_TIER_CRAWL["standard"]["interval_min_seconds"]


def test_apply_explicit_tier_overrides_template():
    out = apply_speed_tier_to_crawl({"speed_tier": "standard"}, "safe")
    assert out["speed_tier"] == "safe"


def test_apply_empty_crawl_defaults_safe():
    out = apply_speed_tier_to_crawl({}, "")
    assert out["speed_tier"] == "safe"
    assert out["download_delay"] == SPEED_TIER_CRAWL["safe"]["download_delay"]


def test_apply_overwrite_true_resets_numbers():
    out = apply_speed_tier_to_crawl({"download_delay": 99}, "safe", overwrite_existing=True)
    assert out["download_delay"] == SPEED_TIER_CRAWL["safe"]["download_delay"]


# --- create_queue 继承矩阵(monkeypatch 掉落盘/DB 依赖,不起进程不联网) ---


def _write_template(template_tier) -> str:
    payload = {
        "site_name": "测试站",
        "start_urls": ["https://example.invalid/list"],
        "crawl": {"download_delay": 99},
    }
    if template_tier:
        payload["crawl"]["speed_tier"] = template_tier
    path = os.path.join(_TMP, f"tpl_{template_tier or 'none'}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)
    return path


def _run_create(template_tier, request_tier, *, allow_turbo=False):
    tpl_path = _write_template(template_tier)
    captured = {}

    def fake_build(app_file, url, base_root, template_path=""):
        with open(template_path, encoding="utf-8") as handle:
            tpl = json.load(handle)
        runtime = {
            "site_name": tpl["site_name"],
            "crawl": dict(tpl.get("crawl") or {}),
            "output_root": os.path.join(_TMP, "out"),
        }
        return os.path.join(_TMP, "state", "runtime_config.json"), runtime

    def fake_create_record(**kwargs):
        captured.update(kwargs)
        record = dict(kwargs)
        record["id"] = "q-test"
        record["desired_state"] = "created"
        return record

    orig_build = qs.build_public_scraper_runtime_config
    orig_record = qs.jobs_db.create_queue_record
    orig_enrich = qs.enrich_queue
    qs.build_public_scraper_runtime_config = fake_build
    qs.jobs_db.create_queue_record = fake_create_record
    qs.enrich_queue = lambda record, **_kw: record
    try:
        qs.create_queue(
            start_url="https://example.invalid/list",
            template_path=tpl_path,
            speed_tier=request_tier,
            allow_turbo=allow_turbo,
        )
    finally:
        qs.build_public_scraper_runtime_config = orig_build
        qs.jobs_db.create_queue_record = orig_record
        qs.enrich_queue = orig_enrich
    return captured


def test_create_inherits_template_standard_on_empty():
    captured = _run_create("standard", "")
    assert captured.get("speed_tier") == "standard"


def test_create_explicit_safe_overrides_template():
    captured = _run_create("standard", "safe")
    assert captured.get("speed_tier") == "safe"


def test_create_template_without_tier_defaults_safe():
    captured = _run_create(None, "")
    assert captured.get("speed_tier") == "safe"


def test_create_turbo_requires_allow_turbo():
    for template_tier, request_tier in (("standard", "turbo"), ("turbo", "")):
        try:
            _run_create(template_tier, request_tier, allow_turbo=False)
        except ValueError as exc:
            assert "allow_turbo" in str(exc)
        else:
            raise AssertionError(
                f"turbo (template={template_tier!r}, request={request_tier!r}) "
                "must be rejected without allow_turbo"
            )


def test_create_turbo_allowed_when_confirmed():
    captured = _run_create("standard", "turbo", allow_turbo=True)
    assert captured.get("speed_tier") == "turbo"


def test_create_keeps_template_crawl_numbers():
    """建队不再重置模板自定义 crawl 数字(overwrite_existing=False)。"""
    _run_create("standard", "")
    config_path = os.path.join(_TMP, "state", "runtime_config.json")
    with open(config_path, encoding="utf-8") as handle:
        runtime = json.load(handle)
    assert runtime["crawl"]["download_delay"] == 99
    assert runtime["crawl"]["speed_tier"] == "standard"


if __name__ == "__main__":
    for fn in (
        test_apply_inherits_template_tier_on_empty,
        test_apply_explicit_tier_overrides_template,
        test_apply_empty_crawl_defaults_safe,
        test_apply_overwrite_true_resets_numbers,
        test_create_inherits_template_standard_on_empty,
        test_create_explicit_safe_overrides_template,
        test_create_template_without_tier_defaults_safe,
        test_create_turbo_requires_allow_turbo,
        test_create_turbo_allowed_when_confirmed,
        test_create_keeps_template_crawl_numbers,
    ):
        fn()
        print(f"PASS {fn.__name__}")
