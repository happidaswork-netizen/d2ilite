# -*- coding: utf-8 -*-
"""A1-4/5 结果库截断可见 + 全量队列摘要(总纲 §6.2 F5/F6)。

校验点:
- 响应新增 truncated 布尔与 per_queue_limit 回显,既有字段一个不少;
- queue_summaries 每项带真实条目总数 total_items 与 per-queue truncated;
- only_queue 只过滤 items,queue_summaries 始终全量返回(F6);
- per_queue_limit 超上限被钳制;截断判定覆盖"磁盘加载被截"与"分页被截"两种。

纯 python assert,可被 pytest 收集,也可直接 `python tests/test_a1_library.py`。
"""

import atexit
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_TMP = tempfile.mkdtemp(prefix="d2i_a1_library_")
os.environ["D2I_SCRAPER_REGISTRY_PATH"] = os.path.join(_TMP, "registry.json")
os.environ["D2I_CLOUD_DATA_ROOT"] = os.path.join(_TMP, "cloud")
os.environ["D2I_CLOUD_JOBS_DB"] = os.path.join(_TMP, "jobs.sqlite")
os.environ["D2I_CLOUD_TASKS_ROOT"] = os.path.join(_TMP, "tasks")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)

from cloud import queue_service as qs  # noqa: E402
from services import desktop_scraper_backend_service as backend  # noqa: E402

LEGACY_KEYS = {"total", "offset", "limit", "previewable", "queue_count", "queues", "items", "filters", "errors"}

_FAKE_TOTALS = {"/fake/qa": 500, "/fake/qb": 2}


def _fake_records():
    return [
        {"id": "qa", "name": "队列A", "output_root": "/fake/qa", "updated_at": "2026-07-26 10:00:00"},
        {"id": "qb", "name": "队列B", "output_root": "/fake/qb", "updated_at": "2026-07-26 11:00:00"},
    ]


def _fake_items(qid, count):
    return [
        {
            "id": f"{qid}-{i}",
            "name": f"{qid}人{i}",
            "has_preview": i % 2 == 0,
            "bucket": "done",
            "status": "completed",
            "reason": "",
            "detail_url": f"https://example.invalid/{qid}/{i}",
            "image_path": "",
            "flags": {},
        }
        for i in range(count)
    ]


def _fake_queue_items(qid, *, limit=200, offset=0, status="", progress_limit=500):
    count = {"qa": 3, "qb": 2}[qid]
    items = _fake_items(qid, count)[:limit]
    return {
        "id": qid,
        "output_root": f"/fake/{qid}",
        "total": count,
        "items": items,
        "counts": {},
    }


def _with_patches(fn):
    orig_list = qs.jobs_db.list_queues
    orig_items = qs.queue_items
    orig_totals = qs._queue_total_items
    qs.jobs_db.list_queues = lambda limit=200: _fake_records()[:limit]
    qs.queue_items = _fake_queue_items
    qs._queue_total_items = lambda root: _FAKE_TOTALS.get(str(root).replace("\\", "/"), 0)
    try:
        return fn()
    finally:
        qs.jobs_db.list_queues = orig_list
        qs.queue_items = orig_items
        qs._queue_total_items = orig_totals


def test_response_keeps_legacy_fields_and_adds_truncation():
    payload = _with_patches(lambda: qs.library_list())
    missing = LEGACY_KEYS - set(payload)
    assert not missing, f"legacy fields dropped: {missing}"
    assert payload["truncated"] is True  # qa 实际 500 条 > 200 条加载上限
    assert payload["per_queue_limit"] == 200
    assert payload["queue_count"] == 2
    assert payload["total"] == 5  # qa 3 + qb 2(聚合的只是加载到的)


def test_queue_summaries_carry_real_totals():
    payload = _with_patches(lambda: qs.library_list())
    by_id = {row["id"]: row for row in payload["queues"]}
    assert by_id["qa"]["total_items"] == 500
    assert by_id["qa"]["truncated"] is True
    assert by_id["qa"]["item_count"] == 3
    assert by_id["qb"]["total_items"] == 2
    assert by_id["qb"]["truncated"] is False


def test_only_queue_filters_items_but_keeps_all_summaries():
    payload = _with_patches(lambda: qs.library_list(queue_id="qb"))
    assert {row["id"] for row in payload["queues"]} == {"qa", "qb"}, "F6: summaries must stay full"
    assert all(item["queue_id"] == "qb" for item in payload["items"])
    assert payload["total"] == 2
    # qb 未截断,且 qa 的 items 未参与聚合 → 顶层 truncated 不为真
    assert payload["truncated"] is False


def test_page_cap_truncation_detected():
    # 分页被截:per_queue_limit=1 时 qb 的 total(2) > 加载(1) → truncated
    payload = _with_patches(lambda: qs.library_list(queue_id="qb", per_queue_limit=1))
    assert payload["truncated"] is True
    assert payload["per_queue_limit"] == 1


def test_per_queue_limit_clamped():
    payload = _with_patches(lambda: qs.library_list(per_queue_limit=9999))
    assert payload["per_queue_limit"] == 2000


def test_selected_existing_queue_never_falls_through_to_another_queue():
    base = os.path.join(_TMP, "tasks")
    selected = os.path.join(base, "older", "exact-queue")
    other = os.path.join(base, "latest", "other-queue")
    os.makedirs(selected, exist_ok=True)
    os.makedirs(other, exist_ok=True)
    rows = [{"root": os.path.abspath(other)}]
    actual = backend._resolve_selected_root(base, selected, rows)
    assert actual == os.path.abspath(selected)


def test_queue_workspace_rejects_another_queues_detail():
    selected = os.path.join(_TMP, "outside", "selected")
    other = os.path.join(_TMP, "tasks", "other")
    os.makedirs(selected, exist_ok=True)
    os.makedirs(other, exist_ok=True)
    original = qs.build_scraper_workspace_payload
    qs.build_scraper_workspace_payload = lambda *_args, **_kwargs: {
        "selected_root": os.path.abspath(other),
        "selected_task": {"root": os.path.abspath(other), "profiles": 99},
        "detail": {"root": os.path.abspath(other), "profile_rows": 99},
    }
    try:
        bundle = qs._workspace_task_row(selected, os.path.join(_TMP, "tasks"))
    finally:
        qs.build_scraper_workspace_payload = original
    assert bundle["detail"] == {}
    assert bundle["selected_task"] == {}


if __name__ == "__main__":
    for fn in (
        test_response_keeps_legacy_fields_and_adds_truncation,
        test_queue_summaries_carry_real_totals,
        test_only_queue_filters_items_but_keeps_all_summaries,
        test_page_cap_truncation_detected,
        test_per_queue_limit_clamped,
        test_selected_existing_queue_never_falls_through_to_another_queue,
        test_queue_workspace_rejects_another_queues_detail,
    ):
        fn()
        print(f"PASS {fn.__name__}")
