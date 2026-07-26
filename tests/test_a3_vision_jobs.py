# -*- coding: utf-8 -*-
"""A3 vision 作业泵与取消语义:F1 requeue 阈值/泵互斥 + F2 取消 CAS + F4 进度回写。

纯 python assert 脚本,直接 `python tests/test_a3_vision_jobs.py` 可跑,
也兼容 pytest 收集。临时 sqlite(D2I_CLOUD_JOBS_DB 指到 tempdir),
run_paths_vision 一律 mock,零 Grok 外呼。
"""

import os
import shutil
import sqlite3
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cloud import jobs_db  # noqa: E402
from cloud import vision_service as vs  # noqa: E402


@contextmanager
def _jobs_env():
    """临时 jobs.sqlite,通过 D2I_CLOUD_JOBS_DB 让默认路径也落到 tempdir。"""
    tmp = Path(tempfile.mkdtemp(prefix="a3jobs_")).resolve()
    db = tmp / "jobs.sqlite"
    old = os.environ.get("D2I_CLOUD_JOBS_DB")
    os.environ["D2I_CLOUD_JOBS_DB"] = str(db)
    try:
        jobs_db.init_db()
        yield db
    finally:
        if old is None:
            os.environ.pop("D2I_CLOUD_JOBS_DB", None)
        else:
            os.environ["D2I_CLOUD_JOBS_DB"] = old
        shutil.rmtree(tmp, ignore_errors=True)


def _backdate_running(db: Path, seconds: float) -> None:
    conn = sqlite3.connect(str(db))
    try:
        past = time.time() - seconds
        conn.execute(
            "UPDATE vision_jobs SET started_at=?, updated_at=? WHERE status='running'",
            (past, past),
        )
        conn.commit()
    finally:
        conn.close()


# ------------------------------------------------------------------------- F1


def test_f1_requeue_respects_staleness_threshold():
    with _jobs_env() as db:
        job = jobs_db.create_vision_job(name="fresh", items=[{"name": "a", "path": "a.jpg"}])
        claimed = jobs_db.claim_next_vision_job()
        assert claimed and claimed["id"] == job["id"]

        # 阈值内(刚 claim)绝不打回:在跑的批不能被二次 claim 双计费
        n = jobs_db.requeue_stale_running_vision_jobs(stale_after_seconds=1800)
        assert n == 0, n
        cur = jobs_db.get_vision_job(job["id"])
        assert cur["status"] == "running", cur["status"]

        # 超阈值(模拟容器死掉留下的孤儿 running)才打回 queued
        _backdate_running(db, 3600)
        n = jobs_db.requeue_stale_running_vision_jobs(stale_after_seconds=1800)
        assert n == 1, n
        cur = jobs_db.get_vision_job(job["id"])
        assert cur["status"] == "queued" and cur["started_at"] is None, cur


def test_f1_pump_busy_returns_without_requeue():
    with _jobs_env():
        calls = {"requeue": 0}
        orig_requeue = jobs_db.requeue_stale_running_vision_jobs

        def counting_requeue(*args, **kwargs):
            calls["requeue"] += 1
            return 0

        jobs_db.requeue_stale_running_vision_jobs = counting_requeue
        vs._set_pump_state(running=True)
        try:
            # 同步路径:泵在跑 → 直接 pump_busy,不 requeue、不进 _pump_loop
            out = vs.pump_vision_jobs(background=False)
            assert out.get("already_running") is True, out
            assert out.get("reason") == "pump_busy", out
            assert out.get("started") is False, out
            assert calls["requeue"] == 0, calls
            # 后台路径同样受守护
            out2 = vs.pump_vision_jobs(background=True)
            assert out2.get("already_running") is True, out2
            assert calls["requeue"] == 0, calls
        finally:
            vs._set_pump_state(running=False)
            jobs_db.requeue_stale_running_vision_jobs = orig_requeue


def test_f1_pump_idle_runs_once_and_releases():
    with _jobs_env():
        calls = {"requeue": 0}
        orig_requeue = jobs_db.requeue_stale_running_vision_jobs

        def counting_requeue(*args, **kwargs):
            calls["requeue"] += 1
            assert kwargs.get("stale_after_seconds") == 1800.0, kwargs
            return 0

        jobs_db.requeue_stale_running_vision_jobs = counting_requeue
        try:
            out = vs.pump_vision_jobs(background=False)  # 空队列,立即返回
            assert out.get("ok") is True, out
            assert out.get("ran_count") == 0, out
            assert calls["requeue"] == 1, calls
            assert vs.pump_state().get("running") is False
        finally:
            jobs_db.requeue_stale_running_vision_jobs = orig_requeue


# ------------------------------------------------------------------------- F2


def test_f2_terminal_cas_keeps_cancelled():
    with _jobs_env():
        job = jobs_db.create_vision_job(name="c1", items=[{"name": "a", "path": "a.jpg"}])
        jid = job["id"]
        claimed = jobs_db.claim_next_vision_job()
        assert claimed and claimed["id"] == jid
        cancelled = jobs_db.cancel_vision_job(jid)
        assert cancelled["status"] == "cancelled", cancelled["status"]

        # 终态 CAS:期望 running 才允许写终态,取消后写不进
        after = jobs_db.update_vision_job(
            jid, status="completed", finished=True, expected_status="running"
        )
        assert after["status"] == "cancelled", after["status"]
        after = jobs_db.update_vision_job(
            jid, status="failed", error="late failure", expected_status="running"
        )
        assert after["status"] == "cancelled", after["status"]

        # 对照组:仍在 running 的作业 CAS 可以写进
        job2 = jobs_db.create_vision_job(name="c2", items=[{"name": "b", "path": "b.jpg"}])
        jobs_db.claim_next_vision_job()
        done = jobs_db.update_vision_job(
            job2["id"], status="completed", finished=True, expected_status="running"
        )
        assert done["status"] == "completed", done["status"]


def _fake_report(paths, names, person_ids):
    items = [
        {
            "ok": True,
            "name": names[i] if i < len(names) else "",
            "person_id": person_ids[i] if i < len(person_ids) else "",
            "path": p,
            "classification": {
                "result": {"person_count": 1, "visual_gender": "男"},
                "model": "grok-mock",
            },
            "people": {"action": "update"},
        }
        for i, p in enumerate(paths)
    ]
    return {
        "ok": True,
        "at": "2026-07-26 00:00:00",
        "model": "grok-mock",
        "prompt_version": "titi-visual-reference-v1",
        "available": True,
        "counts": {
            "candidates": len(paths),
            "classified": len(paths),
            "failed": 0,
            "skipped": 0,
            "people_update": len(paths),
            "missing": 0,
        },
        "items": items,
    }


def test_f2_f4_cancel_mid_run_stops_calls_and_keeps_counts():
    with _jobs_env():
        items = [
            {"name": f"n{i}", "person_id": f"p{i}", "path": f"img{i}.jpg"}
            for i in range(12)
        ]
        job = jobs_db.create_vision_job(name="big", items=items)
        jid = job["id"]
        calls = []

        def fake_run_paths(paths, **kwargs):
            calls.append(list(paths))
            if len(calls) == 2:
                # F4:第一片(5 个)的进度必须已落库,不再恒 0%
                cur = jobs_db.get_vision_job(jid)
                assert int(cur["done_count"]) == 5, cur["done_count"]
                assert int(cur["ok_count"]) == 5, cur["ok_count"]
                # 模拟操作员在第二片执行中点取消
                jobs_db.cancel_vision_job(jid)
            return _fake_report(
                paths, kwargs.get("names") or [], kwargs.get("person_ids") or []
            )

        orig = vs.run_paths_vision
        vs.run_paths_vision = fake_run_paths
        try:
            out = vs.run_vision_job(jid)
        finally:
            vs.run_paths_vision = orig

        # 第三片开始前发现 cancelled → 不再调用(12 项只跑了 2 片 = 10 项)
        assert len(calls) == 2, len(calls)
        assert out.get("ok") is True and out.get("cancelled") is True, out
        final = jobs_db.get_vision_job(jid)
        # 终态保持 cancelled(CAS 写不进 completed),已完成计数保留
        assert final["status"] == "cancelled", final["status"]
        assert int(final["done_count"]) == 10, final["done_count"]
        assert int(final["ok_count"]) == 10, final["ok_count"]


def test_f4_full_run_flushes_progress_and_completes():
    with _jobs_env():
        items = [
            {"name": f"n{i}", "person_id": f"p{i}", "path": f"img{i}.jpg"}
            for i in range(12)
        ]
        job = jobs_db.create_vision_job(name="full", items=items)
        jid = job["id"]
        seen_progress = []

        def fake_run_paths(paths, **kwargs):
            cur = jobs_db.get_vision_job(jid)
            seen_progress.append(int(cur["done_count"]))
            return _fake_report(
                paths, kwargs.get("names") or [], kwargs.get("person_ids") or []
            )

        orig = vs.run_paths_vision
        vs.run_paths_vision = fake_run_paths
        try:
            out = vs.run_vision_job(jid)
        finally:
            vs.run_paths_vision = orig

        assert out.get("ok") is True and out.get("cancelled") is False, out
        # 每片开始时看到的 done_count:0 → 5 → 10(逐片回写,进度在动)
        assert seen_progress == [0, 5, 10], seen_progress
        final = jobs_db.get_vision_job(jid)
        assert final["status"] == "completed", final["status"]
        assert int(final["done_count"]) == 12, final["done_count"]
        assert int(final["ok_count"]) == 12, final["ok_count"]
        assert int(final["total"]) == 12, final["total"]
        # item 结果按 seed 顺序合回,携带视觉字段
        assert len(final["items"]) == 12
        assert final["items"][0]["person_id"] == "p0"
        assert final["items"][0]["visual_gender"] == "男"
        assert len((final["result"] or {}).get("item_results") or []) == 12


if __name__ == "__main__":
    for fn in (
        test_f1_requeue_respects_staleness_threshold,
        test_f1_pump_busy_returns_without_requeue,
        test_f1_pump_idle_runs_once_and_releases,
        test_f2_terminal_cas_keeps_cancelled,
        test_f2_f4_cancel_mid_run_stops_calls_and_keeps_counts,
        test_f4_full_run_flushes_progress_and_completes,
    ):
        fn()
        print(f"PASS {fn.__name__}")
