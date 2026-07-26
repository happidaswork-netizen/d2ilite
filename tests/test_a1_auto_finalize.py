# -*- coding: utf-8 -*-
"""A1-2 H2 自动 finalize 触发放宽(总纲 §2 H2):_should_auto_finalize 各分支。

校验点:
- 进程在跑 → 不触发;
- live status=completed 且有产出 → 触发(原语义保留);
- 已成功 promote 过(幂等) → 不触发;
- 进程停 + images>0 + 静止超阈值 → 触发(新放宽分支);
- 静止不足 / 无图 / paused / cooldown / skip 标记 / 静止时间未知 → 不触发;
- 阈值可被环境变量 D2I_AUTO_FINALIZE_QUIET_MINUTES 覆盖;
- updated_at_disk 缺失时 fallback 日志 mtime。

纯 python assert,可被 pytest 收集,也可直接 `python tests/test_a1_auto_finalize.py`。
"""

import atexit
import os
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_TMP = tempfile.mkdtemp(prefix="d2i_a1_finalize_")
os.environ["D2I_SCRAPER_REGISTRY_PATH"] = os.path.join(_TMP, "registry.json")
os.environ["D2I_CLOUD_DATA_ROOT"] = os.path.join(_TMP, "cloud")
os.environ["D2I_CLOUD_JOBS_DB"] = os.path.join(_TMP, "jobs.sqlite")
os.environ["D2I_CLOUD_TASKS_ROOT"] = os.path.join(_TMP, "tasks")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)

from cloud import queue_service as qs  # noqa: E402


def _stamp(seconds_ago: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - seconds_ago))


def _record(**meta) -> dict:
    return {"id": "q-test", "meta": dict(meta)}


def _live(**over) -> dict:
    base = {
        "status": "stopped",
        "session_running": False,
        "images": 12,
        "profiles": 12,
        "updated_at_disk": _stamp(3600),  # 默认:一小时前就静止了
        "log_path": "",
    }
    base.update(over)
    return base


def test_running_never_triggers():
    assert qs._should_auto_finalize(_record(), _live(session_running=True)) is False


def test_completed_with_output_triggers():
    assert qs._should_auto_finalize(_record(), _live(status="completed")) is True
    # completed 且只有 profiles 无图也放行(原语义保留)
    assert qs._should_auto_finalize(_record(), _live(status="completed", images=0)) is True


def test_already_promoted_is_idempotent():
    meta = {
        "last_promote": {
            "ok": True,
            "dry_run": False,
            "auto": True,
            "counts": {"candidates": 12, "promoted": 12},
        }
    }
    assert qs._should_auto_finalize(_record(**meta), _live(status="completed")) is False
    assert qs._should_auto_finalize(_record(**meta), _live()) is False
    # 但 workspace 又新增了图片时允许重做
    assert qs._should_auto_finalize(_record(**meta), _live(status="completed", images=20)) is True


def test_stopped_quiet_with_images_triggers():
    assert qs._should_auto_finalize(_record(), _live()) is True


def test_stopped_but_fresh_does_not_trigger():
    assert qs._should_auto_finalize(_record(), _live(updated_at_disk=_stamp(30))) is False


def test_no_images_does_not_trigger():
    assert qs._should_auto_finalize(_record(), _live(images=0)) is False


def test_paused_cooldown_error_created_do_not_trigger():
    for status in ("paused", "cooldown", "error", "cancelled", "created"):
        assert qs._should_auto_finalize(_record(), _live(status=status)) is False, status


def test_skip_flags_respected():
    assert qs._should_auto_finalize(_record(skip_auto_finalize=True), _live()) is False
    assert qs._should_auto_finalize(_record(auto_finalize=False), _live()) is False


def test_unknown_quiet_does_not_trigger():
    assert qs._should_auto_finalize(_record(), _live(updated_at_disk="", log_path="")) is False


def test_env_override_quiet_minutes():
    old = os.environ.get("D2I_AUTO_FINALIZE_QUIET_MINUTES")
    try:
        os.environ["D2I_AUTO_FINALIZE_QUIET_MINUTES"] = "1"
        assert qs._auto_finalize_quiet_minutes() == 1.0
        # 2 分钟前静止,阈值 1 分钟 → 触发;默认 10 分钟下则不触发
        assert qs._should_auto_finalize(_record(), _live(updated_at_disk=_stamp(120))) is True
        os.environ["D2I_AUTO_FINALIZE_QUIET_MINUTES"] = "bogus"
        assert qs._auto_finalize_quiet_minutes() == qs.AUTO_FINALIZE_QUIET_MINUTES
    finally:
        if old is None:
            os.environ.pop("D2I_AUTO_FINALIZE_QUIET_MINUTES", None)
        else:
            os.environ["D2I_AUTO_FINALIZE_QUIET_MINUTES"] = old
    assert qs._should_auto_finalize(_record(), _live(updated_at_disk=_stamp(120))) is False


def test_quiet_falls_back_to_log_mtime():
    log_path = os.path.join(_TMP, "quiet.log")
    with open(log_path, "w", encoding="utf-8") as handle:
        handle.write("done\n")
    stale = time.time() - 3600
    os.utime(log_path, (stale, stale))
    live = _live(updated_at_disk="", log_path=log_path)
    assert qs._live_quiet_seconds(live) >= 3000
    assert qs._should_auto_finalize(_record(), live) is True


if __name__ == "__main__":
    for fn in (
        test_running_never_triggers,
        test_completed_with_output_triggers,
        test_already_promoted_is_idempotent,
        test_stopped_quiet_with_images_triggers,
        test_stopped_but_fresh_does_not_trigger,
        test_no_images_does_not_trigger,
        test_paused_cooldown_error_created_do_not_trigger,
        test_skip_flags_respected,
        test_unknown_quiet_does_not_trigger,
        test_env_override_quiet_minutes,
        test_quiet_falls_back_to_log_mtime,
    ):
        fn()
        print(f"PASS {fn.__name__}")
