# -*- coding: utf-8 -*-
"""A1-1 H3 僵尸进程回收(总纲 §2 H3):watcher 注册表判活。

校验点:
- register_process_watcher 起 daemon 线程 wait() 子进程,退出码/结束时间写入注册表;
- 已回收 pid 的 _is_pid_running 必须为 False(Linux 上僵尸 os.kill 仍"存活");
- 注册表查不到时 fallback 到 os.kill(当前进程应判存活);
- 同 pid 重新注册 watcher 时旧记录先清除(防 OS pid 复用误判死)。

纯 python assert,可被 pytest 收集,也可直接 `python tests/test_a1_zombie_watcher.py`。
"""

import atexit
import os
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 隔离运行时落盘位置,避免测试写进仓库/真实数据目录
_TMP = tempfile.mkdtemp(prefix="d2i_a1_watcher_")
os.environ["D2I_SCRAPER_REGISTRY_PATH"] = os.path.join(_TMP, "registry.json")
os.environ["D2I_CLOUD_DATA_ROOT"] = os.path.join(_TMP, "cloud")
os.environ["D2I_CLOUD_JOBS_DB"] = os.path.join(_TMP, "jobs.sqlite")
os.environ["D2I_CLOUD_TASKS_ROOT"] = os.path.join(_TMP, "tasks")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)

from services import desktop_scraper_backend_service as backend  # noqa: E402


def _spawn(code: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_reaped(pid: int, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = backend.reaped_process_info(pid)
        if info is not None:
            return info
        time.sleep(0.05)
    return None


def test_watcher_reaps_and_liveness_flips():
    """短命子进程退出后:注册表有退出码,判活变 False。"""
    proc = _spawn("import sys; sys.exit(7)")
    backend.register_process_watcher(proc)
    info = _wait_reaped(proc.pid)
    assert info is not None, "watcher did not reap the child in time"
    assert info.get("exit_code") == 7, f"exit_code={info.get('exit_code')!r}, want 7"
    assert float(info.get("finished_at") or 0.0) > 0.0
    assert backend._is_pid_running(proc.pid) is False


def test_liveness_fallback_without_registry():
    """注册表查不到时 fallback os.kill:当前进程应判存活。"""
    me = os.getpid()
    assert backend.reaped_process_info(me) is None
    assert backend._is_pid_running(me) is True


def test_register_clears_stale_record_for_reused_pid():
    """OS 复用 pid 时,新注册必须先清掉旧退出记录,避免活进程被判死。"""
    proc = _spawn("import time; time.sleep(60)")
    try:
        # 伪造"上一代同 pid 进程已回收"的旧记录
        with backend._REAPED_PROCS_LOCK:
            backend._REAPED_PROCS[proc.pid] = {"exit_code": 0, "finished_at": time.time()}
        assert backend._is_pid_running(proc.pid) is False, "stale record should read dead"
        backend.register_process_watcher(proc)
        assert backend.reaped_process_info(proc.pid) is None, "register must clear stale record"
        assert backend._is_pid_running(proc.pid) is True, "fresh child must read alive"
    finally:
        proc.kill()
    info = _wait_reaped(proc.pid)
    assert info is not None, "watcher did not reap the killed child"


if __name__ == "__main__":
    for fn in (
        test_watcher_reaps_and_liveness_flips,
        test_liveness_fallback_without_registry,
        test_register_clears_stale_record_for_reused_pid,
    ):
        fn()
        print(f"PASS {fn.__name__}")
