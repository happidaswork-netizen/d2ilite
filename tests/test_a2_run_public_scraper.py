# -*- coding: utf-8 -*-
"""A2 冒烟:import run_public_scraper 无抓取副作用 + M2 磁盘 reconcile_report 含 run_state。

纯 python assert 脚本,直接 `python tests/test_a2_run_public_scraper.py` 可跑,
也兼容 pytest 收集。只用本地临时目录,跑完自清理。
"""

import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = REPO_ROOT / "scraper"
for _p in (str(REPO_ROOT), str(SCRAPER_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def test_import_run_public_scraper_smoke():
    # 模块级只有 import / 常量定义,__main__ 有守卫 → 导入本身不得触发抓取
    import run_public_scraper as runner

    assert callable(runner.main)
    # browser 模式入口现在也走严格闸门:裸职务/导航词不再回落原始文本
    assert runner._prefer_person_name("市长", "") == ""
    assert runner._prefer_person_name("魅力狮城", "") == ""
    assert runner._prefer_person_name("<p>副市长 姜桂海</p>", "") == "姜桂海"


def test_save_json_atomic_writes_and_replaces():
    import run_public_scraper as runner

    with tempfile.TemporaryDirectory() as td:
        target = Path(td) / "reports" / "demo.json"
        runner.save_json_atomic(target, {"a": 1})
        assert json.loads(target.read_text(encoding="utf-8")) == {"a": 1}
        # 覆盖已存在文件(Windows 上 os.replace 也必须成功)
        runner.save_json_atomic(target, {"a": 2})
        assert json.loads(target.read_text(encoding="utf-8")) == {"a": 2}
        assert not target.with_name(target.name + ".tmp").exists()


def test_reconcile_report_run_state_persisted_to_disk():
    import run_public_scraper as runner

    with tempfile.TemporaryDirectory() as td:
        output_root = Path(td)
        report = runner.write_reconcile_report(output_root)
        report_path = output_root / "reports" / "reconcile_report.json"
        assert report_path.exists()
        base = json.loads(report_path.read_text(encoding="utf-8"))
        assert "reconcile" in base

        # 模拟 run() 收尾的补写(与 main 中同一代码路径:enrich 后 save_json_atomic)
        report["run_state"] = "finished"
        report["backoff"] = {"active": False, "blocked_until": "", "blocked_reason": ""}
        runner.save_json_atomic(report_path, report)

        on_disk = json.loads(report_path.read_text(encoding="utf-8"))
        assert on_disk["run_state"] == "finished"
        assert on_disk["backoff"]["active"] is False
        assert not report_path.with_name(report_path.name + ".tmp").exists()


def test_main_wiring_saves_enriched_report():
    # 源码级 wiring 冒烟:run 收尾在 run_state 赋值之后落盘同一路径
    import run_public_scraper as runner

    src = Path(runner.__file__).read_text(encoding="utf-8")
    idx_state = src.index('report["run_state"]')
    idx_save = src.index(
        'save_json_atomic(output_root / "reports" / "reconcile_report.json", report)',
        idx_state,
    )
    assert idx_save > idx_state


if __name__ == "__main__":
    for _name in sorted(k for k in dir() if k.startswith("test_")):
        globals()[_name]()
        print(f"PASS {_name}")
    print("ALL PASS")
