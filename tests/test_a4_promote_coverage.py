# -*- coding: utf-8 -*-
"""A4 promote/coverage 修复测试:M12 五段式路径、M5 精确去重、F7 unit kind。

纯 python assert 脚本,直接 `python tests/test_a4_promote_coverage.py` 可跑,
也兼容 pytest 收集。只用本地临时目录/临时 sqlite,跑完自清理,无网络副作用。
"""

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cloud import coverage_service, promote_service  # noqa: E402


# ---------------------------------------------------------------- M12 路径解析


def test_resolve_admin_context_five_segment_county():
    # 五段式 .../省/市/区县/区县级/单位:区县落 county,市/省不再被挤位
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "山东省" / "淄博市" / "张店区" / "区县级" / "张店区人民政府"
        root.mkdir(parents=True)
        ctx = promote_service.resolve_admin_context(str(root))
        assert ctx["province"] == "山东省", ctx
        assert ctx["city"] == "淄博市", ctx
        assert ctx["county"] == "张店区", ctx
        assert ctx["admin_level"] == "区县级", ctx
        assert ctx["unit"] == "张店区人民政府", ctx


def test_resolve_admin_context_four_segment_regression():
    # 四段式 .../省/市/市级/单位 行为回归:一个字节都不变,county 保持空
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "河北省" / "沧州市" / "市级" / "沧州市人民政府"
        root.mkdir(parents=True)
        ctx = promote_service.resolve_admin_context(str(root))
        assert ctx["province"] == "河北省", ctx
        assert ctx["city"] == "沧州市", ctx
        assert ctx["county"] == "", ctx
        assert ctx["admin_level"] == "市级", ctx
        assert ctx["unit"] == "沧州市人民政府", ctx


def test_resolve_admin_context_rules_admin_precedence():
    # rules.admin 显式给的值优先于路径推断(五段式也不覆盖)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "山东省" / "淄博市" / "张店区" / "区县级" / "某单位"
        root.mkdir(parents=True)
        cfg = {"rules": {"admin": {"province": "山东省", "city": "淄博市", "county": "临淄区"}}}
        ctx = promote_service.resolve_admin_context(str(root), cfg)
        assert ctx["county"] == "临淄区", ctx
        assert ctx["city"] == "淄博市", ctx


# ------------------------------------------------------------------ M5 去重


def _write_profiles(root: Path, rows):
    raw = root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    with (raw / "profiles.jsonl").open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_no_photo_suffix_names_both_registered():
    # 李小明/小明 姓名互为后缀:旧子串去重会把「小明」漏建档,现在两条都在
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "任务根"
        root.mkdir()
        _write_profiles(
            root,
            [
                {"name": "李小明", "detail_url": "http://example.com/a.html", "gender": "男"},
                {"name": "小明", "detail_url": "http://example.com/b.html", "gender": "男"},
            ],
        )
        items = promote_service.collect_promote_candidates(str(root))
        names = sorted(i["name"] for i in items)
        assert names == ["小明", "李小明"], names
        assert all(i["has_image"] is False for i in items), items


def test_same_name_different_detail_url_both_kept():
    # 同名不同 detail_url 是两个人:索引不互踩,no_photo 两条都建档
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "任务根"
        root.mkdir()
        _write_profiles(
            root,
            [
                {"name": "王强", "detail_url": "http://example.com/w1.html", "summary": "现任甲局局长"},
                {"name": "王强", "detail_url": "http://example.com/w2.html", "summary": "现任乙局局长"},
            ],
        )
        items = promote_service.collect_promote_candidates(str(root))
        assert len(items) == 2, items
        urls = sorted(i["detail_url"] for i in items)
        assert urls == ["http://example.com/w1.html", "http://example.com/w2.html"], urls


def test_download_exact_pair_dedup_keeps_other_person():
    # 下载记录只覆盖 (王强, w1):w1 不再进 no_photo,同名 w2 仍作为另一人建档
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "任务根"
        root.mkdir()
        img = root / "王强.jpg"
        img.write_bytes(b"\xff\xd8\xff\xe0 fake-jpg")
        _write_profiles(
            root,
            [
                {"name": "王强", "detail_url": "http://example.com/w1.html"},
                {"name": "王强", "detail_url": "http://example.com/w2.html"},
            ],
        )
        dl = root / "downloads"
        dl.mkdir()
        with (dl / "image_downloads.jsonl").open("w", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {"name": "王强", "detail_url": "http://example.com/w1.html", "saved_path": str(img)},
                    ensure_ascii=False,
                )
                + "\n"
            )
        items = promote_service.collect_promote_candidates(str(root))
        assert len(items) == 2, items
        by_url = {i["detail_url"]: i for i in items}
        assert by_url["http://example.com/w1.html"]["has_image"] is True, items
        assert by_url["http://example.com/w2.html"]["has_image"] is False, items


def test_download_without_detail_degrades_to_exact_name():
    # 下载记录无 detail_url 时退化为姓名精确等值:同名 profile 不重复建档,
    # 但互为后缀的另一个名字不受影响
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "任务根"
        root.mkdir()
        img = root / "小明.jpg"
        img.write_bytes(b"\xff\xd8\xff\xe0 fake-jpg")
        _write_profiles(
            root,
            [
                {"name": "小明", "detail_url": "http://example.com/xm.html"},
                {"name": "李小明", "detail_url": "http://example.com/lxm.html"},
            ],
        )
        dl = root / "downloads"
        dl.mkdir()
        with (dl / "image_downloads.jsonl").open("w", encoding="utf-8") as fh:
            fh.write(json.dumps({"name": "小明", "saved_path": str(img)}, ensure_ascii=False) + "\n")
        items = promote_service.collect_promote_candidates(str(root))
        by_name = {}
        for it in items:
            by_name.setdefault(it["name"], []).append(it)
        assert len(by_name.get("小明") or []) == 1, items
        assert by_name["小明"][0]["has_image"] is True, items
        assert len(by_name.get("李小明") or []) == 1, items
        assert by_name["李小明"][0]["has_image"] is False, items


# ------------------------------------------------------------------ F7 kind


def _make_people_db(tmp: Path) -> Path:
    db = tmp / "people.sqlite"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "CREATE TABLE people (person_id TEXT PRIMARY KEY, name TEXT, province TEXT,"
            " city TEXT, unit_name TEXT, primary_image_path TEXT)"
        )
        conn.executemany(
            "INSERT INTO people VALUES (?,?,?,?,?,?)",
            [
                ("p1", "张三", "河北省", "沧州市", "沧州市人民政府", "/vol1/1001/角色肖像/政府/x/1.jpg"),
                ("p2", "李四", "河北省", "沧州市", "沧州市人民政府", ""),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return db


def test_unit_nodes_have_kind_unit():
    # F7:_load_units_from_people 产出的单位节点 dict 必须带 kind == 'unit'
    with tempfile.TemporaryDirectory() as tmp:
        db = _make_people_db(Path(tmp))
        units = coverage_service._load_units_from_people(db)
        assert units, "expected at least one unit node"
        for u in units:
            assert u.get("kind") == "unit", u


def test_get_node_first_branch_returns_kind():
    # get_node 命中 unit_nodes 第一分支时,前端 kicker 能拿到 kind
    old_env = os.environ.get("D2I_PEOPLE_DB")
    old_cache = dict(coverage_service._CACHE)
    with tempfile.TemporaryDirectory() as tmp:
        db = _make_people_db(Path(tmp))
        os.environ["D2I_PEOPLE_DB"] = str(db)
        coverage_service._CACHE["at"] = 0.0
        coverage_service._CACHE["payload"] = None
        try:
            raw = coverage_service.load_unit_nodes(force=True)
            assert str(raw.get("source") or "").startswith("people:"), raw.get("source")
            units = raw.get("unit_nodes") or []
            assert units, "expected unit nodes from temp people.sqlite"
            detail = coverage_service.get_node(units[0]["node_id"])
            assert detail is not None
            assert detail["node"].get("kind") == "unit", detail["node"]
        finally:
            if old_env is None:
                os.environ.pop("D2I_PEOPLE_DB", None)
            else:
                os.environ["D2I_PEOPLE_DB"] = old_env
            coverage_service._CACHE["at"] = old_cache.get("at", 0.0)
            coverage_service._CACHE["payload"] = old_cache.get("payload")


def test_enrich_node_setdefault_for_legacy_snapshot():
    # 旧 snapshot 的 unit dict 没有 kind → enrich 后补成 unit;已有 kind 的不被覆盖
    legacy = {"node_id": "abc", "unit_name": "某单位", "people_n": 3, "no_path_n": 1, "path_rate": 0.6667}
    out = coverage_service._enrich_node_for_ui(legacy)
    assert out.get("kind") == "unit", out
    city = coverage_service._enrich_node_for_ui({"node_id": "c1", "kind": "city", "people_n": 0, "no_path_n": 0})
    assert city.get("kind") == "city", city


if __name__ == "__main__":
    for fn in (
        test_resolve_admin_context_five_segment_county,
        test_resolve_admin_context_four_segment_regression,
        test_resolve_admin_context_rules_admin_precedence,
        test_no_photo_suffix_names_both_registered,
        test_same_name_different_detail_url_both_kept,
        test_download_exact_pair_dedup_keeps_other_person,
        test_download_without_detail_degrades_to_exact_name,
        test_unit_nodes_have_kind_unit,
        test_get_node_first_branch_returns_kind,
        test_enrich_node_setdefault_for_legacy_snapshot,
    ):
        fn()
        print(f"PASS {fn.__name__}")
    print("ALL A4 TESTS PASS")
