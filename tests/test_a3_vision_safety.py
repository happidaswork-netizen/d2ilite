# -*- coding: utf-8 -*-
"""A3 vision 安全与正确性:H5 外呼白名单 + F3 dry_run 去重 + M6 同名回退。

纯 python assert 脚本,直接 `python tests/test_a3_vision_safety.py` 可跑,
也兼容 pytest 收集。只用临时目录/临时 sqlite,Grok 一律 mock,零外呼。
"""

import os
import shutil
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cloud import vision_service as vs  # noqa: E402
from visual_classifier import VisionRuntime  # noqa: E402


@contextmanager
def _env(**kv):
    """临时环境变量,退出时恢复原值。"""
    old = {k: os.environ.get(k) for k in kv}
    try:
        for k, v in kv.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextmanager
def _sandbox():
    """临时 portrait 根 + 任务根,跑完自清理。"""
    tmp = Path(tempfile.mkdtemp(prefix="a3vision_")).resolve()
    portrait = tmp / "portrait"
    tasks = tmp / "tasks"
    (portrait / "政府").mkdir(parents=True)
    tasks.mkdir(parents=True)
    try:
        with _env(D2I_PORTRAIT_ROOT=str(portrait), D2I_CLOUD_TASKS_ROOT=str(tasks)):
            yield tmp, portrait, tasks
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------- H5 whitelist


def test_h5_allows_portrait_and_tasks_roots():
    with _sandbox() as (tmp, portrait, tasks):
        img = portrait / "政府" / "张三.jpg"
        img.write_bytes(b"fake-jpg")
        out = vs.resolve_image_path(str(img))
        assert out is not None and out == img.resolve(), out

        # 相对路径经 portrait_root 映射后也应放行
        assert vs.resolve_image_path("政府/张三.jpg") == img.resolve()

        task_img = tasks / "q1" / "downloads" / "李四.jpg"
        task_img.parent.mkdir(parents=True)
        task_img.write_bytes(b"fake-jpg")
        assert vs.resolve_image_path(str(task_img)) == task_img.resolve()


def test_h5_blocks_protected_markers():
    with _sandbox() as (tmp, portrait, tasks):
        # 选角/备份/__hdd_prebind 即使真实存在且可读也必须拒绝
        for sub in ("选角", "备份", "角色肖像__hdd_prebind", "repair_backup"):
            bad = portrait / sub / "王五.jpg"
            bad.parent.mkdir(parents=True, exist_ok=True)
            bad.write_bytes(b"fake-jpg")
            assert vs.resolve_image_path(str(bad)) is None, sub
        # 保护判断与 queue_service 语义一致:纯字符串也拦
        assert vs._is_protected_vision_path("/vol1/1001/选角/x.jpg")
        assert vs._is_protected_vision_path("D:/某备份/x.jpg")
        assert vs._is_protected_vision_path("/vol1/1001/角色肖像__hdd_prebind/x.jpg")
        assert not vs._is_protected_vision_path("/vol1/1001/角色肖像/政府/x.jpg")


def test_h5_blocks_dotdot_escape_and_outside_roots():
    with _sandbox() as (tmp, portrait, tasks):
        outside = tmp / "escape.jpg"
        outside.write_bytes(b"fake-jpg")
        # .. 逃逸:词面在 portrait 根内,realpath 后在根外 → 拒绝
        sneaky = f"{portrait}/../escape.jpg"
        assert vs.resolve_image_path(sneaky) is None
        # 白名单外的绝对路径直接拒绝
        assert vs.resolve_image_path(str(outside)) is None


# ------------------------------------------------------- helpers for F3 / M6

_PEOPLE_SCHEMA = """
CREATE TABLE people (
  person_id TEXT PRIMARY KEY,
  name TEXT,
  gender TEXT,
  unit_name TEXT,
  province TEXT,
  city TEXT,
  primary_image_path TEXT,
  person_count INTEGER,
  visual_gender TEXT,
  visual_body_type TEXT,
  visual_hairstyle TEXT,
  visual_pose TEXT,
  visual_age TEXT,
  visual_attire TEXT,
  visual_classification_source TEXT,
  visual_classification_raw TEXT,
  visual_warnings TEXT,
  vision_verified INTEGER,
  vision_model TEXT,
  vision_prompt_version TEXT,
  vision_classified_at TEXT,
  vision_image_sha256 TEXT,
  last_checked_at TEXT
)
"""


def _make_people_db(tmp: Path) -> Path:
    db = tmp / "people.sqlite"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(_PEOPLE_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return db


def _mock_runtime(tmp: Path) -> VisionRuntime:
    key_file = tmp / "mock_vision_key.txt"
    key_file.write_text("test-key", encoding="utf-8")
    return VisionRuntime(
        enabled=True,
        api_base="http://127.0.0.1:1/v1",
        model="grok-mock",
        api_key_file=key_file,
        concurrency=1,
        timeout_seconds=10,
        max_retries=1,
    )


# ------------------------------------------------------------------------- F3


def test_f3_dry_run_dedupes_and_writes_nothing():
    with _sandbox() as (tmp, portrait, tasks):
        db = _make_people_db(tmp)
        img = portrait / "政府" / "肖像.jpg"
        img.write_bytes(b"fake-jpg")
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                "INSERT INTO people (person_id, name, visual_gender, primary_image_path) VALUES (?,?,?,?)",
                ("p1", "王五", "男", str(img)),
            )
            conn.execute(
                "INSERT INTO people (person_id, name, primary_image_path) VALUES (?,?,?)",
                ("p2", "赵六", str(img)),
            )
            conn.commit()
        finally:
            conn.close()

        rt = _mock_runtime(tmp)
        calls = []

        def fake_classify_image(path, **kwargs):
            calls.append(str(path))
            return {
                "person_count": 1,
                "visual_gender": "女",
                "visual_body_type": "标准",
                "visual_hairstyle": "短发",
                "visual_pose": "正面证件照",
            }

        orig = vs.classify_image
        vs.classify_image = fake_classify_image
        try:
            # 已识别的人:dry_run 也必须去重跳过,不打 Grok
            res1 = vs.classify_and_write_person(
                image_path=str(img),
                person_id="p1",
                name="王五",
                dry_run=True,
                write_people=True,
                runtime=rt,
                client=object(),
                people_db=db,
            )
            assert res1.get("ok") and res1.get("skipped"), res1
            assert res1.get("reason") == "visual_gender_present", res1
            assert calls == [], calls

            # 未识别的人:dry_run 会分类(mock)但绝不写 people 库
            res2 = vs.classify_and_write_person(
                image_path=str(img),
                person_id="p2",
                name="赵六",
                dry_run=True,
                write_people=True,
                runtime=rt,
                client=object(),
                people_db=db,
            )
            assert res2.get("ok") and not res2.get("skipped"), res2
            assert (res2.get("people") or {}).get("action") == "dry_update", res2
            assert len(calls) == 1, calls
        finally:
            vs.classify_image = orig

        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT visual_gender, visual_classification_raw, vision_classified_at "
                "FROM people WHERE person_id='p2'"
            ).fetchone()
        finally:
            conn.close()
        assert row == (None, None, None), row


# ------------------------------------------------------------------------- M6


def test_m6_same_name_without_unit_is_ambiguous_not_written():
    tmp = Path(tempfile.mkdtemp(prefix="a3m6_"))
    conn = sqlite3.connect(str(tmp / "people.sqlite"))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(_PEOPLE_SCHEMA)
        conn.execute(
            "INSERT INTO people (person_id, name, unit_name, city) VALUES (?,?,?,?)",
            ("pa", "李四", "淄博市公安局", "淄博"),
        )
        conn.execute(
            "INSERT INTO people (person_id, name, unit_name, city) VALUES (?,?,?,?)",
            ("pb", "李四", "潍坊市司法局", "潍坊"),
        )
        conn.commit()

        result = {"person_count": 1, "visual_gender": "男"}
        # 无 unit/city 约束 + 同名两行 → ambiguous,一行都不写
        rec = vs.write_visual_to_people(conn, name="李四", result=result)
        assert rec.get("action") == "ambiguous", rec
        assert rec.get("error") == "ambiguous_name_match", rec
        n = conn.execute(
            "SELECT COUNT(*) FROM people WHERE visual_gender IS NOT NULL"
        ).fetchone()[0]
        assert n == 0, n

        # 带 unit 约束唯一命中 → 只写对应行
        rec2 = vs.write_visual_to_people(
            conn, name="李四", unit_name="潍坊市司法局", result=result, model="grok-mock"
        )
        assert rec2.get("action") == "update", rec2
        assert rec2.get("person_id") == "pb", rec2
        rows = conn.execute(
            "SELECT person_id, visual_gender FROM people ORDER BY person_id"
        ).fetchall()
        got = {r["person_id"]: r["visual_gender"] for r in rows}
        assert got["pa"] is None and got["pb"] == "男", got

        # 带 city 约束同样生效
        rec3 = vs.write_visual_to_people(
            conn, name="李四", city="淄博", result=result, model="grok-mock"
        )
        assert rec3.get("action") == "update" and rec3.get("person_id") == "pa", rec3
    finally:
        conn.close()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    for fn in (
        test_h5_allows_portrait_and_tasks_roots,
        test_h5_blocks_protected_markers,
        test_h5_blocks_dotdot_escape_and_outside_roots,
        test_f3_dry_run_dedupes_and_writes_nothing,
        test_m6_same_name_without_unit_is_ambiguous_not_written,
    ):
        fn()
        print(f"PASS {fn.__name__}")
