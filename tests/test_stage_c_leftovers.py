# -*- coding: utf-8 -*-
"""Stage-C leftovers + product guards: promote lease, gender, cost summary."""

from __future__ import annotations

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
from cloud import promote_service as ps  # noqa: E402
from cloud import vision_service as vs  # noqa: E402


@contextmanager
def _jobs_env():
    tmp = Path(tempfile.mkdtemp(prefix="stage_c_")).resolve()
    db = tmp / "jobs.sqlite"
    old = os.environ.get("D2I_CLOUD_JOBS_DB")
    os.environ["D2I_CLOUD_JOBS_DB"] = str(db)
    try:
        jobs_db.init_db()
        yield db, tmp
    finally:
        if old is None:
            os.environ.pop("D2I_CLOUD_JOBS_DB", None)
        else:
            os.environ["D2I_CLOUD_JOBS_DB"] = old
        shutil.rmtree(tmp, ignore_errors=True)


def test_m9_lease_exclusive_and_expire():
    with _jobs_env() as (db, _tmp):
        a = jobs_db.try_acquire_queue_lease(
            "q_test", lease_kind="promote", owner="worker-a", ttl_seconds=60, db_path=db
        )
        assert a["acquired"] is True, a
        b = jobs_db.try_acquire_queue_lease(
            "q_test", lease_kind="promote", owner="worker-b", ttl_seconds=60, db_path=db
        )
        assert b["acquired"] is False and b.get("reason") == "held", b
        # same owner refresh ok
        a2 = jobs_db.try_acquire_queue_lease(
            "q_test", lease_kind="promote", owner="worker-a", ttl_seconds=60, db_path=db
        )
        assert a2["acquired"] is True, a2
        jobs_db.release_queue_lease("q_test", lease_kind="promote", owner="worker-a", db_path=db)
        c = jobs_db.try_acquire_queue_lease(
            "q_test", lease_kind="promote", owner="worker-b", ttl_seconds=60, db_path=db
        )
        assert c["acquired"] is True, c
        # expire then steal
        conn = sqlite3.connect(str(db))
        conn.execute(
            "UPDATE queue_leases SET expires_at=? WHERE queue_id=?",
            (time.time() - 10, "q_test"),
        )
        conn.commit()
        conn.close()
        d = jobs_db.try_acquire_queue_lease(
            "q_test", lease_kind="promote", owner="worker-c", ttl_seconds=30, db_path=db
        )
        assert d["acquired"] is True and d["owner"] == "worker-c", d


def test_gender_not_clobbered():
    tmp = Path(tempfile.mkdtemp(prefix="gender_")).resolve()
    try:
        db = tmp / "people.sqlite"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE people (
              person_id TEXT PRIMARY KEY,
              name TEXT,
              gender TEXT,
              province TEXT,
              city TEXT,
              unit_name TEXT,
              primary_image_path TEXT,
              source_url TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO people VALUES (?,?,?,?,?,?,?,?)",
            ("p1", "张三", "男", "河北省", "沧州市", "市政府", "/runtime/portrait/x.jpg", "http://x"),
        )
        conn.commit()
        # update with empty gender should keep 男
        out = ps.upsert_person(
            conn,
            ctx={
                "province": "河北省",
                "city": "沧州市",
                "unit": "市政府",
                "admin_level": "市级",
                "county": "",
                "site_name": "",
            },
            name="张三",
            gender="",  # blank
            position="市长",
            source_url="http://x",
            list_url="",
            image_url="",
            bio="",
            path=None,
            path_for_db="/runtime/portrait/x.jpg",
            no_photo=False,
            dry_run=False,
            queue_id="q1",
        )
        assert out["action"] == "update"
        row = conn.execute("SELECT gender FROM people WHERE person_id='p1'").fetchone()
        assert row[0] == "男", row
        # explicit 女 may update
        ps.upsert_person(
            conn,
            ctx={
                "province": "河北省",
                "city": "沧州市",
                "unit": "市政府",
                "admin_level": "市级",
                "county": "",
                "site_name": "",
            },
            name="张三",
            gender="女",
            position="",
            source_url="http://x",
            list_url="",
            image_url="",
            bio="",
            path=None,
            path_for_db="/runtime/portrait/x.jpg",
            no_photo=False,
            dry_run=False,
            queue_id="q1",
        )
        row2 = conn.execute("SELECT gender FROM people WHERE person_id='p1'").fetchone()
        assert row2[0] == "女", row2
        conn.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_vision_cost_summary():
    with _jobs_env() as (db, _tmp):
        jobs_db.create_vision_job(
            name="t",
            items=[{"name": "a", "path": "a.jpg"}, {"name": "b", "path": "b.jpg"}],
            province="山东省",
            force=False,
            write_people=False,
            dry_run=True,
            db_path=db,
        )
        # manually bump counts
        conn = sqlite3.connect(str(db))
        conn.execute(
            "UPDATE vision_jobs SET ok_count=1, failed_count=1, status='completed', total=2"
        )
        conn.commit()
        conn.close()
        # point env already set; vision_cost_summary uses default jobs path via env
        summary = vs.vision_cost_summary(limit_jobs=50)
        assert summary["ok"] is True
        assert summary["estimated_model_calls"] >= 2, summary
        assert summary["jobs"] >= 1


def main() -> int:
    test_m9_lease_exclusive_and_expire()
    print("PASS m9 lease")
    test_gender_not_clobbered()
    print("PASS gender guard")
    test_vision_cost_summary()
    print("PASS vision cost")
    print("ALL stage-C leftovers PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
