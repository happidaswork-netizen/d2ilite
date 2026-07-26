# -*- coding: utf-8 -*-
"""Library SQLite index: schema + query + reindex smoke tests."""
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cloud import jobs_db, library_index, queue_service


def _tmp_db() -> Path:
    fd, name = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    path = Path(name)
    jobs_db.init_db(path)
    return path


def test_schema_and_replace_query():
    db = _tmp_db()
    try:
        jobs_db.create_queue_record(
            name="沧州 T0",
            template_id="cangzhou",
            template_path="x.json",
            start_url="https://example.com",
            output_root=str(Path(tempfile.mkdtemp())),
            queue_id="q_testindex01",
            db_path=db,
        )
        rows = [
            {
                "library_id": "q_testindex01:done-1",
                "queue_id": "q_testindex01",
                "queue_name": "沧州 T0",
                "item_id": "done-1",
                "name": "姜桂海",
                "detail_url": "https://example.com/a",
                "image_path": "/tmp/a.jpg",
                "bucket": "done",
                "status": "completed",
                "reason": "",
                "has_preview": 1,
                "output_root": "/tmp",
                "indexed_at": time.time(),
            },
            {
                "library_id": "q_testindex01:pending-2",
                "queue_id": "q_testindex01",
                "queue_name": "沧州 T0",
                "item_id": "pending-2",
                "name": "王少杰",
                "detail_url": "https://example.com/b",
                "image_path": "",
                "bucket": "pending",
                "status": "pending",
                "reason": "无图",
                "has_preview": 0,
                "output_root": "/tmp",
                "indexed_at": time.time(),
            },
        ]
        n = jobs_db.replace_library_items_for_queue("q_testindex01", rows, db_path=db)
        assert n == 2, n
        stats = jobs_db.library_items_stats(db_path=db)
        assert stats["total"] == 2
        assert stats["previewable"] == 1
        assert stats["queues"] == 1

        page = jobs_db.query_library_items(limit=10, offset=0, db_path=db)
        assert page["total"] == 2
        assert page["previewable"] == 1
        # has_preview first
        assert page["items"][0]["name"] == "姜桂海"

        only_pending = jobs_db.query_library_items(status="pending", db_path=db)
        assert only_pending["total"] == 1
        assert only_pending["items"][0]["name"] == "王少杰"

        search = jobs_db.query_library_items(q="姜", db_path=db)
        assert search["total"] == 1
        assert search["items"][0]["name"] == "姜桂海"

        summaries = jobs_db.library_queue_summaries(db_path=db)
        assert len(summaries) == 1
        assert summaries[0]["id"] == "q_testindex01"
        assert summaries[0]["total_items"] == 2

        # replace is atomic — second write drops old rows
        n2 = jobs_db.replace_library_items_for_queue(
            "q_testindex01",
            [rows[0]],
            db_path=db,
        )
        assert n2 == 1
        assert jobs_db.library_items_stats(db_path=db)["total"] == 1
        print("PASS test_schema_and_replace_query")
    finally:
        try:
            db.unlink()
        except OSError:
            pass


def test_library_list_prefers_index(monkeypatch_rows=True):
    db = _tmp_db()
    # Point jobs_db default path via env so library_list_from_index uses it.
    old = os.environ.get("D2I_CLOUD_JOBS_DB")
    os.environ["D2I_CLOUD_JOBS_DB"] = str(db)
    try:
        jobs_db.init_db(db)
        jobs_db.create_queue_record(
            name="索引队列",
            template_id="t",
            template_path="t.json",
            start_url="https://example.com",
            output_root=str(Path(tempfile.mkdtemp())),
            queue_id="q_prefindex01",
            db_path=db,
        )
        jobs_db.replace_library_items_for_queue(
            "q_prefindex01",
            [
                {
                    "library_id": "q_prefindex01:done-1",
                    "queue_id": "q_prefindex01",
                    "queue_name": "索引队列",
                    "item_id": "done-1",
                    "name": "刘勇",
                    "detail_url": "https://example.com/c",
                    "image_path": "/tmp/c.jpg",
                    "bucket": "done",
                    "status": "completed",
                    "reason": "",
                    "has_preview": 1,
                    "output_root": "/tmp",
                    "indexed_at": time.time(),
                }
            ],
            db_path=db,
        )
        # library_list_from_index uses jobs_db_path() which reads env
        indexed = library_index.library_list_from_index(limit=10, offset=0)
        assert indexed is not None, "expected index hit"
        assert indexed.get("source") == "index"
        assert indexed["total"] == 1
        assert indexed["items"][0]["name"] == "刘勇"
        assert indexed["items"][0]["preview_url"].endswith("/preview")
        assert indexed["truncated"] is False
        print("PASS test_library_list_prefers_index")
    finally:
        if old is None:
            os.environ.pop("D2I_CLOUD_JOBS_DB", None)
        else:
            os.environ["D2I_CLOUD_JOBS_DB"] = old
        try:
            db.unlink()
        except OSError:
            pass


def test_empty_index_returns_none():
    db = _tmp_db()
    old = os.environ.get("D2I_CLOUD_JOBS_DB")
    os.environ["D2I_CLOUD_JOBS_DB"] = str(db)
    try:
        jobs_db.init_db(db)
        assert library_index.library_list_from_index() is None
        print("PASS test_empty_index_returns_none")
    finally:
        if old is None:
            os.environ.pop("D2I_CLOUD_JOBS_DB", None)
        else:
            os.environ["D2I_CLOUD_JOBS_DB"] = old
        try:
            db.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    test_schema_and_replace_query()
    test_library_list_prefers_index()
    test_empty_index_returns_none()
    print("ALL library index tests PASS")
