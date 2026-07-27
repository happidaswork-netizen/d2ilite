# -*- coding: utf-8 -*-
"""Sprint A (gap P0): inbox lifecycle + rebind-primary + per-person vision.

Pure python asserts; no Grok. Temp cloud data + people sqlite + image fixtures.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cloud import jobs_db  # noqa: E402
from cloud import people_workflow as pw  # noqa: E402
from cloud import vision_service as vs  # noqa: E402


@contextmanager
def _env():
    tmp = Path(tempfile.mkdtemp(prefix="sprint_a_")).resolve()
    data = tmp / "cloud_data"
    data.mkdir(parents=True)
    portrait = tmp / "portrait"
    portrait.mkdir()
    people = tmp / "people.sqlite"
    jobs = data / "jobs.sqlite"

    conn = sqlite3.connect(str(people))
    conn.execute(
        """
        CREATE TABLE people (
            person_id TEXT PRIMARY KEY,
            name TEXT,
            gender TEXT,
            unit_name TEXT,
            province TEXT,
            city TEXT,
            primary_image_path TEXT,
            source_url TEXT,
            image_status TEXT,
            source_page_image_status TEXT,
            source_image_status TEXT,
            file_status TEXT,
            repair_status TEXT,
            visual_gender TEXT,
            has_official_photo INTEGER,
            notes TEXT
        )
        """
    )
    conn.commit()
    conn.close()

    old = {
        "D2I_CLOUD_DATA_ROOT": os.environ.get("D2I_CLOUD_DATA_ROOT"),
        "D2I_CLOUD_JOBS_DB": os.environ.get("D2I_CLOUD_JOBS_DB"),
        "D2I_PEOPLE_DB": os.environ.get("D2I_PEOPLE_DB"),
        "D2I_PORTRAIT_ROOT": os.environ.get("D2I_PORTRAIT_ROOT"),
    }
    os.environ["D2I_CLOUD_DATA_ROOT"] = str(data)
    os.environ["D2I_CLOUD_JOBS_DB"] = str(jobs)
    os.environ["D2I_PEOPLE_DB"] = str(people)
    os.environ["D2I_PORTRAIT_ROOT"] = str(portrait)
    jobs_db.init_db()
    try:
        yield {
            "tmp": tmp,
            "data": data,
            "portrait": portrait,
            "people": people,
            "jobs": jobs,
        }
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(tmp, ignore_errors=True)


def _put_person(people: Path, pid: str, name: str, path: str = "", **extra) -> None:
    conn = sqlite3.connect(str(people))
    cols = {
        "person_id": pid,
        "name": name,
        "primary_image_path": path,
        "province": extra.get("province", "山东省"),
        "city": extra.get("city", "临沂市"),
        "unit_name": extra.get("unit_name", "测试单位"),
        "image_status": extra.get("image_status", "has_photo"),
        "source_page_image_status": extra.get("source_page_image_status", "has_photo"),
        "file_status": extra.get("file_status", "available"),
    }
    conn.execute(
        f"INSERT OR REPLACE INTO people ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
        tuple(cols.values()),
    )
    conn.commit()
    conn.close()


def _make_image(path: Path, size: tuple[int, int] = (120, 160)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=(40, 90, 160)).save(path, format="JPEG", quality=90)
    return path


def test_p0_1_inbox_lifecycle_mark_resolves():
    with _env() as ctx:
        # seed inbox jsonl
        day = "20260727"
        inbox_dir = vs.vision_followup_inbox_dir()
        path = inbox_dir / f"recrawl_{day}.jsonl"
        rows = [
            {
                "at": "2026-07-27",
                "source_job": "vj_x",
                "person_id": "p_a",
                "name": "甲",
                "path": "/runtime/portrait/a.jpg",
                "error_code": "image_too_small",
                "error_bucket": "must_recrawl",
            },
            {
                "at": "2026-07-27",
                "source_job": "vj_x",
                "person_id": "p_b",
                "name": "乙",
                "path": "/runtime/portrait/b.jpg",
                "error_code": "image_truncated",
                "error_bucket": "must_recrawl",
            },
        ]
        path.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
            encoding="utf-8",
        )
        _put_person(ctx["people"], "p_a", "甲")
        _put_person(ctx["people"], "p_b", "乙")

        open0 = vs.list_recrawl_inbox(day=day, status="open")
        assert open0["total"] == 2, open0

        out = pw.mark_person(
            person_id="p_a",
            action="no_photo",
            reason="test",
            people_db=ctx["people"],
        )
        assert out["ok"] is True
        assert out.get("inbox", {}).get("after", {}).get("status") == "resolved"

        open1 = vs.list_recrawl_inbox(day=day, status="open")
        assert open1["total"] == 1, open1
        assert open1["items"][0]["person_id"] == "p_b"
        assert open1["status_counts"]["resolved"] == 1
        assert open1["status_counts"]["open"] == 1

        all_rows = vs.list_recrawl_inbox(day=day, status="all")
        assert all_rows["total"] == 2

        # dismiss remaining
        vs.set_recrawl_inbox_status(person_id="p_b", status="dismissed", action="dismiss")
        open2 = vs.list_recrawl_inbox(day=day, status="open")
        assert open2["total"] == 0, open2


def test_p0_2_rebind_rejects_tiny_accepts_good():
    with _env() as ctx:
        tiny = _make_image(ctx["portrait"] / "tiny.jpg", (20, 20))
        good = _make_image(ctx["portrait"] / "good.jpg", (200, 260))
        _put_person(
            ctx["people"],
            "p_yxg",
            "于昕光",
            path=str(tiny),
            source_page_image_status="unknown",
        )

        try:
            pw.rebind_primary(person_id="p_yxg", path=str(tiny), people_db=ctx["people"])
            raise AssertionError("expected tiny rebind to fail")
        except ValueError as exc:
            assert "image_too_small" in str(exc) or "too small" in str(exc), exc

        out = pw.rebind_primary(
            person_id="p_yxg",
            path=str(good),
            reason="test rebind",
            people_db=ctx["people"],
        )
        assert out["ok"] is True and out["dry_run"] is False
        assert out["inspect"]["ok"] is True
        person = pw.get_person("p_yxg", people_db=ctx["people"])
        assert person is not None
        assert "good.jpg" in str(person.get("primary_image_path") or "")
        assert person.get("source_page_image_status") == "has_photo"
        # inventory must resolve
        inv = vs.inventory_unvisioned(people_db=ctx["people"], only_resolvable=True)
        ids = {x["person_id"] for x in inv["items"]}
        assert "p_yxg" in ids, inv


def test_p0_3_enqueue_person_ids_no_city_spill():
    with _env() as ctx:
        imgs = []
        for i, name in enumerate(["甲", "乙", "丙", "丁"]):
            p = _make_image(ctx["portrait"] / f"{name}.jpg", (100, 120))
            imgs.append(p)
            _put_person(
                ctx["people"],
                f"p_{i}",
                name,
                path=str(p),
                city="临沂市",
                unit_name="同市单位",
            )
        # only enqueue 2 of 4 same-city people
        out = vs.enqueue_person_ids(
            person_ids=["p_0", "p_2"],
            force=True,
            write_people=False,
            dry_run=True,
            people_db=ctx["people"],
        )
        assert out["ok"] is True, out
        assert out["requested"] == 2
        assert out["resolvable"] == 2
        assert out["total_items"] == 2, out
        assert out["created"] == 1
        job = jobs_db.get_vision_job(out["enqueued"][0]["id"])
        assert int(job["total"]) == 2, job
        items = job.get("items") or []
        # items may be stored as json
        if isinstance(items, str):
            items = json.loads(items)
        pids = {str(it.get("person_id")) for it in items}
        assert pids == {"p_0", "p_2"}, pids
        assert "p_1" not in pids and "p_3" not in pids


def test_p0_3_run_person_vision_resolves_path(monkeypatch=None):
    with _env() as ctx:
        img = _make_image(ctx["portrait"] / "zqh.jpg", (128, 160))
        _put_person(ctx["people"], "p_zqh", "庄启华", path=str(img))

        calls = {"n": 0, "paths": []}

        def fake_classify_and_write_person(**kwargs):
            calls["n"] += 1
            calls["paths"].append(str(kwargs.get("image_path") or ""))
            # prove path was auto-resolved to a real file
            assert Path(kwargs["image_path"]).is_file(), kwargs
            assert kwargs.get("person_id") == "p_zqh"
            return {
                "ok": True,
                "person_id": "p_zqh",
                "name": "庄启华",
                "path": kwargs["image_path"],
                "classification": {
                    "result": {
                        "person_count": 1,
                        "visual_gender": "男",
                        "visual_body_type": "标准",
                        "visual_hairstyle": "短发",
                        "visual_pose": "正面人像",
                    }
                },
                "people": {"person_id": "p_zqh", "action": "update"},
            }

        orig = vs.classify_and_write_person
        vs.classify_and_write_person = fake_classify_and_write_person
        try:
            out = vs.run_person_vision(
                person_id="p_zqh",
                force=True,
                write_people=True,
                people_db=ctx["people"],
            )
        finally:
            vs.classify_and_write_person = orig
        assert out["ok"] is True, out
        assert out["requested"] == 1
        assert out["ok_count"] == 1
        assert calls["n"] == 1
        assert calls["paths"][0]
        assert out["results"][0]["person_id"] == "p_zqh"


def main() -> int:
    test_p0_1_inbox_lifecycle_mark_resolves()
    print("PASS p0-1 inbox lifecycle")
    test_p0_2_rebind_rejects_tiny_accepts_good()
    print("PASS p0-2 rebind")
    test_p0_3_enqueue_person_ids_no_city_spill()
    print("PASS p0-3 enqueue person_ids")
    test_p0_3_run_person_vision_resolves_path()
    print("PASS p0-3 run person vision")
    print("ALL Sprint A PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
