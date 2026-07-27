# -*- coding: utf-8 -*-
"""Sprint B/C gap P0 leftovers: source probe, path/bucket, promote QA, search."""

from __future__ import annotations

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

from cloud import people_workflow as pw  # noqa: E402
from cloud import promote_service as ps  # noqa: E402
from cloud import source_probe  # noqa: E402


@contextmanager
def _env():
    tmp = Path(tempfile.mkdtemp(prefix="sprint_bc_")).resolve()
    data = tmp / "cloud_data"
    data.mkdir()
    portrait = tmp / "portrait"
    portrait.mkdir()
    people = tmp / "people.sqlite"
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
    old = {k: os.environ.get(k) for k in (
        "D2I_CLOUD_DATA_ROOT", "D2I_PEOPLE_DB", "D2I_PORTRAIT_ROOT", "D2I_CLOUD_JOBS_DB"
    )}
    os.environ["D2I_CLOUD_DATA_ROOT"] = str(data)
    os.environ["D2I_PEOPLE_DB"] = str(people)
    os.environ["D2I_PORTRAIT_ROOT"] = str(portrait)
    os.environ["D2I_CLOUD_JOBS_DB"] = str(data / "jobs.sqlite")
    try:
        yield {"tmp": tmp, "people": people, "portrait": portrait}
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(tmp, ignore_errors=True)


def _img(path: Path, size=(120, 160)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, (10, 20, 30)).save(path, format="JPEG")
    return path


def test_p0_4_probe_classifier():
    r = source_probe.classify_page(
        url="http://x/i",
        status=200,
        final_url="http://x/i",
        text='<html><title>在线访谈</title><img src="/images/pub-user-portrait.png" width="20" height="20"/></html>',
    )
    assert r["conclusion"] == "placeholder_only", r
    assert r["suggest_action"] == "no_photo", r

    r2 = source_probe.classify_page(
        url="http://x/p",
        status=200,
        final_url="http://x/p",
        text='<html><title>领导简历</title><img class="leader-photo" src="/u/a.jpg" width="200" height="260" alt="张三"/></html>',
    )
    assert r2["conclusion"] == "has_single_portrait", r2

    r3 = source_probe.classify_page(url="http://x", status=404, final_url="http://x", text="")
    assert r3["conclusion"] == "http_404" and r3["suggest_action"] == "no_photo"

    r4 = source_probe.classify_page(
        url="http://x/login",
        status=200,
        final_url="http://x/cas/login",
        text="<html>请先登录 统一身份认证 password</html>",
    )
    assert r4["conclusion"] == "login_wall", r4
    assert r4["suggest_action"] == "blocked"


def test_p1_1_path_info_and_bucket():
    with _env() as ctx:
        good = _img(ctx["portrait"] / "a.jpg")
        conn = sqlite3.connect(str(ctx["people"]))
        conn.execute(
            "INSERT INTO people(person_id,name,primary_image_path,notes,unit_name,province,city) VALUES (?,?,?,?,?,?,?)",
            ("p1", "甲", str(good), "queue_id=q_abc123 promoted_at=x", "单位", "山东省", "临沂市"),
        )
        conn.execute(
            "INSERT INTO people(person_id,name,primary_image_path,unit_name,province,city) VALUES (?,?,?,?,?,?)",
            ("p2", "乙", "/data/photos/originals/x/y.jpg", "旧单位", "山东省", "临沂市"),
        )
        conn.commit()
        conn.close()
        a = pw.get_person("p1", people_db=ctx["people"])
        assert a["path_info"]["on_disk"] is True, a["path_info"]
        assert a["path_info"]["min_edge"] >= 40
        assert a["source_bucket"] == "cloud_queue", a
        b = pw.get_person("p2", people_db=ctx["people"])
        assert b["source_bucket"] == "legacy_import", b
        assert b["path_info"]["on_disk"] is False


def test_p1_4_promote_qa():
    with _env() as ctx:
        tiny = _img(ctx["portrait"] / "pub-user-portrait-copy.jpg", (20, 20))
        # placeholder marker in name
        qa = ps.inspect_promote_image(tiny)
        assert qa["ok"] is False
        assert qa["error_code"] in {"placeholder_avatar", "image_too_small"}, qa

        good = _img(ctx["portrait"] / "leader.jpg", (180, 220))
        qa2 = ps.inspect_promote_image(good)
        assert qa2["ok"] is True, qa2


def test_p1_5_search():
    with _env() as ctx:
        conn = sqlite3.connect(str(ctx["people"]))
        for i, name in enumerate(["张三", "张三", "李四"]):
            conn.execute(
                "INSERT INTO people(person_id,name,unit_name,province,city,primary_image_path) VALUES (?,?,?,?,?,?)",
                (f"p{i}", name, "要闻动态导航" if i == 0 else "气象局", "山东省", "临沂市", ""),
            )
        conn.commit()
        conn.close()
        out = pw.search_people(q="张", people_db=ctx["people"], limit=20)
        assert out["ok"] and out["total"] >= 2, out
        assert any(c["name"] == "张三" for c in out["name_collisions"]), out
        assert any(it.get("dirty_unit") for it in out["items"]), out


def main() -> int:
    test_p0_4_probe_classifier()
    print("PASS p0-4 probe classifier")
    test_p1_1_path_info_and_bucket()
    print("PASS p1-1 path/bucket")
    test_p1_4_promote_qa()
    print("PASS p1-4 promote QA")
    test_p1_5_search()
    print("PASS p1-5 search")
    print("ALL Sprint B/C PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
