#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Smoke test for D2I Cloud queue API (no real scrape start)."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="d2i-cloud-smoke-"))
    os.environ["D2I_CLOUD_DATA_ROOT"] = str(tmp / "cloud")
    os.environ["D2I_CLOUD_TASKS_ROOT"] = str(tmp / "tasks")
    os.environ.pop("D2I_WEB_TOKEN", None)

    from fastapi.testclient import TestClient

    from cloud.api import create_app
    from cloud.speed_tiers import apply_speed_tier_to_crawl, normalize_speed_tier

    assert normalize_speed_tier("安全") == "safe"
    assert normalize_speed_tier("极速") == "turbo"
    crawl = apply_speed_tier_to_crawl({}, "standard", reason="smoke")
    assert crawl["speed_tier"] == "standard"
    assert crawl["interval_min_seconds"] == 3

    app = create_app()
    client = TestClient(app)

    r = client.get("/health")
    assert r.status_code == 200, r.text
    assert r.json().get("ok") is True

    r = client.get("/")
    assert r.status_code == 200, r.text
    assert "D2I Cloud" in r.text
    assert 'src="/static/app.js"' in r.text

    r = client.get("/static/app.js")
    assert r.status_code == 200, r.text
    assert "d2i_cloud_token" in r.text

    r = client.get("/static/styles.css")
    assert r.status_code == 200, r.text
    assert "--primary" in r.text

    r = client.get("/api/v1/status")
    assert r.status_code == 200, r.text
    status = r.json()
    assert status.get("product") == "d2i-cloud"

    r = client.get("/api/v1/templates")
    assert r.status_code == 200, r.text
    templates = r.json().get("templates") or []
    assert isinstance(templates, list)
    assert len(templates) > 0, "expected at least one scraper template"

    sample = next((t for t in templates if t.get("id")), templates[0])
    start_url = ""
    if sample.get("start_urls"):
        start_url = str(sample["start_urls"][0] or "")
    if not start_url:
        start_url = "https://example.org/list"

    r = client.post(
        "/api/v1/queues",
        json={
            "template_id": sample["id"],
            "template_path": sample.get("path") or "",
            "start_url": start_url,
            "name": "smoke-queue",
            "speed_tier": "safe",
            "speed_tier_reason": "cloud api smoke",
            "start": False,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("ok") is True
    queue = body.get("queue") or {}
    qid = queue.get("id")
    assert qid, body
    assert queue.get("speed_tier") == "safe"
    output_root = Path(str(queue.get("output_root") or ""))
    assert output_root.exists(), output_root
    assert (output_root / "state" / "runtime_config.json").is_file()

    r = client.get("/api/v1/queues")
    assert r.status_code == 200, r.text
    assert r.json().get("count", 0) >= 1

    r = client.get(f"/api/v1/queues/{qid}")
    assert r.status_code == 200, r.text
    assert r.json()["queue"]["id"] == qid

    r = client.get(f"/api/v1/queues/{qid}/items")
    assert r.status_code == 200, r.text

    r = client.get(f"/api/v1/queues/{qid}/logs")
    assert r.status_code == 200, r.text

    # turbo without confirm should 400
    r = client.post(
        "/api/v1/queues",
        json={
            "template_id": sample["id"],
            "start_url": start_url,
            "speed_tier": "turbo",
            "allow_turbo": False,
            "start": False,
        },
    )
    assert r.status_code == 400, r.text

    # Coverage graph C0 (snapshot or people db)
    snap = PROJECT_ROOT / "data" / "coverage" / "rollup_snapshot_20260726.json"
    if snap.is_file():
        os.environ["D2I_COVERAGE_SNAPSHOT"] = str(snap)
        os.environ["D2I_PEOPLE_DB"] = str(tmp / "no-people.sqlite")
        # recreate app so path helpers see env (service caches payload)
        from cloud import coverage_service as cov

        cov._CACHE["payload"] = None
        cov._CACHE["at"] = 0.0

        r = client.get("/coverage")
        assert r.status_code == 200, r.text
        assert "覆盖图谱" in r.text

        r = client.get("/static/coverage.js")
        assert r.status_code == 200, r.text
        assert "coverage/tree" in r.text

        r = client.get("/api/v1/coverage/tree")
        assert r.status_code == 200, r.text
        tree = r.json()
        assert tree.get("ok") is True
        assert int(tree.get("people_total") or 0) > 0
        assert isinstance(tree.get("tree"), list) and len(tree["tree"]) > 0

        r = client.get("/api/v1/coverage/geo-summary")
        assert r.status_code == 200, r.text
        geo = r.json()
        assert geo.get("ok") is True
        assert len(geo.get("cities") or []) > 0
        assert isinstance(geo.get("province_series"), list)
        assert geo.get("geojson", {}).get("china")
        assert (geo.get("city_series_by_province") or {})
        assert geo.get("default_metric") == "strength"
        assert "strength" in (geo.get("metrics") or {})
        # tiny complete sets must not outrank large incomplete purely by rate
        from cloud.coverage_service import _strength_score, _heat

        assert _strength_score(5, 0) < _strength_score(500, 150)
        assert _heat(5, 0) != "green"
        series = geo.get("province_series") or []
        assert series and "metrics" in series[0] and "value" in series[0]
        assert series[0]["value"] == series[0].get("strength")

        r = client.get("/static/geo/china_100000_full.json")
        assert r.status_code == 200, r.text
        china = r.json()
        assert len(china.get("features") or []) >= 30

        r = client.get("/static/geo/shandong_370000_full.json")
        assert r.status_code == 200, r.text
        sd = r.json()
        assert len(sd.get("features") or []) >= 10

        r = client.get("/static/coverage.js")
        assert r.status_code == 200, r.text
        assert "enqueue" in r.text
        assert "mapChart" in r.text

        r = client.get("/static/vendor/echarts.min.js")
        assert r.status_code == 200, r.text
        assert len(r.content) > 100_000

        # first unit node
        unit_id = None
        for domain in tree["tree"]:
            for prov in domain.get("children") or []:
                for city in prov.get("children") or []:
                    for unit in city.get("children") or []:
                        unit_id = unit.get("node_id")
                        break
                    if unit_id:
                        break
                if unit_id:
                    break
            if unit_id:
                break
        assert unit_id, "expected at least one unit node"
        r = client.get(f"/api/v1/coverage/nodes/{unit_id}")
        assert r.status_code == 200, r.text
        node_body = r.json()
        assert node_body.get("ok") is True
        assert node_body.get("node", {}).get("node_id") == unit_id

        r = client.post(f"/api/v1/coverage/nodes/{unit_id}/scout")
        assert r.status_code == 200, r.text
        scout = r.json()
        assert scout.get("status") == "proposal_only"
        assert (scout.get("proposal") or {}).get("requires_human_confirm") is True

        # C2: confirm required
        r = client.post(
            f"/api/v1/coverage/nodes/{unit_id}/enqueue",
            json={"action": "g_index", "confirm": False},
        )
        assert r.status_code == 400, r.text

        r = client.post(
            f"/api/v1/coverage/nodes/{unit_id}/enqueue",
            json={"action": "g_index", "confirm": True, "notes": "smoke g_index"},
        )
        assert r.status_code == 200, r.text
        enq = r.json()
        assert enq.get("ok") is True
        assert enq.get("mode") == "intent_only"
        assert enq.get("action") == "g_index"
        assert (enq.get("intent") or {}).get("node_id") == unit_id

        r = client.post(
            f"/api/v1/coverage/nodes/{unit_id}/enqueue",
            json={"action": "ban", "confirm": True, "notes": "smoke ban"},
        )
        assert r.status_code == 200, r.text
        ban = r.json()
        assert ban.get("mode") == "override"
        assert ban.get("action") == "ban"

        # ban blocks download enqueue
        r = client.post(
            f"/api/v1/coverage/nodes/{unit_id}/enqueue",
            json={
                "action": "g_photo",
                "confirm": True,
                "start_url": "https://example.com/smoke-blocked",
            },
        )
        assert r.status_code == 400, r.text

        r = client.post(
            f"/api/v1/coverage/nodes/{unit_id}/enqueue",
            json={"action": "unban", "confirm": True},
        )
        assert r.status_code == 200, r.text

        # real queue path (not started)
        r = client.post(
            f"/api/v1/coverage/nodes/{unit_id}/enqueue",
            json={
                "action": "g_photo",
                "confirm": True,
                "start_url": "https://example.com/coverage-smoke",
                "template_id": sample["id"],
                "speed_tier": "safe",
                "start": False,
                "notes": "smoke g_photo queue",
            },
        )
        assert r.status_code == 200, r.text
        qbody = r.json()
        assert qbody.get("mode") == "queue"
        assert (qbody.get("queue") or {}).get("id")

    print(
        json.dumps(
            {
                "ok": True,
                "queue_id": qid,
                "template_id": sample["id"],
                "output_root": str(output_root),
                "tmp": str(tmp),
                "coverage_snapshot": str(snap) if snap.is_file() else None,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise
