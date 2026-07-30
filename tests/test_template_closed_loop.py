# -*- coding: utf-8 -*-
"""Focused smoke for the lightweight template closed loop."""

import atexit
import json
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_TMP = tempfile.mkdtemp(prefix="d2i_template_loop_")
os.environ["D2I_CLOUD_DATA_ROOT"] = os.path.join(_TMP, "cloud")
os.environ["D2I_CLOUD_JOBS_DB"] = os.path.join(_TMP, "jobs.sqlite")
os.environ["D2I_CLOUD_TASKS_ROOT"] = os.path.join(_TMP, "tasks")
os.environ["D2I_SCRAPER_REGISTRY_PATH"] = os.path.join(_TMP, "registry.json")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)

from cloud import api, template_service  # noqa: E402
from cloud.cli import build_parser  # noqa: E402


def _template():
    return {
        "site_name": "closed_loop_test",
        "start_urls": ["https://people.example.invalid/list"],
        "allowed_domains": ["people.example.invalid"],
        "selectors": {
            "list_item": "xpath://ul/li/a",
            "name": ["xpath:normalize-space(string(.))"],
            "detail_link": ["xpath:./@href"],
            "next_page": [],
            "detail_name": ["xpath://h1/text()"],
            "detail_image": ["xpath://main//img/@src"],
        },
        "rules": {
            "required_fields": ["name", "detail_url"],
            "field_map": {"person": ["name"], "image_url": ["image_url"]},
            "image_download_mode": "requests_jsl",
        },
        "crawl": {
            "speed_tier": "safe",
            "speed_tier_reason": "test",
            "concurrent_requests": 1,
        },
    }


def test_current_contract_allows_no_photo_rows():
    result = template_service.validate_template(_template())
    assert result["ok"], result
    broken = json.loads(json.dumps(_template()))
    broken["rules"]["required_fields"].append("image_url")
    result = template_service.validate_template(broken)
    assert not result["ok"]
    assert any("image_url_must_not_be_required" in item for item in result["errors"])


def test_import_search_show_and_outcome():
    imported = template_service.import_template(
        _template(),
        template_id="闭环测试模板",
        notes="smoke only",
    )
    assert imported["ok"]
    assert imported["id"] == "闭环测试模板"
    shown = template_service.get_template("闭环测试模板")
    assert shown["source"] == "runtime"
    assert shown["notes"] == "smoke only"
    found = template_service.search_templates(url="https://people.example.invalid/list")
    assert found["templates"][0]["id"] == "闭环测试模板"
    outcome = template_service.record_outcome(
        "闭环测试模板",
        {"stats": {"completed": 3}, "recommended": True},
    )
    assert outcome["outcome_count"] == 1
    assert template_service.get_template("闭环测试模板")["outcomes"][0]["stats"]["completed"] == 3


def test_api_routes_and_cli_contract():
    app = api.create_app()
    routes = {
        (route.path, method)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }
    for expected in {
        ("/api/v1/templates/search", "GET"),
        ("/api/v1/templates/{template_id}", "GET"),
        ("/api/v1/templates/validate", "POST"),
        ("/api/v1/templates", "POST"),
        ("/api/v1/templates/{template_id}", "PATCH"),
        ("/api/v1/templates/{template_id}/outcomes", "POST"),
        ("/api/v1/templates/{template_id}/queues", "POST"),
    }:
        assert expected in routes, expected

    parser = build_parser()
    queue_args = parser.parse_args(["queues", "create", "--template", "demo"])
    assert queue_args.speed_tier == ""
    for argv in (
        ["templates", "search", "--url", "https://example.invalid"],
        ["templates", "show", "demo"],
        ["templates", "validate", "demo.json"],
        ["templates", "import", "demo.json"],
        ["templates", "outcome", "demo"],
    ):
        assert callable(parser.parse_args(argv).func)


if __name__ == "__main__":
    for fn in (
        test_current_contract_allows_no_photo_rows,
        test_import_search_show_and_outcome,
        test_api_routes_and_cli_contract,
    ):
        fn()
        print(f"PASS {fn.__name__}")
