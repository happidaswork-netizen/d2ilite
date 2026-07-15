#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse


SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "cookies",
    "password",
    "passwd",
    "token",
    "api_key",
    "apikey",
    "proxy_url",
    "subscription",
}


def nonempty(value):
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def walk_sensitive(value, path="$"):
    findings = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key).strip().lower()
            child_path = f"{path}.{key}"
            if key_text in SENSITIVE_KEYS and nonempty(child):
                findings.append(child_path)
            findings.extend(walk_sensitive(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            findings.extend(walk_sensitive(child, f"{path}[{index}]"))
    return findings


def validate(path: Path, *, allow_legacy: bool = False):
    errors = []
    warnings = []
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return {
            "ok": False,
            "path": str(path),
            "sha256": "",
            "errors": [f"invalid_json:{type(exc).__name__}:{exc}"],
            "warnings": [],
        }

    if not isinstance(payload, dict):
        errors.append("root_must_be_object")
        payload = {}

    if not nonempty(payload.get("site_name")):
        errors.append("missing_site_name")
    if not isinstance(payload.get("template_version", 1), int):
        errors.append("template_version_must_be_integer")

    start_urls = payload.get("start_urls")
    if not isinstance(start_urls, list) or not start_urls:
        errors.append("start_urls_must_be_nonempty_list")
        start_urls = []
    for url in start_urls:
        parsed = urlparse(str(url))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append(f"invalid_start_url:{url}")

    domains = payload.get("allowed_domains")
    if not isinstance(domains, list) or not any(str(item).strip() for item in domains):
        errors.append("allowed_domains_must_be_nonempty_list")

    selectors = payload.get("selectors")
    if not isinstance(selectors, dict):
        errors.append("selectors_must_be_object")
        selectors = {}
    for key in ("list_item", "name", "detail_link"):
        if not nonempty(selectors.get(key)):
            errors.append(f"missing_selector:{key}")
    rules_preview = payload.get("rules") if isinstance(payload.get("rules"), dict) else {}
    field_map_preview = rules_preview.get("field_map") if isinstance(rules_preview.get("field_map"), dict) else {}
    list_only_mode = nonempty(field_map_preview.get("image_url"))
    if not list_only_mode:
        for key in ("detail_name", "detail_image"):
            if not nonempty(selectors.get(key)):
                errors.append(f"missing_selector:{key}")
    if not nonempty(selectors.get("next_page")):
        warnings.append("missing_next_page_selector")

    rules = payload.get("rules")
    if not isinstance(rules, dict):
        errors.append("rules_must_be_object")
        rules = {}
    if rules.get("obey_robots_txt", True) is not True:
        issue = "obey_robots_txt_must_be_true"
        if allow_legacy:
            warnings.append("legacy:" + issue)
        else:
            errors.append(issue)
    mode = str(rules.get("image_download_mode") or "requests_jsl").strip().lower()
    if mode not in {"requests_jsl", "browser", "d2i_browser"}:
        errors.append(f"invalid_image_download_mode:{mode}")
    required = rules.get("required_fields")
    if not isinstance(required, list) or not {"name", "detail_url", "image_url"}.issubset(set(required)):
        warnings.append("required_fields_should_include_name_detail_url_image_url")
    if not isinstance(rules.get("field_map"), dict):
        warnings.append("missing_field_map")

    crawl = payload.get("crawl")
    if not isinstance(crawl, dict):
        errors.append("crawl_must_be_object")
        crawl = {}
    concurrency = crawl.get("concurrent_requests", 1)
    try:
        concurrency = int(concurrency)
        if concurrency < 1 or concurrency > 4:
            errors.append("concurrent_requests_must_be_between_1_and_4")
    except Exception:
        errors.append("concurrent_requests_must_be_integer")
    try:
        delay = float(crawl.get("download_delay", 0))
        if delay < 1:
            warnings.append("download_delay_below_1_second")
    except Exception:
        errors.append("download_delay_must_be_number")

    for sensitive_path in walk_sensitive(payload):
        errors.append(f"sensitive_value_forbidden:{sensitive_path}")

    text = raw.decode("utf-8", errors="ignore")
    if re.search(r"(?i)bearer\s+[a-z0-9._~-]{12,}", text):
        errors.append("possible_bearer_token_in_template")

    return {
        "ok": not errors,
        "path": str(path.resolve()),
        "sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "site_name": payload.get("site_name", ""),
        "mode": mode,
        "errors": errors,
        "warnings": warnings,
    }


def main():
    parser = argparse.ArgumentParser(description="Validate a D2I Lite public scraper template.")
    parser.add_argument("template")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--allow-legacy", action="store_true")
    args = parser.parse_args()
    result = validate(Path(args.template), allow_legacy=args.allow_legacy)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"template={result['path']}")
        print(f"sha256={result['sha256']}")
        print(f"ok={str(result['ok']).lower()}")
        for item in result["errors"]:
            print(f"ERROR {item}")
        for item in result["warnings"]:
            print(f"WARN {item}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
