# -*- coding: utf-8 -*-
"""Public scraper configuration helpers (UI-independent)."""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from services.metadata_service import normalize_http_url
from services.task_service import suggest_public_scraper_output_root

VALID_IMAGE_DOWNLOAD_MODES = {"requests_jsl", "browser"}
DEFAULT_TEMPLATE_HINT = "未选择模板时，需手动输入链接。"


def _json_clone(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    return json.loads(json.dumps(data, ensure_ascii=False))


def default_public_scraper_template() -> Dict[str, Any]:
    return {
        "site_name": "generic_profiles",
        "start_urls": ["https://example.org/list"],
        "allowed_domains": ["example.org"],
        "user_agent": "D2ILiteArchiveBot/1.0 (+local archival use)",
        "default_headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        },
        "image_headers": {},
        "output_root": "data/public_archive/generic_profiles",
        "selectors": {
            "list_item": "article a[href], .list a[href], .news a[href], .item a[href], li a[href], dl dd a[href], a[href]",
            "name": ["::text", "img::attr(alt)", "img::attr(title)"],
            "detail_link": "::attr(href)",
            "list_fields": {},
            "next_page": [
                "a.next::attr(href)",
                "a[rel='next']::attr(href)",
                "xpath://a[contains(@class,'next')]/@href",
                "xpath://a[contains(normalize-space(),'下一页')]/@href",
                "xpath://a[contains(normalize-space(),'下页')]/@href",
            ],
            "detail_name": [
                "h1::text",
                "h2::text",
                ".title::text",
                ".name::text",
                "meta[property='og:title']::attr(content)",
                "title::text",
            ],
            "detail_image": [
                "meta[property='og:image']::attr(content)",
                ".article img::attr(src)",
                ".content img::attr(src)",
                ".detail img::attr(src)",
                ".main img::attr(src)",
                "img::attr(src)",
            ],
            "detail_gender": [
                ".gender::text",
                "xpath:string(//*[contains(normalize-space(),'性别')][1])",
            ],
            "detail_summary": [
                ".article p::text",
                ".content p::text",
                ".detail p::text",
                ".main p::text",
                "article p::text",
                "p::text",
            ],
            "detail_full_text": [],
            "detail_fields": {},
            "detail_field_labels": {},
        },
        "rules": {
            "obey_robots_txt": False,
            "snapshot_html": True,
            "extract_images": True,
            "write_metadata": True,
            "retry_failed_first": True,
            "metadata_write_retries": 3,
            "metadata_write_retry_delay_seconds": 1.2,
            "metadata_write_retry_backoff_factor": 1.5,
            "named_images_dir": "",
            "image_referer_from_detail_url": True,
            "required_fields": ["name", "detail_url", "image_url"],
            "default_gender": "",
            "gender_map": {"男": "male", "女": "female"},
            "field_map": {},
            "detail_field_labels": {},
            "auto_unit_subdir": False,
            "unit_name": "",
            "output_subdir_pattern": "{unit}{year_suffix}",
            "year_hint": "",
            "jsl_clearance_enabled": True,
            "jsl_max_retries": 3,
            "image_download_mode": "requests_jsl",
            "auto_fallback_to_browser": True,
            "disable_page_images_during_crawl": True,
            "browser_engine": "edge",
            "llm_enrich_enabled": False,
            "llm_api_base": "http://127.0.0.1:11434/v1",
            "llm_api_key": "",
            "llm_model": "qwen2.5:7b-instruct",
            "llm_timeout_seconds": 45,
            "llm_max_retries": 2,
            "llm_temperature": 0.1,
            "llm_only_when_missing_fields": True,
            "llm_generate_biography": True,
            "llm_append_biography_to_description": True,
            "llm_cache_enabled": True,
            "llm_max_input_chars": 6000,
            "output_mode": "images_only_with_record",
            "keep_record_file": True,
        },
        "crawl": {
            "concurrent_requests": 1,
            "download_delay": 5,
            "autothrottle_start_delay": 5,
            "autothrottle_max_delay": 8,
            "retry_times": 3,
            "timeout_seconds": 30,
            "blocked_statuses": [403, 429],
            "blocked_backoff_hours": 6,
            "suspect_block_consecutive_failures": 3,
            "interval_min_seconds": 5,
            "interval_max_seconds": 8,
            "image_interval_min_seconds": 5,
            "image_interval_max_seconds": 8,
        },
    }


def guess_public_site_name(start_url: Any) -> str:
    parsed = urllib.parse.urlparse(str(start_url or "").strip())
    host = (parsed.hostname or "site").strip().lower()
    first_path = parsed.path.strip("/").split("/", 1)[0].strip().lower()
    seed = f"{host}_{first_path or 'index'}"
    normalized = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    if normalized:
        return normalized
    return f"site_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def sanitize_public_subdir_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = re.sub(r"\s+", " ", text).strip().strip(". ")
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def extract_public_year_token(*texts: Any) -> str:
    for raw in texts:
        text = str(raw or "")
        match = re.search(r"(?<!\d)(19|20)\d{2}(?!\d)", text)
        if match:
            return match.group(0)
    return ""


def guess_public_unit_name(start_url: Any, payload: Optional[Dict[str, Any]], template_path: str = "") -> str:
    data = payload if isinstance(payload, dict) else {}
    rules = data.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    for key in ("unit_name", "organization_name", "org_name", "unit"):
        candidate = str(rules.get(key, "")).strip()
        if candidate:
            return candidate

    host = (urllib.parse.urlparse(str(start_url or "")).hostname or "").strip().lower()
    if "tiantonglaw.com" in host:
        return "天同律师事务所"
    if host.endswith("mps.gov.cn") or ("mps.gov.cn" in host):
        return "公安部"

    site_name = str(data.get("site_name", "")).strip()
    if site_name:
        cleaned = re.sub(r"[_\-]+", " ", site_name).strip()
        if cleaned:
            return cleaned

    template_name = os.path.splitext(os.path.basename(str(template_path or "").strip()))[0]
    if template_name:
        return template_name

    if host:
        parts = [item for item in host.split(".") if item]
        if len(parts) >= 2:
            return parts[-2]
        return host
    return "单位"


def default_public_scraper_output_root(app_file: str, start_url: Any) -> str:
    return suggest_public_scraper_output_root(app_file, guess_public_site_name(start_url))


def normalize_public_scraper_template_output_root(path_text: Any, app_file: str) -> str:
    raw = str(path_text or "").strip()
    if not raw:
        return ""
    path_obj = Path(raw)
    if not path_obj.is_absolute():
        path_obj = (Path(os.path.dirname(os.path.abspath(str(app_file or "")))) / path_obj).resolve()
    return str(path_obj.resolve())


def resolve_public_task_output_root(
    base_output_root: Any,
    start_url: Any,
    payload: Optional[Dict[str, Any]],
    *,
    template_path: str = "",
    fallback_output_root: str = "",
) -> Tuple[str, Dict[str, Any]]:
    data = _json_clone(payload)
    base_root = os.path.abspath(
        str(base_output_root or "").strip() or str(fallback_output_root or "").strip() or os.getcwd()
    )
    rules = data.get("rules")
    if not isinstance(rules, dict):
        rules = {}
        data["rules"] = rules

    auto_unit_subdir = bool(rules.get("auto_unit_subdir", False))
    if not auto_unit_subdir:
        rules.pop("output_root_parent", None)
        rules.pop("resolved_output_subdir", None)
        rules.pop("resolved_unit_name", None)
        rules.pop("resolved_year", None)
        return base_root, data

    unit_name = sanitize_public_subdir_name(
        str(guess_public_unit_name(start_url, data, template_path) or "")
    )
    site_name = sanitize_public_subdir_name(str(data.get("site_name", "") or ""))
    host = sanitize_public_subdir_name(
        str((urllib.parse.urlparse(str(start_url or "")).hostname or "").strip().lower())
    )
    year = sanitize_public_subdir_name(
        str(
            rules.get("year_hint")
            or extract_public_year_token(
                start_url,
                data.get("site_name", ""),
                os.path.basename(str(template_path or "")),
            )
        )
    )
    year_suffix = f"_{year}" if year else ""

    pattern = str(rules.get("output_subdir_pattern", "{unit}{year_suffix}") or "").strip()
    if not pattern:
        pattern = "{unit}{year_suffix}"

    format_ctx = {
        "unit": unit_name,
        "year": year,
        "year_suffix": year_suffix,
        "site_name": site_name,
        "host": host,
    }

    class _SafeDict(dict):
        def __missing__(self, key: str) -> str:
            return ""

    try:
        subdir_raw = pattern.format_map(_SafeDict(format_ctx))
    except Exception:
        subdir_raw = f"{unit_name}{year_suffix}".strip()
    subdir_name = sanitize_public_subdir_name(subdir_raw)
    if not subdir_name:
        subdir_name = sanitize_public_subdir_name(unit_name or site_name or host)
    if not subdir_name:
        rules.pop("output_root_parent", None)
        rules.pop("resolved_output_subdir", None)
        rules.pop("resolved_unit_name", None)
        rules.pop("resolved_year", None)
        return base_root, data

    resolved_root = os.path.abspath(os.path.join(base_root, subdir_name))
    rules["output_root_parent"] = base_root
    rules["resolved_output_subdir"] = subdir_name
    rules["resolved_unit_name"] = unit_name or site_name or host
    if year:
        rules["resolved_year"] = year
    else:
        rules.pop("resolved_year", None)
    return resolved_root, data


def build_public_scraper_runtime_config(
    app_file: str,
    start_url: str,
    output_root: str,
    template_path: str = "",
) -> Tuple[str, Dict[str, Any]]:
    app_dir = os.path.dirname(os.path.abspath(str(app_file or "")))
    template_candidates = [
        template_path,
        os.path.join(app_dir, "scraper", "config.template.generic.json"),
        os.path.join(app_dir, "scraper", "config.example.json"),
    ]
    payload: Dict[str, Any] = {}
    loaded_template_path = ""
    for candidate in template_candidates:
        if not os.path.exists(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                payload = loaded
                loaded_template_path = os.path.abspath(candidate)
                break
        except Exception:
            continue
    if not payload:
        payload = default_public_scraper_template()

    payload = _json_clone(payload)

    parsed = urllib.parse.urlparse(start_url)
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise ValueError("无效链接：无法解析域名")

    custom_template_path = os.path.abspath(template_path) if template_path else ""
    using_custom_template = bool(
        custom_template_path and loaded_template_path and (custom_template_path == loaded_template_path)
    )

    template_start_urls_raw = payload.get("start_urls")
    template_start_urls: List[str] = []
    if isinstance(template_start_urls_raw, list):
        for item in template_start_urls_raw:
            value = str(item or "").strip()
            if value:
                template_start_urls.append(value)
    elif isinstance(template_start_urls_raw, str):
        value = str(template_start_urls_raw).strip()
        if value:
            template_start_urls.append(value)

    runtime_start_urls: List[str] = []
    if using_custom_template and template_start_urls:
        for item in template_start_urls:
            if item not in runtime_start_urls:
                runtime_start_urls.append(item)
        if start_url not in runtime_start_urls:
            runtime_start_urls.insert(0, start_url)
    else:
        runtime_start_urls = [start_url]

    allowed_domains: List[str] = []
    for seed_url in runtime_start_urls:
        seed_host = (urllib.parse.urlparse(seed_url).hostname or "").strip().lower()
        if not seed_host:
            continue
        for domain in [seed_host, seed_host[4:] if seed_host.startswith("www.") else f"www.{seed_host}"]:
            normalized = str(domain or "").strip().lower()
            if normalized and (normalized not in allowed_domains):
                allowed_domains.append(normalized)

    template_allowed_domains_raw = payload.get("allowed_domains")
    if using_custom_template and isinstance(template_allowed_domains_raw, list):
        for item in template_allowed_domains_raw:
            domain = str(item or "").strip().lower()
            if domain and (domain not in allowed_domains):
                allowed_domains.append(domain)

    site_name = str(payload.get("site_name", "")).strip() if using_custom_template else ""
    if not site_name:
        site_name = guess_public_site_name(start_url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"
    base_output_root = os.path.abspath(
        str(output_root or "").strip() or default_public_scraper_output_root(app_file, start_url)
    )

    payload["site_name"] = site_name
    payload["start_urls"] = runtime_start_urls
    payload["allowed_domains"] = allowed_domains
    payload["output_root"] = base_output_root

    default_headers = payload.get("default_headers")
    if not isinstance(default_headers, dict):
        default_headers = {}
    if not str(default_headers.get("Referer", "")).strip():
        default_headers["Referer"] = referer
    payload["default_headers"] = default_headers

    image_headers = payload.get("image_headers")
    if not isinstance(image_headers, dict):
        image_headers = {}
    if not str(image_headers.get("Referer", "")).strip():
        image_headers["Referer"] = referer
    payload["image_headers"] = image_headers

    defaults = default_public_scraper_template()

    selectors = payload.get("selectors")
    if not isinstance(selectors, dict):
        selectors = {}
    for key, value in defaults["selectors"].items():
        if key not in selectors:
            selectors[key] = value
    payload["selectors"] = selectors

    rules = payload.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    for key, value in defaults["rules"].items():
        if key not in rules:
            rules[key] = value
    rules["named_images_dir"] = ""
    rules["final_output_root"] = ""
    rules["record_root"] = ""
    rules["default_gender"] = ""
    rules["template_source_path"] = os.path.abspath(template_path) if template_path else ""
    payload["rules"] = rules

    resolved_output_root, payload = resolve_public_task_output_root(
        base_output_root,
        start_url,
        payload,
        template_path=template_path,
        fallback_output_root=default_public_scraper_output_root(app_file, start_url),
    )
    payload["output_root"] = resolved_output_root

    crawl = payload.get("crawl")
    if not isinstance(crawl, dict):
        crawl = {}
    for key, value in defaults["crawl"].items():
        if key not in crawl:
            crawl[key] = value
    payload["crawl"] = crawl

    runtime_config_path = os.path.join(resolved_output_root, "state", "runtime_config.json")
    os.makedirs(os.path.dirname(runtime_config_path), exist_ok=True)
    with open(runtime_config_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return runtime_config_path, payload


def build_public_scraper_setup_initial_state(
    source_hint: str,
    app_file: str,
    *,
    global_llm_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    defaults = default_public_scraper_template()
    crawl_defaults = defaults.get("crawl", {})
    rules_defaults = defaults.get("rules", {})
    global_llm = global_llm_settings if isinstance(global_llm_settings, dict) else {}

    initial_url = str(source_hint or "").strip() or "https://"
    initial_output = default_public_scraper_output_root(app_file, initial_url)

    interval_min_default = crawl_defaults.get(
        "interval_min_seconds",
        crawl_defaults.get("image_interval_min_seconds", crawl_defaults.get("download_delay", 5)),
    )
    interval_max_default = crawl_defaults.get(
        "interval_max_seconds",
        crawl_defaults.get("image_interval_max_seconds", max(float(interval_min_default), 8.0)),
    )
    try:
        interval_min_default = float(interval_min_default)
    except Exception:
        interval_min_default = 5.0
    try:
        interval_max_default = float(interval_max_default)
    except Exception:
        interval_max_default = max(interval_min_default, 8.0)
    if interval_max_default < interval_min_default:
        interval_max_default = interval_min_default

    suspect_failures_default = crawl_defaults.get("suspect_block_consecutive_failures", 3)
    try:
        suspect_failures_default = int(suspect_failures_default)
    except Exception:
        suspect_failures_default = 3
    if suspect_failures_default < 2:
        suspect_failures_default = 2

    global_llm_enabled = bool(global_llm.get("enabled_default", False))
    global_llm_model = str(global_llm.get("model", "")).strip()
    global_llm_api_base = str(global_llm.get("api_base", "")).strip()
    global_llm_api_key = str(global_llm.get("api_key", "")).strip()
    image_mode = str(rules_defaults.get("image_download_mode", "requests_jsl")).strip().lower()
    if image_mode not in VALID_IMAGE_DOWNLOAD_MODES:
        image_mode = "requests_jsl"

    return {
        "start_url": initial_url,
        "output_root": initial_output,
        "interval_min": str(interval_min_default),
        "interval_max": str(interval_max_default),
        "timeout_seconds": str(crawl_defaults.get("timeout_seconds", 30)),
        "suspect_block_consecutive_failures": str(suspect_failures_default),
        "jsl_enabled": bool(rules_defaults.get("jsl_clearance_enabled", True)),
        "image_download_mode": image_mode,
        "auto_fallback_to_browser": bool(rules_defaults.get("auto_fallback_to_browser", True)),
        "disable_page_images_during_crawl": bool(rules_defaults.get("disable_page_images_during_crawl", True)),
        "output_minimal": str(rules_defaults.get("output_mode", "images_only_with_record")).strip().lower()
        in {"images_only", "images_only_with_record"},
        "direct_write_images": bool(rules_defaults.get("direct_write_images", True)),
        "llm_enrich_enabled": (
            global_llm_enabled
            if (global_llm_api_base or global_llm_model)
            else bool(rules_defaults.get("llm_enrich_enabled", False))
        ),
        "llm_model": global_llm_model or str(rules_defaults.get("llm_model", "qwen2.5:7b-instruct")),
        "llm_api_base": global_llm_api_base or str(rules_defaults.get("llm_api_base", "http://127.0.0.1:11434/v1")),
        "llm_api_key": global_llm_api_key or str(rules_defaults.get("llm_api_key", "")),
        "template_hint": DEFAULT_TEMPLATE_HINT,
        "template_start_url": "",
        "save_generated_template": True,
        "cleanup_generated_template": True,
        "disable_template_persistence_controls": False,
    }


def build_public_scraper_template_selection_state(
    template_path: str,
    app_file: str,
    *,
    global_llm_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    selected_path = str(template_path or "").strip()
    if not selected_path:
        return {
            "hint": DEFAULT_TEMPLATE_HINT,
            "template_start_url": "",
            "url_locked": False,
            "disable_template_persistence_controls": False,
        }

    try:
        with open(selected_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise ValueError("模板不是 JSON 对象")
    except Exception as exc:
        return {
            "hint": f"模板读取失败：{exc}",
            "template_start_url": "",
            "url_locked": False,
            "disable_template_persistence_controls": False,
        }

    start_url = ""
    start_urls = payload.get("start_urls")
    if isinstance(start_urls, list) and start_urls:
        start_url = normalize_http_url(start_urls[0])
    if not start_url.lower().startswith(("http://", "https://")):
        return {
            "hint": "模板缺少有效 start_urls，无法直接启动。",
            "template_start_url": "",
            "url_locked": False,
            "disable_template_persistence_controls": False,
        }

    defaults = default_public_scraper_template()
    crawl_defaults = defaults.get("crawl", {})
    global_llm = global_llm_settings if isinstance(global_llm_settings, dict) else {}

    crawl_cfg = payload.get("crawl")
    crawl_data = crawl_cfg if isinstance(crawl_cfg, dict) else {}
    rules_cfg = payload.get("rules")
    rules_data = rules_cfg if isinstance(rules_cfg, dict) else {}

    min_val = crawl_data.get(
        "interval_min_seconds",
        crawl_data.get("image_interval_min_seconds", crawl_data.get("download_delay", "")),
    )
    max_val = crawl_data.get("interval_max_seconds", crawl_data.get("image_interval_max_seconds", min_val))
    suspect_val = crawl_data.get(
        "suspect_block_consecutive_failures",
        crawl_defaults.get("suspect_block_consecutive_failures", 3),
    )

    global_llm_enabled = bool(global_llm.get("enabled_default", False))
    global_llm_model = str(global_llm.get("model", "")).strip()
    global_llm_api_base = str(global_llm.get("api_base", "")).strip()
    global_llm_api_key = str(global_llm.get("api_key", "")).strip()

    image_mode = str(rules_data.get("image_download_mode", "requests_jsl")).strip().lower()
    if image_mode not in VALID_IMAGE_DOWNLOAD_MODES:
        image_mode = "requests_jsl"

    output_cfg = normalize_public_scraper_template_output_root(str(payload.get("output_root", "")), app_file)
    return {
        "hint": "已使用模板内置链接，可直接开始任务。",
        "template_start_url": start_url,
        "start_url": start_url,
        "url_locked": True,
        "interval_min": str(min_val),
        "interval_max": str(max_val),
        "timeout_seconds": str(crawl_data.get("timeout_seconds", "")),
        "suspect_block_consecutive_failures": str(suspect_val),
        "jsl_enabled": bool(rules_data.get("jsl_clearance_enabled", True)),
        "image_download_mode": image_mode,
        "auto_fallback_to_browser": bool(rules_data.get("auto_fallback_to_browser", True)),
        "disable_page_images_during_crawl": bool(rules_data.get("disable_page_images_during_crawl", True)),
        "llm_enrich_enabled": (
            global_llm_enabled
            if (global_llm_api_base or global_llm_model)
            else bool(rules_data.get("llm_enrich_enabled", False))
        ),
        "llm_model": global_llm_model or str(rules_data.get("llm_model", "qwen2.5:7b-instruct")),
        "llm_api_base": global_llm_api_base or str(rules_data.get("llm_api_base", "http://127.0.0.1:11434/v1")),
        "llm_api_key": global_llm_api_key or str(rules_data.get("llm_api_key", "")),
        "output_minimal": str(rules_data.get("output_mode", "images_only_with_record")).strip().lower()
        in {"images_only", "images_only_with_record"},
        "direct_write_images": bool(rules_data.get("direct_write_images", True)),
        "output_root": output_cfg or default_public_scraper_output_root(app_file, start_url),
        "save_generated_template": False,
        "cleanup_generated_template": False,
        "disable_template_persistence_controls": True,
    }


def build_public_scraper_setup_submit_plan(
    raw_values: Optional[Dict[str, Any]],
    *,
    app_file: str,
    selected_template_path: str = "",
    template_start_url: str = "",
) -> Dict[str, Any]:
    data = raw_values if isinstance(raw_values, dict) else {}
    selected_path = str(selected_template_path or "").strip()

    if selected_path:
        start_url = str(template_start_url or "").strip() or normalize_http_url(data.get("start_url", ""))
        if not start_url.lower().startswith(("http://", "https://")):
            return {
                "ok": False,
                "title": "模板错误",
                "message": "所选模板缺少有效 start_urls，无法直接启动。",
            }
    else:
        start_url = normalize_http_url(data.get("start_url", ""))
        if not start_url.lower().startswith(("http://", "https://")):
            return {
                "ok": False,
                "title": "链接无效",
                "message": "请输入有效的 http/https 链接。",
            }

    output_root_raw = str(data.get("output_root", "") or "").strip()
    output_root = os.path.abspath(
        output_root_raw if output_root_raw else default_public_scraper_output_root(app_file, start_url)
    )

    try:
        interval_min = float(str(data.get("interval_min", "")).strip())
        interval_max = float(str(data.get("interval_max", "")).strip())
        timeout_seconds = int(str(data.get("timeout_seconds", "")).strip())
        suspect_failures = int(str(data.get("suspect_block_consecutive_failures", "")).strip())
    except Exception:
        return {
            "ok": False,
            "title": "参数错误",
            "message": "间隔、超时、连续失败阈值必须是数字。",
        }

    if interval_min < 0.1:
        return {
            "ok": False,
            "title": "参数错误",
            "message": "统一间隔最小值必须 >= 0.1 秒。",
        }
    if interval_max < interval_min:
        interval_max = interval_min
    if timeout_seconds < 5:
        return {
            "ok": False,
            "title": "参数错误",
            "message": "请求超时必须 >= 5 秒。",
        }
    if suspect_failures < 2:
        return {
            "ok": False,
            "title": "参数错误",
            "message": "连续失败阈值必须 >= 2。",
        }

    mode = str(data.get("image_download_mode", "requests_jsl") or "").strip().lower()
    if mode not in VALID_IMAGE_DOWNLOAD_MODES:
        mode = "requests_jsl"

    return {
        "ok": True,
        "result": {
            "start_url": start_url,
            "output_root": output_root,
            "interval_min": round(interval_min, 3),
            "interval_max": round(interval_max, 3),
            "timeout_seconds": int(timeout_seconds),
            "suspect_block_consecutive_failures": int(suspect_failures),
            "jsl_enabled": bool(data.get("jsl_enabled", False)),
            "image_download_mode": mode,
            "auto_fallback_to_browser": bool(data.get("auto_fallback_to_browser", True)),
            "disable_page_images_during_crawl": bool(data.get("disable_page_images_during_crawl", True)),
            "llm_enrich_enabled": bool(data.get("llm_enrich_enabled", False)),
            "llm_model": str(data.get("llm_model", "") or "").strip(),
            "llm_api_base": str(data.get("llm_api_base", "") or "").strip(),
            "llm_api_key": str(data.get("llm_api_key", "") or "").strip(),
            "output_minimal": bool(data.get("output_minimal", True)),
            "direct_write_images": bool(data.get("direct_write_images", True)),
            "template_path": selected_path,
            "save_generated_template": bool(data.get("save_generated_template", True)) and (not selected_path),
            "cleanup_generated_template": bool(data.get("cleanup_generated_template", True)) and (not selected_path),
        },
    }


def apply_public_scraper_setup_to_runtime_config(
    runtime_config: Optional[Dict[str, Any]],
    setup: Optional[Dict[str, Any]],
    *,
    global_llm_settings: Optional[Dict[str, Any]] = None,
    normalize_api_base_fn: Optional[Callable[[str], str]] = None,
) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    setup_data = setup if isinstance(setup, dict) else {}
    global_llm = global_llm_settings if isinstance(global_llm_settings, dict) else {}

    crawl = config.get("crawl")
    if not isinstance(crawl, dict):
        crawl = {}
    rules = config.get("rules")
    if not isinstance(rules, dict):
        rules = {}

    interval_min = float(setup_data.get("interval_min", 0))
    interval_max = float(setup_data.get("interval_max", interval_min))
    crawl["interval_min_seconds"] = interval_min
    crawl["interval_max_seconds"] = interval_max
    crawl["download_delay"] = interval_min
    crawl["autothrottle_start_delay"] = interval_min
    crawl["autothrottle_max_delay"] = interval_max
    crawl["image_interval_min_seconds"] = interval_min
    crawl["image_interval_max_seconds"] = interval_max
    crawl["timeout_seconds"] = int(setup_data.get("timeout_seconds", crawl.get("timeout_seconds", 30)))
    crawl["suspect_block_consecutive_failures"] = max(
        2,
        int(
            setup_data.get(
                "suspect_block_consecutive_failures",
                crawl.get("suspect_block_consecutive_failures", 3),
            )
        ),
    )

    rules["jsl_clearance_enabled"] = bool(setup_data.get("jsl_enabled", False))
    mode = str(setup_data.get("image_download_mode", "requests_jsl")).strip().lower()
    rules["image_download_mode"] = mode if mode in VALID_IMAGE_DOWNLOAD_MODES else "requests_jsl"
    rules["auto_fallback_to_browser"] = bool(setup_data.get("auto_fallback_to_browser", True))
    rules["disable_page_images_during_crawl"] = bool(setup_data.get("disable_page_images_during_crawl", True))
    rules["direct_write_images"] = bool(setup_data.get("direct_write_images", True))
    rules["llm_enrich_enabled"] = bool(setup_data.get("llm_enrich_enabled", False))

    global_llm_model = str(global_llm.get("model", "")).strip()
    global_llm_api_base = str(global_llm.get("api_base", "")).strip()
    global_llm_api_key = str(global_llm.get("api_key", "")).strip()

    llm_model = str(setup_data.get("llm_model", "")).strip() or global_llm_model
    llm_api_base = str(setup_data.get("llm_api_base", "")).strip() or global_llm_api_base
    llm_api_key = str(setup_data.get("llm_api_key", "")).strip() or global_llm_api_key
    if llm_api_base and callable(normalize_api_base_fn):
        try:
            llm_api_base = normalize_api_base_fn(llm_api_base)
        except Exception:
            llm_api_base = llm_api_base.rstrip("/")
    if llm_model:
        rules["llm_model"] = llm_model
    if llm_api_base:
        rules["llm_api_base"] = llm_api_base
    rules.pop("llm_api_key", None)
    if rules["image_download_mode"] == "browser":
        rules["browser_engine"] = str(rules.get("browser_engine", "edge")).strip().lower() or "edge"
    if bool(setup_data.get("output_minimal", True)):
        rules["output_mode"] = "images_only_with_record"
        rules["keep_record_file"] = True
    else:
        rules["output_mode"] = "full"
        rules["keep_record_file"] = True

    config["crawl"] = crawl
    config["rules"] = rules
    return {
        "runtime_config": config,
        "llm_model": llm_model,
        "llm_api_base": llm_api_base,
        "llm_api_key": llm_api_key,
    }


def save_generated_public_scraper_template(
    start_url: str,
    runtime_config: Optional[Dict[str, Any]],
    templates_dir: str,
) -> str:
    payload = _json_clone(runtime_config)
    site_name = guess_public_site_name(start_url)
    payload["site_name"] = site_name
    payload["output_root"] = f"data/public_archive/{site_name}"
    rules = payload.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    rules.pop("cleanup_paths", None)
    rules.pop("template_source_path", None)
    rules.pop("generated_template_path", None)
    rules.pop("output_root_parent", None)
    rules.pop("resolved_output_subdir", None)
    rules.pop("resolved_unit_name", None)
    rules.pop("resolved_year", None)
    payload["rules"] = rules

    os.makedirs(templates_dir, exist_ok=True)
    base = os.path.join(templates_dir, f"{site_name}.json")
    target = base
    if os.path.exists(target):
        target = os.path.join(
            templates_dir,
            f"{site_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
    with open(target, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return target


def register_generated_template(
    runtime_config: Optional[Dict[str, Any]],
    generated_template_path: str,
    *,
    cleanup_after_run: bool,
) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    path = str(generated_template_path or "").strip()
    if not path:
        return config
    rules = config.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    rules["generated_template_path"] = path
    if cleanup_after_run:
        cleanup_paths = rules.get("cleanup_paths", [])
        if not isinstance(cleanup_paths, list):
            cleanup_paths = []
        if path not in cleanup_paths:
            cleanup_paths.append(path)
        rules["cleanup_paths"] = cleanup_paths
    config["rules"] = rules
    return config
