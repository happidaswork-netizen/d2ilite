import argparse
from collections import deque
import hashlib
import json
import mimetypes
import os
import random
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

import requests
from scrapy import Selector
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from public_profile_spider import PublicProfileSpider, default_output_from_url

# Ensure parent project modules (e.g. metadata_writer.py) are importable
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from metadata_writer import write_xmp_metadata  # type: ignore
    HAS_METADATA_WRITER = True
    METADATA_WRITER_ERROR = ""
except Exception as exc:  # pragma: no cover
    HAS_METADATA_WRITER = False
    METADATA_WRITER_ERROR = str(exc)

try:
    from downloader import ImageDownloader  # type: ignore
    HAS_D2I_DOWNLOADER = True
    D2I_DOWNLOADER_ERROR = ""
except Exception as exc:  # pragma: no cover
    HAS_D2I_DOWNLOADER = False
    D2I_DOWNLOADER_ERROR = str(exc)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue


def count_jsonl(path: Path) -> int:
    return sum(1 for _ in iter_jsonl(path))


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def norm_abs_path(path_value: str) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    try:
        return str(Path(raw).resolve())
    except Exception:
        return raw


def sanitize_filename(name: str, fallback: str = "unnamed") -> str:
    value = str(name or "").strip()
    if not value:
        value = fallback
    # Keep CJK and common unicode chars, only remove Windows-invalid chars.
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value)
    value = re.sub(r"\s+", " ", value).strip().strip(". ")
    return value or fallback


def unique_named_path(base_dir: Path, display_name: str, ext: str, reserved: set[str]) -> Path:
    safe_base = sanitize_filename(display_name, fallback="unnamed")
    suffix = ext if ext.startswith(".") else f".{ext}"
    candidate = (base_dir / f"{safe_base}{suffix}").resolve()
    if str(candidate) not in reserved and not candidate.exists():
        reserved.add(str(candidate))
        return candidate
    counter = 2
    while True:
        candidate = (base_dir / f"{safe_base}_{counter}{suffix}").resolve()
        if str(candidate) not in reserved and not candidate.exists():
            reserved.add(str(candidate))
            return candidate
        counter += 1


def strip_prefixed_label(value: str, labels: List[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for label in labels:
        for sep in ("：", ":"):
            prefix = f"{label}{sep}"
            if text.startswith(prefix):
                return text[len(prefix) :].strip()
    return text


def normalize_gender(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    unknown_tokens = {
        "unknown",
        "unkonw",
        "n/a",
        "na",
        "none",
        "null",
        "未知",
        "未详",
        "不详",
        "待补充",
        "-",
    }
    if lowered in unknown_tokens or raw in unknown_tokens:
        return ""
    if lowered in {"male", "m", "man", "男性"} or raw == "男":
        return "男"
    if lowered in {"female", "f", "woman", "女性"} or raw == "女":
        return "女"
    return raw


def normalize_optional_field(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    unknown_tokens = {"unknown", "unkonw", "n/a", "na", "none", "null", "未知", "未详", "不详", "待补充", "-"}
    if lowered in unknown_tokens or raw in unknown_tokens:
        return ""
    return raw


def humanize_field_label(field_key: str) -> str:
    key = str(field_key or "").strip()
    if not key:
        return ""
    if re.search(r"[\u4e00-\u9fff]", key):
        return key
    key = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", key)
    key = key.replace("_", " ").replace("-", " ")
    key = re.sub(r"\s+", " ", key).strip()
    return key or field_key


def extract_police_id_from_fields(fields: Dict[str, str]) -> str:
    for key in ("police_id", "police_no", "police_number", "badge_no", "badge_id", "badge_number", "officer_id", "警号"):
        value = normalize_optional_field(fields.get(key))
        if value:
            return value
    return ""


def collect_detail_field_labels(config: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, str]:
    selectors_cfg = dict(config.get("selectors", {}))
    labels: Dict[str, str] = {}
    labels_from_selectors = selectors_cfg.get("detail_field_labels")
    labels_from_rules = rules.get("detail_field_labels")
    if isinstance(labels_from_selectors, dict):
        labels.update({str(k): str(v) for k, v in labels_from_selectors.items() if str(k).strip()})
    if isinstance(labels_from_rules, dict):
        labels.update({str(k): str(v) for k, v in labels_from_rules.items() if str(k).strip()})
    return labels


def build_metadata_queue_row_from_profile(profile: Dict[str, Any], image_sha: str, local_path: str) -> Dict[str, Any]:
    return {
        "created_at": utc_now_iso(),
        "name": profile.get("name", ""),
        "gender": normalize_gender(profile.get("gender", "")),
        "summary": _normalize_multiline_text(profile.get("summary", "")),
        "full_content": _normalize_multiline_text(profile.get("full_content", "") or profile.get("summary", "")),
        "fields": profile.get("fields", {}),
        "mapped": profile.get("mapped", {}),
        "detail_url": profile.get("detail_url", ""),
        "source_url": profile.get("list_url", ""),
        "image_url": profile.get("image_url", ""),
        "image_sha256": image_sha,
        "local_image_path": local_path,
    }


def write_metadata_for_queue_row(
    *,
    row: Dict[str, Any],
    source_path: str,
    named_dir: Path,
    reserved_paths: set[str],
    field_labels: Dict[str, str],
    review_path: Path,
    results_path: Path,
    detail_to_final_path: Optional[Dict[str, str]] = None,
    sha_runtime_path: Optional[Dict[str, str]] = None,
) -> Tuple[bool, str, bool]:
    detail_url = str(row.get("detail_url", "")).strip()
    image_sha = str(row.get("image_sha256", "")).strip()

    raw_fields = row.get("fields", {})
    fields: Dict[str, str] = {}
    if isinstance(raw_fields, dict):
        for raw_key, raw_value in raw_fields.items():
            key = str(raw_key or "").strip()
            value = normalize_optional_field(raw_value)
            if key and value:
                fields[key] = value

    raw_mapped = row.get("mapped", {})
    mapped_fields: Dict[str, str] = {}
    if isinstance(raw_mapped, dict):
        for raw_key, raw_value in raw_mapped.items():
            key = str(raw_key or "").strip()
            if not key:
                continue
            if key.lower() == "gender":
                value = normalize_gender(raw_value)
            elif key.lower() in {"description", "summary", "full_content"}:
                value = _normalize_multiline_text(raw_value)
            else:
                value = normalize_optional_field(raw_value)
            if value:
                mapped_fields[key] = value

    title = normalize_optional_field(mapped_fields.get("person") or row.get("name", ""))
    person_name = normalize_optional_field(mapped_fields.get("person") or title)
    summary = _normalize_multiline_text(mapped_fields.get("summary") or row.get("summary", ""))
    full_content = _normalize_multiline_text(
        mapped_fields.get("full_content") or row.get("full_content", "") or summary
    )
    mapped_description = _normalize_multiline_text(mapped_fields.get("description", ""))
    source_list_url = normalize_optional_field(mapped_fields.get("source_url") or row.get("source_url", ""))
    image_url = normalize_optional_field(mapped_fields.get("image_url") or row.get("image_url", ""))
    position = normalize_optional_field(mapped_fields.get("position") or fields.get("title", ""))
    english_name = normalize_optional_field(mapped_fields.get("english_name") or fields.get("english_name", ""))
    mapped_city_text = normalize_optional_field(mapped_fields.get("city"))
    location_text = normalize_optional_field(mapped_city_text or fields.get("location_text", ""))
    email_text = normalize_optional_field(mapped_fields.get("email") or fields.get("email_text", ""))
    location_clean = strip_prefixed_label(location_text, ["工作地点", "地点"])
    email_clean = strip_prefixed_label(email_text, ["邮箱", "Email", "email"])
    gender = normalize_gender(mapped_fields.get("gender") or row.get("gender", ""))
    police_id = normalize_optional_field(mapped_fields.get("police_id")) or extract_police_id_from_fields(fields)
    city_value = location_clean or location_text

    extra_fields: Dict[str, str] = dict(fields)
    dynamic_detail_lines: List[str] = []
    for key, value in extra_fields.items():
        if key in {"title", "english_name", "location_text", "email_text"}:
            continue
        label = str(field_labels.get(key, "")).strip() or humanize_field_label(key)
        if label:
            dynamic_detail_lines.append(f"{label}：{value}")

    desc_parts: List[str] = []
    if title:
        desc_parts.append(f"姓名：{title}")
    if english_name:
        desc_parts.append(f"英文名：{english_name}")
    if position:
        desc_parts.append(f"职位：{position}")
    if location_clean:
        desc_parts.append(f"工作地点：{location_clean}")
    elif location_text:
        desc_parts.append(f"工作地点：{location_text}")
    if email_clean:
        desc_parts.append(f"邮箱：{email_clean}")
    elif email_text:
        desc_parts.append(email_text)
    if police_id:
        desc_parts.append(f"警号：{police_id}")
    if dynamic_detail_lines:
        desc_parts.extend(dynamic_detail_lines)
    if detail_url:
        desc_parts.append(f"详情页：{detail_url}")
    if source_list_url:
        desc_parts.append(f"列表页：{source_list_url}")
    if image_url:
        desc_parts.append(f"原图链接：{image_url}")
    if mapped_description:
        desc_parts.append("说明：")
        desc_parts.append(mapped_description)
    if summary:
        desc_parts.append("简介：")
        desc_parts.append(summary)
    rich_description = _normalize_multiline_text("\n".join(desc_parts))

    keywords: List[str] = []
    for value in [title, english_name, position, city_value, gender, police_id]:
        if value and value not in keywords:
            keywords.append(value)
    for value in extra_fields.values():
        if value and value not in keywords:
            keywords.append(value)
    for value in mapped_fields.values():
        if value and ("\n" not in value) and value not in keywords:
            keywords.append(value)
    if detail_url and "detail-page" not in keywords:
        keywords.append("detail-page")
    if "public-archive" not in keywords:
        keywords.append("public-archive")

    ext = ".jpg"
    source_resolved = Path(source_path).resolve()
    current_local = norm_abs_path(str(row.get("local_image_path", "")))
    existing_local_path = Path(current_local) if current_local else None
    copied_to_named_folder = False
    if existing_local_path and existing_local_path.exists() and existing_local_path.parent.resolve() == named_dir:
        final_path = existing_local_path.resolve()
        reserved_paths.add(str(final_path))
    else:
        final_path = unique_named_path(named_dir, title or "unnamed", ext=ext, reserved=reserved_paths)
        if source_resolved != final_path:
            shutil.copy2(source_resolved, final_path)
            copied_to_named_folder = True

    d2i_profile_payload: Dict[str, Any] = {
        "email": email_text,
        "english_name": english_name,
        "title": position,
        "location": city_value,
        "source_detail_url": detail_url,
        "source_list_url": source_list_url,
    }
    if extra_fields:
        d2i_profile_payload["extra_fields"] = extra_fields
        for key, value in extra_fields.items():
            if key not in d2i_profile_payload:
                d2i_profile_payload[key] = value
    if mapped_fields:
        d2i_profile_payload["mapped_fields"] = mapped_fields
        for key, value in mapped_fields.items():
            if key not in d2i_profile_payload:
                d2i_profile_payload[key] = value
    if gender:
        d2i_profile_payload["gender"] = gender
    if police_id:
        d2i_profile_payload["police_id"] = police_id
    if summary:
        d2i_profile_payload["summary"] = summary
    if full_content:
        d2i_profile_payload["full_content"] = full_content

    payload = {
        "title": title,
        "person": person_name or title,
        "name": person_name or title,
        "description": rich_description,
        "source": detail_url or source_list_url,
        "image_url": image_url,
        "url": image_url,
        "city": city_value,
        "position": position,
        "gender": gender,
        "police_id": police_id,
        "keywords": keywords,
        "role_aliases": [english_name] if english_name else [],
        "d2i_profile": d2i_profile_payload,
    }

    try:
        saved_path = str(write_xmp_metadata(str(final_path), payload))
        saved_path_norm = norm_abs_path(saved_path)
        row["local_image_path"] = saved_path_norm or str(final_path)
        row["file_name"] = Path(row["local_image_path"]).name
        row["file_dir"] = str(Path(row["local_image_path"]).parent.resolve())
        if detail_url and isinstance(detail_to_final_path, dict):
            detail_to_final_path[detail_url] = row["local_image_path"]
        if image_sha and isinstance(sha_runtime_path, dict):
            sha_runtime_path[image_sha] = row["local_image_path"]

        append_jsonl(
            results_path,
            {
                "written_at": utc_now_iso(),
                "detail_url": detail_url,
                "input_path": str(source_resolved),
                "output_path": row["local_image_path"],
                "status": "ok",
            },
        )
        return True, str(row["local_image_path"]), copied_to_named_folder
    except Exception as exc:
        append_jsonl(
            review_path,
            {
                "scraped_at": utc_now_iso(),
                "reason": "metadata_write_failed",
                "detail_url": detail_url,
                "local_image_path": str(final_path),
                "error": str(exc),
            },
        )
        append_jsonl(
            results_path,
            {
                "written_at": utc_now_iso(),
                "detail_url": detail_url,
                "input_path": str(source_resolved),
                "output_path": str(final_path),
                "status": "failed",
                "error": str(exc),
            },
        )
        return False, str(final_path), copied_to_named_folder


def resolve_final_output_root(work_root: Path, rules: Dict[str, Any]) -> Path:
    cfg = str(rules.get("final_output_root", "")).strip()
    if not cfg:
        return work_root.resolve()
    p = Path(cfg)
    if not p.is_absolute():
        p = (work_root / p).resolve()
    return p.resolve()


def resolve_record_root(output_root: Path, rules: Dict[str, Any]) -> Path:
    cfg = str(rules.get("record_root", "")).strip()
    if not cfg:
        return output_root.resolve()
    p = Path(cfg)
    if not p.is_absolute():
        p = (output_root / p).resolve()
    return p.resolve()


def resolve_named_output_dir(work_root: Path, rules: Dict[str, Any]) -> Path:
    final_root = resolve_final_output_root(work_root, rules)
    named_dir_cfg = str(rules.get("named_images_dir", "")).strip()
    if not named_dir_cfg:
        return final_root.resolve()
    named_path = Path(named_dir_cfg)
    if named_path.is_absolute():
        return named_path.resolve()
    return (final_root / named_dir_cfg).resolve()


def load_config(config_path: Path) -> Dict[str, Any]:
    payload = load_json(config_path, {})
    if not isinstance(payload, dict):
        raise ValueError("config file must be a JSON object")
    start_urls = payload.get("start_urls")
    if not start_urls:
        raise ValueError("config.start_urls is required")
    return payload


def resolve_output_root(config: Dict[str, Any], project_root: Path) -> Path:
    configured = str(config.get("output_root", "")).strip()
    if configured:
        output = Path(configured)
        if not output.is_absolute():
            output = (project_root / output).resolve()
        return output

    first_url = str(config["start_urls"][0])
    site_name = str(config.get("site_name", "")).strip() or default_output_from_url(first_url)
    return (project_root / "data" / "public_archive" / site_name).resolve()


def sanitize_subdir_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = re.sub(r"\s+", " ", text).strip().strip(". ")
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def extract_year_token(*texts: Any) -> str:
    for raw in texts:
        text = str(raw or "")
        m = re.search(r"(?<!\d)(19|20)\d{2}(?!\d)", text)
        if m:
            return m.group(0)
    return ""


def guess_unit_name_from_config(config: Dict[str, Any]) -> str:
    rules = config.get("rules")
    if not isinstance(rules, dict):
        rules = {}
    for key in ("unit_name", "organization_name", "org_name", "unit"):
        candidate = str(rules.get(key, "")).strip()
        if candidate:
            return candidate

    first_url = str((config.get("start_urls") or [""])[0] or "")
    host = (urlparse(first_url).hostname or "").strip().lower()
    if "tiantonglaw.com" in host:
        return "天同律师事务所"
    if host.endswith("mps.gov.cn") or ("mps.gov.cn" in host):
        return "公安部"

    site_name = str(config.get("site_name", "")).strip()
    if site_name:
        cleaned = re.sub(r"[_\-]+", " ", site_name).strip()
        if cleaned:
            return cleaned

    if host:
        parts = [p for p in host.split(".") if p]
        if len(parts) >= 2:
            return parts[-2]
        return host
    return "单位"


def resolve_output_root_with_unit_subdir(config: Dict[str, Any], base_root: Path) -> Path:
    rules = config.get("rules")
    if not isinstance(rules, dict):
        rules = {}
        config["rules"] = rules
    if (not bool(rules.get("auto_unit_subdir", False))) or str(rules.get("resolved_output_subdir", "")).strip():
        return base_root.resolve()

    first_url = str((config.get("start_urls") or [""])[0] or "")
    host = sanitize_subdir_name((urlparse(first_url).hostname or "").strip().lower())
    site_name = sanitize_subdir_name(str(config.get("site_name", "") or ""))
    unit = sanitize_subdir_name(guess_unit_name_from_config(config))
    year = sanitize_subdir_name(
        str(rules.get("year_hint") or extract_year_token(first_url, config.get("site_name", "")))
    )
    year_suffix = f"_{year}" if year else ""
    pattern = str(rules.get("output_subdir_pattern", "{unit}{year_suffix}") or "").strip()
    if not pattern:
        pattern = "{unit}{year_suffix}"

    class _SafeDict(dict):
        def __missing__(self, key: str) -> str:
            return ""

    try:
        subdir_raw = pattern.format_map(
            _SafeDict(
                {
                    "unit": unit,
                    "year": year,
                    "year_suffix": year_suffix,
                    "site_name": site_name,
                    "host": host,
                }
            )
        )
    except Exception:
        subdir_raw = f"{unit}{year_suffix}".strip()
    subdir = sanitize_subdir_name(subdir_raw)
    if not subdir:
        subdir = sanitize_subdir_name(unit or site_name or host)
    if not subdir:
        return base_root.resolve()

    parent = base_root.resolve()
    resolved = (parent / subdir).resolve()
    rules["output_root_parent"] = str(parent)
    rules["resolved_output_subdir"] = subdir
    rules["resolved_unit_name"] = unit or site_name or host
    if year:
        rules["resolved_year"] = year
    else:
        rules.pop("resolved_year", None)
    config["output_root"] = str(resolved)
    config["rules"] = rules
    return resolved


def check_backoff(output_root: Path) -> Tuple[bool, Optional[str]]:
    state_path = output_root / "state" / "backoff_state.json"
    payload = load_json(state_path, {})
    if not isinstance(payload, dict):
        return True, None
    blocked_until = str(payload.get("blocked_until", "")).strip()
    if not blocked_until:
        return True, None
    try:
        until_ts = datetime.fromisoformat(blocked_until)
    except ValueError:
        return True, None
    now_ts = datetime.now(timezone.utc)
    if until_ts.tzinfo is None:
        until_ts = until_ts.replace(tzinfo=timezone.utc)
    if until_ts > now_ts:
        return False, blocked_until
    return True, None


def clear_backoff(output_root: Path) -> None:
    state_path = output_root / "state" / "backoff_state.json"
    if state_path.exists():
        state_path.unlink()


def build_scrapy_settings(config: Dict[str, Any], output_root: Path) -> Settings:
    crawl_cfg = dict(config.get("crawl", {}))
    default_headers = dict(config.get("default_headers", {}))
    delay_min = float(crawl_cfg.get("interval_min_seconds", crawl_cfg.get("download_delay", 8)))
    delay_max = float(crawl_cfg.get("interval_max_seconds", crawl_cfg.get("autothrottle_max_delay", max(delay_min, 20))))
    if delay_min < 0.1:
        delay_min = 0.1
    if delay_max < delay_min:
        delay_max = delay_min
    settings = Settings()
    settings.set("ROBOTSTXT_OBEY", bool(config.get("rules", {}).get("obey_robots_txt", True)))
    settings.set("CONCURRENT_REQUESTS", int(crawl_cfg.get("concurrent_requests", 1)))
    settings.set("DOWNLOAD_DELAY", delay_min)
    settings.set("RANDOMIZE_DOWNLOAD_DELAY", True)
    settings.set("AUTOTHROTTLE_ENABLED", True)
    settings.set("AUTOTHROTTLE_START_DELAY", float(crawl_cfg.get("autothrottle_start_delay", delay_min)))
    settings.set("AUTOTHROTTLE_MAX_DELAY", delay_max)
    settings.set("AUTOTHROTTLE_TARGET_CONCURRENCY", 1.0)
    settings.set("RETRY_ENABLED", True)
    settings.set("RETRY_TIMES", int(crawl_cfg.get("retry_times", 3)))
    settings.set("DOWNLOAD_TIMEOUT", int(crawl_cfg.get("timeout_seconds", 30)))
    settings.set("JOBDIR", str((output_root / "state" / "jobdir").resolve()))
    settings.set(
        "USER_AGENT",
        str(
            config.get(
                "user_agent",
                "D2ILiteArchiveBot/1.0 (+local archival use, contact: local-only)",
            )
        ),
    )
    settings.set("TELNETCONSOLE_ENABLED", False)
    settings.set("COOKIES_ENABLED", False)
    if default_headers:
        settings.set("DEFAULT_REQUEST_HEADERS", {str(k): str(v) for k, v in default_headers.items()})
    settings.set("LOG_LEVEL", str(config.get("log_level", "INFO")))
    return settings


def _guess_extension(url: str, content_type: str) -> str:
    candidate = mimetypes.guess_extension(content_type.split(";")[0].strip()) if content_type else None
    if candidate and len(candidate) <= 5:
        return candidate

    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
        return suffix
    return ".bin"


def _normalize_text(value: Any) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""
    if re.search(r"[\u4e00-\u9fff]", text):
        return text
    latin1_like = sum(1 for ch in text if 0x80 <= ord(ch) <= 0xFF)
    if latin1_like < 2:
        return text
    try:
        repaired = text.encode("latin1").decode("utf-8")
    except Exception:
        return text
    if re.search(r"[\u4e00-\u9fff]", repaired):
        return repaired
    return text


def _normalize_multiline_text(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return ""
    lines: List[str] = []
    for raw_line in text.split("\n"):
        line = _normalize_text(raw_line)
        if line:
            lines.append(line)
    return "\n".join(lines)


def _ensure_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def _select_values(selector_source: Any, selector: str) -> List[str]:
    selector_text = str(selector or "").strip()
    if not selector_text:
        return []
    try:
        if selector_text.startswith("xpath:"):
            return selector_source.xpath(selector_text[len("xpath:") :]).getall()
        if selector_text.startswith("/"):
            return selector_source.xpath(selector_text).getall()
        return selector_source.css(selector_text).getall()
    except Exception:
        return []


def _extract_first(selector_source: Any, selector_spec: Any) -> str:
    for selector in _ensure_list(selector_spec):
        for value in _select_values(selector_source, selector):
            normalized = _normalize_text(value)
            if normalized:
                return normalized
    return ""


def _extract_joined_text(selector_source: Any, selector_spec: Any) -> str:
    values: List[str] = []
    for selector in _ensure_list(selector_spec):
        for value in _select_values(selector_source, selector):
            normalized = _normalize_text(value)
            if normalized:
                values.append(normalized)
    return _normalize_multiline_text("\n".join(values))


def _extract_full_content_text(selector_source: Any, selectors: Dict[str, Any]) -> str:
    configured = selectors.get("detail_full_text")
    if configured:
        extracted = _extract_joined_text(selector_source, configured)
        if extracted:
            return extracted

    fallback_xpath = "xpath://body//text()[normalize-space() and not(ancestor::script) and not(ancestor::style) and not(ancestor::noscript)]"
    chunks = _select_values(selector_source, fallback_xpath)
    merged: List[str] = []
    last_line = ""
    for chunk in chunks:
        line = _normalize_text(chunk)
        if not line:
            continue
        if line == last_line:
            continue
        merged.append(line)
        last_line = line
    return _normalize_multiline_text("\n".join(merged))


def _resolve_field_map(config: Dict[str, Any]) -> Dict[str, List[str]]:
    selectors = dict(config.get("selectors", {}))
    rules = dict(config.get("rules", {}))
    merged: Dict[str, Any] = {}
    if isinstance(selectors.get("field_map"), dict):
        merged.update(dict(selectors.get("field_map", {})))
    if isinstance(rules.get("field_map"), dict):
        merged.update(dict(rules.get("field_map", {})))

    normalized: Dict[str, List[str]] = {}
    for target_key, source_spec in merged.items():
        target = str(target_key or "").strip()
        if not target:
            continue
        sources = [str(x).strip() for x in _ensure_list(source_spec) if str(x).strip()]
        if sources:
            normalized[target] = sources
    return normalized


def _resolve_field_value_from_source(
    source_key: str,
    *,
    name: str,
    gender: str,
    summary: str,
    full_content: str,
    detail_url: str,
    list_url: str,
    image_url: str,
    fields: Dict[str, str],
) -> str:
    token = str(source_key or "").strip()
    if not token:
        return ""
    lowered = token.lower()

    if lowered.startswith("field.") or lowered.startswith("fields."):
        field_key = token.split(".", 1)[1].strip()
        return normalize_optional_field(fields.get(field_key, ""))

    builtins: Dict[str, str] = {
        "name": _normalize_text(name),
        "person": _normalize_text(name),
        "gender": normalize_gender(gender),
        "summary": _normalize_multiline_text(summary),
        "full_content": _normalize_multiline_text(full_content),
        "detail_url": _normalize_text(detail_url),
        "source_url": _normalize_text(list_url),
        "list_url": _normalize_text(list_url),
        "image_url": _normalize_text(image_url),
    }
    if lowered in builtins:
        return builtins[lowered]

    # bare token: prefer detail field first, then builtin aliases.
    from_fields = normalize_optional_field(fields.get(token, ""))
    if from_fields:
        return from_fields
    return builtins.get(lowered, "")


def _apply_field_map(
    field_map: Dict[str, List[str]],
    *,
    name: str,
    gender: str,
    summary: str,
    full_content: str,
    detail_url: str,
    list_url: str,
    image_url: str,
    fields: Dict[str, str],
) -> Dict[str, str]:
    mapped: Dict[str, str] = {}
    for target, source_tokens in field_map.items():
        value = ""
        for token in source_tokens:
            candidate = _resolve_field_value_from_source(
                token,
                name=name,
                gender=gender,
                summary=summary,
                full_content=full_content,
                detail_url=detail_url,
                list_url=list_url,
                image_url=image_url,
                fields=fields,
            )
            if candidate:
                value = candidate
                break
        if not value:
            continue
        target_lower = target.lower()
        if target_lower == "gender":
            value = normalize_gender(value)
        elif target_lower in {"description", "summary", "full_content"}:
            value = _normalize_multiline_text(value)
        elif target_lower in {"email", "email_text"}:
            value = strip_prefixed_label(value, ["邮箱", "Email", "email"])
        elif target_lower in {"city", "location", "location_text"}:
            value = strip_prefixed_label(value, ["工作地点", "地点", "城市", "City", "city"])
        else:
            value = normalize_optional_field(value)
        if value:
            mapped[target] = value
    return mapped


def _decode_js_string(value: str) -> str:
    result = str(value or "")
    result = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), result)
    result = result.replace(r"\/", "/")
    result = result.replace(r"\"", '"')
    result = result.replace(r"\'", "'")
    result = result.replace(r"\r", "\r")
    result = result.replace(r"\n", "\n")
    result = result.replace(r"\t", "\t")
    result = result.replace(r"\\", "\\")
    return result


def _extract_document_write_html(payload: str) -> str:
    text = str(payload or "")
    if not text:
        return ""
    pattern = re.compile(r'document\.write\("((?:\\.|[^"\\])*)"\);?', flags=re.S)
    parts: List[str] = []
    for match in pattern.finditer(text):
        decoded = _decode_js_string(match.group(1))
        if decoded.strip():
            parts.append(decoded)
    if parts:
        return "\n".join(parts)
    if text.lstrip().startswith("<"):
        return text
    return ""


def _build_selector_source_from_html(html_payload: str, selectors: Dict[str, Any], phase: str) -> Selector:
    transform_key = f"{phase}_response_transform"
    transform = _normalize_text(str(selectors.get(transform_key, ""))).lower()
    text = html_payload
    if transform == "document_write_html":
        transformed = _extract_document_write_html(html_payload)
        if transformed:
            text = transformed
    return Selector(text=text, type="html")


def _parse_sycms_page_onclick(onclick: str) -> str:
    match = re.search(
        r"SYCmsPage\('(?P<page>\d+)','(?P<obji>[^']*)','(?P<temurl>[^']*)','(?P<query>[^']*)'",
        str(onclick or ""),
    )
    if not match:
        return ""
    page = match.group("page")
    obji = match.group("obji")
    temurl = match.group("temurl").replace(r"\/", "/").strip()
    query = match.group("query").replace(r"\/", "/").lstrip("&")
    if not temurl:
        return ""

    params: Dict[str, str] = {}
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key.lower() == "page":
            continue
        params[key] = value
    params["Page"] = page
    if obji:
        has_obji = any(k.lower() == "obji" for k in params.keys())
        if not has_obji:
            params["ObjI"] = obji

    query_string = urlencode(params)
    delimiter = "&" if "?" in temurl else "?"
    return f"{temurl}{delimiter}{query_string}"


def _normalize_next_page_url(raw_url: str) -> str:
    candidate = _normalize_text(raw_url)
    if (not candidate) or (candidate == "#"):
        return ""
    if "SYCmsPage(" in candidate:
        return _parse_sycms_page_onclick(candidate)
    lower = candidate.lower()
    if "return false" in lower:
        return ""
    if lower.startswith("javascript:"):
        return ""
    return candidate


def _iter_next_pages_from_source(list_source: Any, selectors: Dict[str, Any]) -> Iterable[str]:
    for selector in _ensure_list(selectors.get("next_page")):
        for raw in _select_values(list_source, selector):
            next_url = _normalize_next_page_url(raw)
            if next_url:
                yield next_url


def _url_allowed(url: str, allowed_domains: List[str]) -> bool:
    if not allowed_domains:
        return True
    host = (urlparse(url).hostname or "").strip().lower()
    if not host:
        return False
    for domain in allowed_domains:
        d = str(domain or "").strip().lower().lstrip(".")
        if not d:
            continue
        if host == d or host.endswith(f".{d}"):
            return True
    return False


def _looks_like_browser_challenge(html_payload: str) -> bool:
    body = str(html_payload or "").lower()
    if not body:
        return False
    markers = [
        "checking your browser",
        "just a moment",
        "ddos protection",
        "ray id",
        "__jsl_clearance_s",
        "__jsluid",
        "document.cookie",
    ]
    return any(marker in body for marker in markers)


def _extract_http_status_from_text(text: str) -> Optional[int]:
    raw = str(text or "").strip()
    if not raw:
        return None
    patterns = [
        r"\bhttp\s*([1-5]\d{2})\b",
        r"\bstatus(?:\s*code)?\s*[:=]?\s*([1-5]\d{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            return int(match.group(1))
        except Exception:
            continue
    return None


def _looks_like_browser_challenge_payload(payload: bytes) -> bool:
    if not payload:
        return False
    try:
        sample = payload[:8192].decode("utf-8", errors="ignore")
    except Exception:
        return False
    return _looks_like_browser_challenge(sample)


def _classify_browser_blocked_reason(
    *,
    error_text: str,
    html_payload: str,
    blocked_statuses: set[int],
) -> str:
    lowered = str(error_text or "").strip().lower()
    if "browser_challenge_not_cleared" in lowered:
        return "browser_challenge_not_cleared"
    if _looks_like_browser_challenge(html_payload):
        return "browser_challenge_detected"
    status = _extract_http_status_from_text(lowered)
    blocked_codes = set(int(x) for x in blocked_statuses).union({412, 521})
    if status in blocked_codes:
        return f"http_{status}"
    return ""


def _fetch_html_via_browser(
    downloader: Any,
    *,
    url: str,
    timeout_seconds: int,
    challenge_rounds: int = 8,
    challenge_wait_seconds: float = 1.2,
) -> Tuple[bool, str, str]:
    driver = getattr(downloader, "driver", None)
    if driver is None:
        return False, "", "browser_driver_unavailable"
    try:
        try:
            driver.set_page_load_timeout(max(5, int(timeout_seconds)))
        except Exception:
            pass
        driver.get(url)
        html_payload = str(driver.page_source or "")
        rounds = 0
        while _looks_like_browser_challenge(html_payload) and rounds < max(1, int(challenge_rounds)):
            time.sleep(max(0.2, float(challenge_wait_seconds)))
            html_payload = str(driver.page_source or "")
            rounds += 1
        if _looks_like_browser_challenge(html_payload):
            return False, html_payload, "browser_challenge_not_cleared"
        if not html_payload:
            return False, "", "empty_page_source"
        return True, html_payload, ""
    except Exception as exc:
        return False, "", str(exc)


def _load_existing_detail_urls(path: Path) -> set[str]:
    known: set[str] = set()
    for row in iter_jsonl(path):
        detail_url = str(row.get("detail_url", "")).strip()
        if detail_url:
            known.add(detail_url)
    return known


def _save_snapshot_html(snapshots_dir: Path, phase: str, url: str, html_payload: str) -> None:
    hashed = hashlib.sha1(str(url).encode("utf-8")).hexdigest()
    target = snapshots_dir / f"{phase}_{hashed}.html"
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(str(html_payload or ""), encoding="utf-8", errors="ignore")


def run_crawl_browser_mode(config: Dict[str, Any], output_root: Path) -> None:
    if not HAS_D2I_DOWNLOADER:
        raise RuntimeError(f"browser crawl unavailable: {D2I_DOWNLOADER_ERROR}")

    selectors = dict(config.get("selectors", {}))
    rules = dict(config.get("rules", {}))
    crawl_cfg = dict(config.get("crawl", {}))
    start_urls = [str(u).strip() for u in config.get("start_urls", []) if str(u).strip()]
    if not start_urls:
        raise ValueError("config.start_urls is required")
    allowed_domains = [str(d).strip().lower() for d in config.get("allowed_domains", []) if str(d).strip()]

    raw_dir = output_root / "raw"
    state_dir = output_root / "state"
    snapshots_dir = output_root / "snapshots"
    reports_dir = output_root / "reports"
    list_path = raw_dir / "list_records.jsonl"
    profile_path = raw_dir / "profiles.jsonl"
    review_path = raw_dir / "review_queue.jsonl"
    failures_path = raw_dir / "failures.jsonl"
    crawl_report_path = reports_dir / "crawl_report.json"
    download_root = output_root / "downloads"
    image_root = download_root / "images"
    download_manifest = download_root / "image_downloads.jsonl"
    url_index_path = state_dir / "image_url_index.json"
    sha_index_path = state_dir / "image_sha_index.json"
    backoff_path = state_dir / "backoff_state.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    snapshot_html = bool(rules.get("snapshot_html", True))
    if snapshot_html:
        snapshots_dir.mkdir(parents=True, exist_ok=True)

    list_item_selector = str(selectors.get("list_item", "")).strip()
    if not list_item_selector:
        raise ValueError("selectors.list_item is required")

    default_gender = normalize_gender(rules.get("default_gender", ""))
    field_map = _resolve_field_map(config)
    gender_map: Dict[str, str] = {}
    for k, v in dict(rules.get("gender_map", {})).items():
        key_raw = str(k)
        key_norm = _normalize_text(key_raw)
        value_norm = normalize_gender(v)
        gender_map[key_raw] = value_norm
        if key_norm:
            gender_map[key_norm] = value_norm

    required_fields = _ensure_list(rules.get("required_fields", ["name", "detail_url", "image_url"]))
    timeout_seconds = max(5, int(crawl_cfg.get("timeout_seconds", 30)))
    blocked_statuses = {
        int(s)
        for s in crawl_cfg.get("blocked_statuses", [403, 429])
        if str(s).strip().isdigit()
    }
    backoff_hours = float(crawl_cfg.get("blocked_backoff_hours", 6))
    suspect_failures_threshold = max(2, int(crawl_cfg.get("suspect_block_consecutive_failures", 3)))
    browser_engine = str(rules.get("browser_engine", "auto")).strip().lower()
    if browser_engine not in {"auto", "edge", "chrome"}:
        browser_engine = "auto"
    inline_download_enabled = bool(rules.get("download_images_during_crawl", True))
    interval_min = float(
        crawl_cfg.get(
            "interval_min_seconds",
            crawl_cfg.get("image_interval_min_seconds", crawl_cfg.get("download_delay", 5)),
        )
    )
    interval_max = float(
        crawl_cfg.get(
            "interval_max_seconds",
            crawl_cfg.get("image_interval_max_seconds", max(interval_min, 8)),
        )
    )
    if interval_min < 0.1:
        interval_min = 0.1
    if interval_max < interval_min:
        interval_max = interval_min
    url_index = load_json(url_index_path, {})
    if not isinstance(url_index, dict):
        url_index = {}
    sha_index = load_json(sha_index_path, {})
    if not isinstance(sha_index, dict):
        sha_index = {}

    metrics: Dict[str, int] = {
        "list_rows_seen": 0,
        "detail_requests_enqueued": 0,
        "detail_pages_saved": 0,
        "detail_duplicates_skipped": 0,
        "missing_required_items": 0,
        "failures": 0,
        "blocked_stops": 0,
        "inline_image_candidates": 0,
        "inline_image_downloaded_new": 0,
        "inline_image_reused_by_url": 0,
        "inline_image_reused_by_sha": 0,
        "inline_image_failed": 0,
    }

    known_detail_urls = _load_existing_detail_urls(profile_path)
    seen_list_urls: set[str] = set()
    queued_list_urls: set[str] = set()
    fetch_count = 0

    list_queue: deque[str] = deque()
    for url in start_urls:
        normalized = str(url).strip()
        if not normalized:
            continue
        if (normalized not in queued_list_urls) and _url_allowed(normalized, allowed_domains):
            list_queue.append(normalized)
            queued_list_urls.add(normalized)

    detail_queue: deque[Tuple[str, str, str, Dict[str, str]]] = deque()
    first_inline_download = True
    blocked_until: Optional[str] = None
    blocked_reason: Optional[str] = None
    consecutive_page_failures = 0
    consecutive_inline_image_failures = 0

    temp_root = SCRIPT_DIR / "_tmp_browser_crawl"
    temp_root.mkdir(parents=True, exist_ok=True)
    downloader = ImageDownloader(
        save_dir=str(temp_root),
        interval_min=max(0.1, float(interval_min)),
        interval_max=max(float(interval_min), float(interval_max)),
        timeout=timeout_seconds,
        max_retries=max(1, int(crawl_cfg.get("retry_times", 3))),
        use_browser=True,
        downloaded_urls=set(),
        turbo_mode=True,
        browser_engine=browser_engine,
        disable_page_images=True,
    )

    def _maybe_sleep_between_pages() -> None:
        nonlocal fetch_count
        if fetch_count > 0 and interval_max > 0:
            time.sleep(random.uniform(interval_min, interval_max))
        fetch_count += 1

    def _record_failure(url: str, reason: str, context: Dict[str, Any]) -> None:
        metrics["failures"] += 1
        append_jsonl(
            failures_path,
            {
                "failed_at": utc_now_iso(),
                "url": url,
                "reason": reason,
                "context": context,
            },
        )

    def _activate_backoff(reason: str, url: str, phase: str) -> None:
        nonlocal blocked_until, blocked_reason
        if blocked_until:
            return
        metrics["blocked_stops"] += 1
        blocked_until = (
            datetime.now(timezone.utc).replace(microsecond=0)
            + timedelta(hours=backoff_hours)
        ).isoformat()
        blocked_reason = f"{reason}_at_{url}"
        save_json(
            backoff_path,
            {
                "blocked_until": blocked_until,
                "blocked_reason": blocked_reason,
                "phase": phase,
                "recorded_at": utc_now_iso(),
            },
        )

    try:
        downloader._init_browser()  # type: ignore[attr-defined]

        while list_queue:
            if blocked_until:
                break
            list_url = list_queue.popleft()
            if list_url in seen_list_urls:
                continue
            seen_list_urls.add(list_url)

            if not _url_allowed(list_url, allowed_domains):
                continue

            _maybe_sleep_between_pages()
            ok, html_payload, error = _fetch_html_via_browser(
                downloader,
                url=list_url,
                timeout_seconds=timeout_seconds,
            )
            if not ok:
                consecutive_page_failures += 1
                blocked_tag = _classify_browser_blocked_reason(
                    error_text=error,
                    html_payload=html_payload,
                    blocked_statuses=blocked_statuses,
                )
                if (not blocked_tag) and (consecutive_page_failures >= suspect_failures_threshold):
                    blocked_tag = "suspected_block_consecutive_page_failures"
                if blocked_tag:
                    _activate_backoff(blocked_tag, list_url, phase="list")
                _record_failure(
                    list_url,
                    f"list_browser_fetch_failed:{error}",
                    {"phase": "list"},
                )
                if blocked_tag:
                    break
                continue
            consecutive_page_failures = 0

            if snapshot_html:
                _save_snapshot_html(snapshots_dir, "list", list_url, html_payload)

            list_source = _build_selector_source_from_html(html_payload, selectors, phase="list")
            list_nodes = list_source.css(list_item_selector)
            for node in list_nodes:
                metrics["list_rows_seen"] += 1
                name = _extract_first(node, selectors.get("name"))
                detail_link = _extract_first(node, selectors.get("detail_link"))
                detail_url = urljoin(list_url, detail_link) if detail_link else ""
                list_fields: Dict[str, str] = {}
                for key, selector in dict(selectors.get("list_fields", {})).items():
                    field_key = str(key).strip()
                    if not field_key:
                        continue
                    field_value = normalize_optional_field(_extract_first(node, selector))
                    if field_value:
                        list_fields[field_key] = field_value

                list_record = {
                    "scraped_at": utc_now_iso(),
                    "list_url": list_url,
                    "name": name,
                    "detail_url": detail_url,
                    "fields": list_fields,
                }
                append_jsonl(list_path, list_record)

                if not detail_url:
                    metrics["missing_required_items"] += 1
                    append_jsonl(
                        review_path,
                        {
                            "scraped_at": utc_now_iso(),
                            "reason": "missing_detail_url_from_list",
                            "record": list_record,
                        },
                    )
                    continue

                if not _url_allowed(detail_url, allowed_domains):
                    continue

                if detail_url in known_detail_urls:
                    metrics["detail_duplicates_skipped"] += 1
                    continue

                known_detail_urls.add(detail_url)
                metrics["detail_requests_enqueued"] += 1
                detail_queue.append((detail_url, name, list_url, list_fields))

            for next_raw in _iter_next_pages_from_source(list_source, selectors):
                next_url = urljoin(list_url, next_raw)
                if not _url_allowed(next_url, allowed_domains):
                    continue
                if (next_url in seen_list_urls) or (next_url in queued_list_urls):
                    continue
                queued_list_urls.add(next_url)
                list_queue.append(next_url)

        while detail_queue:
            if blocked_until:
                break
            detail_url, seed_name, list_url, seed_fields = detail_queue.popleft()
            if not _url_allowed(detail_url, allowed_domains):
                continue

            _maybe_sleep_between_pages()
            ok, html_payload, error = _fetch_html_via_browser(
                downloader,
                url=detail_url,
                timeout_seconds=timeout_seconds,
            )
            if not ok:
                consecutive_page_failures += 1
                blocked_tag = _classify_browser_blocked_reason(
                    error_text=error,
                    html_payload=html_payload,
                    blocked_statuses=blocked_statuses,
                )
                if (not blocked_tag) and (consecutive_page_failures >= suspect_failures_threshold):
                    blocked_tag = "suspected_block_consecutive_page_failures"
                if blocked_tag:
                    _activate_backoff(blocked_tag, detail_url, phase="detail")
                _record_failure(
                    detail_url,
                    f"detail_browser_fetch_failed:{error}",
                    {"phase": "detail", "list_url": list_url},
                )
                if blocked_tag:
                    break
                continue
            consecutive_page_failures = 0

            if snapshot_html:
                _save_snapshot_html(snapshots_dir, "detail", detail_url, html_payload)

            detail_source = _build_selector_source_from_html(html_payload, selectors, phase="detail")
            detail_name = _extract_first(detail_source, selectors.get("detail_name"))
            name = detail_name or _normalize_text(seed_name)
            image_raw = _extract_first(detail_source, selectors.get("detail_image"))
            image_url = urljoin(detail_url, image_raw) if image_raw else ""

            gender_text = _extract_first(detail_source, selectors.get("detail_gender"))
            gender_lookup = _normalize_text(gender_text)
            mapped_gender = gender_map.get(gender_text)
            if mapped_gender is None and gender_lookup:
                mapped_gender = gender_map.get(gender_lookup)
            gender = normalize_gender(mapped_gender if mapped_gender is not None else gender_text) or default_gender
            summary = _extract_joined_text(detail_source, selectors.get("detail_summary"))
            full_content = _extract_full_content_text(detail_source, selectors)

            base_fields: Dict[str, str] = {}
            if isinstance(seed_fields, dict):
                for key, value in seed_fields.items():
                    field_key = str(key).strip()
                    field_value = normalize_optional_field(value)
                    if field_key and field_value:
                        base_fields[field_key] = field_value

            extra_fields: Dict[str, str] = {}
            for key, selector in dict(selectors.get("detail_fields", {})).items():
                field_key = str(key).strip()
                if not field_key:
                    continue
                field_value = normalize_optional_field(_extract_first(detail_source, selector))
                if field_value:
                    extra_fields[field_key] = field_value

            merged_fields: Dict[str, str] = dict(base_fields)
            for key, value in extra_fields.items():
                if value:
                    merged_fields[key] = value

            mapped_fields = _apply_field_map(
                field_map,
                name=name,
                gender=gender,
                summary=summary,
                full_content=full_content,
                detail_url=detail_url,
                list_url=list_url,
                image_url=image_url,
                fields=merged_fields,
            )

            record = {
                "scraped_at": utc_now_iso(),
                "name": name,
                "detail_url": detail_url,
                "list_url": list_url,
                "image_url": image_url,
                "gender": gender,
                "gender_raw": gender_text,
                "summary": summary,
                "full_content": full_content,
                "fields": merged_fields,
                "mapped": mapped_fields,
            }
            append_jsonl(profile_path, record)
            metrics["detail_pages_saved"] += 1

            if inline_download_enabled and image_url:
                metrics["inline_image_candidates"] += 1
                if image_url in url_index:
                    metrics["inline_image_reused_by_url"] += 1
                    consecutive_inline_image_failures = 0
                else:
                    if not first_inline_download:
                        time.sleep(random.uniform(interval_min, interval_max))
                    first_inline_download = False
                    ok_img, payload_img, content_type_img, error_img = _download_image_with_d2i_browser(
                        image_url=image_url,
                        detail_url=detail_url,
                        timeout_seconds=timeout_seconds,
                        max_retries=max(1, int(crawl_cfg.get("retry_times", 3))),
                        interval_min=interval_min,
                        interval_max=interval_max,
                        browser_engine=browser_engine,
                        downloader=downloader,
                    )
                    if not ok_img:
                        consecutive_inline_image_failures += 1
                        blocked_tag = _classify_browser_blocked_reason(
                            error_text=error_img,
                            html_payload="",
                            blocked_statuses=blocked_statuses,
                        )
                        if (not blocked_tag) and (consecutive_inline_image_failures >= suspect_failures_threshold):
                            blocked_tag = "suspected_block_consecutive_image_failures"
                        if blocked_tag:
                            _activate_backoff(blocked_tag, image_url, phase="image_inline")
                        metrics["inline_image_failed"] += 1
                        append_jsonl(
                            review_path,
                            {
                                "scraped_at": utc_now_iso(),
                                "reason": "image_download_browser_inline_failed",
                                "image_url": image_url,
                                "detail_url": detail_url,
                                "error": error_img,
                            },
                        )
                        if blocked_tag:
                            break
                    elif not _looks_like_image_payload(content_type_img, payload_img):
                        consecutive_inline_image_failures += 1
                        challenge_payload = _looks_like_browser_challenge_payload(payload_img)
                        blocked_tag = ""
                        if challenge_payload:
                            blocked_tag = "browser_challenge_payload"
                        elif consecutive_inline_image_failures >= suspect_failures_threshold:
                            blocked_tag = "suspected_block_consecutive_image_failures"
                        if blocked_tag:
                            _activate_backoff(blocked_tag, image_url, phase="image_inline")
                        metrics["inline_image_failed"] += 1
                        append_jsonl(
                            review_path,
                            {
                                "scraped_at": utc_now_iso(),
                                "reason": "image_download_browser_inline_not_image",
                                "image_url": image_url,
                                "detail_url": detail_url,
                                "size": len(payload_img),
                            },
                        )
                        if blocked_tag:
                            break
                    else:
                        consecutive_inline_image_failures = 0
                        sha = hashlib.sha256(payload_img).hexdigest()
                        if sha in sha_index:
                            metrics["inline_image_reused_by_sha"] += 1
                            url_index[image_url] = sha
                        else:
                            ext = _guess_extension(image_url, content_type_img)
                            target = image_root / sha[:2] / f"{sha}{ext}"
                            target.parent.mkdir(parents=True, exist_ok=True)
                            target.write_bytes(payload_img)
                            sha_index[sha] = str(target.resolve())
                            url_index[image_url] = sha
                            metrics["inline_image_downloaded_new"] += 1
                            append_jsonl(
                                download_manifest,
                                {
                                    "downloaded_at": utc_now_iso(),
                                    "detail_url": detail_url,
                                    "image_url": image_url,
                                    "name": name,
                                    "sha256": sha,
                                    "saved_path": str(target.resolve()),
                                    "route": "browser_inline",
                                },
                            )

            missing_fields = [field for field in required_fields if not record.get(field)]
            if missing_fields:
                metrics["missing_required_items"] += 1
                append_jsonl(
                    review_path,
                    {
                        "scraped_at": utc_now_iso(),
                        "reason": "missing_required_fields",
                        "missing_fields": missing_fields,
                        "record": record,
                    },
                )
    finally:
        try:
            downloader._close_browser()  # type: ignore[attr-defined]
        except Exception:
            pass

    if inline_download_enabled:
        save_json(url_index_path, url_index)
        save_json(sha_index_path, sha_index)
        save_json(
            reports_dir / "image_download_report.json",
            {
                "generated_at": utc_now_iso(),
                "crawl_mode": "browser",
                "inline_download_enabled": True,
                "browser_engine": browser_engine,
                "totals": {
                    "candidates": metrics["inline_image_candidates"],
                    "downloaded_new": metrics["inline_image_downloaded_new"],
                    "reused_by_url": metrics["inline_image_reused_by_url"],
                    "reused_by_sha": metrics["inline_image_reused_by_sha"],
                    "failed": metrics["inline_image_failed"],
                },
            },
        )

    report = {
        "closed_at": utc_now_iso(),
        "close_reason": "browser_mode_blocked" if blocked_until else "browser_mode_finished",
        "blocked_until": blocked_until,
        "blocked_reason": blocked_reason,
        "crawl_mode": "browser",
        "browser_engine": browser_engine,
        "interval_min_seconds": interval_min,
        "interval_max_seconds": interval_max,
        "inline_download_enabled": inline_download_enabled,
        "metrics_this_run": metrics,
        "totals_on_disk": {
            "list_records": count_jsonl(list_path),
            "profiles": count_jsonl(profile_path),
            "review_queue": count_jsonl(review_path),
            "failures": count_jsonl(failures_path),
        },
    }
    save_json(crawl_report_path, report)


def _is_probable_jsl_challenge(response: requests.Response) -> bool:
    text = response.text or ""
    if "<script" not in text.lower():
        return False
    return (
        ("document.cookie" in text)
        or ("__jsl_clearance_s" in text)
        or ("go({" in text and '"ct":' in text and '"chars":' in text)
    )


def _solve_jsl_clearance_cookie(url: str, payload: str) -> str:
    script_match = re.search(r"<script[^>]*>(.*?)</script>", payload or "", flags=re.S | re.I)
    if not script_match:
        return ""
    script = script_match.group(1)

    parsed = urlparse(url)
    path = parsed.path or "/"
    search = f"?{parsed.query}" if parsed.query else ""

    node_code = (
        "let assigned='';\n"
        "const document={_cookie:'',set cookie(v){assigned=v;this._cookie=v;},get cookie(){return this._cookie;}};\n"
        f"const location={{pathname:{json.dumps(path)},search:{json.dumps(search)},href:''}};\n"
        "const window={location,navigator:{userAgent:'Mozilla/5.0'},outerHeight:1000,innerHeight:1000,"
        "outerWidth:1200,innerWidth:1200,Firebug:false,_phantom:false,__phantomas:false,chrome:{runtime:{}}};\n"
        "global.window=window;global.document=document;global.location=location;\n"
        "global.alert=function(){};global.setTimeout=function(fn,ms){fn();return 0;};\n"
        f"{script}\n"
        "console.log((assigned||document._cookie||'').split(';')[0]);\n"
    )

    try:
        proc = subprocess.run(
            ["node", "-"],
            input=node_code.encode("utf-8"),
            capture_output=True,
            timeout=20,
        )
    except Exception:
        return ""
    if proc.returncode != 0:
        return ""

    lines = proc.stdout.decode("utf-8", errors="ignore").strip().splitlines()
    if not lines:
        return ""
    first = lines[0].strip().split(";", 1)[0].strip()
    if "=" not in first:
        return ""
    return first


def _apply_cookie_pair_to_session(session: requests.Session, url: str, cookie_pair: str) -> None:
    pair = str(cookie_pair or "").strip().split(";", 1)[0].strip()
    if not pair or "=" not in pair:
        return
    key, value = pair.split("=", 1)
    host = (urlparse(url).hostname or "").strip()
    if not host:
        return
    session.cookies.set(key.strip(), value.strip(), domain=host, path="/")


def _request_with_optional_jsl(
    *,
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    timeout_seconds: int,
    enable_jsl: bool,
    jsl_max_retries: int,
) -> requests.Response:
    max_attempts = max(1, int(jsl_max_retries)) + 1
    last_response: Optional[requests.Response] = None
    for _ in range(max_attempts):
        response = session.get(url, timeout=timeout_seconds, headers=headers)
        last_response = response
        if (not enable_jsl) or (not _is_probable_jsl_challenge(response)):
            return response
        cookie_pair = _solve_jsl_clearance_cookie(url, response.text or "")
        if not cookie_pair:
            return response
        _apply_cookie_pair_to_session(session, url, cookie_pair)
    return last_response if last_response is not None else session.get(url, timeout=timeout_seconds, headers=headers)


def _looks_like_image_payload(content_type: str, payload: bytes) -> bool:
    ct = str(content_type or "").lower()
    if ct.startswith("image/"):
        return True
    if not payload:
        return False
    if payload.startswith(b"\xff\xd8\xff"):  # JPEG
        return True
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):  # PNG
        return True
    if payload.startswith(b"GIF87a") or payload.startswith(b"GIF89a"):  # GIF
        return True
    if payload.startswith(b"RIFF") and (len(payload) >= 12) and (payload[8:12] == b"WEBP"):  # WEBP
        return True
    if payload.startswith(b"BM"):  # BMP
        return True
    return False


def _download_image_with_d2i_browser(
    *,
    image_url: str,
    detail_url: str,
    timeout_seconds: int,
    max_retries: int,
    interval_min: float,
    interval_max: float,
    browser_engine: str,
    downloader: Optional[Any] = None,
) -> Tuple[bool, bytes, str, str]:
    if not HAS_D2I_DOWNLOADER:
        return False, b"", "", f"d2i_downloader_unavailable: {D2I_DOWNLOADER_ERROR}"

    temp_root = SCRIPT_DIR / "_tmp_browser_downloads"
    temp_root.mkdir(parents=True, exist_ok=True)

    own_downloader = downloader is None
    if downloader is None:
        downloader = ImageDownloader(
            save_dir=str(temp_root),
            interval_min=max(0.1, float(interval_min)),
            interval_max=max(float(interval_min), float(interval_max)),
            timeout=max(5, int(timeout_seconds)),
            max_retries=max(1, int(max_retries)),
            use_browser=True,
            downloaded_urls=set(),
            turbo_mode=True,
            browser_engine=browser_engine,
        )

    last_error = ""
    tmp_path = temp_root / f"{hashlib.sha1(image_url.encode('utf-8')).hexdigest()}.tmp"
    try:
        for _ in range(max(1, int(max_retries))):
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
                downloader._download_image(image_url, str(tmp_path))  # type: ignore[attr-defined]
                if tmp_path.exists():
                    payload = tmp_path.read_bytes()
                    if payload:
                        return True, payload, "", ""
                    last_error = "empty_payload"
                else:
                    last_error = "browser_no_output_file"
            except Exception as exc:
                last_error = str(exc)
        return False, b"", "", last_error or "browser_download_failed"
    finally:
        if own_downloader:
            try:
                downloader._close_browser()  # type: ignore[attr-defined]
            except Exception:
                pass
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def download_images(config: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    rules = dict(config.get("rules", {}))
    crawl_cfg = dict(config.get("crawl", {}))
    if not bool(rules.get("extract_images", True)):
        return {"enabled": False}

    profiles_path = output_root / "raw" / "profiles.jsonl"
    if not profiles_path.exists():
        return {"enabled": True, "profiles_missing": True}

    download_root = output_root / "downloads"
    image_root = download_root / "images"
    state_root = output_root / "state"
    reports_root = output_root / "reports"
    download_manifest = download_root / "image_downloads.jsonl"
    review_path = output_root / "raw" / "review_queue.jsonl"
    queue_path = output_root / "raw" / "metadata_queue.jsonl"
    metadata_results_path = output_root / "raw" / "metadata_write_results.jsonl"
    metadata_report_path = reports_root / "metadata_write_report.json"

    url_index_path = state_root / "image_url_index.json"
    sha_index_path = state_root / "image_sha_index.json"
    backoff_path = state_root / "backoff_state.json"

    url_index: Dict[str, str] = load_json(url_index_path, {})
    sha_index: Dict[str, str] = load_json(sha_index_path, {})

    blocked_statuses = {
        int(s)
        for s in crawl_cfg.get("blocked_statuses", [403, 429])
        if str(s).strip().isdigit()
    }
    timeout_seconds = int(crawl_cfg.get("timeout_seconds", 30))
    interval_min = float(
        crawl_cfg.get(
            "interval_min_seconds",
            crawl_cfg.get("image_interval_min_seconds", crawl_cfg.get("download_delay", 5)),
        )
    )
    interval_max = float(
        crawl_cfg.get(
            "interval_max_seconds",
            crawl_cfg.get("image_interval_max_seconds", max(interval_min, 8)),
        )
    )
    if interval_min < 0.1:
        interval_min = 0.1
    if interval_max < interval_min:
        interval_max = interval_min
    backoff_hours = float(crawl_cfg.get("blocked_backoff_hours", 6))
    suspect_failures_threshold = max(2, int(crawl_cfg.get("suspect_block_consecutive_failures", 3)))
    user_agent = str(
        config.get(
            "user_agent",
            "D2ILiteArchiveBot/1.0 (+local archival use, contact: local-only)",
        )
    )
    image_headers_cfg = dict(config.get("image_headers", {}))
    image_headers_cfg = {str(k): str(v) for k, v in image_headers_cfg.items()}
    image_referer_from_detail = bool(rules.get("image_referer_from_detail_url", False))
    jsl_clearance_enabled = bool(rules.get("jsl_clearance_enabled", False))
    jsl_max_retries = max(1, int(rules.get("jsl_max_retries", 3)))
    image_download_mode = str(rules.get("image_download_mode", "requests_jsl")).strip().lower()
    browser_mode_selected = image_download_mode in {"browser", "d2i_browser"}
    browser_max_retries = max(1, int(rules.get("browser_max_retries", crawl_cfg.get("retry_times", 3))))
    browser_engine = str(rules.get("browser_engine", "auto")).strip().lower()
    if browser_engine not in {"auto", "edge", "chrome"}:
        browser_engine = "auto"
    inline_metadata_enabled = (
        bool(rules.get("write_metadata_inline_per_item", True))
        and bool(rules.get("write_metadata", True))
        and HAS_METADATA_WRITER
    )

    totals = {
        "profiles_seen": 0,
        "profiles_with_image_url": 0,
        "downloaded_new": 0,
        "reused_by_url": 0,
        "reused_by_sha": 0,
        "failed": 0,
        "blocked_stops": 0,
        "downloaded_via_browser": 0,
        "downloaded_via_requests": 0,
    }
    blocked_until = None
    blocked_reason = None
    consecutive_download_failures = 0
    first_download = True
    session = requests.Session()
    inline_metadata_totals = {
        "rows_seen": 0,
        "written_ok": 0,
        "skipped_existing_detail": 0,
        "skipped_missing_local_path": 0,
        "failed": 0,
        "copied_to_named_folder": 0,
    }
    inline_named_dir = resolve_named_output_dir(output_root, rules)
    inline_reserved_paths: set[str] = set()
    inline_existing_detail_urls: set[str] = set()
    inline_field_labels: Dict[str, str] = {}
    if inline_metadata_enabled:
        inline_named_dir.mkdir(parents=True, exist_ok=True)
        inline_field_labels = collect_detail_field_labels(config, rules)
        inline_existing_detail_urls = _load_existing_detail_urls(queue_path)
        for p in sorted(inline_named_dir.glob("*")):
            if p.is_file():
                inline_reserved_paths.add(str(p.resolve()))

    def _inline_write_for_profile(profile_row: Dict[str, Any], image_sha_value: str, source_path_value: str) -> str:
        if not inline_metadata_enabled:
            return ""
        detail_url = str(profile_row.get("detail_url", "")).strip()
        if not detail_url:
            return ""
        if detail_url in inline_existing_detail_urls:
            inline_metadata_totals["skipped_existing_detail"] += 1
            return ""

        source_norm = norm_abs_path(source_path_value)
        if (not source_norm) or (not Path(source_norm).exists()):
            inline_metadata_totals["skipped_missing_local_path"] += 1
            append_jsonl(
                review_path,
                {
                    "scraped_at": utc_now_iso(),
                    "reason": "metadata_missing_local_image_path",
                    "detail_url": detail_url,
                    "candidates": [source_path_value],
                },
            )
            return ""

        queue_row = build_metadata_queue_row_from_profile(profile_row, image_sha_value, source_norm)
        inline_metadata_totals["rows_seen"] += 1
        ok_meta, final_path, copied_flag = write_metadata_for_queue_row(
            row=queue_row,
            source_path=source_norm,
            named_dir=inline_named_dir,
            reserved_paths=inline_reserved_paths,
            field_labels=inline_field_labels,
            review_path=review_path,
            results_path=metadata_results_path,
        )
        if ok_meta:
            inline_metadata_totals["written_ok"] += 1
            if copied_flag:
                inline_metadata_totals["copied_to_named_folder"] += 1
            append_jsonl(queue_path, queue_row)
            inline_existing_detail_urls.add(detail_url)
            return norm_abs_path(final_path) or final_path
        inline_metadata_totals["failed"] += 1
        return ""

    browser_downloader: Optional[Any] = None
    if browser_mode_selected and HAS_D2I_DOWNLOADER:
        temp_root = SCRIPT_DIR / "_tmp_browser_downloads"
        temp_root.mkdir(parents=True, exist_ok=True)
        browser_downloader = ImageDownloader(
            save_dir=str(temp_root),
            interval_min=max(0.1, float(interval_min)),
            interval_max=max(float(interval_min), float(interval_max)),
            timeout=max(5, int(timeout_seconds)),
            max_retries=max(1, int(browser_max_retries)),
            use_browser=True,
            downloaded_urls=set(),
            turbo_mode=True,
            browser_engine=browser_engine,
        )

    def _activate_backoff(reason: str, url: str, phase: str) -> None:
        nonlocal blocked_until, blocked_reason
        if blocked_until:
            return
        totals["blocked_stops"] += 1
        blocked_reason = f"{reason}_at_{url}"
        blocked_until = (
            datetime.now(timezone.utc).replace(microsecond=0)
            + timedelta(hours=backoff_hours)
        ).isoformat()
        save_json(
            backoff_path,
            {
                "blocked_until": blocked_until,
                "blocked_reason": blocked_reason,
                "phase": phase,
                "recorded_at": utc_now_iso(),
            },
        )

    try:
        for profile in iter_jsonl(profiles_path):
            totals["profiles_seen"] += 1
            image_url = str(profile.get("image_url", "")).strip()
            if not image_url:
                continue
            totals["profiles_with_image_url"] += 1

            if image_url in url_index:
                totals["reused_by_url"] += 1
                if inline_metadata_enabled:
                    sha_cached = str(url_index.get(image_url, "")).strip()
                    source_cached = norm_abs_path(str(sha_index.get(sha_cached, ""))) if sha_cached else ""
                    _inline_write_for_profile(profile, sha_cached, source_cached)
                continue

            if not first_download:
                time.sleep(random.uniform(interval_min, interval_max))
            first_download = False

            detail_url = str(profile.get("detail_url", "")).strip()
            response_status = 200
            payload = b""
            content_type = ""
            route_used = "requests"

            if browser_mode_selected:
                route_used = "browser"
                ok, browser_payload, browser_content_type, browser_error = _download_image_with_d2i_browser(
                    image_url=image_url,
                    detail_url=detail_url,
                    timeout_seconds=timeout_seconds,
                    max_retries=browser_max_retries,
                    interval_min=interval_min,
                    interval_max=interval_max,
                    browser_engine=browser_engine,
                    downloader=browser_downloader,
                )
                if not ok:
                    consecutive_download_failures += 1
                    blocked_tag = _classify_browser_blocked_reason(
                        error_text=browser_error,
                        html_payload="",
                        blocked_statuses=blocked_statuses,
                    )
                    if (not blocked_tag) and (consecutive_download_failures >= suspect_failures_threshold):
                        blocked_tag = "suspected_block_consecutive_image_failures"
                    if blocked_tag:
                        _activate_backoff(blocked_tag, image_url, phase="image_download")
                    totals["failed"] += 1
                    append_jsonl(
                        review_path,
                        {
                            "scraped_at": utc_now_iso(),
                            "reason": "image_download_browser_failed",
                            "image_url": image_url,
                            "detail_url": detail_url,
                            "error": browser_error,
                        },
                    )
                    if blocked_tag:
                        break
                    continue
                consecutive_download_failures = 0
                payload = browser_payload
                content_type = browser_content_type
                response_status = 200
            else:
                try:
                    headers = {"User-Agent": user_agent}
                    headers.update(image_headers_cfg)
                    if image_referer_from_detail and detail_url:
                        headers["Referer"] = detail_url
                    response = _request_with_optional_jsl(
                        session=session,
                        url=image_url,
                        timeout_seconds=timeout_seconds,
                        headers=headers,
                        enable_jsl=jsl_clearance_enabled,
                        jsl_max_retries=jsl_max_retries,
                    )
                except requests.RequestException as exc:
                    consecutive_download_failures += 1
                    totals["failed"] += 1
                    append_jsonl(
                        review_path,
                        {
                            "scraped_at": utc_now_iso(),
                            "reason": "image_download_exception",
                            "image_url": image_url,
                            "detail_url": detail_url,
                            "error": str(exc),
                        },
                    )
                    if consecutive_download_failures >= suspect_failures_threshold:
                        _activate_backoff(
                            "suspected_block_consecutive_image_failures",
                            image_url,
                            phase="image_download",
                        )
                        break
                    continue

                payload = response.content or b""
                content_type = str(response.headers.get("content-type", "") or "")
                response_status = int(response.status_code)

            if (not browser_mode_selected) and (response_status in blocked_statuses):
                _activate_backoff(f"image_http_{response_status}", image_url, phase="image_download")
                break

            if response_status != 200 or (not payload):
                consecutive_download_failures += 1
                totals["failed"] += 1
                append_jsonl(
                    review_path,
                    {
                        "scraped_at": utc_now_iso(),
                        "reason": "image_download_http_error",
                        "image_url": image_url,
                        "detail_url": detail_url,
                        "status_code": response_status,
                        "route": route_used,
                    },
                )
                if consecutive_download_failures >= suspect_failures_threshold:
                    _activate_backoff(
                        "suspected_block_consecutive_image_failures",
                        image_url,
                        phase="image_download",
                    )
                    break
                continue

            if not _looks_like_image_payload(content_type, payload):
                consecutive_download_failures += 1
                browser_challenge_payload = route_used == "browser" and _looks_like_browser_challenge_payload(payload)
                if browser_challenge_payload:
                    _activate_backoff("browser_challenge_payload", image_url, phase="image_download")
                totals["failed"] += 1
                append_jsonl(
                    review_path,
                    {
                        "scraped_at": utc_now_iso(),
                        "reason": "image_download_not_image",
                        "image_url": image_url,
                        "detail_url": detail_url,
                        "status_code": response_status,
                        "content_type": content_type,
                        "size": len(payload),
                        "route": route_used,
                    },
                )
                if browser_challenge_payload:
                    break
                if consecutive_download_failures >= suspect_failures_threshold:
                    _activate_backoff(
                        "suspected_block_consecutive_image_failures",
                        image_url,
                        phase="image_download",
                    )
                    break
                continue

            consecutive_download_failures = 0
            sha = hashlib.sha256(payload).hexdigest()
            if sha in sha_index:
                totals["reused_by_sha"] += 1
                url_index[image_url] = sha
                if inline_metadata_enabled:
                    source_cached = norm_abs_path(str(sha_index.get(sha, "")))
                    _inline_write_for_profile(profile, sha, source_cached)
                continue

            ext = _guess_extension(image_url, content_type)
            target = image_root / sha[:2] / f"{sha}{ext}"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)

            sha_index[sha] = str(target.resolve())
            url_index[image_url] = sha
            totals["downloaded_new"] += 1
            if route_used == "browser":
                totals["downloaded_via_browser"] += 1
            else:
                totals["downloaded_via_requests"] += 1

            inline_named_path = ""
            if inline_metadata_enabled:
                inline_named_path = _inline_write_for_profile(profile, sha, str(target.resolve()))

            manifest_row = {
                "downloaded_at": utc_now_iso(),
                "detail_url": detail_url,
                "image_url": image_url,
                "name": profile.get("name", ""),
                "sha256": sha,
                "saved_path": str(target.resolve()),
                "route": route_used,
            }
            if inline_named_path:
                manifest_row["named_path"] = inline_named_path
            append_jsonl(download_manifest, manifest_row)
    finally:
        if browser_downloader is not None:
            try:
                browser_downloader._close_browser()  # type: ignore[attr-defined]
            except Exception:
                pass

    save_json(url_index_path, url_index)
    save_json(sha_index_path, sha_index)
    if inline_metadata_enabled:
        save_json(
            metadata_report_path,
            {
                "generated_at": utc_now_iso(),
                "enabled": True,
                "writer_available": True,
                "mode": "inline_per_item",
                "named_output_dir": str(inline_named_dir.resolve()),
                "totals": inline_metadata_totals,
            },
        )

    report = {
        "generated_at": utc_now_iso(),
        "totals": totals,
        "image_download_mode": image_download_mode,
        "browser_engine": browser_engine,
        "interval_min_seconds": interval_min,
        "interval_max_seconds": interval_max,
        "browser_mode_selected": browser_mode_selected,
        "browser_downloader_available": HAS_D2I_DOWNLOADER,
        "blocked_until": blocked_until,
        "blocked_reason": blocked_reason,
        "inline_metadata_enabled": inline_metadata_enabled,
        "inline_metadata_totals": inline_metadata_totals if inline_metadata_enabled else {},
    }
    save_json(reports_root / "image_download_report.json", report)
    return report


def build_metadata_queue(output_root: Path) -> Dict[str, Any]:
    profiles_path = output_root / "raw" / "profiles.jsonl"
    queue_path = output_root / "raw" / "metadata_queue.jsonl"
    url_index_path = output_root / "state" / "image_url_index.json"
    sha_index_path = output_root / "state" / "image_sha_index.json"

    url_index: Dict[str, str] = load_json(url_index_path, {})
    sha_index: Dict[str, str] = load_json(sha_index_path, {})

    existing = set()
    for row in iter_jsonl(queue_path):
        key = str(row.get("detail_url", "")).strip()
        if key:
            existing.add(key)

    added = 0
    for profile in iter_jsonl(profiles_path):
        detail_url = str(profile.get("detail_url", "")).strip()
        if not detail_url or detail_url in existing:
            continue
        image_url = str(profile.get("image_url", "")).strip()
        sha = url_index.get(image_url, "")
        local_path = sha_index.get(sha, "") if sha else ""
        row = {
            "created_at": utc_now_iso(),
            "name": profile.get("name", ""),
            "gender": normalize_gender(profile.get("gender", "")),
            "summary": _normalize_multiline_text(profile.get("summary", "")),
            "full_content": _normalize_multiline_text(
                profile.get("full_content", "") or profile.get("summary", "")
            ),
            "fields": profile.get("fields", {}),
            "mapped": profile.get("mapped", {}),
            "detail_url": detail_url,
            "source_url": profile.get("list_url", ""),
            "image_url": image_url,
            "image_sha256": sha,
            "local_image_path": local_path,
        }
        append_jsonl(queue_path, row)
        existing.add(detail_url)
        added += 1

    report = {
        "generated_at": utc_now_iso(),
        "queue_path": str(queue_path.resolve()),
        "total_rows": count_jsonl(queue_path),
        "rows_added_this_run": added,
    }
    save_json(output_root / "reports" / "metadata_queue_report.json", report)
    return report


def write_metadata_for_downloads(output_root: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    rules = dict(config.get("rules", {}))
    enabled = bool(rules.get("write_metadata", True))
    report_path = output_root / "reports" / "metadata_write_report.json"
    queue_path = output_root / "raw" / "metadata_queue.jsonl"
    review_path = output_root / "raw" / "review_queue.jsonl"
    results_path = output_root / "raw" / "metadata_write_results.jsonl"
    downloads_manifest_path = output_root / "downloads" / "image_downloads.jsonl"
    named_dir_cfg = str(rules.get("named_images_dir", "")).strip()
    if not named_dir_cfg:
        named_dir = output_root.resolve()
    else:
        named_dir = (output_root / named_dir_cfg).resolve() if not Path(named_dir_cfg).is_absolute() else Path(named_dir_cfg).resolve()

    if not enabled:
        report = {"generated_at": utc_now_iso(), "enabled": False}
        save_json(report_path, report)
        return report

    if not HAS_METADATA_WRITER:
        report = {
            "generated_at": utc_now_iso(),
            "enabled": True,
            "writer_available": False,
            "error": METADATA_WRITER_ERROR,
        }
        save_json(report_path, report)
        return report

    if not queue_path.exists():
        report = {
            "generated_at": utc_now_iso(),
            "enabled": True,
            "writer_available": True,
            "queue_missing": True,
        }
        save_json(report_path, report)
        return report

    rows = list(iter_jsonl(queue_path))
    named_dir.mkdir(parents=True, exist_ok=True)

    # Reserve existing final names to keep reruns idempotent.
    reserved_paths: set[str] = set()
    for row in rows:
        existing = norm_abs_path(str(row.get("local_image_path", "")))
        if not existing:
            continue
        existing_path = Path(existing)
        if existing_path.exists() and existing_path.parent.resolve() == named_dir:
            reserved_paths.add(str(existing_path.resolve()))

    detail_to_final_path: Dict[str, str] = {}
    sha_index_path = output_root / "state" / "image_sha_index.json"
    sha_index = load_json(sha_index_path, {})
    if not isinstance(sha_index, dict):
        sha_index = {}

    sha_runtime_path: Dict[str, str] = {
        str(k): norm_abs_path(str(v))
        for k, v in sha_index.items()
        if str(k).strip()
    }

    totals = {
        "rows_seen": 0,
        "written_ok": 0,
        "skipped_missing_local_path": 0,
        "failed": 0,
        "copied_to_named_folder": 0,
    }
    field_labels = collect_detail_field_labels(config, rules)

    for row in rows:
        totals["rows_seen"] += 1
        detail_url = str(row.get("detail_url", "")).strip()
        image_sha = str(row.get("image_sha256", "")).strip()
        local_path_candidates: List[str] = []
        row_local = norm_abs_path(str(row.get("local_image_path", "")))
        if row_local:
            local_path_candidates.append(row_local)
        if image_sha and image_sha in sha_runtime_path:
            local_path_candidates.append(norm_abs_path(sha_runtime_path[image_sha]))
        if image_sha and image_sha in sha_index:
            local_path_candidates.append(norm_abs_path(str(sha_index[image_sha])))

        source_path = ""
        for candidate in local_path_candidates:
            if candidate and Path(candidate).exists():
                source_path = candidate
                break

        if not source_path:
            totals["skipped_missing_local_path"] += 1
            append_jsonl(
                review_path,
                {
                    "scraped_at": utc_now_iso(),
                    "reason": "metadata_missing_local_image_path",
                    "detail_url": detail_url,
                    "candidates": local_path_candidates,
                },
            )
            continue

        ok, _final_path, copied_flag = write_metadata_for_queue_row(
            row=row,
            source_path=source_path,
            named_dir=named_dir,
            reserved_paths=reserved_paths,
            field_labels=field_labels,
            review_path=review_path,
            results_path=results_path,
            detail_to_final_path=detail_to_final_path,
            sha_runtime_path=sha_runtime_path,
        )
        if ok:
            totals["written_ok"] += 1
            if copied_flag:
                totals["copied_to_named_folder"] += 1
        else:
            totals["failed"] += 1

    # Persist updated queue with final named paths.
    write_jsonl(queue_path, rows)

    # Save runtime sha index for easier future lookups (path may now point to named file).
    if sha_runtime_path:
        save_json(sha_index_path, sha_runtime_path)

    # Keep original saved_path, add named_path for consumer use.
    if downloads_manifest_path.exists() and detail_to_final_path:
        manifest_rows = list(iter_jsonl(downloads_manifest_path))
        changed = False
        for item in manifest_rows:
            detail = str(item.get("detail_url", "")).strip()
            if detail and detail in detail_to_final_path:
                item["named_path"] = detail_to_final_path[detail]
                changed = True
        if changed:
            write_jsonl(downloads_manifest_path, manifest_rows)

    report = {
        "generated_at": utc_now_iso(),
        "enabled": True,
        "writer_available": True,
        "named_output_dir": str(named_dir),
        "totals": totals,
    }
    save_json(report_path, report)
    return report


def write_reconcile_report(output_root: Path) -> Dict[str, Any]:
    list_count = count_jsonl(output_root / "raw" / "list_records.jsonl")
    profile_count = count_jsonl(output_root / "raw" / "profiles.jsonl")
    review_count = count_jsonl(output_root / "raw" / "review_queue.jsonl")
    manifest_count = count_jsonl(output_root / "downloads" / "image_downloads.jsonl")
    failed_count = count_jsonl(output_root / "raw" / "failures.jsonl")

    report = {
        "generated_at": utc_now_iso(),
        "reconcile": {
            "list_records": list_count,
            "profiles": profile_count,
            "image_download_manifest_rows": manifest_count,
            "review_queue": review_count,
            "failures": failed_count,
        },
    }
    save_json(output_root / "reports" / "reconcile_report.json", report)
    return report


def write_delivery_record(output_root: Path, config: Dict[str, Any], reconcile_report: Dict[str, Any]) -> Path:
    rules = dict(config.get("rules", {}))
    named_dir = resolve_named_output_dir(output_root, rules)
    queue_path = output_root / "raw" / "metadata_queue.jsonl"
    review_path = output_root / "raw" / "review_queue.jsonl"
    failures_path = output_root / "raw" / "failures.jsonl"
    metadata_results_path = output_root / "raw" / "metadata_write_results.jsonl"
    download_manifest_path = output_root / "downloads" / "image_downloads.jsonl"
    images: List[Dict[str, Any]] = []

    if queue_path.exists():
        for row in iter_jsonl(queue_path):
            local_path = norm_abs_path(str(row.get("local_image_path", "")))
            if not local_path:
                continue
            p = Path(local_path)
            if not p.exists():
                continue
            item = {
                "file_name": p.name,
                "file_path": str(p.resolve()),
                "name": str(row.get("name", "")).strip(),
                "gender": normalize_gender(row.get("gender", "")),
                "detail_url": str(row.get("detail_url", "")).strip(),
                "source_url": str(row.get("source_url", "")).strip(),
                "image_url": str(row.get("image_url", "")).strip(),
            }
            images.append(item)

    if (not images) and named_dir.exists():
        for p in sorted(named_dir.glob("*")):
            if p.is_file():
                images.append(
                    {
                        "file_name": p.name,
                        "file_path": str(p.resolve()),
                        "name": p.stem,
                        "gender": "",
                        "detail_url": "",
                        "source_url": "",
                        "image_url": "",
                    }
                )

    review_items = list(iter_jsonl(review_path)) if review_path.exists() else []
    failure_items = list(iter_jsonl(failures_path)) if failures_path.exists() else []
    metadata_results = list(iter_jsonl(metadata_results_path)) if metadata_results_path.exists() else []
    download_manifest = list(iter_jsonl(download_manifest_path)) if download_manifest_path.exists() else []

    named_files = []
    if named_dir.exists():
        named_files = [str(p.resolve()) for p in sorted(named_dir.glob("*")) if p.is_file()]

    expected_count = int(
        (reconcile_report or {}).get("reconcile", {}).get("image_download_manifest_rows", len(download_manifest))
    )
    validation = {
        "expected_image_rows": expected_count,
        "actual_named_images": len(named_files),
        "review_items": len(review_items),
        "failures": len(failure_items),
        "status": "ok"
        if (len(named_files) >= expected_count and len(failure_items) == 0)
        else "check_required",
    }

    template_snapshot = json.loads(json.dumps(config, ensure_ascii=False))
    template_snapshot.pop("output_root", None)
    template_rules = dict(template_snapshot.get("rules", {}))
    template_rules.pop("record_root", None)
    template_rules.pop("final_output_root", None)
    template_rules.pop("cleanup_paths", None)
    template_snapshot["rules"] = template_rules

    record = {
        "generated_at": utc_now_iso(),
        "site_name": str(config.get("site_name", "")).strip(),
        "start_urls": [str(x) for x in config.get("start_urls", [])],
        "named_output_dir": str(named_dir),
        "image_count": len(images),
        "images": images,
        "reconcile": dict(reconcile_report.get("reconcile", {})) if isinstance(reconcile_report, dict) else {},
        "validation": validation,
        "template_snapshot": template_snapshot,
        "template_origin": {
            "source_path": str(rules.get("template_source_path", "")).strip(),
            "generated_path": str(rules.get("generated_template_path", "")).strip(),
        },
        "trace": {
            "download_manifest": download_manifest,
            "metadata_write_results": metadata_results,
            "review_queue": review_items,
            "failures": failure_items,
        },
    }
    record_path = output_root / "crawl_record.json"
    save_json(record_path, record)
    return record_path


def cleanup_intermediate_outputs(output_root: Path, config: Dict[str, Any], record_path: Path) -> Dict[str, Any]:
    rules = dict(config.get("rules", {}))
    mode = str(rules.get("output_mode", "")).strip().lower()
    if mode not in {"images_only", "images_only_with_record"}:
        return {"mode": mode or "full", "cleaned": False}

    can_run, blocked_until = check_backoff(output_root)
    if not can_run:
        backoff_payload = load_json(output_root / "state" / "backoff_state.json", {})
        blocked_reason = ""
        if isinstance(backoff_payload, dict):
            blocked_reason = str(backoff_payload.get("blocked_reason", "")).strip()
        return {
            "mode": mode,
            "cleaned": False,
            "skipped_due_to_backoff": True,
            "blocked_until": blocked_until,
            "blocked_reason": blocked_reason,
        }

    # Keep intermediate evidence when there are unresolved issues.
    failures_count = count_jsonl(output_root / "raw" / "failures.jsonl")
    review_count = count_jsonl(output_root / "raw" / "review_queue.jsonl")
    if failures_count > 0 or review_count > 0:
        return {
            "mode": mode,
            "cleaned": False,
            "skipped_due_to_failures": True,
            "failures": failures_count,
            "review_items": review_count,
        }

    named_dir = resolve_named_output_dir(output_root, rules)
    keep_record = mode == "images_only_with_record"
    removed: List[str] = []
    skipped: List[str] = []

    def _is_parent(parent: Path, child: Path) -> bool:
        try:
            child.resolve().relative_to(parent.resolve())
            return True
        except Exception:
            return False

    def _safe_remove_path(path: Path) -> None:
        if not path.exists():
            return
        # Never remove target folder that contains final named images.
        if (path.resolve() == named_dir.resolve()) or _is_parent(path, named_dir):
            skipped.append(str(path.resolve()))
            return
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=False)
            else:
                path.unlink()
            removed.append(str(path.resolve()))
        except Exception:
            skipped.append(str(path.resolve()))

    _safe_remove_path(output_root / "raw")
    _safe_remove_path(output_root / "state")
    _safe_remove_path(output_root / "reports")
    _safe_remove_path(output_root / "downloads" / "images")
    _safe_remove_path(output_root / "downloads" / "image_downloads.jsonl")
    _safe_remove_path(output_root / "crawl_run_summary.json")

    if (not keep_record) and record_path.exists():
        _safe_remove_path(record_path)

    cleanup_paths = rules.get("cleanup_paths", [])
    if isinstance(cleanup_paths, list):
        for raw_path in cleanup_paths:
            path_text = str(raw_path or "").strip()
            if not path_text:
                continue
            extra = Path(path_text)
            if not extra.is_absolute():
                extra = (output_root / extra).resolve()
            _safe_remove_path(extra)

    # Keep output root clean if downloads directory is now empty.
    downloads_dir = output_root / "downloads"
    if downloads_dir.exists():
        try:
            if (not any(downloads_dir.iterdir())):
                downloads_dir.rmdir()
                removed.append(str(downloads_dir.resolve()))
        except Exception:
            pass

    return {
        "mode": mode,
        "cleaned": True,
        "removed": removed,
        "skipped": skipped,
        "named_output_dir": str(named_dir),
        "record_path": str(record_path.resolve()) if keep_record else "",
    }


def run_crawl(config: Dict[str, Any], output_root: Path) -> None:
    rules = dict(config.get("rules", {}))
    image_download_mode = str(rules.get("image_download_mode", "requests_jsl")).strip().lower()
    if image_download_mode in {"browser", "d2i_browser"}:
        run_crawl_browser_mode(config, output_root)
        return

    settings = build_scrapy_settings(config, output_root)
    process = CrawlerProcess(settings=settings)
    process.crawl(PublicProfileSpider, config=config, output_dir=str(output_root))
    process.start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Public profile scraper for local archival use (slow + resumable + review queue)."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to JSON config file.",
    )
    parser.add_argument(
        "--output-root",
        default="",
        help="Override output root directory.",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip image downloading phase.",
    )
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        help="Skip crawl phase and only run download/reconcile from existing raw data.",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip metadata writing phase for downloaded images.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (project_root / config_path).resolve()
    config = load_config(config_path)

    output_root = Path(args.output_root).resolve() if args.output_root else resolve_output_root(config, project_root)
    output_root = resolve_output_root_with_unit_subdir(config, output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    active_config = config

    def _runtime_flags(cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], str, bool]:
        cfg_rules = dict(cfg.get("rules", {}))
        mode = str(cfg_rules.get("image_download_mode", "requests_jsl")).strip().lower()
        inline = mode in {"browser", "d2i_browser"} and bool(cfg_rules.get("download_images_during_crawl", True))
        return cfg_rules, mode, inline

    def _build_browser_fallback_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
        payload = json.loads(json.dumps(cfg, ensure_ascii=False))
        cfg_rules = payload.get("rules")
        if not isinstance(cfg_rules, dict):
            cfg_rules = {}
        cfg_rules["image_download_mode"] = "browser"
        cfg_rules["download_images_during_crawl"] = True
        if str(cfg_rules.get("browser_engine", "")).strip().lower() not in {"auto", "edge", "chrome"}:
            cfg_rules["browser_engine"] = "edge"
        payload["rules"] = cfg_rules
        return payload

    rules, image_download_mode, crawl_inline_download = _runtime_flags(active_config)
    auto_browser_fallback = bool(rules.get("auto_fallback_to_browser", True))
    fallback_events: List[Dict[str, str]] = []

    can_run, blocked_until = check_backoff(output_root)
    if not can_run:
        print(f"[STOP] backoff active until {blocked_until}, skip this run.")
        return 2

    clear_backoff(output_root)

    blocked_during_run = False
    blocked_after_stage_until = ""
    blocked_after_stage_reason = ""
    inline_metadata_done = False

    if not args.skip_crawl:
        crawl_error: Optional[Exception] = None
        try:
            run_crawl(active_config, output_root)
        except Exception as exc:
            crawl_error = exc
        if (crawl_error is not None) and auto_browser_fallback and (image_download_mode not in {"browser", "d2i_browser"}):
            print(f"[INFO] quick crawl failed ({crawl_error}), fallback to browser mode.")
            clear_backoff(output_root)
            active_config = _build_browser_fallback_config(active_config)
            rules, image_download_mode, crawl_inline_download = _runtime_flags(active_config)
            fallback_events.append(
                {
                    "stage": "crawl",
                    "reason": "crawl_exception",
                    "from_mode": "requests_jsl",
                    "to_mode": "browser",
                }
            )
            run_crawl(active_config, output_root)
            crawl_error = None
        if crawl_error is not None:
            raise crawl_error

        can_continue, blocked_after_stage_until = check_backoff(output_root)
        if (
            (not can_continue)
            and auto_browser_fallback
            and (image_download_mode not in {"browser", "d2i_browser"})
        ):
            backoff_payload = load_json(output_root / "state" / "backoff_state.json", {})
            blocked_after_stage_reason = (
                str(backoff_payload.get("blocked_reason", "")).strip() if isinstance(backoff_payload, dict) else ""
            )
            print(
                "[INFO] quick crawl triggered backoff, fallback to browser mode and retry crawl."
            )
            clear_backoff(output_root)
            active_config = _build_browser_fallback_config(active_config)
            rules, image_download_mode, crawl_inline_download = _runtime_flags(active_config)
            fallback_events.append(
                {
                    "stage": "crawl",
                    "reason": blocked_after_stage_reason or "backoff",
                    "from_mode": "requests_jsl",
                    "to_mode": "browser",
                }
            )
            run_crawl(active_config, output_root)
            can_continue, blocked_after_stage_until = check_backoff(output_root)

        if not can_continue:
            blocked_during_run = True
            backoff_payload = load_json(output_root / "state" / "backoff_state.json", {})
            if isinstance(backoff_payload, dict):
                blocked_after_stage_reason = str(backoff_payload.get("blocked_reason", "")).strip()
            print(
                "[STOP] blocked detected after crawl, pause current run until "
                f"{blocked_after_stage_until}."
            )

    if (not args.skip_images) and (not blocked_during_run):
        skip_download_stage = crawl_inline_download and (not args.skip_crawl)
        if skip_download_stage:
            print(
                "[INFO] image download stage skipped: browser crawl already downloaded images inline."
            )
        else:
            download_report: Dict[str, Any] = {}
            image_error: Optional[Exception] = None
            try:
                download_report = download_images(active_config, output_root)
            except Exception as exc:
                image_error = exc
            if (image_error is not None) and auto_browser_fallback and (image_download_mode not in {"browser", "d2i_browser"}):
                print(f"[INFO] quick image download failed ({image_error}), fallback to browser mode.")
                clear_backoff(output_root)
                active_config = _build_browser_fallback_config(active_config)
                rules, image_download_mode, crawl_inline_download = _runtime_flags(active_config)
                fallback_events.append(
                    {
                        "stage": "image_download",
                        "reason": "download_exception",
                        "from_mode": "requests_jsl",
                        "to_mode": "browser",
                    }
                )
                download_report = download_images(active_config, output_root)
                image_error = None
            if image_error is not None:
                raise image_error

            if isinstance(download_report, dict):
                inline_metadata_done = bool(download_report.get("inline_metadata_enabled", False)) and (not args.skip_crawl)
            can_continue, blocked_after_stage_until = check_backoff(output_root)
            if (
                (not can_continue)
                and auto_browser_fallback
                and (image_download_mode not in {"browser", "d2i_browser"})
            ):
                backoff_payload = load_json(output_root / "state" / "backoff_state.json", {})
                blocked_after_stage_reason = (
                    str(backoff_payload.get("blocked_reason", "")).strip()
                    if isinstance(backoff_payload, dict)
                    else ""
                )
                print(
                    "[INFO] quick image download triggered backoff, fallback to browser mode and retry image stage."
                )
                clear_backoff(output_root)
                active_config = _build_browser_fallback_config(active_config)
                rules, image_download_mode, crawl_inline_download = _runtime_flags(active_config)
                fallback_events.append(
                    {
                        "stage": "image_download",
                        "reason": blocked_after_stage_reason or "backoff",
                        "from_mode": "requests_jsl",
                        "to_mode": "browser",
                    }
                )
                download_report = download_images(active_config, output_root)
                if isinstance(download_report, dict):
                    inline_metadata_done = bool(download_report.get("inline_metadata_enabled", False)) and (
                        not args.skip_crawl
                    )
                can_continue, blocked_after_stage_until = check_backoff(output_root)

            if not can_continue:
                blocked_during_run = True
                backoff_payload = load_json(output_root / "state" / "backoff_state.json", {})
                if isinstance(backoff_payload, dict):
                    blocked_after_stage_reason = str(backoff_payload.get("blocked_reason", "")).strip()
                print(
                    "[STOP] blocked detected during image download, pause current run until "
                    f"{blocked_after_stage_until}."
                )

    skip_metadata_stage = inline_metadata_done and (not args.skip_metadata)
    if skip_metadata_stage:
        print("[INFO] metadata stage skipped: per-item inline metadata already applied during download.")
    else:
        build_metadata_queue(output_root)
        if not args.skip_metadata:
            write_metadata_for_downloads(output_root, active_config)
    report = write_reconcile_report(output_root)
    record_path = write_delivery_record(output_root, active_config, report)
    cleanup_report = cleanup_intermediate_outputs(output_root, active_config, record_path)
    if isinstance(report, dict):
        report["delivery_record_path"] = str(record_path.resolve())
        report["cleanup"] = cleanup_report
        report["run_state"] = "paused_by_backoff" if blocked_during_run else "finished"
        report["backoff"] = {
            "active": blocked_during_run,
            "blocked_until": blocked_after_stage_until if blocked_during_run else "",
            "blocked_reason": blocked_after_stage_reason if blocked_during_run else "",
        }
        report["fallback"] = {
            "enabled": auto_browser_fallback,
            "used": bool(fallback_events),
            "events": fallback_events,
        }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 2 if blocked_during_run else 0


if __name__ == "__main__":
    sys.exit(main())
