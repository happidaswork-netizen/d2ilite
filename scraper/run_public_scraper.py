import argparse
import hashlib
import json
import mimetypes
import os
import random
import re
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
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
    settings = Settings()
    settings.set("ROBOTSTXT_OBEY", bool(config.get("rules", {}).get("obey_robots_txt", True)))
    settings.set("CONCURRENT_REQUESTS", int(crawl_cfg.get("concurrent_requests", 1)))
    settings.set("DOWNLOAD_DELAY", float(crawl_cfg.get("download_delay", 8)))
    settings.set("RANDOMIZE_DOWNLOAD_DELAY", True)
    settings.set("AUTOTHROTTLE_ENABLED", True)
    settings.set("AUTOTHROTTLE_START_DELAY", float(crawl_cfg.get("autothrottle_start_delay", 8)))
    settings.set("AUTOTHROTTLE_MAX_DELAY", float(crawl_cfg.get("autothrottle_max_delay", 20)))
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
    interval_min = float(crawl_cfg.get("image_interval_min_seconds", 8))
    interval_max = float(crawl_cfg.get("image_interval_max_seconds", 20))
    if interval_max < interval_min:
        interval_max = interval_min
    backoff_hours = float(crawl_cfg.get("blocked_backoff_hours", 6))
    user_agent = str(
        config.get(
            "user_agent",
            "D2ILiteArchiveBot/1.0 (+local archival use, contact: local-only)",
        )
    )
    image_headers_cfg = dict(config.get("image_headers", {}))
    image_headers_cfg = {str(k): str(v) for k, v in image_headers_cfg.items()}
    image_referer_from_detail = bool(rules.get("image_referer_from_detail_url", False))

    totals = {
        "profiles_seen": 0,
        "profiles_with_image_url": 0,
        "downloaded_new": 0,
        "reused_by_url": 0,
        "reused_by_sha": 0,
        "failed": 0,
        "blocked_stops": 0,
    }
    blocked_until = None
    blocked_reason = None
    first_download = True

    for profile in iter_jsonl(profiles_path):
        totals["profiles_seen"] += 1
        image_url = str(profile.get("image_url", "")).strip()
        if not image_url:
            continue
        totals["profiles_with_image_url"] += 1

        if image_url in url_index:
            totals["reused_by_url"] += 1
            continue

        if not first_download:
            time.sleep(random.uniform(interval_min, interval_max))
        first_download = False

        try:
            headers = {"User-Agent": user_agent}
            headers.update(image_headers_cfg)
            if image_referer_from_detail:
                detail_url = str(profile.get("detail_url", "")).strip()
                if detail_url:
                    headers["Referer"] = detail_url
            response = requests.get(
                image_url,
                timeout=timeout_seconds,
                headers=headers,
            )
        except requests.RequestException as exc:
            totals["failed"] += 1
            append_jsonl(
                review_path,
                {
                    "scraped_at": utc_now_iso(),
                    "reason": "image_download_exception",
                    "image_url": image_url,
                    "detail_url": profile.get("detail_url", ""),
                    "error": str(exc),
                },
            )
            continue

        if response.status_code in blocked_statuses:
            totals["blocked_stops"] += 1
            blocked_reason = f"image_http_{response.status_code}_at_{image_url}"
            blocked_until = (
                datetime.now(timezone.utc).replace(microsecond=0)
                + timedelta(hours=backoff_hours)
            ).isoformat()
            save_json(
                backoff_path,
                {
                    "blocked_until": blocked_until,
                    "blocked_reason": blocked_reason,
                    "recorded_at": utc_now_iso(),
                },
            )
            break

        if response.status_code != 200 or not response.content:
            totals["failed"] += 1
            append_jsonl(
                review_path,
                {
                    "scraped_at": utc_now_iso(),
                    "reason": "image_download_http_error",
                    "image_url": image_url,
                    "detail_url": profile.get("detail_url", ""),
                    "status_code": response.status_code,
                },
            )
            continue

        payload = response.content
        sha = hashlib.sha256(payload).hexdigest()
        if sha in sha_index:
            totals["reused_by_sha"] += 1
            url_index[image_url] = sha
            continue

        ext = _guess_extension(image_url, response.headers.get("content-type", ""))
        target = image_root / sha[:2] / f"{sha}{ext}"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

        sha_index[sha] = str(target.resolve())
        url_index[image_url] = sha
        totals["downloaded_new"] += 1

        append_jsonl(
            download_manifest,
            {
                "downloaded_at": utc_now_iso(),
                "detail_url": profile.get("detail_url", ""),
                "image_url": image_url,
                "name": profile.get("name", ""),
                "sha256": sha,
                "saved_path": str(target.resolve()),
            },
        )

    save_json(url_index_path, url_index)
    save_json(sha_index_path, sha_index)

    report = {
        "generated_at": utc_now_iso(),
        "totals": totals,
        "blocked_until": blocked_until,
        "blocked_reason": blocked_reason,
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
            "gender": profile.get("gender", "unknown"),
            "summary": profile.get("summary", ""),
            "fields": profile.get("fields", {}),
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
    named_dir_cfg = str(rules.get("named_images_dir", "downloads/named")).strip()
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

        fields = row.get("fields", {})
        if not isinstance(fields, dict):
            fields = {}

        title = str(row.get("name", "")).strip()
        summary = str(row.get("summary", "")).strip()
        source_list_url = str(row.get("source_url", "")).strip()
        image_url = str(row.get("image_url", "")).strip()
        position = str(fields.get("title", "")).strip()
        english_name = str(fields.get("english_name", "")).strip()
        location_text = str(fields.get("location_text", "")).strip()
        email_text = str(fields.get("email_text", "")).strip()
        location_clean = strip_prefixed_label(location_text, ["工作地点", "地点"])
        email_clean = strip_prefixed_label(email_text, ["邮箱", "Email", "email"])
        gender = str(row.get("gender", "unknown")).strip() or "unknown"

        # Build richer description for better inspiration reference and metadata search.
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
        if detail_url:
            desc_parts.append(f"详情页：{detail_url}")
        if source_list_url:
            desc_parts.append(f"列表页：{source_list_url}")
        if image_url:
            desc_parts.append(f"原图链接：{image_url}")
        if summary:
            desc_parts.append("简介：")
            desc_parts.append(summary)
        rich_description = "\n".join(desc_parts)

        keywords: List[str] = []
        for value in [title, english_name, position, location_clean or location_text, gender]:
            if value and value not in keywords:
                keywords.append(value)
        if detail_url and "detail-page" not in keywords:
            keywords.append("detail-page")
        if "public-archive" not in keywords:
            keywords.append("public-archive")

        # Final output name: by person name in a single folder.
        ext = ".jpg"
        current_local = norm_abs_path(str(row.get("local_image_path", "")))
        existing_local_path = Path(current_local) if current_local else None
        if existing_local_path and existing_local_path.exists() and existing_local_path.parent.resolve() == named_dir:
            final_path = existing_local_path.resolve()
            reserved_paths.add(str(final_path))
        else:
            final_path = unique_named_path(named_dir, title or "unnamed", ext=ext, reserved=reserved_paths)
            if Path(source_path).resolve() != final_path:
                shutil.copy2(source_path, final_path)
                totals["copied_to_named_folder"] += 1

        payload = {
            "title": title,
            "person": title,
            "name": title,
            "description": rich_description,
            "source": detail_url or source_list_url,
            "image_url": image_url,
            "url": image_url,
            "city": location_clean or location_text,
            "position": position,
            "gender": gender,
            "keywords": keywords,
            "role_aliases": [english_name] if english_name else [],
            "d2i_profile": {
                "gender": gender,
                "email": email_text,
                "english_name": english_name,
                "title": position,
                "location": location_clean or location_text,
                "source_detail_url": detail_url,
                "source_list_url": source_list_url,
            },
        }

        try:
            saved_path = str(write_xmp_metadata(str(final_path), payload))
            saved_path_norm = norm_abs_path(saved_path)
            row["local_image_path"] = saved_path_norm or str(final_path)
            row["file_name"] = Path(row["local_image_path"]).name
            row["file_dir"] = str(Path(row["local_image_path"]).parent.resolve())
            if detail_url:
                detail_to_final_path[detail_url] = row["local_image_path"]
            if image_sha:
                sha_runtime_path[image_sha] = row["local_image_path"]

            totals["written_ok"] += 1
            append_jsonl(
                results_path,
                {
                    "written_at": utc_now_iso(),
                    "detail_url": detail_url,
                    "input_path": source_path,
                    "output_path": row["local_image_path"],
                    "status": "ok",
                },
            )
        except Exception as exc:
            totals["failed"] += 1
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
                    "input_path": source_path,
                    "output_path": str(final_path),
                    "status": "failed",
                    "error": str(exc),
                },
            )

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


def run_crawl(config: Dict[str, Any], output_root: Path) -> None:
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
    output_root.mkdir(parents=True, exist_ok=True)

    can_run, blocked_until = check_backoff(output_root)
    if not can_run:
        print(f"[STOP] backoff active until {blocked_until}, skip this run.")
        return 2

    clear_backoff(output_root)

    if not args.skip_crawl:
        run_crawl(config, output_root)

    if not args.skip_images:
        download_images(config, output_root)

    build_metadata_queue(output_root)
    if not args.skip_metadata:
        write_metadata_for_downloads(output_root, config)
    report = write_reconcile_report(output_root)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
