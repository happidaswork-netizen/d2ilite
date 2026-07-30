# -*- coding: utf-8 -*-
"""Small, durable template registry for D2I Cloud.

Bundled templates remain read-only inside the release. AI/Hermes imports and
updates live under D2I_CLOUD_DATA_ROOT so a release switch never loses them.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from cloud.paths import PROJECT_ROOT, TEMPLATES_DIR, cloud_data_root
from cloud.speed_tiers import normalize_speed_tier


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
ALLOWED_MODES = {"requests_jsl", "browser", "d2i_browser"}
ALLOWED_SPEED_TIERS = {"safe", "standard", "turbo"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def runtime_templates_dir() -> Path:
    path = cloud_data_root() / "templates"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _registry_dir() -> Path:
    path = cloud_data_root() / "template_registry"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_template_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower().endswith(".json"):
        text = text[:-5].strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", text).strip(" ._")
    if not text or text in {".", ".."}:
        raise ValueError("template_id is required")
    return text[:160]


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temp, path)
    finally:
        try:
            if temp.exists():
                temp.unlink()
        except OSError:
            pass


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _registry_path(template_id: str) -> Path:
    return _registry_dir() / f"{_safe_template_id(template_id)}.json"


def _read_registry(template_id: str) -> Dict[str, Any]:
    return _read_json(_registry_path(template_id))


def _write_registry(template_id: str, record: Dict[str, Any]) -> None:
    payload = dict(record or {})
    payload["id"] = _safe_template_id(template_id)
    _atomic_write_json(_registry_path(template_id), payload)


def _template_roots() -> List[Tuple[str, Path]]:
    # Runtime comes first so an imported fix overrides the bundled copy.
    roots = [
        ("runtime", runtime_templates_dir()),
        ("bundled", TEMPLATES_DIR),
        ("bundled_config", PROJECT_ROOT / "scraper"),
    ]
    return [(kind, path) for kind, path in roots if path.is_dir()]


def list_template_files() -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    for source, root in _template_roots():
        for path in sorted(root.glob("*.json")):
            template_id = path.stem
            if template_id in by_id:
                continue
            payload = _read_json(path)
            crawl = payload.get("crawl") if isinstance(payload.get("crawl"), dict) else {}
            registry = _read_registry(template_id)
            start_urls = payload.get("start_urls") if isinstance(payload.get("start_urls"), list) else []
            allowed_domains = (
                payload.get("allowed_domains") if isinstance(payload.get("allowed_domains"), list) else []
            )
            outcomes = registry.get("outcomes") if isinstance(registry.get("outcomes"), list) else []
            by_id[template_id] = {
                "id": template_id,
                "path": str(path.resolve()),
                "name": str(payload.get("site_name", "") or template_id).strip(),
                "filename": path.name,
                "source": source,
                "speed_tier": normalize_speed_tier(crawl.get("speed_tier"), default="safe"),
                "start_urls": [str(item) for item in start_urls[:5]],
                "allowed_domains": [str(item) for item in allowed_domains[:10]],
                "notes": str(registry.get("notes", "") or ""),
                "outcome_count": len(outcomes),
                "updated_at": str(registry.get("updated_at", "") or ""),
            }
    return sorted(by_id.values(), key=lambda item: str(item["id"]))


def resolve_template_path(template_id: str = "", template_path: str = "") -> Tuple[str, str]:
    explicit = str(template_path or "").strip()
    if explicit:
        path = Path(explicit).expanduser()
        path = (PROJECT_ROOT / path).resolve() if not path.is_absolute() else path.resolve()
        if not path.is_file():
            raise FileNotFoundError(f"template not found: {path}")
        return path.stem, str(path)

    requested = str(template_id or "").strip()
    if not requested:
        return "", ""
    requested = requested[:-5] if requested.lower().endswith(".json") else requested
    for item in list_template_files():
        if item["id"] == requested or item["filename"] == template_id:
            return str(item["id"]), str(item["path"])
    raise FileNotFoundError(f"template id not found: {requested}")


def get_template(template_id: str) -> Dict[str, Any]:
    resolved_id, path_text = resolve_template_path(template_id=template_id)
    path = Path(path_text)
    payload = _read_json(path)
    if not payload:
        raise ValueError(f"template is not a JSON object: {path}")
    registry = _read_registry(resolved_id)
    return {
        "id": resolved_id,
        "path": str(path.resolve()),
        "source": "runtime" if path.parent == runtime_templates_dir() else "bundled",
        "template": payload,
        "notes": str(registry.get("notes", "") or ""),
        "outcomes": list(registry.get("outcomes") or [])[-50:],
        "imported_at": str(registry.get("imported_at", "") or ""),
        "updated_at": str(registry.get("updated_at", "") or ""),
    }


def _walk_sensitive(value: Any, path: str = "$") -> List[str]:
    findings: List[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if str(key).strip().lower() in SENSITIVE_KEYS and child not in (None, "", [], {}):
                findings.append(child_path)
            findings.extend(_walk_sensitive(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            findings.extend(_walk_sensitive(child, f"{path}[{index}]"))
    return findings


def validate_template(template: Any) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    payload = template if isinstance(template, dict) else {}
    if not isinstance(template, dict):
        errors.append("root_must_be_object")

    if not str(payload.get("site_name", "") or "").strip():
        errors.append("missing_site_name")

    start_urls = payload.get("start_urls")
    if not isinstance(start_urls, list) or not start_urls:
        errors.append("start_urls_must_be_nonempty_list")
        start_urls = []
    for raw_url in start_urls:
        parsed = urlparse(str(raw_url or ""))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append(f"invalid_start_url:{raw_url}")

    domains = payload.get("allowed_domains")
    if not isinstance(domains, list) or not any(str(item or "").strip() for item in domains):
        errors.append("allowed_domains_must_be_nonempty_list")

    selectors = payload.get("selectors")
    if not isinstance(selectors, dict):
        errors.append("selectors_must_be_object")
        selectors = {}
    for field in ("list_item", "name", "detail_link"):
        if selectors.get(field) in (None, "", [], {}):
            errors.append(f"missing_selector:{field}")
    if selectors.get("detail_name") in (None, "", [], {}):
        warnings.append("missing_detail_name_selector")
    if selectors.get("detail_image") in (None, "", [], {}):
        warnings.append("missing_detail_image_selector:no_photo_rows_are_allowed")
    if selectors.get("next_page") in (None, "", [], {}):
        warnings.append("missing_next_page_selector")

    rules = payload.get("rules")
    if not isinstance(rules, dict):
        errors.append("rules_must_be_object")
        rules = {}
    required = rules.get("required_fields")
    if not isinstance(required, list):
        errors.append("required_fields_must_be_list")
        required = []
    for field in ("name", "detail_url"):
        if field not in required:
            errors.append(f"required_fields_missing:{field}")
    if "image_url" in required:
        errors.append("image_url_must_not_be_required:no_photo_rows_are_allowed")
    if not isinstance(rules.get("field_map"), dict):
        warnings.append("missing_field_map")
    if rules.get("obey_robots_txt", True) is not True:
        warnings.append("obey_robots_txt_disabled")
    mode = str(rules.get("image_download_mode") or "requests_jsl").strip().lower()
    if mode not in ALLOWED_MODES:
        errors.append(f"invalid_image_download_mode:{mode}")

    crawl = payload.get("crawl")
    if not isinstance(crawl, dict):
        errors.append("crawl_must_be_object")
        crawl = {}
    tier = str(crawl.get("speed_tier", "") or "").strip().lower()
    if tier not in ALLOWED_SPEED_TIERS:
        errors.append("crawl.speed_tier_must_be_safe_standard_or_turbo")
    if not str(crawl.get("speed_tier_reason", "") or "").strip():
        warnings.append("missing_speed_tier_reason")
    try:
        concurrency = int(crawl.get("concurrent_requests", 1))
        if concurrency < 1 or concurrency > 8:
            errors.append("concurrent_requests_must_be_between_1_and_8")
    except Exception:
        errors.append("concurrent_requests_must_be_integer")

    for sensitive_path in _walk_sensitive(payload):
        errors.append(f"sensitive_value_forbidden:{sensitive_path}")
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    if re.search(r"(?i)bearer\s+[a-z0-9._~-]{12,}", raw.decode("utf-8", "ignore")):
        errors.append("possible_bearer_token_in_template")

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "site_name": str(payload.get("site_name", "") or ""),
        "mode": mode,
        "speed_tier": tier,
        "sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
    }


def import_template(
    template: Dict[str, Any],
    *,
    template_id: str = "",
    notes: str = "",
    overwrite: bool = False,
) -> Dict[str, Any]:
    validation = validate_template(template)
    if not validation["ok"]:
        raise ValueError("template validation failed: " + ", ".join(validation["errors"]))
    resolved_id = _safe_template_id(
        template_id or template.get("template_id") or template.get("site_name")
    )
    target = runtime_templates_dir() / f"{resolved_id}.json"
    canonical = json.dumps(template, ensure_ascii=False, indent=2) + "\n"
    new_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    unchanged = False
    if target.exists() and not overwrite:
        old_hash = hashlib.sha256(target.read_bytes()).hexdigest()
        if old_hash != new_hash:
            raise FileExistsError(f"template already exists: {resolved_id}; pass overwrite=true")
        unchanged = True
    if not unchanged:
        _atomic_write_json(target, template)

    record = _read_registry(resolved_id)
    now = _now_iso()
    if not record.get("imported_at"):
        record["imported_at"] = now
    record["updated_at"] = now
    record["sha256"] = "sha256:" + new_hash
    if notes:
        record["notes"] = str(notes).strip()
    record.setdefault("outcomes", [])
    _write_registry(resolved_id, record)
    return {
        "ok": True,
        "id": resolved_id,
        "path": str(target.resolve()),
        "unchanged": unchanged,
        "validation": validation,
        "notes": str(record.get("notes", "") or ""),
    }


def update_template(
    template_id: str,
    *,
    template: Optional[Dict[str, Any]] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    current = get_template(template_id)
    if template is not None:
        return import_template(
            template,
            template_id=current["id"],
            notes=current["notes"] if notes is None else notes,
            overwrite=True,
        )
    record = _read_registry(current["id"])
    record["notes"] = str(notes or "").strip()
    record["updated_at"] = _now_iso()
    record.setdefault("imported_at", record["updated_at"])
    record.setdefault("outcomes", current["outcomes"])
    _write_registry(current["id"], record)
    return get_template(current["id"])


def record_outcome(template_id: str, outcome: Dict[str, Any]) -> Dict[str, Any]:
    current = get_template(template_id)
    record = _read_registry(current["id"])
    outcomes = record.get("outcomes") if isinstance(record.get("outcomes"), list) else []
    entry = dict(outcome or {})
    entry["recorded_at"] = _now_iso()
    outcomes.append(entry)
    record["outcomes"] = outcomes[-50:]
    record["updated_at"] = entry["recorded_at"]
    record.setdefault("imported_at", entry["recorded_at"])
    record.setdefault("notes", current["notes"])
    _write_registry(current["id"], record)
    return {
        "ok": True,
        "id": current["id"],
        "outcome_count": len(record["outcomes"]),
        "outcome": entry,
    }


def search_templates(
    *,
    q: str = "",
    url: str = "",
    domain: str = "",
    limit: int = 10,
) -> Dict[str, Any]:
    requested_domain = str(domain or "").strip().lower()
    if not requested_domain and url:
        requested_domain = (urlparse(str(url)).hostname or "").lower()
    terms = [item for item in re.split(r"\s+", str(q or "").strip().lower()) if item]
    matches: List[Dict[str, Any]] = []
    for item in list_template_files():
        try:
            detail = get_template(str(item["id"]))
            payload = detail["template"]
        except Exception:
            payload = {}
        domains = [str(value or "").strip().lower() for value in payload.get("allowed_domains", [])]
        haystack = " ".join(
            [
                str(item.get("id", "")),
                str(item.get("name", "")),
                str(item.get("notes", "")),
                " ".join(domains),
                " ".join(str(value) for value in payload.get("start_urls", [])),
            ]
        ).lower()
        score = 0
        matched_by: List[str] = []
        if requested_domain:
            for candidate in domains:
                if requested_domain == candidate:
                    score = max(score, 100)
                    matched_by.append("domain_exact")
                elif requested_domain.endswith("." + candidate) or candidate.endswith("." + requested_domain):
                    score = max(score, 70)
                    matched_by.append("domain_related")
        if terms and all(term in haystack for term in terms):
            score += 20 + len(terms)
            matched_by.append("query")
        if not requested_domain and not terms:
            score = 1
        if score <= 0:
            continue
        row = dict(item)
        row["score"] = score
        row["matched_by"] = sorted(set(matched_by))
        matches.append(row)
    matches.sort(key=lambda item: (-int(item.get("score", 0)), str(item.get("id", ""))))
    limited = matches[: max(1, min(50, int(limit or 10)))]
    return {
        "count": len(limited),
        "query": {"q": str(q or ""), "url": str(url or ""), "domain": requested_domain},
        "templates": limited,
    }
