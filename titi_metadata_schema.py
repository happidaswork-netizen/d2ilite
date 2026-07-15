# -*- coding: utf-8 -*-
"""Pure helpers for the TITI metadata specification revision 2.3."""

from __future__ import annotations

import hashlib
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from PIL import Image, ImageOps


TITI_META_SCHEMA = "titi-meta"
TITI_META_SCHEMA_VERSION = 2
TITI_META_SUPPORTED_SCHEMA_VERSIONS = {1, 2}
TITI_SPEC_REVISION = "2.3"
TITI_CONTENT_HASH_VERSION = "titi-pixel-sha256-v1"

PHOTO_AUDIT_SCHEMA = "titi-photo-audit"
PHOTO_AUDIT_SCHEMA_VERSION = 1

PHOTO_AUDIT_STATUSES = {"pending", "reviewed", "approved", "rejected"}
ARCHIVE_GENDER_BUCKETS = {"男", "女", "多人混合", "未知", "待审"}


def _empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _first(*values: Any) -> Any:
    for value in values:
        if not _empty(value):
            return value
    return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def merge_non_empty(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge non-empty values while preserving unknown existing fields."""
    result = deepcopy(base or {})
    for key, value in (incoming or {}).items():
        if _empty(value):
            continue
        previous = result.get(key)
        if isinstance(previous, dict) and isinstance(value, dict):
            result[key] = merge_non_empty(previous, value)
        else:
            result[key] = deepcopy(value)
    return result


def clean_string_list(values: Any) -> List[str]:
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, Iterable):
        return []
    result: List[str] = []
    seen = set()
    for value in values:
        token = _text(value)
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def normalize_gender(value: Any) -> str:
    raw = _text(value)
    lowered = raw.lower()
    mapping = {
        "男": "男",
        "男性": "男",
        "male": "男",
        "m": "男",
        "man": "男",
        "女": "女",
        "女性": "女",
        "female": "女",
        "f": "女",
        "woman": "女",
        "未知": "未知",
        "unknown": "未知",
    }
    return mapping.get(lowered) or mapping.get(raw) or ""


def normalize_archive_gender_bucket(value: Any, *, source_gender: Any = None) -> str:
    explicit = _text(value)
    if explicit in ARCHIVE_GENDER_BUCKETS:
        return explicit
    gender = normalize_gender(source_gender)
    if gender in {"男", "女"}:
        return gender
    return "未知"


def normalize_content_hash(value: Any) -> str:
    raw = _text(value).lower()
    if not raw:
        return ""
    if raw.startswith("sha256:"):
        raw = raw[7:]
    if re.fullmatch(r"[0-9a-f]{64}", raw):
        return f"sha256:{raw}"
    return ""


def compute_titi_content_hash_from_image(image: Image.Image) -> Optional[str]:
    if image is None:
        return None
    try:
        oriented = ImageOps.exif_transpose(image)
    except Exception:
        oriented = image
    try:
        normalized = oriented.convert("RGBA")
        width, height = normalized.size
        payload = normalized.tobytes()
    except Exception:
        return None
    digest = hashlib.sha256()
    digest.update(TITI_CONTENT_HASH_VERSION.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(f"{int(width)}x{int(height)}".encode("utf-8"))
    digest.update(b"\x00")
    digest.update(payload)
    return f"sha256:{digest.hexdigest()}"


def compute_titi_content_hash(path: str | Path) -> Optional[str]:
    try:
        with Image.open(str(path)) as image:
            image.load()
            return compute_titi_content_hash_from_image(image)
    except Exception:
        return None


def stable_source_ref(url: Any) -> str:
    normalized = _text(url)
    if not normalized:
        return ""
    return "src_" + hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:20]


def _merge_profile(base: Any, incoming: Any) -> Dict[str, Any]:
    existing = base if isinstance(base, dict) else {}
    update = incoming if isinstance(incoming, dict) else {}
    return merge_non_empty(existing, update)


def build_titi_meta(
    metadata: Dict[str, Any],
    *,
    existing_json: Optional[Dict[str, Any]] = None,
    existing_asset_id: Optional[str] = None,
    default_component: str = "d2i",
    now: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a schema-v2 payload while preserving unknown fields from existing metadata."""
    payload = dict(metadata or {})
    result = deepcopy(existing_json) if isinstance(existing_json, dict) else {}
    timestamp = now or datetime.now(timezone.utc).isoformat()

    result["schema"] = TITI_META_SCHEMA
    result["schema_version"] = TITI_META_SCHEMA_VERSION
    result["app"] = "PWI"
    component = _text(_first(payload.get("component"), result.get("component"), default_component))
    result["component"] = component if component in {"titi", "d2i"} else default_component
    result["titi_asset_id"] = _text(
        _first(payload.get("titi_asset_id"), existing_asset_id, result.get("titi_asset_id"), uuid.uuid4())
    )
    result["titi_world_id"] = _text(
        _first(payload.get("titi_world_id"), result.get("titi_world_id"), "default")
    )

    content_hash = normalize_content_hash(_first(payload.get("titi_content_hash"), result.get("titi_content_hash")))
    if content_hash:
        result["titi_content_hash"] = content_hash

    incoming_d2i = payload.get("d2i_profile") if isinstance(payload.get("d2i_profile"), dict) else {}
    incoming_d2i = dict(incoming_d2i)
    for large_key in ("full_content", "raw_fields_json", "research_sources", "model_raw_response"):
        incoming_d2i.pop(large_key, None)
    incoming_people = payload.get("people_profile") if isinstance(payload.get("people_profile"), dict) else {}
    name = _text(
        _first(
            payload.get("person"),
            payload.get("name"),
            incoming_people.get("name"),
            incoming_d2i.get("name"),
            result.get("name"),
        )
    )
    if name:
        result["name"] = name

    display_description = _text(
        _first(payload.get("display_description"), payload.get("description"), result.get("display_description"))
    )
    if display_description:
        result["display_description"] = display_description[:2000]

    tags = clean_string_list(_first(payload.get("tags"), payload.get("keywords"), result.get("tags"), []))
    if tags:
        result["tags"] = tags

    for key in ("identity_prompt", "prompt", "prompt_base", "prompt_context", "titi_origin"):
        value = payload.get(key)
        if not _empty(value):
            result[key] = deepcopy(value)

    aliases = clean_string_list(_first(payload.get("role_aliases"), result.get("role_aliases"), []))
    if aliases:
        result["role_aliases"] = aliases

    d2i_profile = _merge_profile(result.get("d2i_profile"), incoming_d2i)
    people_profile = _merge_profile(result.get("people_profile"), incoming_people)

    source = _text(_first(payload.get("source"), payload.get("source_url"), incoming_people.get("source_url")))
    image_url = _text(_first(payload.get("image_url"), payload.get("url"), incoming_d2i.get("image_url")))
    gender = normalize_gender(
        _first(payload.get("gender"), incoming_people.get("gender"), incoming_d2i.get("gender"))
    )
    city = _text(_first(payload.get("city"), incoming_people.get("city"), incoming_d2i.get("city")))
    position = _text(
        _first(payload.get("position"), incoming_people.get("position"), incoming_d2i.get("position"))
    )
    unit_name = _text(
        _first(
            payload.get("unit_name"),
            payload.get("unit"),
            incoming_people.get("unit_name"),
            incoming_d2i.get("unit_name"),
            incoming_d2i.get("unit"),
        )
    )

    if name:
        people_profile["name"] = name
        d2i_profile["name"] = name
    if gender:
        people_profile["gender"] = gender
        d2i_profile["gender"] = gender
    if city:
        people_profile["city"] = city
        d2i_profile["city"] = city
    if position:
        people_profile["position"] = position
        d2i_profile["position"] = position
    if unit_name:
        people_profile["unit_name"] = unit_name
        d2i_profile["unit_name"] = unit_name
        d2i_profile.setdefault("unit", unit_name)
    if source:
        people_profile["source_url"] = source
        d2i_profile["source"] = source
        d2i_profile["source_url"] = source
    if image_url:
        d2i_profile["image_url"] = image_url
        result["source_image"] = image_url
    if display_description:
        people_profile.setdefault("biography", display_description)
        d2i_profile["description"] = display_description
    if tags:
        d2i_profile["keywords"] = tags

    d2i_profile.setdefault("collector_variant", "d2ilite")
    d2i_profile["extracted_at"] = timestamp
    if people_profile:
        result["people_profile"] = people_profile
    if d2i_profile:
        result["d2i_profile"] = d2i_profile

    incoming_audit = payload.get("photo_audit") if isinstance(payload.get("photo_audit"), dict) else None
    if incoming_audit is not None:
        photo_audit = _merge_profile(result.get("photo_audit"), incoming_audit)
        photo_audit["schema"] = PHOTO_AUDIT_SCHEMA
        photo_audit["schema_version"] = PHOTO_AUDIT_SCHEMA_VERSION
        status = _text(photo_audit.get("status"))
        photo_audit["status"] = status if status in PHOTO_AUDIT_STATUSES else "pending"
        bucket = normalize_archive_gender_bucket(photo_audit.get("archive_gender_bucket"), source_gender=gender)
        photo_audit["archive_gender_bucket"] = bucket
        if not photo_audit.get("gender_source"):
            photo_audit["gender_source"] = "source" if gender in {"男", "女"} else "unresolved"
        photo_audit.setdefault("audit_source", "d2ilite-collector")
        photo_audit.setdefault("audited_at", timestamp)
        result["photo_audit"] = photo_audit

    refs = clean_string_list(
        _first(payload.get("research_source_refs"), result.get("research_source_refs"), [])
    )
    if refs:
        result["research_source_refs"] = refs

    if isinstance(payload.get("extensions"), dict):
        result["extensions"] = _merge_profile(result.get("extensions"), payload["extensions"])

    return result


def validate_titi_meta(metadata: Dict[str, Any], level: str = "portable") -> Dict[str, Any]:
    level = _text(level).lower() or "portable"
    errors: List[str] = []
    warnings: List[str] = []
    meta = metadata if isinstance(metadata, dict) else {}

    if meta.get("schema") != TITI_META_SCHEMA:
        errors.append("schema_must_be_titi-meta")
    if meta.get("schema_version") != TITI_META_SCHEMA_VERSION:
        errors.append("new_write_schema_version_must_be_2")
    if meta.get("component") not in {"titi", "d2i"}:
        errors.append("component_must_be_titi_or_d2i")
    if not _text(meta.get("titi_asset_id")):
        errors.append("missing_titi_asset_id")
    else:
        try:
            uuid.UUID(_text(meta.get("titi_asset_id")))
        except Exception:
            warnings.append("titi_asset_id_is_not_uuid_v4")
    if not normalize_content_hash(meta.get("titi_content_hash")):
        errors.append("missing_or_invalid_titi_content_hash")

    people = meta.get("people_profile") if isinstance(meta.get("people_profile"), dict) else {}
    d2i = meta.get("d2i_profile") if isinstance(meta.get("d2i_profile"), dict) else {}
    if not _text(_first(meta.get("name"), people.get("name"), d2i.get("name"))):
        errors.append("missing_name")
    if not _text(_first(people.get("source_url"), d2i.get("source_url"), d2i.get("source"))):
        warnings.append("missing_source_url")

    audit = meta.get("photo_audit") if isinstance(meta.get("photo_audit"), dict) else {}
    if level in {"archive-ready", "primary-ready"}:
        if not audit:
            errors.append("missing_photo_audit")
        else:
            required = (
                "file_status",
                "source_origin_status",
                "source_page_image_status",
                "repair_status",
                "archive_decision",
                "identity_confidence",
                "face_quality",
                "archive_gender_bucket",
            )
            for key in required:
                if _empty(audit.get(key)):
                    errors.append(f"photo_audit_missing_{key}")
            if audit.get("status") not in {"reviewed", "approved", "rejected"}:
                errors.append("photo_audit_not_reviewed")
            if not _text(_first(audit.get("approval_ref"), audit.get("campaign_id"))):
                errors.append("photo_audit_missing_approval_or_campaign_ref")

    if level == "primary-ready":
        if audit.get("archive_decision") != "keep_primary":
            errors.append("primary_requires_keep_primary")
        try:
            if float(audit.get("identity_confidence")) < 0.8:
                errors.append("primary_identity_confidence_below_0_8")
        except Exception:
            errors.append("primary_identity_confidence_invalid")
        if audit.get("face_quality") not in {"excellent", "good", "usable"}:
            errors.append("primary_face_quality_not_usable")
        if audit.get("person_count") not in (None, 1) and not audit.get("subject_unique"):
            errors.append("primary_subject_not_unique")

    return {"ok": not errors, "level": level, "errors": errors, "warnings": warnings}
