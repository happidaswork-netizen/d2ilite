# -*- coding: utf-8 -*-
"""Editor text/profile normalization services."""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List

from services.metadata_service import normalize_http_url


def normalize_single_line_text(value: Any) -> str:
    raw = str(value or "").replace("\x00", " ").strip()
    return re.sub(r"\s+", " ", raw).strip()


def normalize_multiline_editor_text(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return ""
    lines: List[str] = []
    prev_blank = False
    for raw_line in text.split("\n"):
        line = normalize_single_line_text(raw_line)
        if line:
            lines.append(line)
            prev_blank = False
        else:
            if lines and (not prev_blank):
                lines.append("")
            prev_blank = True
    while lines and (not lines[-1]):
        lines.pop()
    return "\n".join(lines).strip()


def normalize_gender_text(value: Any) -> str:
    raw = normalize_single_line_text(value)
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in {"male", "m", "man", "男性"} or raw == "男":
        return "男"
    if lowered in {"female", "f", "woman", "女性"} or raw == "女":
        return "女"
    return raw


def normalize_profile_for_editor(
    value: Any,
    *,
    url_normalizer: Callable[[Any], str] = normalize_http_url,
) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for raw_k, raw_v in value.items():
            key = normalize_single_line_text(raw_k)
            if not key:
                continue
            cleaned = normalize_profile_for_editor(raw_v, url_normalizer=url_normalizer)
            if cleaned in (None, "", [], {}):
                continue
            out[key] = cleaned
        return out
    if isinstance(value, list):
        out_list: List[Any] = []
        for item in value:
            cleaned = normalize_profile_for_editor(item, url_normalizer=url_normalizer)
            if cleaned in (None, "", [], {}):
                continue
            out_list.append(cleaned)
        return out_list
    if isinstance(value, str):
        text = normalize_multiline_editor_text(value)
        if (not text) or ("\n" in text):
            return text
        if ("http://" in text.lower()) or ("https://" in text.lower()):
            normalized_url = url_normalizer(text)
            if normalized_url.lower().startswith(("http://", "https://")):
                return normalized_url
        return text
    return value


def extract_json_payload_from_llm(raw_text: Any) -> Dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    fenced = re.findall(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    for block in fenced:
        try:
            parsed = json.loads(block)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue

    left = text.find("{")
    right = text.rfind("}")
    if left >= 0 and right > left:
        snippet = text[left : right + 1]
        try:
            parsed = json.loads(snippet)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def prune_empty_profile_values(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            cleaned = prune_empty_profile_values(v)
            if cleaned in (None, "", [], {}):
                continue
            out[str(k)] = cleaned
        return out
    if isinstance(value, list):
        out_list: List[Any] = []
        for item in value:
            cleaned = prune_empty_profile_values(item)
            if cleaned in (None, "", [], {}):
                continue
            out_list.append(cleaned)
        return out_list
    return value

