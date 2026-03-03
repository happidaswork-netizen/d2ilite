# -*- coding: utf-8 -*-
"""Scraper monitor / queue utility services."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List


def format_elapsed(seconds: float) -> str:
    s = max(0, int(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def repair_mojibake_utf8_latin1(text: str) -> str:
    raw = str(text or "")
    if not raw:
        return raw
    # Typical mojibake markers when UTF-8 bytes were decoded as latin1/cp1252.
    if not any(ch in raw for ch in ("Ã", "Â", "ä", "å", "æ", "ç", "é", "ï", "¤", "º", "", "")):
        return raw
    try:
        candidate = raw.encode("latin1").decode("utf-8")
    except Exception:
        return raw

    def _score(value: str) -> int:
        cjk = sum(1 for ch in value if "\u4e00" <= ch <= "\u9fff")
        bad = sum(1 for ch in value if ch in {"Ã", "Â", "¤", "º", "", "", "�"})
        return cjk * 2 - bad

    return candidate if _score(candidate) > _score(raw) else raw


def read_text_tail(path: str, max_lines: int = 30) -> str:
    if not path or (not os.path.exists(path)):
        return ""
    try:
        with open(path, "rb") as f:
            data = f.read()
        if not data:
            return ""
        lines = data.splitlines(keepends=True)
        selected = lines[-max(1, int(max_lines or 30)) :]
        decoded_lines: List[str] = []
        for raw in selected:
            line = ""
            for enc in ("utf-8", "utf-8-sig", "gb18030", "cp936"):
                try:
                    line = raw.decode(enc)
                    break
                except Exception:
                    continue
            if not line:
                line = raw.decode("latin1", errors="ignore")
            line = repair_mojibake_utf8_latin1(line)
            decoded_lines.append(line)
        text = "".join(decoded_lines).strip()
        return repair_mojibake_utf8_latin1(text)
    except Exception:
        return ""


def read_jsonl_rows(path: str, max_rows: int = 0) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if (not path) or (not os.path.exists(path)):
        return rows
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
                    if max_rows > 0 and len(rows) >= max_rows:
                        break
    except Exception:
        return []
    return rows


def write_jsonl_rows(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for item in (rows or []):
            if not isinstance(item, dict):
                continue
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def humanize_scraper_reason(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    def _compact_path(value: str) -> str:
        v = str(value or "").strip()
        if not v:
            return ""
        try:
            base = os.path.basename(v)
            return f"...\\{base}" if base else v
        except Exception:
            return v

    def _map_one(part: str) -> str:
        p = str(part or "").strip()
        if not p:
            return ""
        l = p.lower()
        if l.startswith("audit_missing_metadata_fields"):
            missing_raw = ""
            if ":" in p:
                missing_raw = p.split(":", 1)[1].strip()
            fields_map = {
                "gender": "性别",
                "birth_date": "出生日期",
                "photo_taken_at": "拍摄日期",
                "age_at_photo": "拍摄时年龄",
                "position": "职务",
                "city": "城市",
                "unit": "单位",
                "profession": "职业",
                "police_id": "警号",
            }
            if missing_raw:
                tokens = [x.strip().lower() for x in re.split(r"[,，;；\s]+", missing_raw) if x.strip()]
                labels: List[str] = []
                for token in tokens:
                    labels.append(fields_map.get(token, token))
                if labels:
                    return "元数据待补充：" + "、".join(labels)
            return "元数据待补充：关键字段缺失"

        if l == "metadata_missing_local_image_path":
            return "元数据未写入：本地图片缺失"
        if l == "image_download_http_error":
            return "图片下载失败：HTTP 错误"
        if l == "image_download_not_image":
            return "图片下载失败：返回内容不是图片"
        if l == "image_download_request_failed":
            return "图片下载失败：请求异常"
        if l == "image_download_browser_failed":
            return "图片下载失败：浏览器模式异常"
        if l == "missing_detail_url_from_list":
            return "列表项缺少详情链接"
        if "missing_required_fields" in l:
            return "详情页关键字段缺失"
        if l.startswith("list_browser_fetch_failed"):
            return "列表页抓取失败（浏览器）"
        if l.startswith("detail_browser_fetch_failed"):
            return "详情页抓取失败（浏览器）"
        if l.startswith("metadata_write_failed"):
            return "元数据写入失败"

        if p.startswith("安全写入失败:"):
            tail = p.split(":", 1)[1].strip() if ":" in p else ""
            return f"元数据写入失败：{_compact_path(tail)}" if tail else "元数据写入失败"
        if "utf-8" in l and "codec can't decode" in l:
            return "元数据写入失败：编码异常(utf-8)"
        return p

    parts = [x.strip() for x in raw.split("|") if x.strip()]
    if not parts:
        return _map_one(raw)
    mapped_parts: List[str] = []
    for item in parts:
        mapped = _map_one(item)
        if mapped and (mapped not in mapped_parts):
            mapped_parts.append(mapped)
    return " | ".join(mapped_parts)


def merge_status_reason(entry: Dict[str, Any], msg: str) -> None:
    text = humanize_scraper_reason(str(msg or "").strip())
    if not text:
        return
    old = str(entry.get("reason", "")).strip()
    if not old:
        entry["reason"] = text
        return
    if text in old:
        return
    entry["reason"] = f"{old} | {text}"


def normalize_person_key(name: Any) -> str:
    text = str(name or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"\s+", "", text)


def extract_runtime_log_field(line: str, label: str) -> str:
    text = str(line or "")
    key = str(label or "").strip()
    if (not text) or (not key):
        return ""
    m = re.search(rf"{re.escape(key)}\s*:\s*([^|]+)", text)
    if not m:
        return ""
    return str(m.group(1) or "").strip()


def normalize_optional_audit_value(value: Any) -> str:
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
    return raw

