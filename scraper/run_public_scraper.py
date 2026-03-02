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

try:
    from PIL import Image  # type: ignore
    HAS_PIL_IMAGE = True
except Exception:
    Image = None  # type: ignore
    HAS_PIL_IMAGE = False

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

try:
    from metadata_manager import read_image_metadata as _read_image_metadata  # type: ignore
    HAS_METADATA_MANAGER = True
except Exception:
    _read_image_metadata = None  # type: ignore
    HAS_METADATA_MANAGER = False

try:
    from llm_enricher import LLMEnricher  # type: ignore
    HAS_LLM_ENRICHER = True
except Exception:
    LLMEnricher = None  # type: ignore
    HAS_LLM_ENRICHER = False

try:
    from text_parser import extract_person_info as _extract_person_info  # type: ignore
    HAS_TEXT_PARSER = True
except Exception:
    _extract_person_info = None  # type: ignore
    HAS_TEXT_PARSER = False


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


def _compact_url_for_log(value: Any, *, max_len: int = 96) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw[:max_len] if len(raw) > max_len else raw
    host = parsed.netloc
    path = parsed.path or ""
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2:
        short_path = f".../{parts[-2]}/{parts[-1]}"
    elif parts:
        short_path = f".../{parts[-1]}"
    else:
        short_path = "/"
    compact = f"{host}{short_path}"
    if parsed.query:
        compact += "?..."
    if len(compact) > max_len:
        compact = compact[: max_len - 3] + "..."
    return compact


def _format_runtime_log_value(value: Any, field_key: str = "") -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    field = str(field_key or "").strip().lower()
    if field in {"url", "detail", "image"} and text.startswith(("http://", "https://")):
        return _compact_url_for_log(text)
    text = re.sub(r"\s+", " ", text)
    if len(text) > 240:
        text = text[:237] + "..."
    return text


_RUNTIME_LOG_LEVEL_ZH: Dict[str, str] = {
    "RUN": "运行",
    "CONF": "配置",
    "STAGE": "阶段",
    "STEP": "步骤",
    "STAT": "统计",
    "INFO": "信息",
    "WARN": "警告",
    "FAIL": "失败",
    "STOP": "停止",
    "DONE": "完成",
    "HINT": "提示",
    "RETRY": "重试",
}

_RUNTIME_LOG_MESSAGE_ZH: Dict[str, str] = {
    "public scraper started": "公共抓取任务启动",
    "runtime summary": "运行参数摘要",
    "counts on disk before run": "运行前磁盘计数",
    "counts on disk after run": "运行后磁盘计数",
    "backoff active, skip this run": "退避生效，跳过本次运行",
    "metadata pre-retry start (retry failed first)": "元数据预重试开始（失败优先）",
    "metadata pre-retry queue built": "元数据预重试队列已构建",
    "metadata pre-retry end": "元数据预重试结束",
    "crawl stage start": "抓取阶段开始",
    "quick crawl failed, fallback to browser mode": "快速抓取失败，回退到浏览器模式",
    "crawl stage end": "抓取阶段结束",
    "quick crawl triggered backoff, fallback to browser mode and retry crawl": "快速抓取触发退避，回退浏览器模式并重试抓取",
    "blocked detected after crawl, pause current run": "抓取后检测到阻断，暂停本次运行",
    "crawl stage skipped by flag": "抓取阶段按参数跳过",
    "crawl skipped snapshot": "抓取阶段跳过快照",
    "image retry scan": "图片重试扫描",
    "image stage forced: auto retry unresolved image items": "图片阶段强制执行：自动重试未完成项",
    "image stage skipped: browser crawl already downloaded images inline": "图片阶段已跳过：浏览器抓取已内联下载",
    "image stage skipped snapshot": "图片阶段跳过快照",
    "image stage start": "图片阶段开始",
    "quick image download failed, fallback to browser mode": "快速下载失败，回退到浏览器模式",
    "image stage end": "图片阶段结束",
    "image stage end snapshot": "图片阶段结束快照",
    "quick image download triggered backoff, fallback to browser mode and retry image stage": "快速下载触发退避，回退浏览器模式并重试图片阶段",
    "blocked detected during image download, pause current run": "图片下载检测到阻断，暂停本次运行",
    "image stage skipped by flag": "图片阶段按参数跳过",
    "image skipped snapshot": "图片阶段跳过快照",
    "image stage skipped because run is paused by backoff": "图片阶段跳过：任务处于退避暂停",
    "image skipped due backoff snapshot": "图片阶段因退避跳过快照",
    "metadata stage skipped: inline metadata already applied": "元数据阶段已跳过：已完成内联写入",
    "metadata skipped snapshot": "元数据阶段跳过快照",
    "metadata stage start": "元数据阶段开始",
    "metadata queue built": "元数据队列已构建",
    "metadata stage end": "元数据阶段结束",
    "metadata audit start": "元数据质检开始",
    "metadata audit end": "元数据质检结束",
    "metadata audit skipped by config": "元数据质检已按配置跳过",
    "metadata audit review items queued": "元数据质检缺失项已加入复核队列",
    "public scraper finished": "公共抓取任务结束",
    "run paused by backoff, retry later with continue": "任务因退避暂停，请稍后点继续任务",
    "some items need manual review or retry": "存在需要人工复核或重试的条目",
    "metadata row retry policy active": "元数据行启用重试策略",
    "metadata retry succeeded": "元数据重试成功",
    "metadata write failed, scheduling retry": "元数据写入失败，计划延迟重试",
    "metadata write failed, retry immediately": "元数据写入失败，立即重试",
    "metadata write failed after retries": "元数据写入重试后仍失败",
    "inline metadata skipped: local image missing": "内联元数据跳过：本地图片不存在",
    "inline metadata write failed": "内联元数据写入失败",
    "crawl page failed": "页面抓取失败",
    "backoff activated during crawl": "抓取阶段触发退避",
    "crawl list page": "抓取列表页",
    "crawl detail page": "抓取详情页",
    "inline image download failed": "内联图片下载失败",
    "inline image response is not image": "内联响应不是图片",
    "inline image downloaded": "内联图片下载成功",
    "image download stage scanning started": "图片下载阶段扫描开始",
    "backoff activated during image download": "图片下载阶段触发退避",
    "image download candidate": "图片下载候选",
    "image download failed (browser)": "图片下载失败（浏览器模式）",
    "image download request exception": "图片下载请求异常",
    "image download http error": "图片下载 HTTP 错误",
    "image download payload is not image": "图片下载结果不是图片",
    "image downloaded": "图片下载成功",
    "metadata write stage scanning queue": "元数据写入阶段扫描队列",
    "metadata row": "处理元数据条目",
    "metadata skipped: source image missing": "元数据跳过：源图片缺失",
    "metadata written": "元数据写入成功",
    "metadata write failed": "元数据写入失败",
}

_RUNTIME_LOG_FIELD_ZH: Dict[str, str] = {
    "output_root": "输出目录",
    "mode": "模式",
    "skip_crawl": "跳过抓取",
    "skip_images": "跳过图片",
    "skip_metadata": "跳过元数据",
    "blocked_until": "阻断到期",
    "reason": "原因",
    "phase": "阶段",
    "url": "URL",
    "detail": "详情页",
    "image": "图片URL",
    "name": "姓名",
    "person": "人物",
    "idx": "序号",
    "queued": "队列剩余",
    "saved": "已保存",
    "discovered": "已发现",
    "error": "错误",
    "status": "状态码",
    "status_code": "状态码",
    "content_type": "类型",
    "size": "大小",
    "route": "通道",
    "sha": "哈希",
    "retry_failed": "失败重试",
    "retry_previous_fail": "历史失败重试",
    "attempt": "尝试次数",
    "attempts": "总尝试",
    "max_attempts": "最大尝试",
    "next_delay": "下次延迟",
    "queue": "队列总数",
    "added": "新增",
    "profiles_with_image": "有图档案数",
    "pending_images": "待重试图片",
    "missing_url_index": "缺失URL索引",
    "stale_cache": "失效缓存",
    "review_image_failures": "复核中的图片失败项",
    "pending_meta": "待写元数据",
    "list": "列表数",
    "profiles": "详情数",
    "images": "图片数",
    "metadata": "元数据数",
    "review": "复核数",
    "failures": "失败数",
    "delta_profiles": "详情增量",
    "delta_images": "图片增量",
    "delta_metadata": "元数据增量",
    "delta_review": "复核增量",
    "delta_failures": "失败增量",
    "fallback": "自动回退",
    "obey_robots": "遵守robots",
    "interval": "间隔秒",
    "timeout": "超时秒",
    "retry": "重试次数",
    "run_state": "运行状态",
    "candidates": "候选数",
    "downloaded_new": "新下载",
    "reused_url": "URL复用",
    "reused_sha": "SHA复用",
    "failed": "失败",
    "failed_retry_candidates": "失败重试候选",
    "failed_first": "失败优先数",
    "pending": "待处理",
    "ok_deferred": "已成功延后",
    "skip_write": "仅建队列不写入",
    "retry_failed_first": "失败优先重试",
    "browser_challenge": "挑战页",
    "checked": "已检查",
    "missing_items": "缺失条目",
    "missing_ratio": "缺失占比",
    "missing_fields": "缺失字段",
    "review_added": "新增复核",
    "review_pruned": "清理已解决",
    "review_updated": "更新复核",
    "field": "字段",
}


def _localize_runtime_level(level: str) -> str:
    raw = str(level or "").strip().upper()
    return _RUNTIME_LOG_LEVEL_ZH.get(raw, raw or "日志")


def _localize_runtime_message(message: str) -> str:
    raw = str(message or "").strip()
    return _RUNTIME_LOG_MESSAGE_ZH.get(raw, raw)


def _localize_runtime_field(key: str) -> str:
    raw = str(key or "").strip()
    return _RUNTIME_LOG_FIELD_ZH.get(raw, raw)


def _display_person_name(name: Any, detail_url: str = "") -> str:
    raw = str(name or "").strip()
    lowered = raw.lower()
    unknown_tokens = {"unknown", "unkonw", "n/a", "na", "none", "null", "-", "未知", "不详", "未详"}
    if raw and lowered not in unknown_tokens:
        return raw
    detail = str(detail_url or "").strip()
    if detail:
        parsed = urlparse(detail)
        token = str(parsed.path or "").strip("/").split("/")[-1].strip()
        if token:
            token = re.sub(r"\.(html?|php|aspx?)$", "", token, flags=re.IGNORECASE)
            token = token.strip()
            if token:
                return f"未命名({token})"
    return "未命名人物"


def runtime_log(level: str, message: str, **fields: Any) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    suffix_parts: List[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        text = _format_runtime_log_value(value, key)
        if text == "":
            continue
        suffix_parts.append(f"{_localize_runtime_field(key)}: {text}")
    suffix = (" | " + " | ".join(suffix_parts)) if suffix_parts else ""
    print(
        f"{ts} [{_localize_runtime_level(level)}] {_localize_runtime_message(message)}{suffix}",
        flush=True,
    )


def append_llm_report(report_path: Path, phase: str, stats: Dict[str, Any]) -> None:
    payload = load_json(report_path, {})
    if not isinstance(payload, dict):
        payload = {}
    history = payload.get("history", [])
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "generated_at": utc_now_iso(),
            "phase": str(phase or "").strip() or "unknown",
            "stats": dict(stats or {}),
        }
    )
    payload["generated_at"] = utc_now_iso()
    payload["latest_phase"] = str(phase or "").strip() or "unknown"
    payload["latest_stats"] = dict(stats or {})
    payload["history"] = history[-20:]
    save_json(report_path, payload)


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


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    """
    Best-effort atomic file write to avoid partial/corrupted outputs when the process is killed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    tmp.write_bytes(payload)
    tmp.replace(path)


def norm_abs_path(path_value: str) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    try:
        return str(Path(raw).resolve())
    except Exception:
        return raw


def scoped_temp_dir(base_name: str, scope_hint: str = "") -> Path:
    base = SCRIPT_DIR / str(base_name or "_tmp")
    hint = norm_abs_path(scope_hint)
    if not hint:
        base.mkdir(parents=True, exist_ok=True)
        return base
    token = hashlib.sha1(hint.encode("utf-8")).hexdigest()[:16]
    scoped = base / token
    scoped.mkdir(parents=True, exist_ok=True)
    return scoped


def _is_usable_cached_image(path_value: str) -> bool:
    path_text = norm_abs_path(path_value)
    if not path_text:
        return False
    p = Path(path_text)
    if (not p.exists()) or (not p.is_file()):
        return False
    try:
        if p.stat().st_size <= 0:
            return False
    except Exception:
        return False
    if HAS_PIL_IMAGE:
        try:
            with Image.open(p) as img:
                img.verify()
        except Exception:
            return False
    return True


def _resolve_cached_source_by_image_url(
    image_url: str,
    url_index: Dict[str, str],
    sha_index: Dict[str, str],
) -> Tuple[str, str]:
    image_url_norm = str(image_url or "").strip()
    if not image_url_norm:
        return "", ""
    sha_cached = str(url_index.get(image_url_norm, "")).strip()
    if not sha_cached:
        return "", ""
    source_cached = norm_abs_path(str(sha_index.get(sha_cached, "")))
    if _is_usable_cached_image(source_cached):
        return sha_cached, source_cached
    return sha_cached, ""


def _drop_stale_cache_index_entries(
    image_url: str,
    sha_value: str,
    url_index: Dict[str, str],
    sha_index: Dict[str, str],
) -> int:
    removed = 0
    image_url_norm = str(image_url or "").strip()
    sha_norm = str(sha_value or "").strip()

    if image_url_norm and image_url_norm in url_index:
        try:
            del url_index[image_url_norm]
            removed += 1
        except Exception:
            pass

    if sha_norm and sha_norm in sha_index:
        cached = norm_abs_path(str(sha_index.get(sha_norm, "")))
        if not _is_usable_cached_image(cached):
            try:
                del sha_index[sha_norm]
                removed += 1
            except Exception:
                pass
    return removed


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


def _normalize_field_lookup_key(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("：", ":")
    text = re.sub(r"[\s_\-.:]+", "", text)
    return text


def extract_field_by_aliases(
    fields: Dict[str, str],
    aliases: List[str],
    *,
    strip_labels: Optional[List[str]] = None,
) -> str:
    if not isinstance(fields, dict) or (not fields):
        return ""
    alias_set = {_normalize_field_lookup_key(x) for x in aliases if str(x or "").strip()}
    if not alias_set:
        return ""
    for raw_key, raw_value in fields.items():
        key_norm = _normalize_field_lookup_key(str(raw_key or ""))
        if (not key_norm) or (key_norm not in alias_set):
            continue
        value = normalize_optional_field(raw_value)
        if not value:
            continue
        if strip_labels:
            value = strip_prefixed_label(value, strip_labels)
        return normalize_optional_field(value)
    return ""


_PROFESSION_KEYWORD_RULES: List[Tuple[str, List[str]]] = [
    ("警察", ["民警", "辅警", "警长", "警员", "刑警", "交警", "特警", "网警", "法警", "狱警", "公安", "派出所"]),
    ("律师", ["律师", "律所", "律师事务所"]),
    ("法官", ["法官", "审判员", "法院"]),
    ("检察官", ["检察官", "检察院"]),
    ("医生", ["医生", "医师", "主任医师", "副主任医师", "主治医师", "住院医师", "医院"]),
    ("护士", ["护士", "护师", "护士长"]),
    ("教师", ["教师", "老师", "教授", "讲师", "副教授", "大学", "学院"]),
    ("工程师", ["工程师", "技术员", "程序员", "架构师"]),
    ("军人", ["军人", "武警", "解放军", "部队"]),
]


def infer_profession_keywords(*texts: Any) -> List[str]:
    merged = " ".join(str(x or "") for x in texts).strip()
    if not merged:
        return []
    tags: List[str] = []
    lowered = merged.lower()
    for label, hints in _PROFESSION_KEYWORD_RULES:
        for hint in hints:
            hint_text = str(hint or "").strip()
            if not hint_text:
                continue
            # CJK use substring directly; latin hints compare by lower().
            if re.search(r"[\u4e00-\u9fff]", hint_text):
                matched = hint_text in merged
            else:
                matched = hint_text.lower() in lowered
            if not matched:
                continue
            if label not in tags:
                tags.append(label)
            # Specific police aliases are also useful retrieval tags.
            if label == "警察" and hint_text in {"民警", "辅警", "刑警", "交警", "特警", "网警", "法警", "狱警"}:
                if hint_text not in tags:
                    tags.append(hint_text)
            break
    return tags


_GENDER_EXPLICIT_PATTERN = re.compile(r"性别\s*[：:]\s*(男|女)")
_GENDER_MALE_HINT_PATTERNS: List[str] = [
    r"男性",
    r"男子",
    r"先生",
    r"其父",
    r"丈夫",
    r"(?:^|[，,。；;、\s（(【\[])(?:男)(?:[，,。；;、\s）)】\]]|$)",
    r"(?:^|[，,。；;、\s（(【\[])(?:他|他们)(?:[，,。；;、\s）)】\]]|$|的|是|在|于|曾|将|已|被|为|与|和|生于|出生|牺牲|任职|工作)",
]
_GENDER_FEMALE_HINT_PATTERNS: List[str] = [
    r"女性",
    r"女子",
    r"女士",
    r"其母",
    r"妻子",
    r"(?:^|[，,。；;、\s（(【\[])(?:女)(?:[，,。；;、\s）)】\]]|$)",
    r"(?:^|[，,。；;、\s（(【\[])(?:她|她们)(?:[，,。；;、\s）)】\]]|$|的|是|在|于|曾|将|已|被|为|与|和|生于|出生|牺牲|任职|工作)",
]


def infer_gender_from_texts(*texts: Any) -> str:
    samples: List[str] = []
    for raw in texts:
        text = _normalize_multiline_text(raw)
        if text:
            samples.append(text)
    if not samples:
        return ""

    merged = "\n".join(samples)
    explicit_match = _GENDER_EXPLICIT_PATTERN.search(merged)
    if explicit_match:
        return normalize_gender(explicit_match.group(1))

    male_hits = 0
    female_hits = 0
    for pattern in _GENDER_MALE_HINT_PATTERNS:
        male_hits += len(re.findall(pattern, merged))
    for pattern in _GENDER_FEMALE_HINT_PATTERNS:
        female_hits += len(re.findall(pattern, merged))

    if male_hits > female_hits:
        return "男"
    if female_hits > male_hits:
        return "女"

    if HAS_TEXT_PARSER and (_extract_person_info is not None):
        male_votes = 0
        female_votes = 0
        for text in samples:
            try:
                info = _extract_person_info(text) or {}
            except Exception:
                info = {}
            parsed = normalize_gender(info.get("gender", ""))
            if parsed == "男":
                male_votes += 1
            elif parsed == "女":
                female_votes += 1
        if male_votes > female_votes:
            return "男"
        if female_votes > male_votes:
            return "女"
    return ""


def build_position_keyword(position_text: str) -> str:
    text = normalize_optional_field(position_text)
    if not text:
        return ""
    parts = [x.strip() for x in re.split(r"[，,;；/、\|]+", text) if x.strip()]
    if not parts:
        return text
    first = parts[0]
    # Prefer concise role token, e.g. "合伙人，管理委员会委员" -> "合伙人"
    if len(first) <= 10:
        return first
    return text


def build_source_host_keyword(*urls: str) -> str:
    for raw in urls:
        url_text = str(raw or "").strip()
        if not url_text:
            continue
        host = urlparse(url_text).netloc.lower().strip()
        if not host:
            continue
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        # Keep only first label for concise retrieval token.
        token = host.split(".", 1)[0].strip()
        token = re.sub(r"[^a-z0-9\-]", "", token)
        if 1 <= len(token) <= 10:
            return token
    return ""


def _parse_date_token(raw_value: Any) -> Tuple[str, Optional[datetime]]:
    text = str(raw_value or "").strip()
    if not text:
        return "", None
    text = re.sub(r"\s+", " ", text)

    # Full date, optionally followed by time (e.g. 2021:04:02 10:20:30 / 2021年4月2日)
    full_match = re.search(
        r"([12]\d{3})\s*[年:/\-.]\s*([01]?\d)\s*[月:/\-.]\s*([0-3]?\d)\s*日?"
        r"(?:\s+([0-2]?\d):([0-5]?\d)(?::([0-5]?\d))?)?",
        text,
    )
    if full_match:
        year = int(full_match.group(1))
        month = int(full_match.group(2))
        day = int(full_match.group(3))
        if 1800 <= year <= 2100:
            try:
                dt = datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}", dt
            except Exception:
                pass

    # Year-month
    ym_match = re.search(r"([12]\d{3})\s*[年:/\-.]\s*([01]?\d)\s*月?", text)
    if ym_match:
        year = int(ym_match.group(1))
        month = int(ym_match.group(2))
        if 1800 <= year <= 2100 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}", None

    # Year only
    y_match = re.search(r"([12]\d{3})\s*年?", text)
    if y_match:
        year = int(y_match.group(1))
        if 1800 <= year <= 2100:
            return f"{year:04d}", None
    return "", None


def extract_birth_date_from_texts(*texts: Any) -> Tuple[str, Optional[datetime]]:
    patterns = [
        r"(?:出生日期|出生时间|出生年月|生日|出生于|生于|出生)\s*[：: ]\s*([12]\d{3}[^\n，,。；;]{0,20})",
        r"(?:出生于|生于)\s*([12]\d{3}(?:\s*[年:/\-.]\s*[01]?\d(?:\s*[月:/\-.]\s*[0-3]?\d\s*日?)?)?)",
        r"([12]\d{3}\s*[年:/\-.]\s*[01]?\d\s*[月:/\-.]\s*[0-3]?\d\s*日?)\s*(?:出生|生于|生)",
        r"([12]\d{3}\s*[年:/\-.]\s*[01]?\d\s*月?)\s*(?:出生|生于|生)",
        r"([12]\d{3}\s*年)\s*(?:出生|生于|生)",
    ]
    for raw in texts:
        text = str(raw or "").strip()
        if not text:
            continue
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                value = str(match.group(1) or "").strip()
                normalized, dt = _parse_date_token(value)
                if normalized:
                    return normalized, dt
        compact = re.sub(r"\s+", "", text)
        if len(compact) <= 24:
            normalized, dt = _parse_date_token(text)
            if normalized:
                return normalized, dt

    # Fallback: leverage existing text parser birth_year extraction.
    if HAS_TEXT_PARSER and (_extract_person_info is not None):
        for raw in texts:
            text = str(raw or "").strip()
            if not text:
                continue
            try:
                info = _extract_person_info(text) or {}
            except Exception:
                info = {}
            year_text = str(info.get("birth_year", "")).strip()
            if re.fullmatch(r"[12]\d{3}", year_text):
                year = int(year_text)
                if 1800 <= year <= 2100:
                    return f"{year:04d}", None
    return "", None


def extract_photo_taken_date_from_image(image_path: str) -> Tuple[str, Optional[datetime]]:
    if not HAS_PIL_IMAGE:
        return "", None
    source = norm_abs_path(image_path)
    if (not source) or (not Path(source).exists()):
        return "", None
    try:
        with Image.open(source) as img:  # type: ignore[attr-defined]
            exif = img.getexif()
            if exif:
                for tag in (36867, 36868, 306):  # DateTimeOriginal, DateTimeDigitized, DateTime
                    value = exif.get(tag)
                    normalized, dt = _parse_date_token(value)
                    if normalized:
                        return normalized, dt

            info_map = getattr(img, "info", {}) if isinstance(getattr(img, "info", {}), dict) else {}
            for key in ("date:create", "date:modify", "creation_time", "Creation Time", "DateTimeOriginal", "DateTime"):
                if key in info_map:
                    normalized, dt = _parse_date_token(info_map.get(key))
                    if normalized:
                        return normalized, dt

            # Some formats expose XMP payload as bytes/string in info.
            for xmp_key in ("xmp", "XML:com.adobe.xmp", "Raw profile type xmp"):
                if xmp_key not in info_map:
                    continue
                payload = info_map.get(xmp_key)
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", errors="ignore")
                payload_text = str(payload or "").strip()
                if not payload_text:
                    continue
                normalized, dt = _parse_date_token(payload_text)
                if normalized:
                    return normalized, dt
    except Exception:
        return "", None
    return "", None


def compute_age_at_photo(
    birth_dt: Optional[datetime],
    photo_dt: Optional[datetime],
    *,
    birth_token: str = "",
) -> str:
    if photo_dt is None:
        return ""

    if birth_dt is not None:
        if photo_dt < birth_dt:
            return ""
        age = photo_dt.year - birth_dt.year
        if (photo_dt.month, photo_dt.day) < (birth_dt.month, birth_dt.day):
            age -= 1
        if 0 <= age <= 130:
            return str(age)

    # Birth date may only have year/year-month precision.
    year_match = re.search(r"([12]\d{3})", str(birth_token or ""))
    if year_match:
        birth_year = int(year_match.group(1))
        age_by_year = photo_dt.year - birth_year
        if 0 <= age_by_year <= 130:
            return str(age_by_year)
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


def _path_exists(path_value: str) -> bool:
    path_text = norm_abs_path(path_value)
    if not path_text:
        return False
    try:
        return Path(path_text).exists()
    except Exception:
        return False


def _pick_existing_or_first(paths: List[str]) -> str:
    normalized: List[str] = []
    for raw in paths:
        path_text = norm_abs_path(raw)
        if path_text and (path_text not in normalized):
            normalized.append(path_text)
    for candidate in normalized:
        if _path_exists(candidate):
            return candidate
    return normalized[0] if normalized else ""


def _load_download_manifest_lookups(manifest_path: Path) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    by_detail: Dict[str, Dict[str, str]] = {}
    by_image: Dict[str, Dict[str, str]] = {}
    for row in iter_jsonl(manifest_path):
        if not isinstance(row, dict):
            continue
        detail_url = str(row.get("detail_url", "")).strip()
        image_url = str(row.get("image_url", "")).strip()
        sha = str(row.get("sha256", "") or row.get("image_sha256", "")).strip()
        named_path = norm_abs_path(str(row.get("named_path", "")))
        saved_path = norm_abs_path(str(row.get("saved_path", "")))
        chosen_path = _pick_existing_or_first([named_path, saved_path])
        payload = {"sha": sha, "path": chosen_path}

        if detail_url:
            previous = by_detail.get(detail_url, {})
            previous_path = norm_abs_path(str(previous.get("path", "")))
            if (not previous) or ((not _path_exists(previous_path)) and _path_exists(chosen_path)):
                by_detail[detail_url] = payload
        if image_url:
            previous = by_image.get(image_url, {})
            previous_path = norm_abs_path(str(previous.get("path", "")))
            if (not previous) or ((not _path_exists(previous_path)) and _path_exists(chosen_path)):
                by_image[image_url] = payload
    return by_detail, by_image


def _resolve_metadata_source_path(
    *,
    detail_url: str,
    image_url: str,
    image_sha: str,
    row_local_path: str,
    url_index: Dict[str, str],
    sha_index: Dict[str, str],
    manifest_by_detail: Dict[str, Dict[str, str]],
    manifest_by_image: Dict[str, Dict[str, str]],
) -> Tuple[str, str, List[str]]:
    detail_key = str(detail_url or "").strip()
    image_key = str(image_url or "").strip()
    sha_resolved = str(image_sha or "").strip()
    row_local = norm_abs_path(row_local_path)

    manifest_detail = manifest_by_detail.get(detail_key, {}) if detail_key else {}
    manifest_image = manifest_by_image.get(image_key, {}) if image_key else {}
    manifest_detail_sha = str(manifest_detail.get("sha", "")).strip()
    manifest_image_sha = str(manifest_image.get("sha", "")).strip()
    manifest_detail_path = norm_abs_path(str(manifest_detail.get("path", "")))
    manifest_image_path = norm_abs_path(str(manifest_image.get("path", "")))

    url_sha = str(url_index.get(image_key, "")).strip() if image_key else ""
    if not sha_resolved:
        for sha_candidate in (url_sha, manifest_detail_sha, manifest_image_sha):
            if sha_candidate:
                sha_resolved = sha_candidate
                break

    candidates: List[str] = []
    for candidate in (
        row_local,
        norm_abs_path(str(sha_index.get(sha_resolved, ""))) if sha_resolved else "",
        norm_abs_path(str(sha_index.get(url_sha, ""))) if url_sha else "",
        norm_abs_path(str(sha_index.get(manifest_detail_sha, ""))) if manifest_detail_sha else "",
        norm_abs_path(str(sha_index.get(manifest_image_sha, ""))) if manifest_image_sha else "",
        manifest_detail_path,
        manifest_image_path,
    ):
        if candidate and (candidate not in candidates):
            candidates.append(candidate)

    source_path = ""
    for candidate in candidates:
        if _path_exists(candidate):
            source_path = candidate
            break
    return sha_resolved, source_path, candidates


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
    llm_enricher: Optional[Any] = None,
    record_failure: bool = True,
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
    inferred_position = extract_field_by_aliases(
        fields,
        [
            "position",
            "title",
            "job_title",
            "role",
            "occupation",
            "post",
            "职位",
            "职务",
            "职称",
            "岗位",
            "身份",
        ],
        strip_labels=["职位", "职务", "职称", "岗位", "Title", "title", "Position", "position"],
    )
    position = normalize_optional_field(
        mapped_fields.get("position")
        or mapped_fields.get("title")
        or fields.get("title", "")
        or inferred_position
    )
    english_name = normalize_optional_field(mapped_fields.get("english_name") or fields.get("english_name", ""))
    inferred_location = extract_field_by_aliases(
        fields,
        [
            "city",
            "location",
            "location_text",
            "office",
            "office_location",
            "workplace",
            "work_location",
            "region",
            "地址",
            "地点",
            "城市",
            "工作地点",
            "所在地",
            "所在城市",
            "办公室",
            "所在办公室",
        ],
        strip_labels=[
            "工作地点",
            "地点",
            "城市",
            "所在地",
            "所在城市",
            "办公室",
            "所在办公室",
            "City",
            "city",
            "Location",
            "location",
            "Office",
            "office",
        ],
    )
    mapped_city_text = normalize_optional_field(
        mapped_fields.get("city") or mapped_fields.get("location") or mapped_fields.get("location_text")
    )
    location_text = normalize_optional_field(mapped_city_text or fields.get("location_text", "") or inferred_location)
    email_text = normalize_optional_field(mapped_fields.get("email") or fields.get("email_text", ""))
    location_clean = strip_prefixed_label(location_text, ["工作地点", "地点"])
    email_clean = strip_prefixed_label(email_text, ["邮箱", "Email", "email"])
    gender = normalize_gender(mapped_fields.get("gender") or row.get("gender", ""))
    if not gender:
        gender = infer_gender_from_texts(
            summary,
            full_content,
            mapped_description,
            " ".join(str(v) for v in fields.values()),
            " ".join(str(v) for v in mapped_fields.values()),
        )
    police_id = normalize_optional_field(mapped_fields.get("police_id")) or extract_police_id_from_fields(fields)
    birth_date_raw = normalize_optional_field(
        mapped_fields.get("birth_date")
        or mapped_fields.get("birthday")
        or extract_field_by_aliases(
            fields,
            [
                "birth_date",
                "birthday",
                "date_of_birth",
                "出生日期",
                "出生时间",
                "出生年月",
                "生日",
                "出生",
            ],
        )
    )
    birth_date_norm, birth_date_obj = extract_birth_date_from_texts(
        birth_date_raw,
        summary,
        full_content,
    )
    photo_taken_at, photo_taken_obj = extract_photo_taken_date_from_image(source_path)
    age_at_photo = compute_age_at_photo(
        birth_date_obj,
        photo_taken_obj,
        birth_token=(birth_date_norm or birth_date_raw),
    )
    city_value = location_clean or location_text
    unit_text = normalize_optional_field(
        mapped_fields.get("unit")
        or mapped_fields.get("organization")
        or mapped_fields.get("org")
        or extract_field_by_aliases(
            fields,
            [
                "unit",
                "org",
                "organization",
                "department",
                "company",
                "firm",
                "hospital",
                "school",
                "单位",
                "机构",
                "部门",
                "单位名称",
                "工作单位",
                "所在单位",
                "律所",
                "医院",
                "学院",
                "学校",
                "公安局",
                "分局",
                "派出所",
            ],
        )
    )
    extra_fields: Dict[str, str] = dict(fields)
    llm_result: Dict[str, Any] = {}
    if llm_enricher is not None:
        try:
            llm_result = llm_enricher.enrich_row(
                row=row,
                position=position,
                city=city_value,
                unit=unit_text,
                summary=summary,
                full_content=full_content,
                extra_fields=extra_fields,
                mapped_fields=mapped_fields,
            )
        except Exception:
            llm_result = {}

    llm_position = normalize_optional_field(llm_result.get("position", ""))
    llm_city = normalize_optional_field(llm_result.get("city", ""))
    llm_unit = normalize_optional_field(llm_result.get("unit", ""))
    llm_biography_short = _normalize_multiline_text(llm_result.get("biography_short", ""))

    if (not position) and llm_position:
        position = llm_position
    if (not city_value) and llm_city:
        city_value = llm_city
        location_text = llm_city
    if (not unit_text) and llm_unit:
        unit_text = llm_unit

    location_clean = strip_prefixed_label(location_text, ["工作地点", "地点"])
    city_value = location_clean or location_text or city_value

    profession_tags = infer_profession_keywords(
        position,
        summary,
        full_content,
        unit_text,
        " ".join(extra_fields.values()),
        " ".join(mapped_fields.values()),
    )
    llm_profession = normalize_optional_field(llm_result.get("profession", ""))
    if llm_profession and llm_profession not in profession_tags:
        profession_tags.insert(0, llm_profession)
    llm_profession_tags_raw = llm_result.get("profession_tags", [])
    if isinstance(llm_profession_tags_raw, list):
        for tag in llm_profession_tags_raw:
            token = normalize_optional_field(tag)
            if token and token not in profession_tags:
                profession_tags.append(token)

    llm_extra_keywords: List[str] = []
    llm_keywords_raw = llm_result.get("keywords_extra", [])
    if isinstance(llm_keywords_raw, list):
        for item in llm_keywords_raw:
            token = normalize_optional_field(item)
            if token and token not in llm_extra_keywords:
                llm_extra_keywords.append(token)

    position_kw = build_position_keyword(position)
    source_host_kw = build_source_host_keyword(detail_url, source_list_url, image_url)

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
    if unit_text:
        desc_parts.append(f"单位：{unit_text}")
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
    if birth_date_norm:
        desc_parts.append(f"出生日期：{birth_date_norm}")
    if photo_taken_at:
        desc_parts.append(f"拍摄日期：{photo_taken_at}")
    if age_at_photo:
        desc_parts.append(f"拍摄时年龄：{age_at_photo}岁")
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
    append_bio_to_desc = bool(getattr(llm_enricher, "append_biography_to_description", True))
    if llm_biography_short and append_bio_to_desc:
        desc_parts.append("小传：")
        desc_parts.append(llm_biography_short)
    rich_description = _normalize_multiline_text("\n".join(desc_parts))

    keywords: List[str] = []

    def _add_keyword(value: Any) -> None:
        token = normalize_optional_field(value)
        if token and token not in keywords:
            keywords.append(token)

    # Minimal and high-value keyword set (clean_keywords() keeps top-N order later).
    _add_keyword(title)
    for tag in profession_tags:
        _add_keyword(tag)
    _add_keyword(position_kw or position)
    _add_keyword(unit_text)
    _add_keyword(city_value)
    _add_keyword(gender)
    _add_keyword(police_id)
    _add_keyword(english_name)
    _add_keyword(source_host_kw)
    for token in llm_extra_keywords:
        _add_keyword(token)
    _add_keyword("public-archive")

    ext = ".jpg"
    source_resolved = Path(source_path).resolve()
    current_local = norm_abs_path(str(row.get("local_image_path", "")))
    existing_local_path = Path(current_local) if current_local else None
    copied_to_named_folder = False

    # File naming must be person-centric for monitor/open-row consistency.
    display_name_for_file = normalize_optional_field(row.get("name", "")) or person_name or title or "unnamed"
    desired_base = sanitize_filename(display_name_for_file, fallback="unnamed")
    keep_existing_named_file = False
    if existing_local_path and existing_local_path.exists() and existing_local_path.parent.resolve() == named_dir:
        current_base = sanitize_filename(existing_local_path.stem, fallback="")
        if current_base == desired_base or current_base.startswith(f"{desired_base}_"):
            keep_existing_named_file = True

    if keep_existing_named_file:
        final_path = existing_local_path.resolve()  # type: ignore[union-attr]
        reserved_paths.add(str(final_path))
    else:
        final_path = unique_named_path(named_dir, display_name_for_file, ext=ext, reserved=reserved_paths)
        if source_resolved != final_path:
            shutil.copy2(source_resolved, final_path)
            copied_to_named_folder = True

    d2i_profile_payload: Dict[str, Any] = {
        "email": email_text,
        "english_name": english_name,
        "title": position,
        "unit": unit_text,
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
    if birth_date_norm:
        d2i_profile_payload["birth_date"] = birth_date_norm
    if photo_taken_at:
        d2i_profile_payload["photo_taken_at"] = photo_taken_at
    if age_at_photo:
        d2i_profile_payload["age_at_photo"] = age_at_photo
    if profession_tags:
        d2i_profile_payload["profession"] = profession_tags[0]
        d2i_profile_payload["profession_tags"] = profession_tags
    if llm_biography_short:
        d2i_profile_payload["biography_short"] = llm_biography_short
    if llm_result:
        d2i_profile_payload["llm_enriched"] = True
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
    audit_snapshot = {
        "name": person_name or title,
        "gender": gender,
        "birth_date": birth_date_norm,
        "photo_taken_at": photo_taken_at,
        "age_at_photo": age_at_photo,
        "position": position,
        "city": city_value,
        "unit": unit_text,
        "profession": (profession_tags[0] if profession_tags else ""),
        "police_id": police_id,
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
                "name": person_name or title,
                "audit": audit_snapshot,
            },
        )
        return True, str(row["local_image_path"]), copied_to_named_folder
    except Exception as exc:
        if record_failure:
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
                    "name": person_name or title,
                    "audit": audit_snapshot,
                },
            )
        return False, str(final_path), copied_to_named_folder


def _parse_bool_rule(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def resolve_metadata_retry_settings(rules: Dict[str, Any]) -> Dict[str, Any]:
    try:
        max_attempts = int(rules.get("metadata_write_retries", 3))
    except Exception:
        max_attempts = 3
    if max_attempts < 1:
        max_attempts = 1

    try:
        retry_delay_seconds = float(rules.get("metadata_write_retry_delay_seconds", 1.2))
    except Exception:
        retry_delay_seconds = 1.2
    if retry_delay_seconds < 0:
        retry_delay_seconds = 0.0

    try:
        retry_backoff_factor = float(rules.get("metadata_write_retry_backoff_factor", 1.5))
    except Exception:
        retry_backoff_factor = 1.5
    if retry_backoff_factor < 1.0:
        retry_backoff_factor = 1.0

    retry_failed_first = _parse_bool_rule(rules.get("retry_failed_first", True), default=True)
    return {
        "max_attempts": max_attempts,
        "retry_delay_seconds": retry_delay_seconds,
        "retry_backoff_factor": retry_backoff_factor,
        "retry_failed_first": retry_failed_first,
    }


def _load_latest_metadata_status(results_path: Path) -> Dict[str, str]:
    latest_status: Dict[str, str] = {}
    for row in iter_jsonl(results_path):
        detail_url = str(row.get("detail_url", "")).strip()
        if not detail_url:
            continue
        latest_status[detail_url] = str(row.get("status", "")).strip().lower()
    return latest_status


def _order_metadata_rows_by_retry_priority(
    rows: List[Dict[str, Any]],
    latest_status: Dict[str, str],
    retry_failed_first: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    if not retry_failed_first:
        return list(rows), {"failed_rows": 0, "pending_rows": len(rows), "ok_rows": 0}

    failed_rows: List[Dict[str, Any]] = []
    pending_rows: List[Dict[str, Any]] = []
    ok_rows: List[Dict[str, Any]] = []
    for row in rows:
        detail_url = str(row.get("detail_url", "")).strip()
        status = str(latest_status.get(detail_url, "")).strip().lower() if detail_url else ""
        if status == "ok":
            ok_rows.append(row)
        elif status:
            failed_rows.append(row)
        else:
            pending_rows.append(row)

    ordered_rows = failed_rows + pending_rows + ok_rows
    return ordered_rows, {
        "failed_rows": len(failed_rows),
        "pending_rows": len(pending_rows),
        "ok_rows": len(ok_rows),
    }


def write_metadata_for_queue_row_with_retries(
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
    llm_enricher: Optional[Any] = None,
    max_attempts: int = 1,
    retry_delay_seconds: float = 0.0,
    retry_backoff_factor: float = 1.0,
) -> Tuple[bool, str, bool, int]:
    attempts = max(1, int(max_attempts))
    delay_seconds = max(0.0, float(retry_delay_seconds))
    backoff_factor = max(1.0, float(retry_backoff_factor))

    final_path = ""
    copied_any = False
    detail_url = str(row.get("detail_url", "")).strip()
    row_name = _display_person_name(row.get("name", ""), str(row.get("detail_url", "")))
    if attempts > 1:
        runtime_log(
            "STEP",
            f"{row_name}元数据将按重试策略执行",
            detail=detail_url,
            person=row_name,
            max_attempts=attempts,
        )
    for attempt_idx in range(1, attempts + 1):
        ok, final_path, copied_flag = write_metadata_for_queue_row(
            row=row,
            source_path=source_path,
            named_dir=named_dir,
            reserved_paths=reserved_paths,
            field_labels=field_labels,
            review_path=review_path,
            results_path=results_path,
            detail_to_final_path=detail_to_final_path,
            sha_runtime_path=sha_runtime_path,
            llm_enricher=llm_enricher,
            record_failure=(attempt_idx >= attempts),
        )
        if copied_flag:
            copied_any = True
        if ok:
            if attempt_idx > 1:
                runtime_log(
                    "RETRY",
                    f"{row_name}元数据重试成功",
                    detail=detail_url,
                    person=row_name,
                    attempt=attempt_idx,
                    max_attempts=attempts,
                )
            return True, final_path, copied_any, attempt_idx

        if final_path:
            row["local_image_path"] = norm_abs_path(final_path) or final_path

        if attempt_idx < attempts and delay_seconds > 0:
            sleep_seconds = delay_seconds * (backoff_factor ** (attempt_idx - 1))
            runtime_log(
                "RETRY",
                f"{row_name}元数据写入失败，准备延迟重试",
                detail=detail_url,
                person=row_name,
                attempt=attempt_idx,
                max_attempts=attempts,
                next_delay=f"{sleep_seconds:.2f}s",
            )
            time.sleep(max(0.0, sleep_seconds))
        elif attempt_idx < attempts:
            runtime_log(
                "RETRY",
                f"{row_name}元数据写入失败，立即重试",
                detail=detail_url,
                person=row_name,
                attempt=attempt_idx,
                max_attempts=attempts,
            )

    runtime_log(
        "FAIL",
        f"{row_name}元数据写入失败（已达最大重试次数）",
        detail=detail_url,
        person=row_name,
        attempts=attempts,
    )
    return False, final_path, copied_any, attempts


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


def manual_pause_flag_path(output_root: Path) -> Path:
    return output_root / "state" / "manual_pause.flag"


def wait_if_manual_paused(output_root: Path, stage: str = "") -> None:
    flag_path = manual_pause_flag_path(output_root)
    announced = False
    stage_text = str(stage or "").strip() or "unknown"
    while flag_path.exists():
        if not announced:
            print(f"[PAUSE] manual pause active; waiting ({stage_text})")
            announced = True
        time.sleep(1.0)
    if announced:
        print(f"[PAUSE] manual pause released; continue ({stage_text})")


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
    merged = "\n".join(lines).strip()
    if not merged:
        return ""

    # Some sites split date tokens into one token per line, e.g.
    # "2019\n年\n9\n月\n11\n日". Merge these back to readable phrases.
    merged = re.sub(r"^\s*[\"'`“”‘’]+\s*\n?", "", merged)
    merged = re.sub(
        r"(\d{4})\s*\n\s*年\s*\n\s*(\d{1,2})\s*\n\s*月\s*\n\s*(\d{1,2})\s*\n\s*日",
        r"\1年\2月\3日",
        merged,
    )
    merged = re.sub(r"(\d{4})\s*\n\s*年", r"\1年", merged)
    merged = re.sub(r"(\d{1,2})\s*\n\s*月", r"\1月", merged)
    merged = re.sub(r"(\d{1,2})\s*\n\s*日", r"\1日", merged)
    merged = re.sub(r"(\d{1,2})\s*\n\s*号", r"\1号", merged)
    merged = re.sub(r"(\d{1,2})\s*\n\s*次", r"\1次", merged)
    merged = re.sub(r"\n{3,}", "\n\n", merged)
    return merged.strip()


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


def _select_nodes(selector_source: Any, selector: str) -> List[Any]:
    selector_text = str(selector or "").strip()
    if not selector_text:
        return []
    try:
        if selector_text.startswith("xpath:"):
            return list(selector_source.xpath(selector_text[len("xpath:") :]))
        if selector_text.startswith("/"):
            return list(selector_source.xpath(selector_text))
        return list(selector_source.css(selector_text))
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
    queue_path = raw_dir / "metadata_queue.jsonl"
    metadata_results_path = raw_dir / "metadata_write_results.jsonl"
    metadata_report_path = reports_dir / "metadata_write_report.json"
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
    disable_page_images_during_crawl = bool(rules.get("disable_page_images_during_crawl", True))
    inline_download_enabled = bool(rules.get("download_images_during_crawl", True))
    inline_metadata_enabled = (
        bool(rules.get("write_metadata_inline_per_item", True))
        and bool(rules.get("write_metadata", True))
        and HAS_METADATA_WRITER
    )
    direct_write_images = _parse_bool_rule(rules.get("direct_write_images", False), default=False)
    metadata_retry_settings = resolve_metadata_retry_settings(rules)
    metadata_write_retries = int(metadata_retry_settings.get("max_attempts", 1))
    metadata_retry_delay_seconds = float(metadata_retry_settings.get("retry_delay_seconds", 0.0))
    metadata_retry_backoff_factor = float(metadata_retry_settings.get("retry_backoff_factor", 1.0))
    llm_report_path = reports_dir / "llm_enrichment_report.json"
    llm_enricher: Optional[Any] = None
    if HAS_LLM_ENRICHER:
        try:
            llm_enricher = LLMEnricher(rules, state_dir)  # type: ignore[misc]
        except Exception:
            llm_enricher = None
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

    url_index: Dict[str, str] = {}
    sha_index: Dict[str, str] = {}
    if not direct_write_images:
        url_index_raw = load_json(url_index_path, {})
        sha_index_raw = load_json(sha_index_path, {})
        if isinstance(url_index_raw, dict):
            url_index = {
                str(k): str(v) for k, v in url_index_raw.items() if str(k).strip() and str(v).strip()
            }
        if isinstance(sha_index_raw, dict):
            sha_index = {
                str(k): str(v) for k, v in sha_index_raw.items() if str(k).strip() and str(v).strip()
            }
    index_dirty_count = 0

    def _checkpoint_indexes(force: bool = False) -> None:
        nonlocal index_dirty_count
        if direct_write_images:
            return
        if not inline_download_enabled:
            return
        if not force:
            if index_dirty_count <= 0:
                return
        save_json(url_index_path, url_index)
        save_json(sha_index_path, sha_index)
        index_dirty_count = 0

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

    inline_metadata_totals = {
        "rows_seen": 0,
        "written_ok": 0,
        "skipped_existing_detail": 0,
        "skipped_missing_local_path": 0,
        "failed": 0,
        "copied_to_named_folder": 0,
        "retried_rows": 0,
        "retry_attempts": 0,
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

    direct_manifest_by_detail: Dict[str, Dict[str, str]] = {}
    if direct_write_images:
        inline_named_dir.mkdir(parents=True, exist_ok=True)
        if not inline_reserved_paths:
            for p in sorted(inline_named_dir.glob("*")):
                if p.is_file():
                    inline_reserved_paths.add(str(p.resolve()))
        direct_manifest_by_detail, _ = _load_download_manifest_lookups(download_manifest)

    def _inline_write_for_profile(profile_row: Dict[str, Any], image_sha_value: str, source_path_value: str) -> str:
        if not inline_metadata_enabled:
            return ""
        detail_url_inline = str(profile_row.get("detail_url", "")).strip()
        person_name = _display_person_name(profile_row.get("name", ""), detail_url_inline)
        if not detail_url_inline:
            return ""
        if detail_url_inline in inline_existing_detail_urls:
            inline_metadata_totals["skipped_existing_detail"] += 1
            return ""

        source_norm = norm_abs_path(source_path_value)
        if (not source_norm) or (not Path(source_norm).exists()):
            inline_metadata_totals["skipped_missing_local_path"] += 1
            runtime_log(
                "WARN",
                f"{person_name}元数据写入跳过：本地图片不存在",
                detail=detail_url_inline,
                person=person_name,
                candidate=source_path_value,
            )
            append_jsonl(
                review_path,
                {
                    "scraped_at": utc_now_iso(),
                    "reason": "metadata_missing_local_image_path",
                    "detail_url": detail_url_inline,
                    "candidates": [source_path_value],
                },
            )
            return ""

        queue_row = build_metadata_queue_row_from_profile(profile_row, image_sha_value, source_norm)
        inline_metadata_totals["rows_seen"] += 1
        ok_meta, final_path, copied_flag, attempts_used = write_metadata_for_queue_row_with_retries(
            row=queue_row,
            source_path=source_norm,
            named_dir=inline_named_dir,
            reserved_paths=inline_reserved_paths,
            field_labels=inline_field_labels,
            review_path=review_path,
            results_path=metadata_results_path,
            llm_enricher=llm_enricher,
            max_attempts=metadata_write_retries,
            retry_delay_seconds=metadata_retry_delay_seconds,
            retry_backoff_factor=metadata_retry_backoff_factor,
        )
        if attempts_used > 1:
            inline_metadata_totals["retried_rows"] += 1
            inline_metadata_totals["retry_attempts"] += (attempts_used - 1)
        if ok_meta:
            inline_metadata_totals["written_ok"] += 1
            if copied_flag:
                inline_metadata_totals["copied_to_named_folder"] += 1
            append_jsonl(queue_path, queue_row)
            inline_existing_detail_urls.add(detail_url_inline)
            return norm_abs_path(final_path) or final_path
        inline_metadata_totals["failed"] += 1
        runtime_log(
            "FAIL",
            f"{person_name}元数据写入失败",
            detail=detail_url_inline,
            person=person_name,
        )
        return ""

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

    temp_root = scoped_temp_dir("_tmp_browser_crawl", scope_hint=str(output_root))
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
        disable_page_images=disable_page_images_during_crawl,
    )

    def _maybe_sleep_between_pages() -> None:
        nonlocal fetch_count
        if fetch_count > 0 and interval_max > 0:
            time.sleep(random.uniform(interval_min, interval_max))
        fetch_count += 1

    def _record_failure(url: str, reason: str, context: Dict[str, Any]) -> None:
        metrics["failures"] += 1
        phase = str(context.get("phase", "")).strip() if isinstance(context, dict) else ""
        person_name = ""
        if isinstance(context, dict):
            person_name = _display_person_name(context.get("name", ""), str(context.get("detail_url", "") or url))
        runtime_log(
            "FAIL",
            (f"{person_name}页面抓取失败" if person_name else "页面抓取失败"),
            phase=phase,
            url=url,
            reason=reason,
            person=person_name,
        )
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
        runtime_log(
            "STOP",
            "backoff activated during crawl",
            phase=phase,
            url=url,
            reason=reason,
            blocked_until=blocked_until,
        )

    try:
        downloader._init_browser()  # type: ignore[attr-defined]

        while list_queue:
            if blocked_until:
                break
            wait_if_manual_paused(output_root, stage="crawl:list")
            list_url = list_queue.popleft()
            runtime_log(
                "STEP",
                "crawl list page",
                url=list_url,
                queued=len(list_queue),
                discovered=metrics.get("list_rows_seen", 0),
            )
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
            list_nodes = _select_nodes(list_source, list_item_selector)
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
            wait_if_manual_paused(output_root, stage="crawl:detail")
            detail_url, seed_name, list_url, seed_fields = detail_queue.popleft()
            person_seed = _display_person_name(seed_name, detail_url)
            runtime_log(
                "STEP",
                f"正在抓取{person_seed}的详情页",
                detail=detail_url,
                person=person_seed,
                queued=len(detail_queue),
                saved=metrics.get("detail_pages_saved", 0),
            )
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
                    {"phase": "detail", "list_url": list_url, "name": seed_name, "detail_url": detail_url},
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
            person_name = _display_person_name(name, detail_url)
            image_raw = _extract_first(detail_source, selectors.get("detail_image"))
            image_url = urljoin(detail_url, image_raw) if image_raw else ""

            gender_text = _extract_first(detail_source, selectors.get("detail_gender"))
            gender_lookup = _normalize_text(gender_text)
            mapped_gender = gender_map.get(gender_text)
            if mapped_gender is None and gender_lookup:
                mapped_gender = gender_map.get(gender_lookup)
            gender = normalize_gender(mapped_gender if mapped_gender is not None else gender_text)
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

            if not gender:
                gender = infer_gender_from_texts(
                    gender_text,
                    summary,
                    full_content,
                    " ".join(str(v) for v in merged_fields.values()),
                ) or default_gender

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

            # Fallback for sites where detail pages are unstable but list cards already contain image links.
            if not image_url:
                mapped_image_url = normalize_optional_field(mapped_fields.get("image_url", ""))
                if mapped_image_url:
                    image_url = urljoin((detail_url or list_url), mapped_image_url)
            if not image_url:
                for fallback_key in ("image_url", "list_image_url", "avatar_url", "photo_url", "portrait_url"):
                    fallback_raw = normalize_optional_field(merged_fields.get(fallback_key, ""))
                    if not fallback_raw:
                        continue
                    image_url = urljoin((detail_url or list_url), fallback_raw)
                    break

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
                reused_cached_image = False
                if direct_write_images:
                    previous = direct_manifest_by_detail.get(detail_url, {}) if detail_url else {}
                    previous_sha = str(previous.get("sha", "")).strip()
                    previous_path = norm_abs_path(str(previous.get("path", "")))
                    if previous_path and _is_usable_cached_image(previous_path):
                        consecutive_inline_image_failures = 0
                        _inline_write_for_profile(record, previous_sha, previous_path)
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
                            output_root_hint=str(output_root),
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
                            runtime_log(
                                "FAIL",
                                f"{person_name}图片下载失败（内联）",
                                detail=detail_url,
                                image=image_url,
                                person=person_name,
                                error=error_img,
                                blocked=bool(blocked_tag),
                            )
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
                            runtime_log(
                                "FAIL",
                                f"{person_name}图片下载失败（内联响应不是图片）",
                                detail=detail_url,
                                image=image_url,
                                person=person_name,
                                size=len(payload_img),
                                blocked=bool(blocked_tag),
                            )
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
                            target_named: Optional[Path] = None
                            if previous_path:
                                try:
                                    previous_p = Path(previous_path)
                                    if (
                                        previous_p.exists()
                                        and previous_p.is_file()
                                        and previous_p.parent.resolve() == inline_named_dir.resolve()
                                    ):
                                        target_named = previous_p.resolve()
                                except Exception:
                                    target_named = None
                            if target_named is None:
                                target_named = unique_named_path(
                                    inline_named_dir,
                                    normalize_optional_field(name) or person_name,
                                    ext=".jpg",
                                    reserved=inline_reserved_paths,
                                )
                            _atomic_write_bytes(target_named, payload_img)
                            metrics["inline_image_downloaded_new"] += 1
                            runtime_log(
                                "STEP",
                                f"{person_name}图片下载成功（内联）",
                                detail=detail_url,
                                image=image_url,
                                person=person_name,
                                sha=sha[:12],
                            )
                            inline_named_path = _inline_write_for_profile(record, sha, str(target_named.resolve()))
                            saved_path = str(target_named.resolve())
                            named_path = norm_abs_path(inline_named_path) or saved_path
                            append_jsonl(
                                download_manifest,
                                {
                                    "downloaded_at": utc_now_iso(),
                                    "detail_url": detail_url,
                                    "image_url": image_url,
                                    "name": name,
                                    "sha256": sha,
                                    "saved_path": saved_path,
                                    "named_path": named_path,
                                    "route": "browser_inline_direct",
                                },
                            )
                            if detail_url:
                                direct_manifest_by_detail[detail_url] = {"sha": sha, "path": named_path}

                if (not direct_write_images) and image_url in url_index:
                    sha_cached, source_cached = _resolve_cached_source_by_image_url(image_url, url_index, sha_index)
                    if source_cached:
                        metrics["inline_image_reused_by_url"] += 1
                        consecutive_inline_image_failures = 0
                        _inline_write_for_profile(record, sha_cached, source_cached)
                        reused_cached_image = True
                    else:
                        dropped = _drop_stale_cache_index_entries(image_url, sha_cached, url_index, sha_index)
                        if dropped > 0:
                            index_dirty_count += dropped
                            _checkpoint_indexes()
                if (not direct_write_images) and (not reused_cached_image):
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
                    output_root_hint=str(output_root),
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
                        runtime_log(
                            "FAIL",
                            f"{person_name}图片下载失败（内联）",
                            detail=detail_url,
                            image=image_url,
                            person=person_name,
                            error=error_img,
                            blocked=bool(blocked_tag),
                        )
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
                        runtime_log(
                            "FAIL",
                            f"{person_name}图片下载失败（内联响应不是图片）",
                            detail=detail_url,
                            image=image_url,
                            person=person_name,
                            size=len(payload_img),
                            blocked=bool(blocked_tag),
                        )
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
                        source_cached = norm_abs_path(str(sha_index.get(sha, ""))) if sha in sha_index else ""
                        if sha in sha_index and _is_usable_cached_image(source_cached):
                            metrics["inline_image_reused_by_sha"] += 1
                            url_index[image_url] = sha
                            index_dirty_count += 1
                            _checkpoint_indexes()
                            _inline_write_for_profile(record, sha, source_cached)
                        else:
                            if sha in sha_index:
                                try:
                                    del sha_index[sha]
                                    index_dirty_count += 1
                                except Exception:
                                    pass
                            ext = _guess_extension(image_url, content_type_img)
                            target = image_root / sha[:2] / f"{sha}{ext}"
                            target.parent.mkdir(parents=True, exist_ok=True)
                            target.write_bytes(payload_img)
                            sha_index[sha] = str(target.resolve())
                            url_index[image_url] = sha
                            index_dirty_count += 1
                            _checkpoint_indexes()
                            metrics["inline_image_downloaded_new"] += 1
                            runtime_log(
                                "STEP",
                                f"{person_name}图片下载成功（内联）",
                                detail=detail_url,
                                image=image_url,
                                person=person_name,
                                sha=sha[:12],
                            )
                            inline_named_path = _inline_write_for_profile(record, sha, str(target.resolve()))
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
                                    **({"named_path": inline_named_path} if inline_named_path else {}),
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

    if llm_enricher is not None:
        try:
            llm_enricher.flush_cache()
            append_llm_report(llm_report_path, "crawl_browser_mode_inline_metadata", llm_enricher.report())
        except Exception:
            pass

    if inline_download_enabled:
        _checkpoint_indexes(force=True)
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
                    "llm_enrichment": (
                        llm_enricher.report() if llm_enricher is not None else {"enabled": False}
                    ),
                },
            )

    report = {
        "closed_at": utc_now_iso(),
        "close_reason": "browser_mode_blocked" if blocked_until else "browser_mode_finished",
        "blocked_until": blocked_until,
        "blocked_reason": blocked_reason,
        "crawl_mode": "browser",
        "browser_engine": browser_engine,
        "disable_page_images_during_crawl": disable_page_images_during_crawl,
        "interval_min_seconds": interval_min,
        "interval_max_seconds": interval_max,
        "inline_download_enabled": inline_download_enabled,
        "inline_metadata_enabled": inline_metadata_enabled,
        "inline_metadata_totals": inline_metadata_totals if inline_metadata_enabled else {},
        "llm_enrichment": llm_enricher.report() if llm_enricher is not None else {"enabled": False},
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
    output_root_hint: str = "",
) -> Tuple[bool, bytes, str, str]:
    if not HAS_D2I_DOWNLOADER:
        return False, b"", "", f"d2i_downloader_unavailable: {D2I_DOWNLOADER_ERROR}"

    temp_root = scoped_temp_dir("_tmp_browser_downloads", scope_hint=output_root_hint)

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

    direct_write_images = _parse_bool_rule(rules.get("direct_write_images", False), default=False)

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

    direct_named_dir: Optional[Path] = None
    direct_reserved_paths: set[str] = set()
    manifest_by_detail: Dict[str, Dict[str, str]] = {}
    if direct_write_images:
        direct_named_dir = resolve_named_output_dir(output_root, rules)
        direct_named_dir.mkdir(parents=True, exist_ok=True)
        for p in sorted(direct_named_dir.glob("*")):
            if p.is_file():
                direct_reserved_paths.add(str(p.resolve()))
        manifest_by_detail, _ = _load_download_manifest_lookups(download_manifest)

    url_index: Dict[str, str] = {}
    sha_index: Dict[str, str] = {}
    if not direct_write_images:
        url_index_raw = load_json(url_index_path, {})
        sha_index_raw = load_json(sha_index_path, {})
        if isinstance(url_index_raw, dict):
            url_index = {str(k): str(v) for k, v in url_index_raw.items() if str(k).strip() and str(v).strip()}
        if isinstance(sha_index_raw, dict):
            sha_index = {str(k): str(v) for k, v in sha_index_raw.items() if str(k).strip() and str(v).strip()}
    index_dirty_count = 0

    def _checkpoint_indexes(force: bool = False) -> None:
        nonlocal index_dirty_count
        if direct_write_images:
            return
        if not force:
            if index_dirty_count <= 0:
                return
        save_json(url_index_path, url_index)
        save_json(sha_index_path, sha_index)
        index_dirty_count = 0

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
    metadata_retry_settings = resolve_metadata_retry_settings(rules)
    metadata_write_retries = int(metadata_retry_settings.get("max_attempts", 1))
    metadata_retry_delay_seconds = float(metadata_retry_settings.get("retry_delay_seconds", 0.0))
    metadata_retry_backoff_factor = float(metadata_retry_settings.get("retry_backoff_factor", 1.0))
    llm_report_path = reports_root / "llm_enrichment_report.json"
    llm_enricher: Optional[Any] = None
    if HAS_LLM_ENRICHER:
        try:
            llm_enricher = LLMEnricher(rules, state_root)  # type: ignore[misc]
        except Exception:
            llm_enricher = None

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
        "retried_rows": 0,
        "retry_attempts": 0,
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
            runtime_log(
                "WARN",
                "inline metadata skipped: local image missing",
                detail=detail_url,
                candidate=source_path_value,
            )
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
        ok_meta, final_path, copied_flag, attempts_used = write_metadata_for_queue_row_with_retries(
            row=queue_row,
            source_path=source_norm,
            named_dir=inline_named_dir,
            reserved_paths=inline_reserved_paths,
            field_labels=inline_field_labels,
            review_path=review_path,
            results_path=metadata_results_path,
            llm_enricher=llm_enricher,
            max_attempts=metadata_write_retries,
            retry_delay_seconds=metadata_retry_delay_seconds,
            retry_backoff_factor=metadata_retry_backoff_factor,
        )
        if attempts_used > 1:
            inline_metadata_totals["retried_rows"] += 1
            inline_metadata_totals["retry_attempts"] += (attempts_used - 1)
        if ok_meta:
            inline_metadata_totals["written_ok"] += 1
            if copied_flag:
                inline_metadata_totals["copied_to_named_folder"] += 1
            append_jsonl(queue_path, queue_row)
            inline_existing_detail_urls.add(detail_url)
            return norm_abs_path(final_path) or final_path
        inline_metadata_totals["failed"] += 1
        runtime_log(
            "FAIL",
            "inline metadata write failed",
            detail=detail_url,
            name=profile_row.get("name", ""),
        )
        return ""

    failed_image_urls: set[str] = set()
    if review_path.exists():
        for review_item in iter_jsonl(review_path):
            reason = str(review_item.get("reason", "")).strip().lower()
            if reason.startswith("image_download_"):
                image_url_retry = str(review_item.get("image_url", "")).strip()
                if image_url_retry:
                    failed_image_urls.add(image_url_retry)

    runtime_log(
        "STAGE",
        "image download stage scanning started",
        mode=image_download_mode,
        failed_retry_candidates=len(failed_image_urls),
    )

    browser_downloader: Optional[Any] = None
    if browser_mode_selected and HAS_D2I_DOWNLOADER:
        temp_root = scoped_temp_dir("_tmp_browser_downloads", scope_hint=str(output_root))
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
        runtime_log(
            "STOP",
            "backoff activated during image download",
            phase=phase,
            url=url,
            reason=reason,
            blocked_until=blocked_until,
        )

    try:
        for profile in iter_jsonl(profiles_path):
            wait_if_manual_paused(output_root, stage="download:images")
            totals["profiles_seen"] += 1
            image_url = str(profile.get("image_url", "")).strip()
            if not image_url:
                continue
            totals["profiles_with_image_url"] += 1
            retrying_failed = image_url in failed_image_urls
            detail_url = str(profile.get("detail_url", "")).strip()
            person_name = _display_person_name(profile.get("name", ""), detail_url)
            runtime_log(
                "STEP",
                f"正在下载{person_name}的图片",
                idx=totals["profiles_with_image_url"],
                retry_failed=retrying_failed,
                person=person_name,
                detail=detail_url,
                image=image_url,
            )

            if direct_write_images and direct_named_dir is not None and detail_url:
                previous = manifest_by_detail.get(detail_url, {})
                previous_sha = str(previous.get("sha", "")).strip()
                previous_path = norm_abs_path(str(previous.get("path", "")))
                if previous_path and _is_usable_cached_image(previous_path):
                    # If a previous run left the file outside the final folder (e.g. old sha cache),
                    # copy it into named output for a cleaner pipeline in direct-write mode.
                    try:
                        prev_p = Path(previous_path).resolve()
                        if prev_p.parent.resolve() != direct_named_dir.resolve():
                            target_named = unique_named_path(
                                direct_named_dir,
                                normalize_optional_field(profile.get("name", "")) or person_name,
                                ext=".jpg",
                                reserved=direct_reserved_paths,
                            )
                            shutil.copy2(prev_p, target_named)
                            copied_path = str(target_named.resolve())
                            append_jsonl(
                                download_manifest,
                                {
                                    "downloaded_at": utc_now_iso(),
                                    "detail_url": detail_url,
                                    "image_url": image_url,
                                    "name": profile.get("name", ""),
                                    "sha256": previous_sha,
                                    "saved_path": copied_path,
                                    "named_path": copied_path,
                                    "route": "direct_copy_existing",
                                },
                            )
                            manifest_by_detail[detail_url] = {"sha": previous_sha, "path": copied_path}
                            if inline_metadata_enabled:
                                _inline_write_for_profile(profile, previous_sha, copied_path)
                            continue
                    except Exception:
                        pass
                    if inline_metadata_enabled:
                        _inline_write_for_profile(profile, previous_sha, previous_path)
                    continue

            if (not direct_write_images) and image_url in url_index:
                sha_cached, source_cached = _resolve_cached_source_by_image_url(image_url, url_index, sha_index)
                if source_cached:
                    totals["reused_by_url"] += 1
                    if inline_metadata_enabled:
                        _inline_write_for_profile(profile, sha_cached, source_cached)
                    continue
                dropped = _drop_stale_cache_index_entries(image_url, sha_cached, url_index, sha_index)
                if dropped > 0:
                    index_dirty_count += dropped
                    _checkpoint_indexes()

            if not first_download:
                time.sleep(random.uniform(interval_min, interval_max))
            first_download = False

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
                    output_root_hint=str(output_root),
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
                    runtime_log(
                        "FAIL",
                        f"{person_name}图片下载失败（浏览器模式）",
                        detail=detail_url,
                        image=image_url,
                        person=person_name,
                        error=browser_error,
                        blocked=bool(blocked_tag),
                    )
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
                    runtime_log(
                        "FAIL",
                        f"{person_name}图片下载请求异常",
                        detail=detail_url,
                        image=image_url,
                        person=person_name,
                        error=str(exc),
                    )
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
                runtime_log(
                    "FAIL",
                    f"{person_name}图片下载失败（HTTP错误）",
                    detail=detail_url,
                    image=image_url,
                    person=person_name,
                    status=response_status,
                    route=route_used,
                )
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
                runtime_log(
                    "FAIL",
                    f"{person_name}图片下载失败（响应不是图片）",
                    detail=detail_url,
                    image=image_url,
                    person=person_name,
                    status=response_status,
                    content_type=content_type,
                    size=len(payload),
                    route=route_used,
                    browser_challenge=browser_challenge_payload,
                )
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

            if direct_write_images and direct_named_dir is not None:
                target_named: Optional[Path] = None
                previous = manifest_by_detail.get(detail_url, {}) if detail_url else {}
                previous_path = norm_abs_path(str(previous.get("path", "")))
                if previous_path:
                    try:
                        previous_p = Path(previous_path)
                        if (
                            previous_p.exists()
                            and previous_p.is_file()
                            and previous_p.parent.resolve() == direct_named_dir.resolve()
                        ):
                            target_named = previous_p.resolve()
                    except Exception:
                        target_named = None

                if target_named is None:
                    target_named = unique_named_path(
                        direct_named_dir,
                        normalize_optional_field(profile.get("name", "")) or person_name,
                        ext=".jpg",
                        reserved=direct_reserved_paths,
                    )

                _atomic_write_bytes(target_named, payload)

                totals["downloaded_new"] += 1
                if route_used == "browser":
                    totals["downloaded_via_browser"] += 1
                else:
                    totals["downloaded_via_requests"] += 1
                runtime_log(
                    "STEP",
                    f"{person_name}图片下载成功",
                    detail=detail_url,
                    image=image_url,
                    person=person_name,
                    route=route_used,
                    sha=sha[:12],
                )

                inline_named_path = ""
                if inline_metadata_enabled:
                    inline_named_path = _inline_write_for_profile(profile, sha, str(target_named.resolve()))

                saved_path = str(target_named.resolve())
                named_path = norm_abs_path(inline_named_path) or saved_path
                append_jsonl(
                    download_manifest,
                    {
                        "downloaded_at": utc_now_iso(),
                        "detail_url": detail_url,
                        "image_url": image_url,
                        "name": profile.get("name", ""),
                        "sha256": sha,
                        "saved_path": saved_path,
                        "named_path": named_path,
                        "route": f"{route_used}_direct",
                    },
                )
                if detail_url:
                    manifest_by_detail[detail_url] = {"sha": sha, "path": named_path}
                continue

            source_cached = norm_abs_path(str(sha_index.get(sha, ""))) if sha in sha_index else ""
            if sha in sha_index and _is_usable_cached_image(source_cached):
                totals["reused_by_sha"] += 1
                url_index[image_url] = sha
                index_dirty_count += 1
                _checkpoint_indexes()
                if inline_metadata_enabled:
                    _inline_write_for_profile(profile, sha, source_cached)
                continue
            if sha in sha_index:
                try:
                    del sha_index[sha]
                    index_dirty_count += 1
                except Exception:
                    pass

            ext = _guess_extension(image_url, content_type)
            target = image_root / sha[:2] / f"{sha}{ext}"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)

            sha_index[sha] = str(target.resolve())
            url_index[image_url] = sha
            index_dirty_count += 1
            _checkpoint_indexes()
            totals["downloaded_new"] += 1
            if route_used == "browser":
                totals["downloaded_via_browser"] += 1
            else:
                totals["downloaded_via_requests"] += 1
            runtime_log(
                "STEP",
                f"{person_name}图片下载成功",
                detail=detail_url,
                image=image_url,
                person=person_name,
                route=route_used,
                sha=sha[:12],
            )

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

    _checkpoint_indexes(force=True)
    if llm_enricher is not None:
        try:
            llm_enricher.flush_cache()
            append_llm_report(llm_report_path, "download_images_inline_metadata", llm_enricher.report())
        except Exception:
            pass
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
                "llm_enrichment": (
                    llm_enricher.report() if llm_enricher is not None else {"enabled": False}
                ),
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
        "llm_enrichment": llm_enricher.report() if llm_enricher is not None else {"enabled": False},
    }
    save_json(reports_root / "image_download_report.json", report)
    return report


def build_metadata_queue(output_root: Path) -> Dict[str, Any]:
    profiles_path = output_root / "raw" / "profiles.jsonl"
    queue_path = output_root / "raw" / "metadata_queue.jsonl"
    url_index_path = output_root / "state" / "image_url_index.json"
    sha_index_path = output_root / "state" / "image_sha_index.json"
    downloads_manifest_path = output_root / "downloads" / "image_downloads.jsonl"

    url_index_raw = load_json(url_index_path, {})
    sha_index_raw = load_json(sha_index_path, {})
    url_index: Dict[str, str] = {}
    sha_index: Dict[str, str] = {}
    if isinstance(url_index_raw, dict):
        for raw_key, raw_value in url_index_raw.items():
            key = str(raw_key or "").strip()
            value = str(raw_value or "").strip()
            if key and value:
                url_index[key] = value
    if isinstance(sha_index_raw, dict):
        for raw_key, raw_value in sha_index_raw.items():
            key = str(raw_key or "").strip()
            value = norm_abs_path(str(raw_value or ""))
            if key and value:
                sha_index[key] = value

    manifest_by_detail, manifest_by_image = _load_download_manifest_lookups(downloads_manifest_path)

    profiles: List[Dict[str, Any]] = []
    profiles_by_detail: Dict[str, Dict[str, Any]] = {}
    for profile in iter_jsonl(profiles_path):
        if not isinstance(profile, dict):
            continue
        profiles.append(profile)
        detail_url = str(profile.get("detail_url", "")).strip()
        if detail_url:
            profiles_by_detail[detail_url] = profile

    queue_rows: List[Dict[str, Any]] = []
    existing_details: set[str] = set()
    refreshed = 0
    for row_raw in iter_jsonl(queue_path):
        if not isinstance(row_raw, dict):
            continue
        row = dict(row_raw)
        detail_url = str(row.get("detail_url", "")).strip()
        if not detail_url:
            continue

        profile = profiles_by_detail.get(detail_url, {})
        changed = False

        if not str(row.get("name", "")).strip():
            profile_name = str(profile.get("name", "")).strip() if isinstance(profile, dict) else ""
            if profile_name:
                row["name"] = profile_name
                changed = True
        if not str(row.get("gender", "")).strip():
            profile_gender = normalize_gender(profile.get("gender", "")) if isinstance(profile, dict) else ""
            if profile_gender:
                row["gender"] = profile_gender
                changed = True
        if not _normalize_multiline_text(row.get("summary", "")):
            profile_summary = _normalize_multiline_text(profile.get("summary", "")) if isinstance(profile, dict) else ""
            if profile_summary:
                row["summary"] = profile_summary
                changed = True
        if not _normalize_multiline_text(row.get("full_content", "")):
            profile_content = (
                _normalize_multiline_text(profile.get("full_content", "") or profile.get("summary", ""))
                if isinstance(profile, dict)
                else ""
            )
            if profile_content:
                row["full_content"] = profile_content
                changed = True
        if (not isinstance(row.get("fields"), dict)) or (not row.get("fields")):
            profile_fields = profile.get("fields", {}) if isinstance(profile, dict) else {}
            if isinstance(profile_fields, dict) and profile_fields:
                row["fields"] = profile_fields
                changed = True
        if (not isinstance(row.get("mapped"), dict)) or (not row.get("mapped")):
            profile_mapped = profile.get("mapped", {}) if isinstance(profile, dict) else {}
            if isinstance(profile_mapped, dict) and profile_mapped:
                row["mapped"] = profile_mapped
                changed = True

        profile_image_url = str(profile.get("image_url", "")).strip() if isinstance(profile, dict) else ""
        if (not str(row.get("image_url", "")).strip()) and profile_image_url:
            row["image_url"] = profile_image_url
            changed = True
        profile_source_url = str(profile.get("list_url", "")).strip() if isinstance(profile, dict) else ""
        if (not str(row.get("source_url", "")).strip()) and profile_source_url:
            row["source_url"] = profile_source_url
            changed = True

        image_url = str(row.get("image_url", "")).strip() or profile_image_url
        row_sha = str(row.get("image_sha256", "")).strip()
        row_local_path = str(row.get("local_image_path", "")).strip()
        resolved_sha, resolved_path, _ = _resolve_metadata_source_path(
            detail_url=detail_url,
            image_url=image_url,
            image_sha=row_sha,
            row_local_path=row_local_path,
            url_index=url_index,
            sha_index=sha_index,
            manifest_by_detail=manifest_by_detail,
            manifest_by_image=manifest_by_image,
        )
        if resolved_sha and (resolved_sha != row_sha):
            row["image_sha256"] = resolved_sha
            changed = True
        if resolved_path and (norm_abs_path(resolved_path) != norm_abs_path(row_local_path)):
            row["local_image_path"] = resolved_path
            changed = True

        if changed:
            refreshed += 1

        queue_rows.append(row)
        existing_details.add(detail_url)

    added = 0
    for profile in profiles:
        detail_url = str(profile.get("detail_url", "")).strip()
        if not detail_url or detail_url in existing_details:
            continue
        image_url = str(profile.get("image_url", "")).strip()
        sha = str(url_index.get(image_url, "")).strip()
        local_path = norm_abs_path(str(sha_index.get(sha, ""))) if sha else ""
        resolved_sha, resolved_path, _ = _resolve_metadata_source_path(
            detail_url=detail_url,
            image_url=image_url,
            image_sha=sha,
            row_local_path=local_path,
            url_index=url_index,
            sha_index=sha_index,
            manifest_by_detail=manifest_by_detail,
            manifest_by_image=manifest_by_image,
        )
        row = build_metadata_queue_row_from_profile(
            profile,
            resolved_sha or sha,
            resolved_path or local_path,
        )
        queue_rows.append(row)
        existing_details.add(detail_url)
        added += 1

    write_jsonl(queue_path, queue_rows)

    report = {
        "generated_at": utc_now_iso(),
        "queue_path": str(queue_path.resolve()),
        "total_rows": len(queue_rows),
        "rows_added_this_run": added,
        "rows_refreshed_this_run": refreshed,
    }
    save_json(output_root / "reports" / "metadata_queue_report.json", report)
    return report


def write_metadata_for_downloads(output_root: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    rules = dict(config.get("rules", {}))
    enabled = bool(rules.get("write_metadata", True))
    direct_write_images = _parse_bool_rule(rules.get("direct_write_images", False), default=False)
    metadata_retry_settings = resolve_metadata_retry_settings(rules)
    metadata_write_retries = int(metadata_retry_settings.get("max_attempts", 1))
    metadata_retry_delay_seconds = float(metadata_retry_settings.get("retry_delay_seconds", 0.0))
    metadata_retry_backoff_factor = float(metadata_retry_settings.get("retry_backoff_factor", 1.0))
    retry_failed_first = bool(metadata_retry_settings.get("retry_failed_first", True))
    report_path = output_root / "reports" / "metadata_write_report.json"
    llm_report_path = output_root / "reports" / "llm_enrichment_report.json"
    queue_path = output_root / "raw" / "metadata_queue.jsonl"
    review_path = output_root / "raw" / "review_queue.jsonl"
    results_path = output_root / "raw" / "metadata_write_results.jsonl"
    downloads_manifest_path = output_root / "downloads" / "image_downloads.jsonl"
    named_dir_cfg = str(rules.get("named_images_dir", "")).strip()
    if not named_dir_cfg:
        named_dir = output_root.resolve()
    else:
        named_dir = (output_root / named_dir_cfg).resolve() if not Path(named_dir_cfg).is_absolute() else Path(named_dir_cfg).resolve()

    llm_enricher: Optional[Any] = None
    if HAS_LLM_ENRICHER:
        try:
            llm_enricher = LLMEnricher(rules, output_root / "state")  # type: ignore[misc]
        except Exception:
            llm_enricher = None

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
    latest_status_map = _load_latest_metadata_status(results_path)
    rows, row_priority_stats = _order_metadata_rows_by_retry_priority(
        rows,
        latest_status_map,
        retry_failed_first=retry_failed_first,
    )
    runtime_log(
        "STAGE",
        "metadata write stage scanning queue",
        queue=len(rows),
        retry_failed_first=retry_failed_first,
        failed_first=row_priority_stats.get("failed_rows", 0),
        pending=row_priority_stats.get("pending_rows", 0),
        ok_deferred=row_priority_stats.get("ok_rows", 0),
    )
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
    url_index_path = output_root / "state" / "image_url_index.json"
    sha_index_path = output_root / "state" / "image_sha_index.json"
    url_index_raw = load_json(url_index_path, {})
    sha_index = load_json(sha_index_path, {})
    if not isinstance(url_index_raw, dict):
        url_index_raw = {}
    if not isinstance(sha_index, dict):
        sha_index = {}
    url_index: Dict[str, str] = {
        str(k): str(v)
        for k, v in url_index_raw.items()
        if str(k).strip() and str(v).strip()
    }
    manifest_by_detail, manifest_by_image = _load_download_manifest_lookups(downloads_manifest_path)

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
        "retried_rows": 0,
        "retry_attempts": 0,
        "failed_rows_prioritized": int(row_priority_stats.get("failed_rows", 0)),
        "pending_rows_prioritized": int(row_priority_stats.get("pending_rows", 0)),
        "ok_rows_deferred": int(row_priority_stats.get("ok_rows", 0)),
    }
    field_labels = collect_detail_field_labels(config, rules)

    for row in rows:
        wait_if_manual_paused(output_root, stage="metadata:write")
        totals["rows_seen"] += 1
        detail_url = str(row.get("detail_url", "")).strip()
        row_name = _display_person_name(row.get("name", ""), detail_url)
        last_status = str(latest_status_map.get(detail_url, "")).strip().lower() if detail_url else ""
        runtime_log(
            "STEP",
            f"正在写入{row_name}的元数据",
            idx=totals["rows_seen"],
            detail=detail_url,
            person=row_name,
            retry_previous_fail=(last_status not in {"", "ok"}),
        )
        image_url = str(row.get("image_url", "")).strip()
        image_sha = str(row.get("image_sha256", "")).strip()
        row_local = norm_abs_path(str(row.get("local_image_path", "")))
        resolved_sha, resolved_path, local_path_candidates = _resolve_metadata_source_path(
            detail_url=detail_url,
            image_url=image_url,
            image_sha=image_sha,
            row_local_path=row_local,
            url_index=url_index,
            sha_index={**sha_index, **sha_runtime_path},
            manifest_by_detail=manifest_by_detail,
            manifest_by_image=manifest_by_image,
        )
        source_path = resolved_path if (resolved_path and _path_exists(resolved_path)) else ""
        if resolved_sha and (resolved_sha != image_sha):
            row["image_sha256"] = resolved_sha
            image_sha = resolved_sha
        if source_path and (norm_abs_path(source_path) != row_local):
            row["local_image_path"] = source_path
        if image_sha and source_path:
            sha_runtime_path[image_sha] = source_path

        if not source_path:
            totals["skipped_missing_local_path"] += 1
            runtime_log(
                "WARN",
                f"{row_name}元数据写入跳过：源图片缺失",
                detail=detail_url,
                person=row_name,
                candidates="; ".join(local_path_candidates),
            )
            append_jsonl(
                review_path,
                {
                    "scraped_at": utc_now_iso(),
                    "reason": "metadata_missing_local_image_path",
                    "detail_url": detail_url,
                    "candidates": local_path_candidates,
                },
            )
            append_jsonl(
                results_path,
                {
                    "processed_at": utc_now_iso(),
                    "detail_url": detail_url,
                    "name": row_name,
                    "status": "fail",
                    "error": "metadata_missing_local_image_path",
                    "output_path": "",
                    "attempts": 0,
                    "audit": {
                        "name": row_name,
                        "gender": normalize_gender(row.get("gender", "")),
                        "birth_date": normalize_optional_field((row.get("mapped") or {}).get("birth_date", "")) if isinstance(row.get("mapped"), dict) else "",
                        "photo_taken_at": "",
                        "age_at_photo": "",
                    },
                },
            )
            continue

        ok, _final_path, copied_flag, attempts_used = write_metadata_for_queue_row_with_retries(
            row=row,
            source_path=source_path,
            named_dir=named_dir,
            reserved_paths=reserved_paths,
            field_labels=field_labels,
            review_path=review_path,
            results_path=results_path,
            detail_to_final_path=detail_to_final_path,
            sha_runtime_path=sha_runtime_path,
            llm_enricher=llm_enricher,
            max_attempts=metadata_write_retries,
            retry_delay_seconds=metadata_retry_delay_seconds,
            retry_backoff_factor=metadata_retry_backoff_factor,
        )
        if attempts_used > 1:
            totals["retried_rows"] += 1
            totals["retry_attempts"] += (attempts_used - 1)
        if ok:
            totals["written_ok"] += 1
            if copied_flag:
                totals["copied_to_named_folder"] += 1
            runtime_log(
                "STEP",
                f"{row_name}元数据写入成功",
                detail=detail_url,
                person=row_name,
                attempts=attempts_used,
            )
        else:
            totals["failed"] += 1
            runtime_log(
                "FAIL",
                f"{row_name}元数据写入失败",
                detail=detail_url,
                person=row_name,
                attempts=attempts_used,
            )

    # Persist updated queue with final named paths.
    write_jsonl(queue_path, rows)

    # Save runtime sha index for easier future lookups (path may now point to named file).
    if sha_runtime_path and (not direct_write_images):
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

    if llm_enricher is not None:
        try:
            llm_enricher.flush_cache()
            append_llm_report(llm_report_path, "metadata_write_phase", llm_enricher.report())
        except Exception:
            pass

    report = {
        "generated_at": utc_now_iso(),
        "enabled": True,
        "writer_available": True,
        "named_output_dir": str(named_dir),
        "totals": totals,
        "retry_policy": {
            "retry_failed_first": retry_failed_first,
            "metadata_write_retries": metadata_write_retries,
            "metadata_write_retry_delay_seconds": metadata_retry_delay_seconds,
            "metadata_write_retry_backoff_factor": metadata_retry_backoff_factor,
        },
        "llm_enrichment": llm_enricher.report() if llm_enricher is not None else {"enabled": False},
    }
    save_json(report_path, report)
    return report


def _resolve_metadata_audit_fields(rules: Dict[str, Any]) -> List[str]:
    default_fields = ["gender", "birth_date", "photo_taken_at", "age_at_photo"]
    allowed = {
        "gender",
        "birth_date",
        "photo_taken_at",
        "age_at_photo",
        "position",
        "city",
        "unit",
        "profession",
        "police_id",
    }
    raw_fields = rules.get("metadata_audit_required_fields", default_fields)
    if isinstance(raw_fields, str):
        candidates = [x.strip() for x in re.split(r"[;,，、\s]+", raw_fields) if x.strip()]
    elif isinstance(raw_fields, list):
        candidates = [str(x or "").strip() for x in raw_fields if str(x or "").strip()]
    else:
        candidates = list(default_fields)

    normalized: List[str] = []
    for token in candidates:
        field = str(token or "").strip().lower()
        if (not field) or (field not in allowed):
            continue
        if field not in normalized:
            normalized.append(field)
    return normalized or list(default_fields)


def write_metadata_audit_report(output_root: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    rules = dict(config.get("rules", {}))
    enabled = _parse_bool_rule(rules.get("metadata_audit_enabled", True), default=True)
    required_fields = _resolve_metadata_audit_fields(rules)
    try:
        max_items = int(rules.get("metadata_audit_max_items", 300))
    except Exception:
        max_items = 300
    max_items = max(20, min(2000, max_items))
    push_review_queue = _parse_bool_rule(rules.get("metadata_audit_push_review_queue", True), default=True)

    report_path = output_root / "reports" / "metadata_audit_report.json"
    results_path = output_root / "raw" / "metadata_write_results.jsonl"
    review_path = output_root / "raw" / "review_queue.jsonl"

    if not enabled:
        report = {
            "generated_at": utc_now_iso(),
            "enabled": False,
            "required_fields": required_fields,
            "results_path": str(results_path.resolve()),
        }
        save_json(report_path, report)
        runtime_log("STAGE", "metadata audit skipped by config")
        return report

    runtime_log(
        "STAGE",
        "metadata audit start",
        missing_fields=",".join(required_fields),
    )

    def _normalize_audit_value(field: str, value: Any) -> str:
        if field == "gender":
            return normalize_gender(value)
        return normalize_optional_field(value)

    def _audit_from_file(output_path: str, fallback: Dict[str, Any]) -> Dict[str, Any]:
        if (not HAS_METADATA_MANAGER) or (_read_image_metadata is None):
            return dict(fallback or {})
        source_path = norm_abs_path(output_path)
        if (not source_path) or (not Path(source_path).exists()):
            return dict(fallback or {})
        try:
            info = _read_image_metadata(source_path)  # type: ignore[misc]
        except Exception:
            return dict(fallback or {})
        profile: Dict[str, Any] = {}
        try:
            if isinstance(getattr(info, "titi_json", None), dict):
                prof_raw = (info.titi_json or {}).get("d2i_profile")
                if isinstance(prof_raw, dict):
                    profile = prof_raw
        except Exception:
            profile = {}
        # Keep output stable even when file metadata is partially missing.
        return {
            "name": normalize_optional_field(getattr(info, "person", "") or getattr(info, "title", "") or fallback.get("name", "")),
            "gender": normalize_gender(getattr(info, "gender", "") or profile.get("gender", "")),
            "birth_date": normalize_optional_field(profile.get("birth_date", "")),
            "photo_taken_at": normalize_optional_field(profile.get("photo_taken_at", "")),
            "age_at_photo": normalize_optional_field(profile.get("age_at_photo", "")),
            "position": normalize_optional_field(getattr(info, "position", "") or profile.get("title", "") or profile.get("position", "")),
            "city": normalize_optional_field(getattr(info, "city", "") or profile.get("location", "") or profile.get("city", "")),
            "unit": normalize_optional_field(profile.get("unit", "")),
            "profession": normalize_optional_field(profile.get("profession", "")),
            "police_id": normalize_optional_field(getattr(info, "police_id", "") or profile.get("police_id", "")),
        }

    totals = {
        "rows_total": 0,
        "rows_ok": 0,
        "rows_failed": 0,
        "rows_checked": 0,
        "rows_with_missing": 0,
    }
    missing_by_field: Dict[str, int] = {field: 0 for field in required_fields}
    missing_items: List[Dict[str, Any]] = []
    missing_map: Dict[str, List[str]] = {}
    values_map: Dict[str, Dict[str, str]] = {}
    name_map: Dict[str, str] = {}

    for row in iter_jsonl(results_path):
        totals["rows_total"] += 1
        status = str(row.get("status", "")).strip().lower()
        if status == "ok":
            totals["rows_ok"] += 1
        else:
            totals["rows_failed"] += 1
            continue

        audit_raw = row.get("audit", {})
        audit = audit_raw if isinstance(audit_raw, dict) else {}
        detail_url = str(row.get("detail_url", "")).strip()
        output_path = norm_abs_path(str(row.get("output_path", "")))
        effective_audit = _audit_from_file(output_path, audit)
        name = normalize_optional_field(effective_audit.get("name") or row.get("name", ""))

        values: Dict[str, str] = {}
        missing_fields: List[str] = []
        for field in required_fields:
            value = _normalize_audit_value(field, effective_audit.get(field, ""))
            values[field] = value
            if not value:
                missing_fields.append(field)

        totals["rows_checked"] += 1
        if detail_url:
            missing_map[detail_url] = list(missing_fields)
            values_map[detail_url] = dict(values)
            name_map[detail_url] = name
        if missing_fields:
            totals["rows_with_missing"] += 1
            for field in missing_fields:
                missing_by_field[field] = int(missing_by_field.get(field, 0)) + 1
            if len(missing_items) < max_items:
                missing_items.append(
                    {
                        "detail_url": detail_url,
                        "name": name,
                        "output_path": output_path,
                        "missing_fields": missing_fields,
                        "values": values,
                    }
                )

    review_added = 0
    review_pruned = 0
    review_updated = 0
    if push_review_queue:
        existing_review_rows = list(iter_jsonl(review_path)) if review_path.exists() else []
        kept_review_rows: List[Dict[str, Any]] = []
        known_keys: set[Tuple[str, str]] = set()

        for item in existing_review_rows:
            if not isinstance(item, dict):
                continue
            reason_raw = str(item.get("reason", "")).strip()
            reason_lower = reason_raw.lower()
            if not reason_lower.startswith("audit_missing_metadata_fields"):
                kept_review_rows.append(item)
                continue

            detail_url = str(item.get("detail_url", "")).strip()
            missing_fields_now = missing_map.get(detail_url, [])
            if not missing_fields_now:
                review_pruned += 1
                continue

            desired_reason = f"audit_missing_metadata_fields:{','.join(missing_fields_now)}"
            if reason_raw != desired_reason:
                item["reason"] = desired_reason
                item["missing_fields"] = list(missing_fields_now)
                item["audit_values"] = values_map.get(detail_url, {})
                review_updated += 1
            kept_review_rows.append(item)
            known_keys.add((detail_url, desired_reason.lower()))

        for detail_url, missing_fields_now in missing_map.items():
            if not detail_url or (not missing_fields_now):
                continue
            desired_reason = f"audit_missing_metadata_fields:{','.join(missing_fields_now)}"
            key = (detail_url, desired_reason.lower())
            if key in known_keys:
                continue
            kept_review_rows.append(
                {
                    "scraped_at": utc_now_iso(),
                    "reason": desired_reason,
                    "detail_url": detail_url,
                    "name": name_map.get(detail_url, ""),
                    "missing_fields": list(missing_fields_now),
                    "audit_values": values_map.get(detail_url, {}),
                }
            )
            known_keys.add(key)
            review_added += 1

        if (review_added + review_pruned + review_updated) > 0:
            write_jsonl(review_path, kept_review_rows)

    checked = int(totals.get("rows_checked", 0))
    missing = int(totals.get("rows_with_missing", 0))
    missing_ratio = (missing / checked * 100.0) if checked > 0 else 0.0
    missing_field_summary = "; ".join(f"{field}:{int(missing_by_field.get(field, 0))}" for field in required_fields)

    report = {
        "generated_at": utc_now_iso(),
        "enabled": True,
        "required_fields": required_fields,
        "results_path": str(results_path.resolve()),
        "totals": totals,
        "missing_by_field": missing_by_field,
        "missing_ratio_pct": round(missing_ratio, 2),
        "review_queue_added": review_added,
        "review_queue_pruned": review_pruned,
        "review_queue_updated": review_updated,
        "items": missing_items,
    }
    save_json(report_path, report)
    runtime_log(
        "STAT",
        "metadata audit end",
        checked=checked,
        missing_items=missing,
        missing_ratio=f"{missing_ratio:.1f}%",
        missing_fields=missing_field_summary,
    )
    if (review_added + review_pruned + review_updated) > 0:
        runtime_log(
            "STAT",
            "metadata audit review items queued",
            review_added=review_added,
            review_pruned=review_pruned,
            review_updated=review_updated,
        )
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
    metadata_audit_path = output_root / "reports" / "metadata_audit_report.json"
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
    metadata_audit = load_json(metadata_audit_path, {}) if metadata_audit_path.exists() else {}
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
            "metadata_audit": metadata_audit if isinstance(metadata_audit, dict) else {},
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
    wait_if_manual_paused(output_root, stage="crawl:start")
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

    def _log(level: str, message: str, **fields: Any) -> None:
        runtime_log(level, message, **fields)

    def _counts_snapshot() -> Dict[str, int]:
        return {
            "list": count_jsonl(output_root / "raw" / "list_records.jsonl"),
            "profiles": count_jsonl(output_root / "raw" / "profiles.jsonl"),
            "images": count_jsonl(output_root / "downloads" / "image_downloads.jsonl"),
            "metadata": count_jsonl(output_root / "raw" / "metadata_write_results.jsonl"),
            "review": count_jsonl(output_root / "raw" / "review_queue.jsonl"),
            "failures": count_jsonl(output_root / "raw" / "failures.jsonl"),
        }

    def _log_counts(stage_label: str, *, previous: Optional[Dict[str, int]] = None) -> Dict[str, int]:
        current = _counts_snapshot()
        payload: Dict[str, Any] = {
            "list": current["list"],
            "profiles": current["profiles"],
            "images": current["images"],
            "metadata": current["metadata"],
            "review": current["review"],
            "failures": current["failures"],
            "pending_meta": max(0, current["profiles"] - current["metadata"]),
        }
        if previous:
            payload["delta_profiles"] = current["profiles"] - int(previous.get("profiles", 0))
            payload["delta_images"] = current["images"] - int(previous.get("images", 0))
            payload["delta_metadata"] = current["metadata"] - int(previous.get("metadata", 0))
            payload["delta_review"] = current["review"] - int(previous.get("review", 0))
            payload["delta_failures"] = current["failures"] - int(previous.get("failures", 0))
        _log("STAT", stage_label, **payload)
        return current

    def _log_runtime_summary(cfg: Dict[str, Any], mode: str) -> None:
        cfg_rules = dict(cfg.get("rules", {}))
        crawl_cfg = dict(cfg.get("crawl", {}))
        _log(
            "CONF",
            "runtime summary",
            mode=mode,
            fallback=bool(cfg_rules.get("auto_fallback_to_browser", True)),
            obey_robots=bool(cfg_rules.get("obey_robots_txt", True)),
            interval=f"{crawl_cfg.get('image_interval_min_seconds', '')}-{crawl_cfg.get('image_interval_max_seconds', '')}",
            timeout=crawl_cfg.get("timeout_seconds", ""),
            retry=crawl_cfg.get("retry_times", ""),
            output_root=str(output_root),
        )

    _log(
        "RUN",
        "public scraper started",
        output_root=str(output_root),
        mode=image_download_mode,
        skip_crawl=args.skip_crawl,
        skip_images=args.skip_images,
        skip_metadata=args.skip_metadata,
    )
    _log_runtime_summary(active_config, image_download_mode)
    run_counts_start = _log_counts("counts on disk before run")

    can_run, blocked_until = check_backoff(output_root)
    if not can_run:
        _log("STOP", "backoff active, skip this run", blocked_until=blocked_until)
        return 2

    clear_backoff(output_root)
    wait_if_manual_paused(output_root, stage="main:before-stages")

    blocked_during_run = False
    blocked_after_stage_until = ""
    blocked_after_stage_reason = ""
    inline_metadata_done = False
    retry_failed_first = bool(rules.get("retry_failed_first", True))

    def _metadata_queue_has_pending_retry() -> bool:
        queue_path = output_root / "raw" / "metadata_queue.jsonl"
        if not queue_path.exists():
            return False

        queue_details: set[str] = set()
        for row in iter_jsonl(queue_path):
            detail_url = str(row.get("detail_url", "")).strip()
            if detail_url:
                queue_details.add(detail_url)
        if not queue_details:
            return False

        results_path = output_root / "raw" / "metadata_write_results.jsonl"
        latest_status: Dict[str, str] = {}
        if results_path.exists():
            for row in iter_jsonl(results_path):
                detail_url = str(row.get("detail_url", "")).strip()
                if not detail_url:
                    continue
                latest_status[detail_url] = str(row.get("status", "")).strip().lower()

        for detail_url in queue_details:
            if latest_status.get(detail_url, "") != "ok":
                return True
        return False

    def _inline_metadata_fully_synced() -> bool:
        profile_rows = count_jsonl(output_root / "raw" / "profiles.jsonl")
        if profile_rows <= 0:
            return False
        queue_rows = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        if queue_rows < profile_rows:
            return False
        if _metadata_queue_has_pending_retry():
            return False
        return True

    def _metadata_pre_retry_needed() -> bool:
        if args.skip_metadata:
            return False
        profile_rows = count_jsonl(output_root / "raw" / "profiles.jsonl")
        if profile_rows <= 0:
            return False
        queue_rows = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        if queue_rows < profile_rows:
            return True
        if _metadata_queue_has_pending_retry():
            return True
        return False

    def _scan_pending_image_retries() -> Dict[str, int]:
        profiles_path = output_root / "raw" / "profiles.jsonl"
        url_index_path = output_root / "state" / "image_url_index.json"
        sha_index_path = output_root / "state" / "image_sha_index.json"
        review_path = output_root / "raw" / "review_queue.jsonl"

        direct_write_images = _parse_bool_rule(rules.get("direct_write_images", False), default=False)
        url_index: Dict[str, str] = {}
        sha_index: Dict[str, str] = {}
        manifest_by_detail: Dict[str, Dict[str, str]] = {}
        if direct_write_images:
            downloads_manifest_path = output_root / "downloads" / "image_downloads.jsonl"
            manifest_by_detail, _ = _load_download_manifest_lookups(downloads_manifest_path)
        else:
            url_index_raw = load_json(url_index_path, {})
            sha_index_raw = load_json(sha_index_path, {})
            if isinstance(url_index_raw, dict):
                url_index = {
                    str(k): str(v) for k, v in url_index_raw.items() if str(k).strip() and str(v).strip()
                }
            if isinstance(sha_index_raw, dict):
                sha_index = {
                    str(k): str(v) for k, v in sha_index_raw.items() if str(k).strip() and str(v).strip()
                }

        profiles_with_image = 0
        pending_images = 0
        missing_url_index = 0
        stale_cache = 0
        for profile in iter_jsonl(profiles_path):
            image_url = str(profile.get("image_url", "")).strip()
            if not image_url:
                continue
            profiles_with_image += 1
            if direct_write_images:
                detail_url = str(profile.get("detail_url", "")).strip()
                entry = manifest_by_detail.get(detail_url, {}) if detail_url else {}
                cached_path = norm_abs_path(str(entry.get("path", "")))
                if not cached_path:
                    pending_images += 1
                    missing_url_index += 1
                    continue
                if not _is_usable_cached_image(cached_path):
                    pending_images += 1
                    stale_cache += 1
                continue

            image_sha = str(url_index.get(image_url, "")).strip()
            if not image_sha:
                pending_images += 1
                missing_url_index += 1
                continue
            cached_path = norm_abs_path(str(sha_index.get(image_sha, "")))
            if not _is_usable_cached_image(cached_path):
                pending_images += 1
                stale_cache += 1

        review_image_failures = 0
        for item in iter_jsonl(review_path):
            reason = str(item.get("reason", "")).strip().lower()
            if reason.startswith("image_download_"):
                review_image_failures += 1

        return {
            "profiles_with_image": profiles_with_image,
            "pending_images": pending_images,
            "missing_url_index": missing_url_index,
            "stale_cache": stale_cache,
            "review_image_failures": review_image_failures,
        }

    if retry_failed_first and (not blocked_during_run) and _metadata_pre_retry_needed():
        wait_if_manual_paused(output_root, stage="main:before-metadata-pre-retry")
        counts_before_meta_retry = _counts_snapshot()
        queue_before_retry = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        _log(
            "STAGE",
            "metadata pre-retry start (retry failed first)",
            queue=queue_before_retry,
        )
        build_metadata_queue(output_root)
        queue_after_retry_build = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        _log(
            "STAT",
            "metadata pre-retry queue built",
            queue=queue_after_retry_build,
            added=max(0, queue_after_retry_build - queue_before_retry),
        )
        write_metadata_for_downloads(output_root, active_config)
        _log_counts("metadata pre-retry end", previous=counts_before_meta_retry)

    if not args.skip_crawl:
        wait_if_manual_paused(output_root, stage="main:before-crawl")
        counts_before_crawl = _counts_snapshot()
        _log("STAGE", "crawl stage start", mode=image_download_mode)
        crawl_error: Optional[Exception] = None
        try:
            run_crawl(active_config, output_root)
        except Exception as exc:
            crawl_error = exc
        if (crawl_error is not None) and auto_browser_fallback and (image_download_mode not in {"browser", "d2i_browser"}):
            _log("INFO", "quick crawl failed, fallback to browser mode", error=crawl_error)
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

        _log_counts("crawl stage end", previous=counts_before_crawl)

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
            _log("INFO", "quick crawl triggered backoff, fallback to browser mode and retry crawl")
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
            _log(
                "STOP",
                "blocked detected after crawl, pause current run",
                blocked_until=blocked_after_stage_until,
                reason=blocked_after_stage_reason,
            )
    else:
        _log("STAGE", "crawl stage skipped by flag")
        _log_counts("crawl skipped snapshot")

    if (not args.skip_images) and (not blocked_during_run):
        wait_if_manual_paused(output_root, stage="main:before-download")
        image_retry_scan = _scan_pending_image_retries()
        _log(
            "STAT",
            "image retry scan",
            profiles_with_image=image_retry_scan.get("profiles_with_image", 0),
            pending_images=image_retry_scan.get("pending_images", 0),
            missing_url_index=image_retry_scan.get("missing_url_index", 0),
            stale_cache=image_retry_scan.get("stale_cache", 0),
            review_image_failures=image_retry_scan.get("review_image_failures", 0),
        )
        force_image_retry_stage = int(image_retry_scan.get("pending_images", 0)) > 0
        if crawl_inline_download and (not args.skip_crawl) and force_image_retry_stage:
            _log(
                "STAGE",
                "image stage forced: auto retry unresolved image items",
                pending_images=image_retry_scan.get("pending_images", 0),
            )
        skip_download_stage = crawl_inline_download and (not args.skip_crawl) and (not force_image_retry_stage)
        if skip_download_stage:
            _log("STAGE", "image stage skipped: browser crawl already downloaded images inline")
            runtime_rules = dict(active_config.get("rules", {}))
            inline_metadata_done = (
                bool(runtime_rules.get("write_metadata_inline_per_item", True))
                and bool(runtime_rules.get("write_metadata", True))
                and HAS_METADATA_WRITER
                and (not args.skip_crawl)
                and _inline_metadata_fully_synced()
            )
            _log_counts("image stage skipped snapshot")
        else:
            counts_before_image = _counts_snapshot()
            _log("STAGE", "image stage start", mode=image_download_mode)
            download_report: Dict[str, Any] = {}
            image_error: Optional[Exception] = None
            try:
                download_report = download_images(active_config, output_root)
            except Exception as exc:
                image_error = exc
            if (image_error is not None) and auto_browser_fallback and (image_download_mode not in {"browser", "d2i_browser"}):
                _log("INFO", "quick image download failed, fallback to browser mode", error=image_error)
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
                inline_metadata_done = (
                    bool(download_report.get("inline_metadata_enabled", False))
                    and (not args.skip_crawl)
                    and _inline_metadata_fully_synced()
                )
                totals = download_report.get("totals", {})
                if isinstance(totals, dict):
                    _log(
                        "STAGE",
                        "image stage end",
                        candidates=totals.get("profiles_with_image_url", ""),
                        downloaded_new=totals.get("downloaded_new", ""),
                        reused_url=totals.get("reused_by_url", ""),
                        reused_sha=totals.get("reused_by_sha", ""),
                        failed=totals.get("failed", ""),
                    )
            _log_counts("image stage end snapshot", previous=counts_before_image)
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
                _log("INFO", "quick image download triggered backoff, fallback to browser mode and retry image stage")
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
                    inline_metadata_done = (
                        bool(download_report.get("inline_metadata_enabled", False))
                        and (not args.skip_crawl)
                        and _inline_metadata_fully_synced()
                    )
                can_continue, blocked_after_stage_until = check_backoff(output_root)

            if not can_continue:
                blocked_during_run = True
                backoff_payload = load_json(output_root / "state" / "backoff_state.json", {})
                if isinstance(backoff_payload, dict):
                    blocked_after_stage_reason = str(backoff_payload.get("blocked_reason", "")).strip()
                _log(
                    "STOP",
                    "blocked detected during image download, pause current run",
                    blocked_until=blocked_after_stage_until,
                    reason=blocked_after_stage_reason,
                )
    elif args.skip_images:
        _log("STAGE", "image stage skipped by flag")
        _log_counts("image skipped snapshot")
    elif blocked_during_run:
        _log("STAGE", "image stage skipped because run is paused by backoff")
        _log_counts("image skipped due backoff snapshot")

    skip_metadata_stage = inline_metadata_done and (not args.skip_metadata)
    if skip_metadata_stage:
        queue_rows = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        _log("STAGE", "metadata stage skipped: inline metadata already applied", queue=queue_rows)
        _log_counts("metadata skipped snapshot")
    else:
        wait_if_manual_paused(output_root, stage="main:before-metadata")
        counts_before_meta = _counts_snapshot()
        queue_before = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        _log("STAGE", "metadata stage start", queue=queue_before, skip_write=args.skip_metadata)
        build_metadata_queue(output_root)
        queue_after_build = count_jsonl(output_root / "raw" / "metadata_queue.jsonl")
        _log("STAT", "metadata queue built", queue=queue_after_build, added=max(0, queue_after_build - queue_before))
        if not args.skip_metadata:
            write_metadata_for_downloads(output_root, active_config)
        _log_counts("metadata stage end", previous=counts_before_meta)
    metadata_audit_report = write_metadata_audit_report(output_root, active_config)
    report = write_reconcile_report(output_root)
    record_path = write_delivery_record(output_root, active_config, report)
    cleanup_report = cleanup_intermediate_outputs(output_root, active_config, record_path)
    if isinstance(report, dict):
        report["metadata_audit"] = metadata_audit_report
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
    final_counts = _counts_snapshot()
    _log_counts("counts on disk after run", previous=run_counts_start)
    _log(
        "DONE",
        "public scraper finished",
        run_state=("paused_by_backoff" if blocked_during_run else "finished"),
        list=final_counts["list"],
        profiles=final_counts["profiles"],
        images=final_counts["images"],
        metadata=final_counts["metadata"],
        review=final_counts["review"],
        failures=final_counts["failures"],
    )
    if blocked_during_run:
        _log(
            "HINT",
            "run paused by backoff, retry later with continue",
            blocked_until=blocked_after_stage_until,
            reason=blocked_after_stage_reason,
        )
    elif final_counts["review"] > 0 or final_counts["failures"] > 0:
        _log(
            "HINT",
            "some items need manual review or retry",
            review=final_counts["review"],
            failures=final_counts["failures"],
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 2 if blocked_during_run else 0


if __name__ == "__main__":
    sys.exit(main())
