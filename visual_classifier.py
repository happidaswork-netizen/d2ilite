from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from PIL import Image, ImageOps

from image_asset_safety import sha256_file
from llm_client import OpenAICompatibleClient, normalize_api_base


PROMPT_VERSION = "titi-visual-reference-v1"
MAX_VISION_CONCURRENCY = 3
VISUAL_FIELDS = (
    "person_count",
    "visual_gender",
    "visual_body_type",
    "visual_hairstyle",
    "visual_pose",
)
VISUAL_GENDERS = {"男", "女", "多人混合", "不确定", "不适用"}
VISUAL_BODY_TYPES = {"瘦削", "标准", "健壮", "微胖", "肥胖", "多人", "不确定", "不适用"}
VISUAL_HAIRSTYLES = {"光头", "短发", "中发", "长发", "束发", "多人", "不确定", "不适用"}
VISUAL_POSES = {
    "头像",
    "正面证件照",
    "正面人像",
    "侧面人像",
    "半身照",
    "全身照",
    "多人",
    "其他",
    "不确定",
    "不适用",
}

VISION_PROMPT = """你是图片资料库的视觉分类器。只判断图片本身，不使用姓名、网页文字或来源性别。
先判断图片中可辨认的人数，再一次性返回五个固定字段。

规则：
1. 没有可辨认人物时 person_count=0，其余四项填“不适用”。
2. 单人头像、面部近照也必须对体型作参考性判断，不增加置信度或解释字段。
3. 多人图片的 visual_gender 填“多人混合”，体型、发型、姿态填“多人”。
4. 单人字段判断不了时填“不确定”，不要猜测身份，不要输出证据或说明。
5. 只能使用以下枚举：
   visual_gender: 男、女、不确定
   visual_body_type: 瘦削、标准、健壮、微胖、肥胖、不确定
   visual_hairstyle: 光头、短发、中发、长发、束发、不确定
   visual_pose: 头像、正面证件照、正面人像、侧面人像、半身照、全身照、其他、不确定

只输出一个 JSON 对象，严格包含 person_count、visual_gender、visual_body_type、
visual_hairstyle、visual_pose，不要 Markdown，不要任何其他字段。"""


def _env_enabled(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


@dataclass(frozen=True)
class VisionRuntime:
    enabled: bool
    api_base: str
    model: str
    api_key_file: Path
    concurrency: int
    timeout_seconds: int
    max_retries: int

    @classmethod
    def from_environment(cls, environ: Optional[Mapping[str, str]] = None) -> "VisionRuntime":
        env = environ if environ is not None else os.environ
        return cls(
            enabled=_env_enabled(env.get("D2I_VISION_ENABLED", "0")),
            api_base=normalize_api_base(env.get("D2I_VISION_API_BASE", "")),
            model=str(env.get("D2I_VISION_MODEL", "") or "").strip(),
            api_key_file=Path(
                str(
                    env.get(
                        "D2I_VISION_API_KEY_FILE",
                        "/run/secrets/d2i_vision_api_key",
                    )
                    or ""
                )
            ),
            concurrency=_clamp_int(
                env.get("D2I_VISION_CONCURRENCY", MAX_VISION_CONCURRENCY),
                MAX_VISION_CONCURRENCY,
                1,
                MAX_VISION_CONCURRENCY,
            ),
            timeout_seconds=_clamp_int(
                env.get("D2I_VISION_TIMEOUT_SECONDS", 90),
                90,
                10,
                300,
            ),
            max_retries=_clamp_int(env.get("D2I_VISION_MAX_RETRIES", 1), 1, 1, 2),
        )

    def api_key(self) -> str:
        # Prefer dedicated key file; fall back to env (docker env_file injects
        # D2I_LLM_API_KEY / D2I_VISION_API_KEY even when the host secret is root-only).
        try:
            if self.api_key_file.is_file():
                value = self.api_key_file.read_text(encoding="utf-8").strip()
                if value:
                    return value
        except Exception:
            pass
        for env_name in ("D2I_VISION_API_KEY", "D2I_LLM_API_KEY"):
            value = str(os.environ.get(env_name, "") or "").strip()
            if value:
                return value
        return ""

    def available(self) -> bool:
        return bool(self.enabled and self.api_base and self.model and self.api_key())


def vision_runtime_available(environ: Optional[Mapping[str, str]] = None) -> bool:
    return VisionRuntime.from_environment(environ).available()


def normalize_visual_result(value: Any) -> Dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    try:
        person_count = int(raw.get("person_count"))
    except Exception as exc:
        raise ValueError("invalid_person_count") from exc
    if person_count < 0:
        raise ValueError("invalid_person_count")

    if person_count == 0:
        return {
            "person_count": 0,
            "visual_gender": "不适用",
            "visual_body_type": "不适用",
            "visual_hairstyle": "不适用",
            "visual_pose": "不适用",
        }
    if person_count >= 2:
        return {
            "person_count": person_count,
            "visual_gender": "多人混合",
            "visual_body_type": "多人",
            "visual_hairstyle": "多人",
            "visual_pose": "多人",
        }

    def enum_value(key: str, allowed: set[str]) -> str:
        candidate = str(raw.get(key) or "").strip()
        if candidate in {"多人", "多人混合", "不适用"}:
            return "不确定"
        return candidate if candidate in allowed else "不确定"

    return {
        "person_count": 1,
        "visual_gender": enum_value("visual_gender", VISUAL_GENDERS),
        "visual_body_type": enum_value("visual_body_type", VISUAL_BODY_TYPES),
        "visual_hairstyle": enum_value("visual_hairstyle", VISUAL_HAIRSTYLES),
        "visual_pose": enum_value("visual_pose", VISUAL_POSES),
    }


def _extract_json_object(text: Any) -> Dict[str, Any]:
    raw = str(text or "").strip()
    fence = chr(96) * 3
    if raw.startswith(fence):
        raw = raw.strip(chr(96)).strip()
        if raw.lower().startswith("json"):
            raw = raw[4:].lstrip()
    decoder = json.JSONDecoder()
    for index, char in enumerate(raw):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(raw[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ValueError("vision_response_missing_json")


def _first_message_content(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return ""
    message = choices[0].get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(item.get("text") or "")
            for item in content
            if isinstance(item, dict) and item.get("type") in {"text", "output_text"}
        )
    return str(content or "")


def _image_data_url(image_path: Path, max_edge: int = 1280) -> str:
    with Image.open(image_path) as source:
        source.seek(0)
        image = ImageOps.exif_transpose(source).copy()
    if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
        rgba = image.convert("RGBA")
        flattened = Image.new("RGB", rgba.size, "white")
        flattened.paste(rgba, mask=rgba.getchannel("A"))
        image = flattened
    elif image.mode != "RGB":
        image = image.convert("RGB")

    longest = max(image.size)
    if longest > max_edge:
        scale = max_edge / float(longest)
        size = (
            max(1, int(round(image.width * scale))),
            max(1, int(round(image.height * scale))),
        )
        resampling = getattr(Image, "Resampling", Image).LANCZOS
        image = image.resize(size, resampling)

    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=90, optimize=True)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"


def classify_image(
    image_path: Path,
    *,
    expected_sha256: str,
    runtime: VisionRuntime,
    client: OpenAICompatibleClient,
) -> Dict[str, Any]:
    before_sha = sha256_file(image_path)
    expected = str(expected_sha256 or "").strip().lower()
    if expected and expected != before_sha:
        raise ValueError("source_sha256_mismatch")
    data_url = _image_data_url(image_path)
    payload = client.chat_completions(
        model=runtime.model,
        messages=[
            {"role": "system", "content": VISION_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "分类这张图片，只返回约定 JSON。"},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
        temperature=0.0,
        stream=False,
    )
    after_sha = sha256_file(image_path)
    if after_sha != before_sha:
        raise RuntimeError("source_image_changed_during_visual_classification")
    return normalize_visual_result(_extract_json_object(_first_message_content(payload)))


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _read_jsonl(path: Path) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if not path.is_file():
        return rows
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def _cache_key(image_sha256: str, model: str) -> str:
    identity = f"{image_sha256.lower()}:{model}:{PROMPT_VERSION}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _normalized_sha256(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("sha256:"):
        text = text[7:]
    return text if len(text) == 64 else ""


def _resolve_source_image(
    output_root: Path,
    row: Dict[str, Any],
) -> tuple[Optional[Path], str, str]:
    expected_sha = _normalized_sha256(
        row.get("source_file_sha256") or row.get("image_sha256")
    )
    candidates: list[Path] = []
    if expected_sha:
        immutable_dir = output_root / "downloads" / "images" / expected_sha[:2]
        if immutable_dir.is_dir():
            candidates.extend(sorted(immutable_dir.glob(f"{expected_sha}.*")))
    local_text = str(row.get("local_image_path") or "").strip()
    if local_text:
        local_path = Path(local_text)
        if local_path not in candidates:
            candidates.append(local_path)

    first_existing: Optional[tuple[Path, str]] = None
    for candidate in candidates:
        if not candidate.is_file():
            continue
        actual_sha = sha256_file(candidate)
        if first_existing is None:
            first_existing = (candidate, actual_sha)
        if not expected_sha or actual_sha == expected_sha:
            return candidate, actual_sha, expected_sha
    if first_existing is not None:
        return first_existing[0], first_existing[1], expected_sha
    return None, "", expected_sha


def classify_metadata_queue(
    output_root: Path,
    config: Dict[str, Any],
    *,
    client_factory: Optional[Callable[[VisionRuntime], OpenAICompatibleClient]] = None,
) -> Dict[str, Any]:
    root = Path(output_root)
    rules = config.get("rules") if isinstance(config.get("rules"), dict) else {}
    enabled = _env_enabled(rules.get("visual_classification_enabled", False))
    queue_path = root / "raw" / "metadata_queue.jsonl"
    results_path = root / "raw" / "visual_classification_results.jsonl"
    report_path = root / "reports" / "visual_classification_report.json"
    cache_path = root / "state" / "visual_classification_cache.json"
    rows = _read_jsonl(queue_path)
    runtime = VisionRuntime.from_environment()

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "enabled": enabled,
        "runtime_available": runtime.available(),
        "prompt_version": PROMPT_VERSION,
        "model": runtime.model if enabled else "",
        "concurrency": runtime.concurrency if enabled else 0,
        "total_rows": len(rows),
        "candidate_rows": 0,
        "api_requests": 0,
        "cache_hits": 0,
        "classified": 0,
        "failed": 0,
        "skipped_missing_image": 0,
    }
    if not enabled:
        _atomic_write_json(report_path, report)
        return report
    if not runtime.available():
        report["error"] = "vision_runtime_unavailable"
        _atomic_write_json(report_path, report)
        return report
    if not queue_path.is_file():
        report["error"] = "metadata_queue_missing"
        _atomic_write_json(report_path, report)
        return report

    try:
        loaded_cache = json.loads(cache_path.read_text(encoding="utf-8"))
        cache_payload = loaded_cache if isinstance(loaded_cache, dict) else {}
    except Exception:
        cache_payload = {}
    entries = cache_payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    cache_payload = {
        "schema": "d2i-visual-classification-cache",
        "schema_version": 1,
        "prompt_version": PROMPT_VERSION,
        "entries": entries,
    }

    result_rows: list[Dict[str, Any]] = []
    task_rows: Dict[str, list[int]] = {}
    task_specs: Dict[str, tuple[Path, str]] = {}
    for index, row in enumerate(rows):
        image_path, actual_sha, expected_sha = _resolve_source_image(root, row)
        if image_path is None:
            report["skipped_missing_image"] += 1
            report["failed"] += 1
            result_rows.append(
                {
                    "status": "fail",
                    "name": str(row.get("name") or ""),
                    "detail_url": str(row.get("detail_url") or ""),
                    "error": "visual_source_image_missing",
                }
            )
            continue
        report["candidate_rows"] += 1
        if expected_sha and expected_sha != actual_sha:
            report["failed"] += 1
            result_rows.append(
                {
                    "status": "fail",
                    "name": str(row.get("name") or ""),
                    "detail_url": str(row.get("detail_url") or ""),
                    "image_sha256": actual_sha,
                    "error": "source_sha256_mismatch",
                }
            )
            continue
        key = _cache_key(actual_sha, runtime.model)
        cached = entries.get(key)
        if isinstance(cached, dict) and isinstance(cached.get("result"), dict):
            try:
                normalized = normalize_visual_result(cached["result"])
            except Exception:
                normalized = {}
            if normalized:
                rows[index]["visual_classification"] = normalized
                report["cache_hits"] += 1
                report["classified"] += 1
                result_rows.append(
                    {
                        "status": "ok",
                        "cache_hit": True,
                        "name": str(row.get("name") or ""),
                        "detail_url": str(row.get("detail_url") or ""),
                        "image_sha256": actual_sha,
                        "result": normalized,
                    }
                )
                continue
        task_rows.setdefault(key, []).append(index)
        task_specs[key] = (image_path, actual_sha)

    factory = client_factory or (
        lambda cfg: OpenAICompatibleClient(
            api_base=cfg.api_base,
            api_key=cfg.api_key(),
            timeout_seconds=cfg.timeout_seconds,
            max_retries=cfg.max_retries,
        )
    )
    client = factory(runtime)
    report["api_requests"] = len(task_specs)
    with ThreadPoolExecutor(max_workers=runtime.concurrency) as executor:
        futures = {
            executor.submit(
                classify_image,
                image_path,
                expected_sha256=image_sha,
                runtime=runtime,
                client=client,
            ): key
            for key, (image_path, image_sha) in task_specs.items()
        }
        for future in as_completed(futures):
            key = futures[future]
            image_path, image_sha = task_specs[key]
            indexes = task_rows.get(key, [])
            try:
                result = future.result()
                entries[key] = {
                    "image_sha256": image_sha,
                    "model": runtime.model,
                    "prompt_version": PROMPT_VERSION,
                    "result": result,
                }
                _atomic_write_json(cache_path, cache_payload)
                for index in indexes:
                    rows[index]["visual_classification"] = result
                    report["classified"] += 1
                    result_rows.append(
                        {
                            "status": "ok",
                            "cache_hit": False,
                            "name": str(rows[index].get("name") or ""),
                            "detail_url": str(rows[index].get("detail_url") or ""),
                            "image_sha256": image_sha,
                            "result": result,
                        }
                    )
            except Exception as exc:
                error = f"{type(exc).__name__}:{exc}"
                for index in indexes:
                    report["failed"] += 1
                    result_rows.append(
                        {
                            "status": "fail",
                            "cache_hit": False,
                            "name": str(rows[index].get("name") or ""),
                            "detail_url": str(rows[index].get("detail_url") or ""),
                            "image_sha256": image_sha,
                            "source_path": str(image_path),
                            "error": error,
                        }
                    )

    _atomic_write_jsonl(queue_path, rows)
    _atomic_write_jsonl(results_path, result_rows)
    _atomic_write_json(report_path, report)
    return report
