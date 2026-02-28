import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from llm_client import OpenAICompatibleClient, normalize_api_base


def _normalize_text(value: Any) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def _clean_token(value: Any, max_len: int = 24) -> str:
    text = _normalize_text(value).strip("，,。；;：:|/\\")
    if not text:
        return ""
    if len(text) > max_len:
        return ""
    lowered = text.lower()
    if lowered in {"unknown", "unkonw", "n/a", "na", "none", "null", "-", "未知", "不详", "未详"}:
        return ""
    if re.search(r"\s", text):
        return ""
    return text


def _extract_json_payload(raw_text: str) -> Dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    # fenced markdown fallback
    fenced = re.findall(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    for block in fenced:
        try:
            parsed = json.loads(block)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue

    # greedy first-object fallback
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


class LLMEnricher:
    def __init__(self, rules: Dict[str, Any], state_dir: Path):
        self.enabled = bool(rules.get("llm_enrich_enabled", False))
        self.only_when_missing = bool(rules.get("llm_only_when_missing_fields", True))
        self.generate_biography = bool(rules.get("llm_generate_biography", True))
        self.append_biography_to_description = bool(rules.get("llm_append_biography_to_description", True))
        self.max_input_chars = max(500, int(rules.get("llm_max_input_chars", 6000)))
        self.timeout_seconds = max(5, int(rules.get("llm_timeout_seconds", 45)))
        self.max_retries = max(1, int(rules.get("llm_max_retries", 2)))
        self.temperature = float(rules.get("llm_temperature", 0.1))
        self.model = str(rules.get("llm_model", "")).strip() or os.environ.get("D2I_LLM_MODEL", "").strip()

        api_base_rule = str(rules.get("llm_api_base", "")).strip()
        self.api_base = api_base_rule or os.environ.get("D2I_LLM_API_BASE", "http://127.0.0.1:11434/v1").strip()
        self.api_base = normalize_api_base(self.api_base)
        self.api_key = str(rules.get("llm_api_key", "")).strip() or os.environ.get("D2I_LLM_API_KEY", "").strip()

        self._client: Optional[OpenAICompatibleClient] = None
        if self.api_base:
            self._client = OpenAICompatibleClient(
                api_base=self.api_base,
                api_key=self.api_key,
                timeout_seconds=self.timeout_seconds,
                max_retries=self.max_retries,
            )

        self.cache_enabled = bool(rules.get("llm_cache_enabled", True))
        self.cache_path = state_dir / "llm_enrichment_cache.json"
        self.cache: Dict[str, Any] = {}
        self.cache_dirty = False

        self.stats = {
            "enabled": self.enabled,
            "calls_total": 0,
            "calls_success": 0,
            "calls_failed": 0,
            "cache_hits": 0,
            "skipped_by_policy": 0,
            "rows_enriched": 0,
        }

        if self.cache_enabled and self.cache_path.exists():
            try:
                payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    self.cache = payload
            except Exception:
                self.cache = {}

    def available(self) -> bool:
        return bool(self.enabled and self.model and self.api_base)

    def _build_messages(self, input_payload: Dict[str, Any]) -> List[Dict[str, str]]:
        system_prompt = (
            "你是结构化信息抽取助手。只能基于给定文本，不得编造。"
            "对不确定字段必须留空。输出必须是 JSON 对象，不要任何额外文字。"
        )
        user_prompt = {
            "task": "提取人物结构化字段并生成简短中文个人小传。",
            "constraints": {
                "no_guess": True,
                "keywords_max": 6,
                "biography_max_chars": 220,
                "keep_empty_when_unknown": True,
            },
            "output_schema": {
                "position": "string",
                "city": "string",
                "unit": "string",
                "profession": "string",
                "profession_tags": ["string"],
                "keywords_extra": ["string"],
                "biography_short": "string",
            },
            "input": input_payload,
        }
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
        ]

    def _hash_input(self, payload: Dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        key_raw = f"{self.model}|{raw}"
        return hashlib.sha1(key_raw.encode("utf-8")).hexdigest()

    def _request_llm(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        client = self._client
        if client is None:
            client = OpenAICompatibleClient(
                api_base=self.api_base,
                api_key=self.api_key,
                timeout_seconds=self.timeout_seconds,
                max_retries=self.max_retries,
            )
            self._client = client

        last_error = ""
        for _ in range(self.max_retries):
            try:
                data = client.chat_completions(
                    model=self.model,
                    messages=self._build_messages(payload),
                    temperature=self.temperature,
                    stream=False,
                )
                content = client.extract_first_message_content(data)
                result = _extract_json_payload(content)
                if result:
                    return result
                last_error = "invalid_json_content"
            except Exception as exc:
                last_error = str(exc)
        raise RuntimeError(last_error or "llm_request_failed")

    def _sanitize_result(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        position = _normalize_text(payload.get("position", ""))
        city = _normalize_text(payload.get("city", ""))
        unit = _normalize_text(payload.get("unit", ""))
        profession = _clean_token(payload.get("profession", ""), max_len=12)
        biography_short = _normalize_text(payload.get("biography_short", ""))
        if len(biography_short) > 260:
            biography_short = biography_short[:257] + "..."

        profession_tags_raw = payload.get("profession_tags", [])
        profession_tags: List[str] = []
        if isinstance(profession_tags_raw, list):
            for item in profession_tags_raw:
                token = _clean_token(item, max_len=12)
                if token and token not in profession_tags:
                    profession_tags.append(token)

        keywords_raw = payload.get("keywords_extra", [])
        keywords_extra: List[str] = []
        if isinstance(keywords_raw, list):
            for item in keywords_raw:
                token = _clean_token(item, max_len=12)
                if token and token not in keywords_extra:
                    keywords_extra.append(token)

        if position:
            result["position"] = position
        if city:
            result["city"] = city
        if unit:
            result["unit"] = unit
        if profession:
            result["profession"] = profession
            if profession not in profession_tags:
                profession_tags.insert(0, profession)
        if profession_tags:
            result["profession_tags"] = profession_tags[:4]
        if keywords_extra:
            result["keywords_extra"] = keywords_extra[:4]
        if biography_short:
            result["biography_short"] = biography_short
        return result

    def enrich_row(
        self,
        *,
        row: Dict[str, Any],
        position: str,
        city: str,
        unit: str,
        summary: str,
        full_content: str,
        extra_fields: Dict[str, str],
        mapped_fields: Dict[str, str],
    ) -> Dict[str, Any]:
        if not self.available():
            self.stats["skipped_by_policy"] += 1
            return {}

        if self.only_when_missing:
            if position and city and unit and (not self.generate_biography):
                self.stats["skipped_by_policy"] += 1
                return {}

        input_payload = {
            "name": _normalize_text(row.get("name", "")),
            "detail_url": _normalize_text(row.get("detail_url", "")),
            "source_url": _normalize_text(row.get("source_url", "")),
            "position_current": _normalize_text(position),
            "city_current": _normalize_text(city),
            "unit_current": _normalize_text(unit),
            "summary": _normalize_text(summary)[: self.max_input_chars // 2],
            "full_content_excerpt": _normalize_text(full_content)[: self.max_input_chars],
            "fields": extra_fields,
            "mapped_fields": mapped_fields,
        }

        cache_key = self._hash_input(input_payload)
        if self.cache_enabled and cache_key in self.cache:
            self.stats["cache_hits"] += 1
            cached = self.cache.get(cache_key)
            return cached if isinstance(cached, dict) else {}

        self.stats["calls_total"] += 1
        try:
            raw = self._request_llm(input_payload)
            parsed = self._sanitize_result(raw)
            if not self.generate_biography and "biography_short" in parsed:
                parsed.pop("biography_short", None)
            if parsed:
                self.stats["calls_success"] += 1
                self.stats["rows_enriched"] += 1
            if self.cache_enabled:
                self.cache[cache_key] = parsed
                self.cache_dirty = True
            return parsed
        except Exception:
            self.stats["calls_failed"] += 1
            if self.cache_enabled:
                self.cache[cache_key] = {}
                self.cache_dirty = True
            return {}

    def flush_cache(self) -> None:
        if not self.cache_enabled or (not self.cache_dirty):
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(
            json.dumps(self.cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.cache_dirty = False

    def report(self) -> Dict[str, Any]:
        return dict(self.stats)
