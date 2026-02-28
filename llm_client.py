# -*- coding: utf-8 -*-
"""
Unified LLM client (OpenAI-compatible).

Goal:
- One place to handle API base/key/model plumbing.
- Reused by GUI (global settings) and scraper LLM enrichment.

Assumptions:
- Provider supports OpenAI-style endpoints:
  - GET  /v1/models
  - POST /v1/chat/completions
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests


def normalize_api_base(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    raw = raw.rstrip("/")
    try:
        parsed = urlparse(raw)
        if parsed.scheme and parsed.netloc:
            path = (parsed.path or "").rstrip("/")
            # Common user input: "https://api.openai.com" -> treat as base, append "/v1".
            if path in {"", "/"}:
                parsed = parsed._replace(path="/v1")
                raw = urlunparse(parsed).rstrip("/")
            else:
                raw = urlunparse(parsed._replace(path=path)).rstrip("/")
    except Exception:
        pass
    return raw


def _snip(text: str, limit: int = 240) -> str:
    s = str(text or "")
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 3)] + "..."


@dataclass
class LLMConfig:
    api_base: str = ""
    api_key: str = ""
    model: str = ""
    timeout_seconds: int = 45
    max_retries: int = 2
    temperature: float = 0.1

    def normalized(self) -> "LLMConfig":
        cfg = LLMConfig(**{k: getattr(self, k) for k in self.__dataclass_fields__})
        cfg.api_base = normalize_api_base(cfg.api_base)
        cfg.api_key = str(cfg.api_key or "").strip()
        cfg.model = str(cfg.model or "").strip()
        try:
            cfg.timeout_seconds = max(5, int(cfg.timeout_seconds))
        except Exception:
            cfg.timeout_seconds = 45
        try:
            cfg.max_retries = max(1, int(cfg.max_retries))
        except Exception:
            cfg.max_retries = 2
        try:
            cfg.temperature = float(cfg.temperature)
        except Exception:
            cfg.temperature = 0.1
        return cfg


class OpenAICompatibleClient:
    def __init__(
        self,
        *,
        api_base: str,
        api_key: str = "",
        timeout_seconds: int = 45,
        max_retries: int = 2,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.api_base = normalize_api_base(api_base)
        self.api_key = str(api_key or "").strip()
        self.timeout_seconds = max(5, int(timeout_seconds or 45))
        self.max_retries = max(1, int(max_retries or 2))
        self.extra_headers = dict(extra_headers or {})

    def _build_headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        headers.update({str(k): str(v) for k, v in (self.extra_headers or {}).items() if str(k).strip()})
        return headers

    def list_models(self) -> List[str]:
        if not self.api_base:
            raise RuntimeError("missing_api_base")
        endpoint = f"{self.api_base}/models"
        last_err = ""
        for _ in range(self.max_retries):
            try:
                resp = requests.get(endpoint, headers=self._build_headers(), timeout=self.timeout_seconds)
                if resp.status_code >= 400:
                    last_err = f"http_{resp.status_code}:{_snip(resp.text)}"
                    continue
                payload = resp.json()
                models: List[str] = []
                if isinstance(payload, dict):
                    data = payload.get("data")
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get("id"):
                                models.append(str(item["id"]))
                            elif isinstance(item, str):
                                models.append(item)
                elif isinstance(payload, list):
                    models = [str(x) for x in payload if str(x).strip()]
                models = [m.strip() for m in models if str(m).strip()]
                # de-dup while keeping order
                seen: set[str] = set()
                uniq: List[str] = []
                for m in models:
                    if m in seen:
                        continue
                    seen.add(m)
                    uniq.append(m)
                return uniq
            except Exception as exc:
                last_err = str(exc)
        raise RuntimeError(last_err or "list_models_failed")

    def chat_completions(
        self,
        *,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.1,
        stream: bool = False,
    ) -> Dict[str, Any]:
        if not self.api_base:
            raise RuntimeError("missing_api_base")
        endpoint = f"{self.api_base}/chat/completions"
        body = {
            "model": str(model or "").strip(),
            "messages": messages,
            "temperature": float(temperature),
            "stream": bool(stream),
        }
        last_err = ""
        for _ in range(self.max_retries):
            try:
                resp = requests.post(
                    endpoint,
                    headers=self._build_headers(),
                    data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                    timeout=self.timeout_seconds,
                )
                if resp.status_code >= 400:
                    last_err = f"http_{resp.status_code}:{_snip(resp.text)}"
                    continue
                data = resp.json()
                return data if isinstance(data, dict) else {"raw": data}
            except Exception as exc:
                last_err = str(exc)
        raise RuntimeError(last_err or "chat_completions_failed")

    @staticmethod
    def extract_first_message_content(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        choices = payload.get("choices", [])
        if not isinstance(choices, list) or (not choices):
            return ""
        msg = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
        if not isinstance(msg, dict):
            return ""
        return str(msg.get("content", "") or "")

