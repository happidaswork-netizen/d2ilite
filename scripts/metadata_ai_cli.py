# -*- coding: utf-8 -*-
"""Headless metadata AI helpers used by D2I Lite Next."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from llm_client import OpenAICompatibleClient, normalize_api_base
from metadata_manager import clean_keywords, read_image_metadata, suggest_metadata_fill
from services.editor_text_service import (
    extract_json_payload_from_llm,
    normalize_gender_text,
    normalize_multiline_editor_text,
    normalize_profile_for_editor,
    normalize_single_line_text,
)
from services.metadata_service import normalize_http_url
from services.settings_service import load_app_settings


ALLOWED_INPUT_MODES = {"filename", "metadata", "filename_metadata"}


def _json_line(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _split_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        return [normalize_single_line_text(item) for item in value if normalize_single_line_text(item)]
    text = str(value or "")
    out: List[str] = []
    for token in text.replace("，", ",").replace("、", ",").replace(";", ",").replace("；", ",").split(","):
        cleaned = normalize_single_line_text(token)
        if cleaned:
            out.append(cleaned)
    return out


def _metadata_to_payload(info: Any) -> Dict[str, Any]:
    profile_from_file: Dict[str, Any] = {}
    titi_json = getattr(info, "titi_json", None)
    if isinstance(titi_json, dict):
        profile = titi_json.get("d2i_profile")
        if isinstance(profile, dict):
            profile_from_file = dict(profile)
    return {
        "title": normalize_single_line_text(getattr(info, "title", "")),
        "person": normalize_single_line_text(getattr(info, "person", "")),
        "gender": normalize_gender_text(getattr(info, "gender", "")),
        "position": normalize_single_line_text(getattr(info, "position", "")),
        "city": normalize_single_line_text(getattr(info, "city", "")),
        "source": normalize_http_url(getattr(info, "source", "")),
        "image_url": normalize_http_url(getattr(info, "image_url", "")),
        "keywords": clean_keywords(list(getattr(info, "keywords", []) or []), max_count=8),
        "description": normalize_multiline_editor_text(getattr(info, "description", "")),
        "adaptive_fields": normalize_profile_for_editor(profile_from_file),
        "profile_from_file": normalize_profile_for_editor(profile_from_file),
    }


def _form_to_payload(form: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": normalize_single_line_text(form.get("title")),
        "person": normalize_single_line_text(form.get("person")),
        "gender": normalize_gender_text(form.get("gender")),
        "position": normalize_single_line_text(form.get("position")),
        "city": normalize_single_line_text(form.get("city")),
        "source": normalize_http_url(form.get("source")),
        "image_url": normalize_http_url(form.get("image_url")),
        "keywords": clean_keywords(_split_keywords(form.get("keywords") or form.get("keywords_text")), max_count=8),
        "description": normalize_multiline_editor_text(form.get("description")),
        "adaptive_fields": {},
        "profile_from_file": {},
    }


def _build_input_payload(image_path: str, input_mode: str, form: Dict[str, Any] | None = None) -> Dict[str, Any]:
    filename = os.path.basename(image_path)
    stem = os.path.splitext(filename)[0]
    payload: Dict[str, Any] = {
        "filename": filename,
        "filename_stem": stem,
    }
    if input_mode == "filename":
        return payload

    source_payload: Dict[str, Any] = {}
    if isinstance(form, dict) and form:
        source_payload = _form_to_payload(form)
    else:
        info = read_image_metadata(image_path)
        source_payload = _metadata_to_payload(info)

    if input_mode == "metadata":
        return source_payload
    payload.update(source_payload)
    return payload


def _load_llm_config() -> Dict[str, Any]:
    settings = load_app_settings()
    llm = settings.get("llm") if isinstance(settings, dict) else {}
    llm = dict(llm) if isinstance(llm, dict) else {}
    api_base = normalize_api_base(str(llm.get("api_base", "")).strip())
    model = str(llm.get("model", "")).strip()
    if not api_base:
        raise RuntimeError("未配置 API Base。请先在全局设置中填写。")
    if not model:
        raise RuntimeError("未配置模型名称。请先在全局设置中填写 Model。")
    return {
        "api_base": api_base,
        "api_key": str(llm.get("api_key", "")).strip(),
        "model": model,
        "timeout_seconds": max(5, _safe_int(llm.get("timeout_seconds"), 45)),
        "max_retries": max(1, _safe_int(llm.get("max_retries"), 2)),
        "temperature": _safe_float(llm.get("temperature"), 0.1),
    }


def _run_suggest(args: argparse.Namespace) -> int:
    image_path = os.path.abspath(str(args.image or "").strip().strip('"'))
    if not image_path or not os.path.isfile(image_path):
        _json_line({"ok": False, "error": "image not found", "detail": image_path})
        return 2
    try:
        info = read_image_metadata(image_path)
        suggestion = suggest_metadata_fill(info)
    except Exception as exc:
        _json_line({"ok": False, "error": "suggest failed", "detail": str(exc)})
        return 1
    _json_line({"ok": True, "suggestion": suggestion})
    return 0


def _run_autofill(args: argparse.Namespace) -> int:
    image_path = os.path.abspath(str(args.image or "").strip().strip('"'))
    if not image_path or not os.path.isfile(image_path):
        _json_line({"ok": False, "error": "image not found", "detail": image_path})
        return 2

    input_mode = str(args.input_mode or "filename_metadata").strip()
    if input_mode not in ALLOWED_INPUT_MODES:
        _json_line({"ok": False, "error": "invalid input mode", "detail": input_mode})
        return 2

    form: Dict[str, Any] | None = None
    if args.form_json:
        try:
            parsed = json.loads(args.form_json)
            form = parsed if isinstance(parsed, dict) else None
        except Exception as exc:
            _json_line({"ok": False, "error": "invalid form json", "detail": str(exc)})
            return 2

    try:
        llm_cfg = _load_llm_config()
        input_payload = _build_input_payload(image_path, input_mode, form=form)
        client = OpenAICompatibleClient(
            api_base=str(llm_cfg["api_base"]),
            api_key=str(llm_cfg["api_key"]),
            timeout_seconds=int(llm_cfg["timeout_seconds"]),
            max_retries=int(llm_cfg["max_retries"]),
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "你是图片元数据补全助手。只能基于输入内容，不得编造。"
                    "输出必须是 JSON 对象，不要额外文字。"
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": "补全缺失字段，尽量留空不确定项，并返回可合并的扩展字段。",
                        "input_mode": input_mode,
                        "constraints": {
                            "no_guess": True,
                            "gender_only": ["男", "女", ""],
                            "keywords_max": 8,
                            "leave_unknown_empty": True,
                            "filename_only_means_do_not_use_metadata": input_mode == "filename",
                        },
                        "output_schema": {
                            "title": "string",
                            "person": "string",
                            "gender": "string",
                            "position": "string",
                            "city": "string",
                            "source": "string",
                            "image_url": "string",
                            "description": "string",
                            "keywords": ["string"],
                            "extra_fields": {"key": "value"},
                        },
                        "input": input_payload,
                    },
                    ensure_ascii=False,
                ),
            },
        ]
        response = client.chat_completions(
            model=str(llm_cfg["model"]),
            messages=messages,
            temperature=float(llm_cfg["temperature"]),
            stream=False,
        )
        content = client.extract_first_message_content(response)
        payload = extract_json_payload_from_llm(content)
        if not payload:
            raise RuntimeError("模型返回不是有效 JSON。")
    except Exception as exc:
        _json_line({"ok": False, "error": "autofill failed", "detail": str(exc)})
        return 1

    _json_line({"ok": True, "result": payload, "input_mode": input_mode})
    return 0


def _run_biography(args: argparse.Namespace) -> int:
    image_path = os.path.abspath(str(args.image or "").strip().strip('"'))
    if not image_path or not os.path.isfile(image_path):
        _json_line({"ok": False, "error": "image not found", "detail": image_path})
        return 2

    form: Dict[str, Any] | None = None
    if args.form_json:
        try:
            parsed = json.loads(args.form_json)
            form = parsed if isinstance(parsed, dict) else None
        except Exception as exc:
            _json_line({"ok": False, "error": "invalid form json", "detail": str(exc)})
            return 2

    try:
        llm_cfg = _load_llm_config()
        input_payload = _build_input_payload(image_path, "filename_metadata", form=form)
        client = OpenAICompatibleClient(
            api_base=str(llm_cfg["api_base"]),
            api_key=str(llm_cfg["api_key"]),
            timeout_seconds=int(llm_cfg["timeout_seconds"]),
            max_retries=int(llm_cfg["max_retries"]),
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "你是图片人物小传助手。只能基于输入内容，不得编造。"
                    "输出必须是 JSON 对象，不要额外文字。"
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": "根据已有元数据生成简洁人物小传。信息不足时返回空字符串。",
                        "constraints": {
                            "no_guess": True,
                            "max_chars": 220,
                            "leave_unknown_empty": True,
                        },
                        "output_schema": {
                            "biography_short": "string",
                            "description": "string",
                        },
                        "input": input_payload,
                    },
                    ensure_ascii=False,
                ),
            },
        ]
        response = client.chat_completions(
            model=str(llm_cfg["model"]),
            messages=messages,
            temperature=float(llm_cfg["temperature"]),
            stream=False,
        )
        content = client.extract_first_message_content(response)
        payload = extract_json_payload_from_llm(content)
        if not payload:
            raise RuntimeError("模型返回不是有效 JSON。")
        biography = normalize_multiline_editor_text(payload.get("biography_short") or payload.get("description") or "")
        description = normalize_multiline_editor_text(payload.get("description") or biography)
    except Exception as exc:
        _json_line({"ok": False, "error": "biography failed", "detail": str(exc)})
        return 1

    _json_line(
        {
            "ok": True,
            "result": {
                "biography_short": biography,
                "description": description,
            },
        }
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="D2I Lite metadata AI CLI")
    subparsers = parser.add_subparsers(dest="action", required=True)

    suggest = subparsers.add_parser("suggest", help="Suggest local non-LLM metadata fills")
    suggest.add_argument("--image", required=True)
    suggest.set_defaults(func=_run_suggest)

    autofill = subparsers.add_parser("autofill", help="Run LLM metadata autofill")
    autofill.add_argument("--image", required=True)
    autofill.add_argument(
        "--input-mode",
        default="filename_metadata",
        choices=("filename", "metadata", "filename_metadata"),
    )
    autofill.add_argument("--form-json", default="")
    autofill.set_defaults(func=_run_autofill)

    biography = subparsers.add_parser("biography", help="Run LLM biography generation")
    biography.add_argument("--image", required=True)
    biography.add_argument("--form-json", default="")
    biography.set_defaults(func=_run_biography)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
