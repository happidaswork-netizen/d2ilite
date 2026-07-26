# -*- coding: utf-8 -*-
"""HTTP-only CLI for D2I Cloud (Hermes / operator). No local scraper imports."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional


def _api_base() -> str:
    return str(os.environ.get("D2I_CLOUD_API", "http://127.0.0.1:8787") or "").rstrip("/")


def _token() -> str:
    env = str(os.environ.get("D2I_WEB_TOKEN", "") or "").strip()
    if env:
        return env
    candidates = [
        Path(os.environ.get("D2I_WEB_TOKEN_FILE", "") or ""),
        Path("/runtime/d2i-cloud-data/web_token.txt"),
        Path("/vol4/1001/hermes-runtime/d2i-cloud-data/web_token.txt"),
    ]
    for path in candidates:
        if not path or not str(path):
            continue
        try:
            if path.is_file():
                return path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
    return ""


def _print(payload: Any) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _fail(message: str, code: int = 1) -> int:
    print(json.dumps({"ok": False, "error": message}, ensure_ascii=False), file=sys.stderr)
    return code


def _request(
    method: str,
    path: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Any:
    base = _api_base()
    if not base:
        raise RuntimeError("D2I_CLOUD_API is empty")
    url = f"{base}{path}"
    if query:
        q = {k: v for k, v in query.items() if v is not None and v != ""}
        if q:
            url = f"{url}?{urllib.parse.urlencode(q)}"
    data = None
    headers = {"Accept": "application/json"}
    token = _token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            if not raw:
                return {}
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(detail) if detail else {}
            msg = parsed.get("detail") or parsed.get("error") or detail or exc.reason
        except Exception:
            msg = detail or str(exc.reason)
        raise RuntimeError(f"HTTP {exc.code}: {msg}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"connect failed: {exc.reason}") from exc


def cmd_status(_args: argparse.Namespace) -> int:
    return _print(_request("GET", "/api/v1/status"))


def cmd_templates_list(_args: argparse.Namespace) -> int:
    return _print(_request("GET", "/api/v1/templates"))


def cmd_queues_list(args: argparse.Namespace) -> int:
    return _print(_request("GET", "/api/v1/queues", query={"limit": int(args.limit or 200)}))


def cmd_queues_show(args: argparse.Namespace) -> int:
    qid = urllib.parse.quote(str(args.queue_id), safe="")
    return _print(_request("GET", f"/api/v1/queues/{qid}"))


def cmd_queues_create(args: argparse.Namespace) -> int:
    body = {
        "start_url": str(args.start_url or ""),
        "template_id": str(args.template or ""),
        "template_path": str(args.template_path or ""),
        "name": str(args.name or ""),
        "output_root": str(args.output_root or ""),
        "speed_tier": str(args.speed_tier or "safe"),
        "speed_tier_reason": str(args.speed_tier_reason or ""),
        "notes": str(args.notes or ""),
        "start": bool(args.start),
        "allow_turbo": bool(args.allow_turbo),
    }
    return _print(_request("POST", "/api/v1/queues", body=body))


def cmd_queues_action(args: argparse.Namespace) -> int:
    qid = urllib.parse.quote(str(args.queue_id), safe="")
    action = str(args.action)
    return _print(_request("POST", f"/api/v1/queues/{qid}/{action}", body={"options": {}}))


def cmd_queues_logs(args: argparse.Namespace) -> int:
    qid = urllib.parse.quote(str(args.queue_id), safe="")
    return _print(
        _request("GET", f"/api/v1/queues/{qid}/logs", query={"lines": int(args.lines or 80)})
    )


def cmd_queues_items(args: argparse.Namespace) -> int:
    qid = urllib.parse.quote(str(args.queue_id), safe="")
    return _print(
        _request(
            "GET",
            f"/api/v1/queues/{qid}/items",
            query={"limit": int(args.limit or 100), "status": str(args.status or "")},
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="d2i", description="D2I Cloud HTTP CLI")
    sub = parser.add_subparsers(dest="group", required=True)

    status = sub.add_parser("status", help="service status")
    status.set_defaults(func=cmd_status)

    templates = sub.add_parser("templates", help="template experience library")
    t_sub = templates.add_subparsers(dest="templates_cmd", required=True)
    t_list = t_sub.add_parser("list", help="list known templates")
    t_list.set_defaults(func=cmd_templates_list)

    queues = sub.add_parser("queues", help="queue management")
    q_sub = queues.add_subparsers(dest="queues_cmd", required=True)

    q_list = q_sub.add_parser("list", help="list queues")
    q_list.add_argument("--limit", type=int, default=200)
    q_list.set_defaults(func=cmd_queues_list)

    q_show = q_sub.add_parser("show", help="show one queue")
    q_show.add_argument("queue_id")
    q_show.set_defaults(func=cmd_queues_show)

    q_create = q_sub.add_parser("create", help="create queue from template/url")
    q_create.add_argument("--start-url", default="")
    q_create.add_argument("--template", default="", help="template id/stem")
    q_create.add_argument("--template-path", default="")
    q_create.add_argument("--name", default="")
    q_create.add_argument("--output-root", default="")
    q_create.add_argument("--speed-tier", default="safe", choices=["safe", "standard", "turbo"])
    q_create.add_argument("--speed-tier-reason", default="")
    q_create.add_argument("--notes", default="")
    q_create.add_argument("--start", action="store_true")
    q_create.add_argument("--allow-turbo", action="store_true")
    q_create.set_defaults(func=cmd_queues_create)

    for action in ("start", "pause", "resume", "retry", "cancel"):
        q_act = q_sub.add_parser(action, help=f"{action} queue")
        q_act.add_argument("queue_id")
        q_act.set_defaults(func=cmd_queues_action, action=action)

    q_logs = q_sub.add_parser("logs", help="tail queue log")
    q_logs.add_argument("queue_id")
    q_logs.add_argument("--lines", type=int, default=80)
    q_logs.set_defaults(func=cmd_queues_logs)

    q_items = q_sub.add_parser("items", help="list queue items")
    q_items.add_argument("queue_id")
    q_items.add_argument("--limit", type=int, default=100)
    q_items.add_argument("--status", default="")
    q_items.set_defaults(func=cmd_queues_items)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    func = getattr(args, "func", None)
    if not callable(func):
        parser.print_help()
        return 2
    try:
        return int(func(args))
    except Exception as exc:
        return _fail(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
