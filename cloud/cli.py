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


def cmd_queues_finalize(args: argparse.Namespace) -> int:
    qid = urllib.parse.quote(str(args.queue_id), safe="")
    body = {
        "dry_run": bool(args.dry_run),
        "limit": int(args.limit or 0),
        "write_people": not bool(args.skip_people),
    }
    return _print(_request("POST", f"/api/v1/queues/{qid}/finalize", body=body, timeout=120.0))


def cmd_vision_status(_args: argparse.Namespace) -> int:
    return _print(_request("GET", "/api/v1/ai/vision/status"))


def cmd_vision_run(args: argparse.Namespace) -> int:
    path = str(args.path or "").strip()
    qid = str(args.queue_id or "").strip()
    if qid:
        body = {
            "dry_run": bool(args.dry_run),
            "limit": int(args.limit or 0),
            "force": bool(args.force),
            "write_people": not bool(args.skip_people),
            "steps": ["vision"],
        }
        if args.name:
            body["names"] = [str(args.name)]
        q = urllib.parse.quote(qid, safe="")
        return _print(
            _request("POST", f"/api/v1/queues/{q}/ai/run", body=body, timeout=float(args.timeout or 600))
        )
    if not path:
        return _fail("provide --path or --queue-id")
    body = {
        "path": path,
        "name": str(args.name or ""),
        "person_id": str(args.person_id or ""),
        "write_people": not bool(args.skip_people),
        "dry_run": bool(args.dry_run),
        "force": bool(args.force),
    }
    return _print(_request("POST", "/api/v1/ai/vision", body=body, timeout=float(args.timeout or 180)))


def cmd_vision_report(args: argparse.Namespace) -> int:
    qid = str(args.queue_id or "").strip()
    if not qid:
        return _fail("queue_id required")
    q = urllib.parse.quote(qid, safe="")
    return _print(_request("GET", f"/api/v1/queues/{q}/ai/vision/report", timeout=60.0))


def cmd_vision_recrawl_plan(args: argparse.Namespace) -> int:
    qid = str(args.queue_id or "").strip()
    if not qid:
        return _fail("queue_id required")
    q = urllib.parse.quote(qid, safe="")
    path = f"/api/v1/queues/{q}/ai/vision/recrawl-plan"
    if args.include_review:
        path += "?include_review=true"
    return _print(_request("GET", path, timeout=60.0))


def cmd_vision_recrawl(args: argparse.Namespace) -> int:
    qid = str(args.queue_id or "").strip()
    if not qid:
        return _fail("queue_id required")
    body = {
        "include_review": bool(args.include_review),
        "create_draft": bool(args.create_draft),
        "confirm": bool(args.confirm),
        "name": str(args.name or ""),
        "notes": str(args.notes or ""),
    }
    q = urllib.parse.quote(qid, safe="")
    return _print(
        _request("POST", f"/api/v1/queues/{q}/ai/vision/recrawl", body=body, timeout=120.0)
    )



def cmd_vision_inventory(args: argparse.Namespace) -> int:
    return _print(
        _request(
            "GET",
            "/api/v1/ai/vision/inventory",
            query={
                "province": str(args.province or ""),
                "city": str(args.city or ""),
                "unit_like": str(args.unit_like or ""),
                "limit": int(args.limit or 5000),
                "only_resolvable": "true" if args.only_resolvable else "false",
            },
            timeout=120.0,
        )
    )


def cmd_vision_plan(args: argparse.Namespace) -> int:
    return _print(
        _request(
            "GET",
            "/api/v1/ai/vision/plan",
            query={
                "batch_size": int(args.batch_size or 40),
                "province": str(args.province or ""),
                "city": str(args.city or ""),
                "unit_like": str(args.unit_like or ""),
                "limit": int(args.limit or 20000),
            },
            timeout=180.0,
        )
    )


def cmd_vision_enqueue(args: argparse.Namespace) -> int:
    body = {
        "batch_size": int(args.batch_size or 40),
        "province": str(args.province or ""),
        "city": str(args.city or ""),
        "unit_like": str(args.unit_like or ""),
        "limit": int(args.limit or 20000),
        "max_batches": int(args.max_batches or 0),
        "force": bool(args.force),
        "write_people": not bool(args.skip_people),
        "dry_run": bool(args.dry_run),
        "start": bool(args.start),
        "max_running": int(args.max_running or 1),
    }
    return _print(_request("POST", "/api/v1/ai/vision/enqueue", body=body, timeout=float(args.timeout or 600)))


def cmd_vision_jobs(args: argparse.Namespace) -> int:
    return _print(
        _request(
            "GET",
            "/api/v1/ai/vision/jobs",
            query={"limit": int(args.limit or 100), "status": str(args.status or "")},
            timeout=60.0,
        )
    )


def cmd_vision_job_show(args: argparse.Namespace) -> int:
    jid = str(args.job_id or "").strip()
    if not jid:
        return _fail("job_id required")
    q = urllib.parse.quote(jid, safe="")
    return _print(_request("GET", f"/api/v1/ai/vision/jobs/{q}", timeout=60.0))


def cmd_vision_pump(args: argparse.Namespace) -> int:
    body = {
        "max_running": int(args.max_running or 1),
        "max_claim": int(args.max_claim or 0),
        "background": bool(args.background),
    }
    return _print(_request("POST", "/api/v1/ai/vision/pump", body=body, timeout=float(args.timeout or 3600)))


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

    q_fin = q_sub.add_parser("finalize", help="promote queue images to 角色肖像 + people writeback")
    q_fin.add_argument("queue_id")
    q_fin.add_argument("--dry-run", action="store_true")
    q_fin.add_argument("--limit", type=int, default=0)
    q_fin.add_argument("--skip-people", action="store_true")
    q_fin.set_defaults(func=cmd_queues_finalize)

    vision = sub.add_parser("vision", help="Grok vision stage (classify + report + recrawl plan)")
    v_sub = vision.add_subparsers(dest="vision_cmd", required=True)
    v_status = v_sub.add_parser("status", help="vision runtime status")
    v_status.set_defaults(func=cmd_vision_status)
    v_inv = v_sub.add_parser("inventory", help="list people with photo but no visual_gender")
    v_inv.add_argument("--province", default="")
    v_inv.add_argument("--city", default="")
    v_inv.add_argument("--unit-like", default="")
    v_inv.add_argument("--limit", type=int, default=5000)
    v_inv.add_argument("--only-resolvable", action="store_true")
    v_inv.set_defaults(func=cmd_vision_inventory)
    v_plan = v_sub.add_parser("plan", help="split unvisioned into multi batches (no enqueue)")
    v_plan.add_argument("--batch-size", type=int, default=40)
    v_plan.add_argument("--province", default="")
    v_plan.add_argument("--city", default="")
    v_plan.add_argument("--unit-like", default="")
    v_plan.add_argument("--limit", type=int, default=20000)
    v_plan.set_defaults(func=cmd_vision_plan)
    v_enq = v_sub.add_parser("enqueue", help="enqueue unvisioned as many vision_jobs")
    v_enq.add_argument("--batch-size", type=int, default=40)
    v_enq.add_argument("--province", default="")
    v_enq.add_argument("--city", default="")
    v_enq.add_argument("--unit-like", default="")
    v_enq.add_argument("--limit", type=int, default=20000)
    v_enq.add_argument("--max-batches", type=int, default=0)
    v_enq.add_argument("--force", action="store_true")
    v_enq.add_argument("--skip-people", action="store_true")
    v_enq.add_argument("--dry-run", action="store_true")
    v_enq.add_argument("--start", action="store_true", help="pump after enqueue")
    v_enq.add_argument("--max-running", type=int, default=1)
    v_enq.add_argument("--timeout", type=float, default=600.0)
    v_enq.set_defaults(func=cmd_vision_enqueue)
    v_jobs = v_sub.add_parser("jobs", help="list vision jobs queue")
    v_jobs.add_argument("--limit", type=int, default=100)
    v_jobs.add_argument("--status", default="")
    v_jobs.set_defaults(func=cmd_vision_jobs)
    v_job = v_sub.add_parser("job", help="show one vision job")
    v_job.add_argument("job_id")
    v_job.set_defaults(func=cmd_vision_job_show)
    v_pump = v_sub.add_parser("pump", help="run queued vision jobs (serial by default)")
    v_pump.add_argument("--max-running", type=int, default=1)
    v_pump.add_argument("--max-claim", type=int, default=0, help="0=drain all queued under cap")
    v_pump.add_argument(
        "--background",
        action="store_true",
        help="daemon drain (return immediately; poll vision status/jobs)",
    )
    v_pump.add_argument("--timeout", type=float, default=3600.0)
    v_pump.set_defaults(func=cmd_vision_pump)
    v_run = v_sub.add_parser("run", help="stage-2 classify + write vision_report.json")
    v_run.add_argument("--path", default="", help="image path (host or container)")
    v_run.add_argument("--queue-id", default="", help="classify portraits for queue")
    v_run.add_argument("--name", default="")
    v_run.add_argument("--person-id", default="")
    v_run.add_argument("--dry-run", action="store_true")
    v_run.add_argument("--force", action="store_true", help="re-classify even if visual_gender set")
    v_run.add_argument("--skip-people", action="store_true")
    v_run.add_argument("--limit", type=int, default=0)
    v_run.add_argument("--timeout", type=float, default=600.0)
    v_run.set_defaults(func=cmd_vision_run)
    v_report = v_sub.add_parser("report", help="fetch stage-2 vision report for a queue")
    v_report.add_argument("queue_id")
    v_report.set_defaults(func=cmd_vision_report)
    v_plan = v_sub.add_parser("recrawl-plan", help="problem-name recrawl plan (no start)")
    v_plan.add_argument("queue_id")
    v_plan.add_argument("--include-review", action="store_true")
    v_plan.set_defaults(func=cmd_vision_recrawl_plan)
    v_recrawl = v_sub.add_parser(
        "recrawl",
        help="build recrawl plan; optional draft queue with --create-draft --confirm (never auto-start)",
    )
    v_recrawl.add_argument("queue_id")
    v_recrawl.add_argument("--include-review", action="store_true")
    v_recrawl.add_argument("--create-draft", action="store_true")
    v_recrawl.add_argument("--confirm", action="store_true", help="required with --create-draft")
    v_recrawl.add_argument("--name", default="", help="draft queue name")
    v_recrawl.add_argument("--notes", default="")
    v_recrawl.set_defaults(func=cmd_vision_recrawl)

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
