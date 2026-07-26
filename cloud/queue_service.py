# -*- coding: utf-8 -*-
"""Queue domain for D2I Cloud — wraps public scraper create/control."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cloud import jobs_db
from cloud.paths import APP_FILE, PROJECT_ROOT, SCRAPER_SCRIPT, TEMPLATES_DIR, default_tasks_base_root
from cloud.speed_tiers import apply_speed_tier_to_crawl, normalize_speed_tier
from services.desktop_scraper_backend_service import (
    build_scraper_workspace_payload,
    execute_scraper_control_action,
    register_process_watcher,
)
from services.public_scraper_config_service import build_public_scraper_runtime_config
from services.runtime_service import build_utf8_subprocess_env, resolve_python_cli_executable
from services.task_orchestration_service import (
    build_public_scraper_command,
    build_public_scraper_log_banner,
    resolve_named_images_dir,
)
from services.task_service import (
    count_jsonl_rows,
    normalize_public_task_root,
    set_public_scraper_manual_pause_flag,
    summarize_public_task,
)

_AUTO_FINALIZE_LOCK = threading.Lock()
_AUTO_FINALIZING: set[str] = set()
# H2: live status "completed" requires metadata ok on every profile, so queues with
# no-photo people never reach it. A stopped queue with images that stayed quiet for
# this long is treated as finished and allowed one auto-promote.
AUTO_FINALIZE_QUIET_MINUTES = 10.0
_RECONCILE_LOCK = threading.Lock()
_RECONCILE_STATE: Dict[str, Any] = {
    "running": False,
    "interval_seconds": 30.0,
    "last_at": "",
    "last_error": "",
    "cycles": 0,
    "thread_name": "",
}


def start_background_reconciler(*, interval_seconds: float = 30.0) -> Dict[str, Any]:
    """Daemon loop: keep desired_state honest and drain one auto-finalize per cycle."""
    interval = max(5.0, float(interval_seconds or 30.0))
    with _RECONCILE_LOCK:
        if _RECONCILE_STATE.get("running"):
            _RECONCILE_STATE["interval_seconds"] = interval
            return dict(_RECONCILE_STATE)

        def _loop() -> None:
            with _RECONCILE_LOCK:
                _RECONCILE_STATE["running"] = True
                _RECONCILE_STATE["thread_name"] = threading.current_thread().name
                _RECONCILE_STATE["interval_seconds"] = interval
            while True:
                try:
                    # Reconcile all; promote at most one completed queue per tick.
                    list_enriched_queues(limit=500, auto_finalize=True)
                    with _RECONCILE_LOCK:
                        _RECONCILE_STATE["last_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                        _RECONCILE_STATE["last_error"] = ""
                        _RECONCILE_STATE["cycles"] = int(_RECONCILE_STATE.get("cycles") or 0) + 1
                except Exception as exc:  # noqa: BLE001
                    with _RECONCILE_LOCK:
                        _RECONCILE_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
                        _RECONCILE_STATE["last_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                time.sleep(max(5.0, float(_RECONCILE_STATE.get("interval_seconds") or interval)))

        t = threading.Thread(target=_loop, name="d2i-queue-reconciler", daemon=True)
        t.start()
        _RECONCILE_STATE["running"] = True
        _RECONCILE_STATE["interval_seconds"] = interval
        _RECONCILE_STATE["thread_name"] = t.name
        return dict(_RECONCILE_STATE)


def reconciler_state() -> Dict[str, Any]:
    with _RECONCILE_LOCK:
        return dict(_RECONCILE_STATE)


def _templates_search_roots() -> List[Path]:
    roots = [TEMPLATES_DIR, PROJECT_ROOT / "scraper"]
    return [path for path in roots if path.is_dir()]


def list_template_files() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for root in _templates_search_roots():
        for path in sorted(root.glob("*.json")):
            name = path.name
            # Prefer scraper/templates over loose config samples when same stem collides by path
            key = str(path.resolve())
            if key in seen:
                continue
            if name.startswith("config.") and "templates" not in str(path.parent).replace("\\", "/"):
                # keep config.*.json as templates too (common in this repo)
                pass
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            site_name = str(payload.get("site_name", "") or path.stem).strip()
            crawl = payload.get("crawl") if isinstance(payload.get("crawl"), dict) else {}
            items.append(
                {
                    "id": path.stem,
                    "path": str(path.resolve()),
                    "name": site_name or path.stem,
                    "filename": path.name,
                    "speed_tier": normalize_speed_tier(crawl.get("speed_tier"), default="safe"),
                    "start_urls": list(payload.get("start_urls") or [])[:5],
                }
            )
            seen.add(key)
    # de-dupe by id preferring templates/ dir
    by_id: Dict[str, Dict[str, Any]] = {}
    for item in items:
        existing = by_id.get(item["id"])
        if existing is None:
            by_id[item["id"]] = item
            continue
        if "templates" in item["path"].replace("\\", "/") and "templates" not in existing["path"].replace("\\", "/"):
            by_id[item["id"]] = item
    return sorted(by_id.values(), key=lambda x: x["id"])


def resolve_template_path(template_id: str = "", template_path: str = "") -> Tuple[str, str]:
    explicit = str(template_path or "").strip()
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        else:
            path = path.resolve()
        if not path.is_file():
            raise FileNotFoundError(f"template not found: {path}")
        return path.stem, str(path)

    tid = str(template_id or "").strip()
    if not tid:
        return "", ""

    for item in list_template_files():
        if item["id"] == tid or item["filename"] == tid or item["filename"] == f"{tid}.json":
            return item["id"], item["path"]

    # direct path under templates
    candidate = TEMPLATES_DIR / (tid if tid.endswith(".json") else f"{tid}.json")
    if candidate.is_file():
        return candidate.stem, str(candidate.resolve())
    raise FileNotFoundError(f"template id not found: {tid}")


def _map_status_to_api(status: str, *, session_running: bool, manual_paused: bool) -> str:
    text = str(status or "").strip()
    if manual_paused or "暂停" in text:
        return "paused"
    if session_running or text in {"运行中", "继续运行中", "失败重试中", "元数据重写中"}:
        return "running"
    if text == "已完成":
        return "completed"
    if text == "风控暂停":
        return "cooldown"
    if text in {"未完成", "初始化"}:
        return "idle" if text == "初始化" else "stopped"
    return "idle"


def _workspace_task_row(
    output_root: str,
    base_root: str = "",
    *,
    progress_limit: int = 50,
    log_lines: int = 40,
) -> Dict[str, Any]:
    base = normalize_public_task_root(base_root) or default_tasks_base_root()
    workspace = build_scraper_workspace_payload(
        base,
        selected_root=output_root,
        progress_limit=max(20, int(progress_limit or 50)),
        log_lines=max(20, int(log_lines or 40)),
    )
    detail = workspace.get("detail") if isinstance(workspace.get("detail"), dict) else {}
    selected = workspace.get("selected_task") if isinstance(workspace.get("selected_task"), dict) else {}
    return {
        "workspace": workspace,
        "detail": detail,
        "selected_task": selected,
    }


_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"}


def _is_protected_path(path: str) -> bool:
    """选角 and similar must never be served via preview."""
    text = str(path or "").replace("\\", "/")
    lowered = text.lower()
    parts = [p for p in text.split("/") if p]
    if any(p == "选角" for p in parts):
        return True
    if "/选角/" in f"/{text.strip('/')}/":
        return True
    if "repair_backup" in lowered or "__hdd_prebind" in lowered:
        return True
    return False


def _safe_existing_image(path_value: Any, *, output_root: str = "") -> str:
    from services.task_service import normalize_existing_path

    raw = normalize_existing_path(path_value)
    if not raw:
        return ""
    if _is_protected_path(raw):
        return ""
    try:
        resolved = Path(raw).resolve()
    except Exception:
        return ""
    if not resolved.is_file():
        return ""
    if resolved.suffix.lower() not in _IMAGE_EXTS:
        return ""
    root = normalize_public_task_root(output_root)
    if root:
        try:
            resolved.relative_to(Path(root).resolve())
        except Exception:
            # Allow absolute saved_path that still lives under tasks base when root remap lags.
            base = default_tasks_base_root()
            try:
                if base:
                    resolved.relative_to(Path(base).resolve())
                else:
                    return ""
            except Exception:
                return ""
    return str(resolved)


def _stable_token(text: str) -> str:
    digest = hashlib.sha1(str(text or "").encode("utf-8", errors="ignore")).hexdigest()
    return digest[:10]


def _item_id_for_row(row: Dict[str, Any], *, bucket: str, index: int) -> str:
    idx = str(row.get("idx") or "").strip()
    if idx:
        return f"{bucket}-{idx}"
    detail = str(row.get("detail_url") or "").strip()
    if detail:
        return f"{bucket}-u{_stable_token(detail)}"
    name = str(row.get("name") or "").strip() or "row"
    return f"{bucket}-{index}-{_stable_token(name)}"


def _flag_ok(value: Any) -> bool:
    return str(value or "").strip() in {"√", "✓"}


def _enrich_item_row(
    row: Dict[str, Any],
    *,
    queue_id: str,
    bucket: str,
    index: int,
    output_root: str,
) -> Dict[str, Any]:
    data = dict(row) if isinstance(row, dict) else {}
    item_id = _item_id_for_row(data, bucket=bucket, index=index)
    image_path = str(data.get("image_path") or "").strip()
    safe_path = _safe_existing_image(image_path, output_root=output_root)
    has_file = bool(safe_path)
    detail_ok = _flag_ok(data.get("detail"))
    image_ok = _flag_ok(data.get("image"))
    meta_ok = _flag_ok(data.get("meta"))
    if bucket == "done" or (detail_ok and image_ok and meta_ok):
        item_status = "completed"
    elif image_ok or has_file:
        item_status = "downloaded"
    else:
        item_status = "pending"
    preview_url = ""
    if has_file:
        preview_url = f"/api/v1/queues/{queue_id}/items/{item_id}/preview"
    return {
        "id": item_id,
        "idx": str(data.get("idx") or "").strip(),
        "name": str(data.get("name") or "").strip() or "未命名",
        "detail": str(data.get("detail") or "").strip(),
        "image": str(data.get("image") or "").strip(),
        "meta": str(data.get("meta") or "").strip(),
        "reason": str(data.get("reason") or "").strip(),
        "detail_url": str(data.get("detail_url") or "").strip(),
        "image_path": image_path,
        "image_exists": has_file,
        "has_preview": has_file,
        "preview_url": preview_url,
        "bucket": bucket,
        "status": item_status,
        "flags": {
            "detail_ok": detail_ok,
            "image_ok": image_ok,
            "meta_ok": meta_ok,
        },
    }


def _iter_queue_item_rows(queue_id: str, *, progress_limit: int = 500) -> Tuple[Dict[str, Any], str, List[Dict[str, Any]]]:
    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    root = normalize_public_task_root(record.get("output_root"))
    bundle = _workspace_task_row(root, progress_limit=progress_limit, log_lines=40)
    detail = bundle.get("detail") if isinstance(bundle.get("detail"), dict) else {}
    pending_raw = list(detail.get("pending_rows") or [])
    done_raw = list(detail.get("done_rows") or [])
    items: List[Dict[str, Any]] = []
    for index, row in enumerate(done_raw):
        items.append(
            _enrich_item_row(row, queue_id=queue_id, bucket="done", index=index, output_root=root or "")
        )
    for index, row in enumerate(pending_raw):
        items.append(
            _enrich_item_row(row, queue_id=queue_id, bucket="pending", index=index, output_root=root or "")
        )
    return record, root or "", items


def _last_promote_meta(record: Dict[str, Any]) -> Dict[str, Any]:
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    last = meta.get("last_promote")
    return last if isinstance(last, dict) else {}


def _promote_already_done(record: Dict[str, Any], live: Dict[str, Any]) -> bool:
    last = _last_promote_meta(record)
    if not last or last.get("dry_run") or not last.get("ok"):
        return False
    counts = last.get("counts") if isinstance(last.get("counts"), dict) else {}
    prev = max(int(counts.get("candidates") or 0), int(counts.get("promoted") or 0))
    now_imgs = max(int(live.get("images") or 0), int(live.get("downloaded") or 0))
    # Allow redo only when workspace gained more images than last promote saw.
    if prev > 0 and now_imgs > prev:
        return False
    return True


def _auto_finalize_quiet_minutes() -> float:
    raw = str(os.environ.get("D2I_AUTO_FINALIZE_QUIET_MINUTES", "") or "").strip()
    if raw:
        try:
            return max(1.0, float(raw))
        except ValueError:
            pass
    return AUTO_FINALIZE_QUIET_MINUTES


def _live_quiet_seconds(live: Dict[str, Any]) -> float:
    """Seconds since the workspace last changed on disk; -1 when unknown."""
    stamp = str(live.get("updated_at_disk") or "").strip()
    if stamp and stamp != "-":
        try:
            parsed = time.mktime(time.strptime(stamp, "%Y-%m-%d %H:%M:%S"))
            return max(0.0, time.time() - parsed)
        except Exception:
            pass
    log_path = str(live.get("log_path") or "").strip()
    if log_path and os.path.isfile(log_path):
        try:
            return max(0.0, time.time() - os.path.getmtime(log_path))
        except Exception:
            pass
    return -1.0


def _should_auto_finalize(record: Dict[str, Any], live: Dict[str, Any]) -> bool:
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    if meta.get("auto_finalize") is False or meta.get("skip_auto_finalize"):
        return False
    if bool(live.get("session_running")):
        return False
    if _promote_already_done(record, live):
        return False
    status = str(live.get("status") or "")
    if status == "completed":
        return int(live.get("images") or 0) > 0 or int(live.get("profiles") or 0) > 0
    # H2 relaxed trigger: process dead + images landed + workspace quiet long enough.
    # paused/cooldown/error/cancelled stay hands-off (operator or backoff owns them).
    if status in {"paused", "cooldown", "error", "cancelled", "created"}:
        return False
    if int(live.get("images") or 0) <= 0:
        return False
    quiet = _live_quiet_seconds(live)
    if quiet < 0:
        return False
    return quiet >= _auto_finalize_quiet_minutes() * 60.0


def _reconcile_desired_state(record: Dict[str, Any], live: Dict[str, Any]) -> Dict[str, Any]:
    """Align jobs.desired_state with live runtime so completed queues stop looking 'running'."""
    queue_id = str(record.get("id") or "").strip()
    if not queue_id:
        return record
    desired = str(record.get("desired_state") or "").strip()
    status = str(live.get("status") or "").strip()
    running = bool(live.get("session_running"))
    paused = bool(live.get("manual_paused"))
    target = ""
    if running:
        target = "paused" if paused else "running"
    elif status == "completed":
        target = "completed"
    elif status == "paused":
        target = "paused"
    elif status == "cancelled":
        target = "cancelled"
    elif status == "error":
        target = "error"
    elif desired == "running" and status in {"stopped", "idle", "created"} and not running:
        target = status if status in {"stopped", "idle", "created"} else "stopped"
    if not target or target == desired:
        return record
    try:
        jobs_db.update_queue(queue_id, desired_state=target)
        refreshed = jobs_db.get_queue(queue_id)
        return refreshed or record
    except Exception:
        return record


def _maybe_auto_finalize(record: Dict[str, Any], live: Dict[str, Any]) -> Dict[str, Any]:
    queue_id = str(record.get("id") or "").strip()
    if not queue_id or not _should_auto_finalize(record, live):
        return record
    with _AUTO_FINALIZE_LOCK:
        if queue_id in _AUTO_FINALIZING:
            return record
        _AUTO_FINALIZING.add(queue_id)
    try:
        from cloud import promote_service

        report = promote_service.promote_queue_record(
            record,
            dry_run=False,
            limit=0,
            write_people=True,
        )
        meta_patch = {
            "last_promote": {
                "at": report.get("promoted_at"),
                "dry_run": False,
                "auto": True,
                "counts": report.get("counts") or {},
                "final_base": report.get("final_base") or "",
                "ok": bool(report.get("ok")),
            }
        }
        patch: Dict[str, Any] = {"last_error": "", "meta": meta_patch}
        if bool(report.get("ok")) and str(live.get("status") or "") == "completed":
            patch["desired_state"] = "completed"
        jobs_db.update_queue(queue_id, **patch)
        return jobs_db.get_queue(queue_id) or record
    except Exception as exc:
        try:
            jobs_db.update_queue(
                queue_id,
                meta={
                    "last_promote": {
                        "at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "dry_run": False,
                        "auto": True,
                        "ok": False,
                        "error": str(exc)[:500],
                    }
                },
            )
            return jobs_db.get_queue(queue_id) or record
        except Exception:
            return record
    finally:
        with _AUTO_FINALIZE_LOCK:
            _AUTO_FINALIZING.discard(queue_id)


def enrich_queue(
    record: Dict[str, Any],
    *,
    base_root: str = "",
    auto_finalize: bool = True,
) -> Dict[str, Any]:
    root = normalize_public_task_root(record.get("output_root"))
    base = normalize_public_task_root(base_root) or default_tasks_base_root()
    live = {
        "status": "created",
        "session_running": False,
        "manual_paused": False,
        "pid": 0,
        "profiles": 0,
        "images": 0,
        "metadata_ok": 0,
        "pending": 0,
        "review": 0,
        "failures": 0,
        "discovered": 0,
        "downloaded": 0,
        "completed": 0,
        "progress_text": "",
        "log_path": "",
        "output_path": root,
        "can_pause": False,
        "can_continue": False,
        "can_retry": False,
        "runtime_state": "",
        "updated_at_disk": "",
    }
    if root and os.path.isdir(root):
        try:
            bundle = _workspace_task_row(root, base)
            detail = bundle["detail"]
            selected = bundle["selected_task"]
            summary = selected or summarize_public_task(
                root,
                count_jsonl_rows_fn=lambda p: 0,
            )
            # Prefer workspace serialization when available
            if detail:
                live.update(
                    {
                        "session_running": bool(detail.get("session_running")),
                        "manual_paused": bool(detail.get("manual_paused")),
                        "pid": int(detail.get("pid") or 0) if bool(detail.get("session_running")) else 0,
                        "profiles": int(detail.get("profile_rows") or 0),
                        "images": int(detail.get("image_rows") or 0),
                        "metadata_ok": int(detail.get("metadata_rows") or 0),
                        "pending": len(detail.get("pending_rows") or []),
                        "review": int(detail.get("review_rows") or 0),
                        "failures": int(detail.get("failure_rows") or 0),
                        "discovered": int(detail.get("discovered_rows") or 0),
                        "downloaded": int(detail.get("downloaded_rows") or 0),
                        "completed": int(detail.get("completed_rows") or 0),
                        "progress_text": str(detail.get("progress_text") or ""),
                        "log_path": str(detail.get("log_path") or ""),
                        "output_path": str(detail.get("output_path") or root),
                        "can_pause": bool(detail.get("can_pause")),
                        "can_continue": bool(detail.get("can_continue")),
                        "can_retry": bool(detail.get("can_retry")),
                        "runtime_state": str(detail.get("runtime_state") or ""),
                        "updated_at_disk": str(detail.get("updated_at") or ""),
                        "status": _map_status_to_api(
                            str(detail.get("status") or summary.get("status") or ""),
                            session_running=bool(detail.get("session_running")),
                            manual_paused=bool(detail.get("manual_paused")),
                        ),
                    }
                )
            elif selected:
                live.update(
                    {
                        "session_running": bool(selected.get("session_running")),
                        "manual_paused": bool(selected.get("manual_paused")),
                        "pid": int(selected.get("pid") or 0) if bool(selected.get("session_running")) else 0,
                        "profiles": int(selected.get("profiles") or 0),
                        "images": int(selected.get("images") or 0),
                        "metadata_ok": int(selected.get("metadata_ok") or 0),
                        "pending": int(selected.get("pending") or 0),
                        "review": int(selected.get("review") or 0),
                        "failures": int(selected.get("failures") or 0),
                        "runtime_state": str(selected.get("runtime_state") or ""),
                        "updated_at_disk": str(selected.get("updated_at") or ""),
                        "status": _map_status_to_api(
                            str(selected.get("status") or ""),
                            session_running=bool(selected.get("session_running")),
                            manual_paused=bool(selected.get("manual_paused")),
                        ),
                    }
                )
        except Exception as exc:
            live["status"] = "error"
            live["progress_text"] = f"live status failed: {exc}"
    elif record.get("desired_state") == "created":
        live["status"] = "created"

    # If process is dead but disk status still "运行中", force stopped instead of fake running.
    if not live["session_running"] and live["status"] == "running":
        live["status"] = "stopped"
        live["pid"] = 0

    record = _reconcile_desired_state(record, live)
    if auto_finalize:
        record = _maybe_auto_finalize(record, live)

    last_promote = _last_promote_meta(record)
    can_finalize = (not live["session_running"]) and (
        live["status"] == "completed"
        or int(live["images"] or 0) > 0
        or int(live["profiles"] or 0) > 0
    )

    return {
        "id": record.get("id"),
        "name": record.get("name"),
        "template_id": record.get("template_id"),
        "template_path": record.get("template_path"),
        "start_url": record.get("start_url"),
        "output_root": root or record.get("output_root"),
        "speed_tier": record.get("speed_tier") or "safe",
        "speed_tier_reason": record.get("speed_tier_reason") or "",
        "desired_state": record.get("desired_state"),
        "last_error": record.get("last_error") or "",
        "notes": record.get("notes") or "",
        "meta": record.get("meta") or {},
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "kpi": {
            "discovered": live["discovered"],
            "downloaded": live["downloaded"],
            "completed": live["completed"],
            "profiles": live["profiles"],
            "images": live["images"],
            "metadata_ok": live["metadata_ok"],
            "pending": live["pending"],
            "review": live["review"],
            "failures": live["failures"],
        },
        "runtime": {
            "status": live["status"],
            "session_running": live["session_running"],
            "manual_paused": live["manual_paused"],
            "pid": live["pid"],
            "runtime_state": live["runtime_state"],
            "progress_text": live["progress_text"],
            "log_path": live["log_path"],
            "output_path": live["output_path"],
            "can_pause": live["can_pause"],
            "can_continue": live["can_continue"],
            "can_retry": live["can_retry"],
            "can_finalize": can_finalize,
            "promoted": bool(last_promote.get("ok")) and not bool(last_promote.get("dry_run")),
            "final_base": str(last_promote.get("final_base") or ""),
            "updated_at_disk": live["updated_at_disk"],
        },
    }


def list_enriched_queues(*, limit: int = 200, base_root: str = "", auto_finalize: bool = False) -> List[Dict[str, Any]]:
    # Reconcile desired_state for all rows; promote at most one eligible queue per list call
    # so polling can drain backlog without multi-minute hangs.
    rows = jobs_db.list_queues(limit=limit)
    out: List[Dict[str, Any]] = []
    promoted_one = False
    for row in rows:
        do_finalize = bool(auto_finalize) and not promoted_one
        before = _last_promote_meta(row)
        enriched = enrich_queue(row, base_root=base_root, auto_finalize=do_finalize)
        if do_finalize:
            after = ((enriched.get("meta") or {}).get("last_promote") or {}) if isinstance(enriched.get("meta"), dict) else {}
            if isinstance(after, dict) and after.get("auto") and after != before:
                promoted_one = True
        out.append(enriched)
    return out


def get_enriched_queue(
    queue_id: str,
    *,
    base_root: str = "",
    auto_finalize: bool = True,
) -> Optional[Dict[str, Any]]:
    row = jobs_db.get_queue(queue_id)
    if not row:
        return None
    return enrich_queue(row, base_root=base_root, auto_finalize=auto_finalize)


def create_queue(
    *,
    start_url: str = "",
    template_id: str = "",
    template_path: str = "",
    name: str = "",
    output_root: str = "",
    # Empty string = inherit template crawl.speed_tier; explicit value overrides it.
    speed_tier: str = "",
    speed_tier_reason: str = "",
    notes: str = "",
    start: bool = False,
    allow_turbo: bool = False,
) -> Dict[str, Any]:
    tid, tpath = resolve_template_path(template_id=template_id, template_path=template_path)
    url = str(start_url or "").strip()
    if not url and tpath:
        try:
            payload = json.loads(Path(tpath).read_text(encoding="utf-8"))
            urls = payload.get("start_urls") if isinstance(payload, dict) else []
            if isinstance(urls, list) and urls:
                url = str(urls[0] or "").strip()
        except Exception:
            pass
    if not url:
        raise ValueError("start_url is required (or template must include start_urls)")

    tier = normalize_speed_tier(speed_tier, default="safe")
    if tier == "turbo" and not allow_turbo:
        raise ValueError("turbo speed_tier requires allow_turbo=true (user confirm)")

    base_root = str(output_root or "").strip() or default_tasks_base_root()
    config_path, runtime_config = build_public_scraper_runtime_config(
        APP_FILE,
        url,
        base_root,
        template_path=tpath,
    )
    crawl = runtime_config.get("crawl") if isinstance(runtime_config.get("crawl"), dict) else {}
    # Inherit template tier only when caller omitted speed_tier entirely.
    if not str(speed_tier or "").strip() and crawl.get("speed_tier"):
        tier = normalize_speed_tier(crawl.get("speed_tier"), default="safe")
        if not str(speed_tier_reason or "").strip():
            speed_tier_reason = str(crawl.get("speed_tier_reason") or "")
    # 铁则: turbo (inherited or explicit) still needs an explicit user confirm.
    if tier == "turbo" and not allow_turbo:
        raise ValueError("turbo speed_tier requires allow_turbo=true (user confirm)")
    # overwrite_existing=False: keep template-tuned crawl numbers, only fill gaps.
    runtime_config["crawl"] = apply_speed_tier_to_crawl(
        crawl,
        tier,
        reason=speed_tier_reason,
        overwrite_existing=False,
    )
    resolved_root = normalize_public_task_root(runtime_config.get("output_root")) or normalize_public_task_root(base_root)
    runtime_config["output_root"] = resolved_root
    Path(config_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config_path).write_text(json.dumps(runtime_config, ensure_ascii=False, indent=2), encoding="utf-8")

    site_name = str(runtime_config.get("site_name") or tid or "queue").strip()
    display_name = str(name or "").strip() or site_name
    record = jobs_db.create_queue_record(
        name=display_name,
        template_id=tid,
        template_path=tpath,
        start_url=url,
        output_root=resolved_root,
        speed_tier=tier,
        speed_tier_reason=str(speed_tier_reason or runtime_config["crawl"].get("speed_tier_reason") or ""),
        notes=notes,
        meta={"config_path": config_path, "site_name": site_name},
    )

    if start:
        start_queue(record["id"])
        refreshed = jobs_db.get_queue(record["id"])
        if refreshed:
            record = refreshed
    return enrich_queue(record)


def _start_process_for_root(output_root: str) -> Dict[str, Any]:
    root = normalize_public_task_root(output_root)
    if not root:
        raise RuntimeError("invalid output_root")
    script_path = str(SCRAPER_SCRIPT)
    if not os.path.exists(script_path):
        raise RuntimeError(f"scraper script missing: {script_path}")
    config_path = os.path.join(root, "state", "runtime_config.json")
    if not os.path.exists(config_path):
        raise RuntimeError(f"runtime_config missing: {config_path}")

    with open(config_path, "r", encoding="utf-8") as handle:
        runtime_config = json.load(handle)
    if not isinstance(runtime_config, dict):
        raise RuntimeError("runtime_config is not an object")

    rules = runtime_config.get("rules") if isinstance(runtime_config.get("rules"), dict) else {}
    named_dir = resolve_named_images_dir(root, rules)
    log_path = os.path.join(root, "reports", "gui_public_scraper.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    set_public_scraper_manual_pause_flag(root, paused=False)

    with open(log_path, "a", encoding="utf-8") as log_handle:
        log_handle.write(build_public_scraper_log_banner("Cloud", time.strftime("%Y-%m-%d %H:%M:%S")))
        log_handle.flush()
        python_exec = resolve_python_cli_executable()
        cmd = build_public_scraper_command(python_exec, script_path, config_path, root)
        env = build_utf8_subprocess_env()
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(script_path) or ".",
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=creationflags,
        )

    # H3: daemon watcher wait()s the child (reaps Linux zombies) and records the exit
    # code, so session_running checks stop treating <defunct> pids as alive.
    register_process_watcher(proc)

    # Mirror into desktop registry so pause/continue/workspace stay consistent
    try:
        from services.desktop_scraper_backend_service import (
            _load_runtime_registry,
            _normalize_registry_entry,
            _save_runtime_registry,
        )

        registry = _load_runtime_registry()
        tasks = registry.get("tasks") if isinstance(registry.get("tasks"), dict) else {}
        tasks[root] = _normalize_registry_entry(
            {
                "pid": proc.pid,
                "config_path": config_path,
                "log_path": log_path,
                "named_dir": named_dir,
                "runtime_state": "运行中",
                "manual_paused": False,
                "active_template_path": str(rules.get("template_source_path") or ""),
                "last_exit_code": None,
                "updated_at_ts": time.time(),
                "started_at": time.time(),
            }
        )
        registry["tasks"] = tasks
        registry["active_root"] = root
        _save_runtime_registry(registry)
    except Exception:
        pass

    return {"pid": proc.pid, "log_path": log_path, "named_dir": named_dir, "config_path": config_path}


def start_queue(queue_id: str) -> Dict[str, Any]:
    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    root = normalize_public_task_root(record.get("output_root"))
    try:
        result = _start_process_for_root(root)
        jobs_db.update_queue(queue_id, desired_state="running", last_error="", meta={"last_start": result})
    except Exception as exc:
        jobs_db.update_queue(queue_id, desired_state="error", last_error=str(exc))
        raise
    return get_enriched_queue(queue_id) or {}


def finalize_queue(
    queue_id: str,
    *,
    dry_run: bool = False,
    limit: int = 0,
    write_people: bool = True,
) -> Dict[str, Any]:
    """Promote task images into 角色肖像 and write back people.sqlite."""
    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    from cloud import promote_service

    report = promote_service.promote_queue_record(
        record,
        dry_run=bool(dry_run),
        limit=int(limit or 0),
        write_people=bool(write_people),
    )
    meta_patch = {
        "last_promote": {
            "at": report.get("promoted_at"),
            "dry_run": bool(dry_run),
            "auto": False,
            "counts": report.get("counts") or {},
            "final_base": report.get("final_base") or "",
            "ok": bool(report.get("ok")),
        }
    }
    if not dry_run:
        # Manual finalize also marks desired_state completed when process is done.
        patch_kwargs: Dict[str, Any] = {"last_error": "", "meta": meta_patch}
        live_status = ""
        try:
            enriched_before = get_enriched_queue(queue_id, auto_finalize=False) or {}
            live_status = str((enriched_before.get("runtime") or {}).get("status") or "")
            if live_status == "completed":
                patch_kwargs["desired_state"] = "completed"
        except Exception:
            pass
        jobs_db.update_queue(queue_id, **patch_kwargs)
    enriched = get_enriched_queue(queue_id, auto_finalize=False) or {}
    return {"queue": enriched, "promote": report}


def control_queue(queue_id: str, action: str, *, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    root = normalize_public_task_root(record.get("output_root"))
    action_text = str(action or "").strip().lower()
    opts = options if isinstance(options, dict) else {}
    if action_text == "start":
        return start_queue(queue_id)
    if action_text in {"finalize", "promote"}:
        result = finalize_queue(
            queue_id,
            dry_run=bool(opts.get("dry_run")),
            limit=int(opts.get("limit") or 0),
            write_people=False if opts.get("write_people") in {False, "false", "0", 0} else True,
        )
        return result.get("queue") or get_enriched_queue(queue_id) or {}
    if action_text == "cancel":
        # best-effort: pause flag + mark cancelled (hard kill deferred)
        try:
            execute_scraper_control_action("pause", output_root=root, base_root=default_tasks_base_root())
        except Exception:
            set_public_scraper_manual_pause_flag(root, paused=True)
        jobs_db.update_queue(queue_id, desired_state="cancelled", last_error="")
        return get_enriched_queue(queue_id) or {}

    mapped = {
        "pause": "pause",
        "resume": "continue",
        "continue": "continue",
        "retry": "retry",
    }.get(action_text)
    if not mapped:
        raise ValueError(f"unsupported action: {action_text}")
    try:
        payload = execute_scraper_control_action(
            mapped,
            output_root=root,
            base_root=default_tasks_base_root(),
            options=opts or None,
        )
        desired = {
            "pause": "paused",
            "continue": "running",
            "retry": "running",
        }.get(mapped, action_text)
        jobs_db.update_queue(queue_id, desired_state=desired, last_error="", meta={"last_control": payload.get("message")})
    except Exception as exc:
        jobs_db.update_queue(queue_id, last_error=str(exc))
        raise
    return get_enriched_queue(queue_id) or {}


def queue_logs(queue_id: str, *, lines: int = 80) -> Dict[str, Any]:
    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    enriched = enrich_queue(record)
    log_path = str(enriched.get("runtime", {}).get("log_path") or "")
    text = ""
    if log_path and os.path.isfile(log_path):
        from services.scraper_monitor_service import read_text_tail

        text = read_text_tail(log_path, max_lines=max(20, int(lines or 80)))
    return {
        "id": queue_id,
        "log_path": log_path,
        "lines": max(20, int(lines or 80)),
        "tail": text or "",
    }


def queue_items(
    queue_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    status: str = "",
    progress_limit: int = 500,
) -> Dict[str, Any]:
    _record, root, items = _iter_queue_item_rows(queue_id, progress_limit=progress_limit)
    status_filter = str(status or "").strip().lower()
    if status_filter in {"failed", "fail", "pending"}:
        filtered = [row for row in items if row.get("bucket") == "pending"]
    elif status_filter in {"done", "completed", "ok"}:
        filtered = [row for row in items if row.get("bucket") == "done"]
    elif status_filter in {"image", "downloaded", "has_image"}:
        filtered = [row for row in items if row.get("has_preview")]
    else:
        filtered = items

    # Prefer rows with real images first for the default "all" browse view.
    if not status_filter:
        filtered = sorted(
            filtered,
            key=lambda row: (
                0 if row.get("has_preview") else 1,
                0 if row.get("bucket") == "done" else 1,
                str(row.get("name") or ""),
            ),
        )

    start = max(0, int(offset or 0))
    size = max(1, min(int(limit or 100), 2000))
    page = filtered[start : start + size]
    previewable = sum(1 for row in filtered if row.get("has_preview"))
    return {
        "id": queue_id,
        "output_root": root,
        "total": len(filtered),
        "offset": start,
        "limit": size,
        "previewable": previewable,
        "items": page,
        "counts": {
            "all": len(items),
            "done": sum(1 for row in items if row.get("bucket") == "done"),
            "pending": sum(1 for row in items if row.get("bucket") == "pending"),
            "previewable": sum(1 for row in items if row.get("has_preview")),
        },
    }


def resolve_queue_item_preview_path(queue_id: str, item_id: str, *, progress_limit: int = 500) -> str:
    item_key = str(item_id or "").strip()
    if not item_key:
        raise KeyError("item_id required")
    _record, _root, items = _iter_queue_item_rows(queue_id, progress_limit=progress_limit)
    for row in items:
        if str(row.get("id") or "") == item_key:
            path = _safe_existing_image(row.get("image_path"), output_root=_root)
            if not path:
                raise FileNotFoundError(f"preview not available: {item_key}")
            return path
    raise KeyError(f"item not found: {item_key}")


def _queue_total_items(output_root: str) -> int:
    """Cheap real entry count for one queue (jsonl line counts, no item loading)."""
    root = normalize_public_task_root(output_root)
    if not root:
        return 0
    try:
        return max(
            count_jsonl_rows(os.path.join(root, "raw", "list_records.jsonl")),
            count_jsonl_rows(os.path.join(root, "raw", "profiles.jsonl")),
        )
    except Exception:
        return 0


def library_list(
    *,
    limit: int = 60,
    offset: int = 0,
    status: str = "",
    q: str = "",
    queue_id: str = "",
    queue_limit: int = 40,
    per_queue_limit: int = 200,
    prefer_index: bool = True,
) -> Dict[str, Any]:
    """Cross-queue result browser over Cloud queue output roots (not full portrait disk).

    When the library_items index is populated, prefer SQL paging (no per-queue cap).
    Empty index falls back to the legacy file-scan path so first boot still works.
    """
    if prefer_index:
        try:
            from cloud import library_index

            indexed = library_index.library_list_from_index(
                limit=limit,
                offset=offset,
                status=status,
                q=q,
                queue_id=queue_id,
            )
            if indexed is not None:
                return indexed
        except Exception:
            pass
    status_filter = str(status or "").strip().lower()
    query = str(q or "").strip().lower()
    only_queue = str(queue_id or "").strip()
    per_limit = max(1, min(int(per_queue_limit or 200), 2000))
    progress_cap = max(200, per_limit)
    queue_cap = max(1, min(int(queue_limit or 40), 200))

    records = jobs_db.list_queues(limit=queue_cap)
    # F5: surface truncation instead of silently hiding rows beyond the caps.
    truncated = len(records) >= queue_cap

    aggregated: List[Dict[str, Any]] = []
    queue_summaries: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    for record in records:
        qid = str(record.get("id") or "").strip()
        if not qid:
            continue
        qname = str(record.get("name") or qid).strip() or qid
        try:
            payload = queue_items(
                qid,
                limit=per_limit,
                offset=0,
                status=status_filter if status_filter not in {"", "all"} else "",
                progress_limit=progress_cap,
            )
        except Exception as exc:
            errors.append({"queue_id": qid, "error": str(exc)})
            continue

        rows = list(payload.get("items") or [])
        previewable = sum(1 for row in rows if row.get("has_preview"))
        # Real per-queue total: jsonl line counts, with the loaded/filtered counts as
        # free lower bounds (covers queues whose raw jsonl is missing, e.g. archives).
        total_items = max(
            _queue_total_items(str(payload.get("output_root") or record.get("output_root") or "")),
            int(payload.get("total") or 0),
            len(rows),
        )
        queue_truncated = bool(
            int(payload.get("total") or 0) > len(rows) or total_items > progress_cap
        )
        queue_summaries.append(
            {
                "id": qid,
                "name": qname,
                "output_root": payload.get("output_root") or record.get("output_root") or "",
                "item_count": len(rows),
                "total_items": total_items,
                "truncated": queue_truncated,
                "previewable": previewable,
                "updated_at": record.get("updated_at"),
            }
        )
        # F6: only_queue narrows items only; summaries above always stay full so the
        # queue dropdown keeps every option after filtering.
        if only_queue and qid != only_queue:
            continue
        if queue_truncated:
            truncated = True
        for row in rows:
            item = dict(row)
            item_id = str(item.get("id") or "").strip()
            item["queue_id"] = qid
            item["queue_name"] = qname
            item["library_id"] = f"{qid}:{item_id}" if item_id else qid
            if item.get("has_preview") and item_id:
                item["preview_url"] = f"/api/v1/queues/{qid}/items/{item_id}/preview"
            else:
                item["preview_url"] = ""
            if query:
                hay = " ".join(
                    [
                        str(item.get("name") or ""),
                        str(item.get("reason") or ""),
                        str(item.get("detail_url") or ""),
                        str(item.get("image_path") or ""),
                        qname,
                        qid,
                    ]
                ).lower()
                if query not in hay:
                    continue
            aggregated.append(item)

    aggregated.sort(
        key=lambda row: (
            0 if row.get("has_preview") else 1,
            0 if row.get("bucket") == "done" else 1,
            str(row.get("queue_name") or ""),
            str(row.get("name") or ""),
        )
    )

    start = max(0, int(offset or 0))
    size = max(1, min(int(limit or 60), 500))
    page = aggregated[start : start + size]
    return {
        "total": len(aggregated),
        "offset": start,
        "limit": size,
        "previewable": sum(1 for row in aggregated if row.get("has_preview")),
        "queue_count": len(queue_summaries),
        "queues": queue_summaries,
        "items": page,
        "truncated": truncated,
        "per_queue_limit": per_limit,
        "filters": {
            "status": status_filter or "all",
            "q": str(q or "").strip(),
            "queue_id": only_queue,
        },
        "errors": errors[:20],
    }


def status_payload() -> Dict[str, Any]:
    # Keep status snappy: reconcile desired_state, but do not block on promote I/O.
    queues = list_enriched_queues(limit=500, auto_finalize=False)
    running = sum(
        1
        for q in queues
        if q.get("runtime", {}).get("session_running")
        or str(q.get("runtime", {}).get("status") or "") == "running"
    )
    paused = sum(1 for q in queues if q.get("runtime", {}).get("status") == "paused")
    completed = sum(1 for q in queues if q.get("runtime", {}).get("status") == "completed")
    promoted = sum(1 for q in queues if q.get("runtime", {}).get("promoted"))
    desired_running = sum(1 for q in queues if str(q.get("desired_state") or "") == "running")
    return {
        "ok": True,
        "product": "d2i-cloud",
        "version": "0.1.1",
        "tasks_root": default_tasks_base_root(),
        "queue_count": len(queues),
        "running": running,
        "paused": paused,
        "completed": completed,
        "promoted": promoted,
        "desired_running": desired_running,
        "scraper_script_exists": SCRAPER_SCRIPT.is_file(),
    }
