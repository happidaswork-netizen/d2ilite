# -*- coding: utf-8 -*-
"""D2I Cloud HTTP API (FastAPI)."""

from __future__ import annotations

import hmac
import os
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from cloud import coverage_service
from cloud import jobs_db
from cloud import queue_service
from cloud import vision_service
from cloud.paths import cloud_data_root, jobs_db_path

WEB_DIR = Path(__file__).resolve().parent / "web"


def _auth_enabled() -> bool:
    return bool(str(os.environ.get("D2I_WEB_TOKEN", "") or "").strip())


def require_auth(authorization: Optional[str] = Header(default=None)) -> None:
    """Bearer gate for the whole /api/v1 router; no-op when D2I_WEB_TOKEN unset (local dev)."""
    token = str(os.environ.get("D2I_WEB_TOKEN", "") or "").strip()
    if not token:
        return
    raw = str(authorization or "").strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    # Constant-time compare: plain == leaks match length via timing.
    if not hmac.compare_digest(raw.encode("utf-8"), token.encode("utf-8")):
        raise HTTPException(status_code=401, detail="unauthorized")


class CreateQueueBody(BaseModel):
    start_url: str = ""
    template_id: str = ""
    template_path: str = ""
    name: str = ""
    output_root: str = ""
    # Empty = inherit the template's crawl.speed_tier (final default stays safe).
    speed_tier: str = ""
    speed_tier_reason: str = ""
    notes: str = ""
    start: bool = False
    allow_turbo: bool = False


class ControlBody(BaseModel):
    options: Dict[str, Any] = Field(default_factory=dict)


class FinalizeBody(BaseModel):
    dry_run: bool = False
    limit: int = 0
    write_people: bool = True


class VisionClassifyBody(BaseModel):
    path: str = ""
    paths: list[str] = Field(default_factory=list)
    person_id: str = ""
    person_ids: list[str] = Field(default_factory=list)
    name: str = ""
    names: list[str] = Field(default_factory=list)
    primary_image_path: str = ""
    write_people: bool = True
    dry_run: bool = False
    force: bool = False


class VisionEnqueueBody(BaseModel):
    batch_size: int = 40
    province: str = ""
    city: str = ""
    unit_like: str = ""
    limit: int = 20000
    max_batches: int = 0
    force: bool = False
    write_people: bool = True
    dry_run: bool = False
    start: bool = False
    max_running: int = 1


class VisionPumpBody(BaseModel):
    max_running: int = 1
    max_claim: int = 0
    # True: daemon thread drain (full backfill). False: sync (smoke / one batch).
    background: bool = False


class VisionRequeueBody(BaseModel):
    job_id: str = ""
    batch_size: int = 20
    start: bool = False
    max_running: int = 1


class QueueVisionBody(BaseModel):
    dry_run: bool = False
    limit: int = 0
    force: bool = False
    write_people: bool = True
    names: list[str] = Field(default_factory=list)
    steps: list[str] = Field(default_factory=lambda: ["vision"])
    auto: bool = False


class QueueVisionRecrawlBody(BaseModel):
    include_review: bool = False
    # Materialize a draft queue only when confirm=true; never auto-start.
    confirm: bool = False
    create_draft: bool = False
    name: str = ""
    notes: str = ""


class CoverageEnqueueBody(BaseModel):
    action: str
    confirm: bool = False
    start_url: str = ""
    template_id: str = ""
    template_path: str = ""
    speed_tier: str = "safe"
    start: bool = False
    notes: str = ""


def create_app() -> FastAPI:
    jobs_db.init_db()
    app = FastAPI(title="D2I Cloud", version="0.1.0", description="NAS queue API for d2ilite")
    app.add_middleware(
        CORSMiddleware,
        # Converged origins: public console + local dev shells; wildcard with
        # credentials was an open door for any site to ride the Bearer token.
        allow_origins=[
            "https://d2i.517411.xyz",
            "http://127.0.0.1:8787",
            "http://localhost:8787",
            "http://127.0.0.1:8791",
            "http://localhost:8791",
            "http://192.168.5.36:8787",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # All /api/v1 routes behind one Bearer gate (except health, re-attached below).
    api = APIRouter(prefix="/api/v1", dependencies=[Depends(require_auth)])

    @app.middleware("http")
    async def _static_no_cache(request: Request, call_next):
        # Releases swap JS/CSS in place; force revalidation so the UI never runs stale scripts.
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/static/") or path in {"/", "/queues", "/coverage", "/library", "/vision"}:
            response.headers["Cache-Control"] = "no-cache"
        return response

    @app.on_event("startup")
    def _start_reconciler() -> None:
        interval = float(os.environ.get("D2I_RECONCILE_SECONDS", "30") or 30)
        queue_service.start_background_reconciler(interval_seconds=interval)

    if WEB_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

        @app.get("/")
        def index_page() -> FileResponse:
            return FileResponse(WEB_DIR / "index.html")

        @app.get("/queues")
        def queues_page() -> FileResponse:
            return FileResponse(WEB_DIR / "index.html")

        @app.get("/coverage")
        def coverage_page() -> FileResponse:
            return FileResponse(WEB_DIR / "coverage.html")

        @app.get("/library")
        def library_page() -> FileResponse:
            return FileResponse(WEB_DIR / "library.html")

        @app.get("/vision")
        def vision_page() -> FileResponse:
            return FileResponse(WEB_DIR / "vision.html")

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"ok": True, "service": "d2i-cloud"}

    @app.get("/api/v1/health")
    def api_health() -> Dict[str, Any]:
        # Health stays open: the web shell probes it before any token is configured.
        return {"ok": True, "service": "d2i-cloud"}

    @api.get("/status")
    def status() -> Dict[str, Any]:
        payload = queue_service.status_payload()
        payload["data_root"] = str(cloud_data_root())
        payload["jobs_db"] = str(jobs_db_path())
        payload["auth_enabled"] = _auth_enabled()
        try:
            payload["vision"] = vision_service.status_payload()
        except Exception as exc:
            payload["vision"] = {"ok": False, "error": str(exc)}
        return payload

    @api.get("/templates")
    def templates() -> Dict[str, Any]:
        items = queue_service.list_template_files()
        return {"count": len(items), "templates": items}

    @api.get("/queues")
    def list_queues(
        limit: int = Query(default=200, ge=1, le=1000),
        # Promotion now runs in the background reconciler; reads stay pure by default.
        auto_finalize: bool = Query(default=False),
    ) -> Dict[str, Any]:
        rows = queue_service.list_enriched_queues(limit=limit, auto_finalize=bool(auto_finalize))
        return {"count": len(rows), "queues": rows}

    @api.post("/queues")
    def create_queue(body: CreateQueueBody) -> Dict[str, Any]:
        try:
            queue = queue_service.create_queue(
                start_url=body.start_url,
                template_id=body.template_id,
                template_path=body.template_path,
                name=body.name,
                output_root=body.output_root,
                speed_tier=body.speed_tier,
                speed_tier_reason=body.speed_tier_reason,
                notes=body.notes,
                start=body.start,
                allow_turbo=body.allow_turbo,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": True, "queue": queue}

    @api.get("/queues/{queue_id}")
    def get_queue(queue_id: str) -> Dict[str, Any]:
        queue = queue_service.get_enriched_queue(queue_id, auto_finalize=False)
        if not queue:
            raise HTTPException(status_code=404, detail=f"queue not found: {queue_id}")
        return {"queue": queue}

    def _control(queue_id: str, action: str, body: Optional[ControlBody] = None) -> Dict[str, Any]:
        try:
            queue = queue_service.control_queue(
                queue_id,
                action,
                options=(body.options if body else None),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": True, "action": action, "queue": queue}

    @api.post("/queues/{queue_id}/start")
    def start_queue(queue_id: str) -> Dict[str, Any]:
        return _control(queue_id, "start")

    @api.post("/queues/{queue_id}/pause")
    def pause_queue(queue_id: str) -> Dict[str, Any]:
        return _control(queue_id, "pause")

    @api.post("/queues/{queue_id}/resume")
    def resume_queue(queue_id: str, body: Optional[ControlBody] = None) -> Dict[str, Any]:
        return _control(queue_id, "resume", body)

    @api.post("/queues/{queue_id}/retry")
    def retry_queue(queue_id: str, body: Optional[ControlBody] = None) -> Dict[str, Any]:
        return _control(queue_id, "retry", body)

    @api.post("/queues/{queue_id}/cancel")
    def cancel_queue(queue_id: str) -> Dict[str, Any]:
        return _control(queue_id, "cancel")

    @api.post("/queues/{queue_id}/finalize")
    def finalize_queue(queue_id: str, body: Optional[FinalizeBody] = None) -> Dict[str, Any]:
        payload = body or FinalizeBody()
        try:
            result = queue_service.finalize_queue(
                queue_id,
                dry_run=bool(payload.dry_run),
                limit=int(payload.limit or 0),
                write_people=bool(payload.write_people),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": True, "action": "finalize", **result}

    @api.get("/queues/{queue_id}/logs")
    def queue_logs(queue_id: str, lines: int = Query(default=80, ge=20, le=2000)) -> Dict[str, Any]:
        try:
            return queue_service.queue_logs(queue_id, lines=lines)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @api.get("/queues/{queue_id}/items")
    def queue_items(
        queue_id: str,
        limit: int = Query(default=100, ge=1, le=2000),
        offset: int = Query(default=0, ge=0),
        status: str = Query(default=""),
    ) -> Dict[str, Any]:
        try:
            return queue_service.queue_items(
                queue_id,
                limit=limit,
                offset=offset,
                status=status,
                progress_limit=max(limit + offset, 500),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @api.get("/queues/{queue_id}/items/{item_id}")
    def queue_item_detail(queue_id: str, item_id: str) -> Dict[str, Any]:
        try:
            payload = queue_service.queue_items(queue_id, limit=2000, offset=0, status="", progress_limit=2000)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        for row in payload.get("items") or []:
            if str(row.get("id") or "") == str(item_id or "").strip():
                return {"queue_id": queue_id, "item": row}
        raise HTTPException(status_code=404, detail=f"item not found: {item_id}")

    @api.get("/queues/{queue_id}/items/{item_id}/preview")
    def queue_item_preview(queue_id: str, item_id: str) -> FileResponse:
        try:
            path = queue_service.resolve_queue_item_preview_path(queue_id, item_id, progress_limit=2000)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        suffix = Path(path).suffix.lower()
        media = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".webp": "image/webp",
            ".gif": "image/gif",
            ".bmp": "image/bmp",
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
        }.get(suffix, "application/octet-stream")
        return FileResponse(
            path,
            media_type=media,
            filename=Path(path).name,
            headers={"Cache-Control": "private, max-age=120"},
        )

    @api.get("/library")
    def library(
        limit: int = Query(default=60, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        status: str = Query(default=""),
        q: str = Query(default=""),
        queue_id: str = Query(default=""),
        queue_limit: int = Query(default=40, ge=1, le=200),
        # F5: caller-tunable per-queue cap; hard ceiling 1000 to keep NAS I/O sane.
        per_queue_limit: int = Query(default=200, ge=1, le=1000),
    ) -> Dict[str, Any]:
        return queue_service.library_list(
            limit=limit,
            offset=offset,
            status=status,
            q=q,
            queue_id=queue_id,
            queue_limit=queue_limit,
            per_queue_limit=per_queue_limit,
        )

    @api.post("/library/reindex")
    def library_reindex(queue_id: str = Query(default="")) -> Dict[str, Any]:
        """Rebuild the library_items SQLite index from workspace progress files."""
        from cloud import library_index

        qid = str(queue_id or "").strip()
        if qid:
            return library_index.index_queue(qid)
        return library_index.index_all_queues()

    # --- Vision / AI (Grok 4.5 OpenAI-compatible) ---

    @api.get("/ai/vision/status")
    def vision_status() -> Dict[str, Any]:
        payload = vision_service.status_payload()
        try:
            payload["jobs"] = jobs_db.vision_job_counts()
        except Exception as exc:
            payload["jobs_error"] = str(exc)
        try:
            payload["pump"] = vision_service.pump_state()
        except Exception as exc:
            payload["pump_error"] = str(exc)
        return payload

    @api.get("/ai/vision/inventory")
    def vision_inventory(
        province: str = Query(default=""),
        city: str = Query(default=""),
        unit_like: str = Query(default=""),
        limit: int = Query(default=5000, ge=1, le=20000),
        only_resolvable: bool = Query(default=False),
    ) -> Dict[str, Any]:
        try:
            return vision_service.inventory_unvisioned(
                province=province,
                city=city,
                unit_like=unit_like,
                limit=limit,
                only_resolvable=only_resolvable,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.get("/ai/vision/plan")
    def vision_plan(
        batch_size: int = Query(default=40, ge=5, le=200),
        province: str = Query(default=""),
        city: str = Query(default=""),
        unit_like: str = Query(default=""),
        limit: int = Query(default=20000, ge=1, le=20000),
    ) -> Dict[str, Any]:
        try:
            return vision_service.plan_vision_batches(
                batch_size=batch_size,
                province=province,
                city=city,
                unit_like=unit_like,
                limit=limit,
                only_resolvable=True,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.post("/ai/vision/enqueue")
    def vision_enqueue(body: Optional[VisionEnqueueBody] = None) -> Dict[str, Any]:
        payload = body or VisionEnqueueBody()
        try:
            return vision_service.enqueue_unvisioned_batches(
                batch_size=int(payload.batch_size or 40),
                province=str(payload.province or ""),
                city=str(payload.city or ""),
                unit_like=str(payload.unit_like or ""),
                limit=int(payload.limit or 20000),
                max_batches=int(payload.max_batches or 0),
                force=bool(payload.force),
                write_people=bool(payload.write_people),
                dry_run=bool(payload.dry_run),
                start=bool(payload.start),
                max_running=int(payload.max_running or 1),
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.get("/ai/vision/jobs")
    def vision_jobs_list(
        limit: int = Query(default=100, ge=1, le=500),
        status: str = Query(default=""),
    ) -> Dict[str, Any]:
        rows = jobs_db.list_vision_jobs(limit=limit, status=status)
        # drop bulky items from list view
        slim = []
        for row in rows:
            item = dict(row)
            items = item.pop("items", None)
            item["item_count"] = len(items or [])
            slim.append(item)
        return {"ok": True, "jobs": slim, "counts": jobs_db.vision_job_counts()}

    @api.get("/ai/vision/jobs/{job_id}")
    def vision_job_get(job_id: str) -> Dict[str, Any]:
        job = jobs_db.get_vision_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"vision job not found: {job_id}")
        try:
            job = vision_service.enrich_vision_job(job)
        except Exception:
            pass
        return {"ok": True, "job": job}

    @api.post("/ai/vision/jobs/{job_id}/run")
    def vision_job_run(job_id: str) -> Dict[str, Any]:
        try:
            return vision_service.run_vision_job(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.post("/ai/vision/jobs/{job_id}/cancel")
    def vision_job_cancel(job_id: str) -> Dict[str, Any]:
        job = jobs_db.cancel_vision_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"vision job not found: {job_id}")
        return {"ok": True, "job": job}

    @api.post("/ai/vision/pump")
    def vision_pump(body: Optional[VisionPumpBody] = None) -> Dict[str, Any]:
        payload = body or VisionPumpBody()
        try:
            return vision_service.pump_vision_jobs(
                max_running=int(payload.max_running or 1),
                max_claim=int(payload.max_claim or 0),
                background=bool(payload.background),
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.post("/ai/vision/requeue-failed")
    def vision_requeue_failed(body: Optional[VisionRequeueBody] = None) -> Dict[str, Any]:
        payload = body or VisionRequeueBody()
        try:
            return vision_service.requeue_failed_vision_items(
                job_id=str(payload.job_id or ""),
                batch_size=int(payload.batch_size or 20),
                start=bool(payload.start),
                max_running=int(payload.max_running or 1),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.post("/ai/vision/normalize-status")
    def vision_normalize_status() -> Dict[str, Any]:
        try:
            out = jobs_db.normalize_vision_job_statuses()
            out["counts"] = jobs_db.vision_job_counts()
            return out
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @api.post("/ai/vision")
    def vision_classify(body: VisionClassifyBody) -> Dict[str, Any]:
        paths = [p for p in (body.paths or []) if str(p or "").strip()]
        single = str(body.path or body.primary_image_path or "").strip()
        if single and single not in paths:
            paths = [single, *paths]
        if not paths:
            raise HTTPException(status_code=400, detail="path or paths required")
        if len(paths) == 1 and not body.names and not body.person_ids:
            try:
                result = vision_service.classify_and_write_person(
                    image_path=paths[0],
                    person_id=body.person_id,
                    name=body.name,
                    primary_image_path=body.primary_image_path or paths[0],
                    write_people=bool(body.write_people),
                    dry_run=bool(body.dry_run),
                    force=bool(body.force),
                )
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc
            return {"ok": bool(result.get("ok")), "action": "vision", **result}
        try:
            person_ids = list(body.person_ids or [])
            if body.person_id and not person_ids:
                person_ids = [body.person_id]
            report = vision_service.run_paths_vision(
                paths,
                dry_run=bool(body.dry_run),
                write_people=bool(body.write_people),
                force=bool(body.force),
                names=list(body.names or ([body.name] if body.name else [])),
                person_ids=person_ids,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": bool(report.get("ok")), "action": "vision", **report}

    @api.post("/queues/{queue_id}/ai/run")
    def queue_ai_run(queue_id: str, body: Optional[QueueVisionBody] = None) -> Dict[str, Any]:
        payload = body or QueueVisionBody()
        steps = [str(s or "").strip().lower() for s in (payload.steps or ["vision"]) if str(s or "").strip()]
        if not steps:
            steps = ["vision"]
        unsupported = [s for s in steps if s not in {"vision"}]
        if unsupported:
            raise HTTPException(
                status_code=400,
                detail=f"unsupported ai steps: {unsupported}; only vision is implemented",
            )
        try:
            report = vision_service.run_queue_vision(
                queue_id,
                dry_run=bool(payload.dry_run),
                limit=int(payload.limit or 0),
                force=bool(payload.force),
                write_people=bool(payload.write_people),
                names=list(payload.names or []) or None,
                auto=bool(payload.auto),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": bool(report.get("ok")), "action": "ai_run", "steps": steps, "vision": report}

    @api.get("/queues/{queue_id}/ai/vision/report")
    def queue_vision_report(queue_id: str) -> Dict[str, Any]:
        """Stage-2 vision report (漏图/错图/冲突) + recrawl plan snapshot."""
        try:
            report = vision_service.load_vision_report(queue_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not report:
            raise HTTPException(
                status_code=404,
                detail=f"vision report not found for queue {queue_id}; run POST .../ai/run first",
            )
        return {"ok": True, "action": "vision_report", "queue_id": queue_id, "report": report}

    @api.get("/queues/{queue_id}/ai/vision/recrawl-plan")
    def queue_vision_recrawl_plan(
        queue_id: str,
        include_review: bool = Query(default=False),
    ) -> Dict[str, Any]:
        """Problem-name plan for selective re-crawl. Never starts a queue."""
        try:
            plan = vision_service.build_recrawl_plan(queue_id, include_review=bool(include_review))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": bool(plan.get("ok")), "action": "vision_recrawl_plan", **plan}

    @api.post("/queues/{queue_id}/ai/vision/recrawl")
    def queue_vision_recrawl(
        queue_id: str,
        body: Optional[QueueVisionRecrawlBody] = None,
    ) -> Dict[str, Any]:
        """Build recrawl plan; optionally create a draft queue (start=false) after confirm."""
        payload = body or QueueVisionRecrawlBody()
        try:
            plan = vision_service.build_recrawl_plan(
                queue_id,
                include_review=bool(payload.include_review),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        out: Dict[str, Any] = {
            "ok": True,
            "action": "vision_recrawl",
            "plan": plan,
            "draft_queue": None,
            "created": False,
            "started": False,
            "policy": {
                "auto_recrawl": False,
                "requires_confirm": True,
                "start_forbidden_here": True,
            },
        }
        if not payload.create_draft:
            return out
        if not payload.confirm:
            raise HTTPException(
                status_code=400,
                detail="create_draft requires confirm=true; recrawl never auto-starts",
            )
        if int(plan.get("target_count") or 0) <= 0:
            out["ok"] = False
            out["error"] = "no_recrawl_targets"
            return out

        suggested = plan.get("suggested_create") if isinstance(plan.get("suggested_create"), dict) else {}
        template_id = str(suggested.get("template_id") or plan.get("template_id") or "").strip()
        if not template_id:
            raise HTTPException(status_code=400, detail="template_id missing on source queue / plan")
        names = [str(n) for n in (plan.get("names") or []) if str(n).strip()]
        notes_bits = [
            str(payload.notes or "").strip(),
            str(suggested.get("notes") or "").strip(),
            f"recrawl_names={','.join(names[:30])}",
        ]
        draft_name = str(payload.name or suggested.get("name") or f"vision-recrawl-{queue_id}").strip()
        try:
            created = queue_service.create_queue(
                template_id=template_id,
                name=draft_name,
                speed_tier=str(suggested.get("speed_tier") or "safe"),
                start=False,
                notes=" | ".join(b for b in notes_bits if b),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        # Stamp source + target names on draft meta for Hermes
        try:
            qid = str((created or {}).get("id") or (created or {}).get("queue_id") or "")
            if qid:
                jobs_db.update_queue(
                    qid,
                    meta={
                        "vision_recrawl": {
                            "source_queue_id": queue_id,
                            "names": names,
                            "detail_urls": list(plan.get("detail_urls") or []),
                            "include_review": bool(payload.include_review),
                            "must_recrawl_count": int(plan.get("must_recrawl_count") or 0),
                            "review_count": int(plan.get("review_count") or 0),
                            "auto_start_forbidden": True,
                        }
                    },
                )
                refreshed = jobs_db.get_queue(qid) or created
            else:
                refreshed = created
        except Exception:
            refreshed = created

        out["created"] = True
        out["draft_queue"] = refreshed
        out["message"] = "draft queue created with start=false; operator/Hermes must start explicitly"
        return out

    # --- Coverage graph (C0 tree/geo, C1 map series, C2 confirm enqueue) ---

    @api.get("/coverage/tree")
    def coverage_tree(domain: str = Query(default="")) -> Dict[str, Any]:
        return coverage_service.build_tree(domain=domain or None)

    @api.get("/coverage/geo-summary")
    def coverage_geo() -> Dict[str, Any]:
        return coverage_service.build_geo_summary()

    @api.get("/coverage/nodes/{node_id}")
    def coverage_node(node_id: str) -> Dict[str, Any]:
        detail = coverage_service.get_node(node_id)
        if not detail:
            raise HTTPException(status_code=404, detail=f"node not found: {node_id}")
        return detail

    @api.post("/coverage/nodes/{node_id}/scout")
    def coverage_scout(node_id: str) -> Dict[str, Any]:
        try:
            return coverage_service.scout_stub(node_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @api.post("/coverage/nodes/{node_id}/enqueue")
    def coverage_enqueue(node_id: str, body: CoverageEnqueueBody) -> Dict[str, Any]:
        try:
            return coverage_service.enqueue_from_node(
                node_id,
                action=body.action,
                confirm=body.confirm,
                start_url=body.start_url,
                template_id=body.template_id,
                template_path=body.template_path,
                speed_tier=body.speed_tier or "safe",
                start=body.start,
                notes=body.notes,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    app.include_router(api)
    return app


app = create_app()
