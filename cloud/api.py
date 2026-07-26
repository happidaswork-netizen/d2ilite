# -*- coding: utf-8 -*-
"""D2I Cloud HTTP API (FastAPI)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from cloud import coverage_service
from cloud import jobs_db
from cloud import queue_service
from cloud.paths import cloud_data_root, jobs_db_path

WEB_DIR = Path(__file__).resolve().parent / "web"


class CreateQueueBody(BaseModel):
    start_url: str = ""
    template_id: str = ""
    template_path: str = ""
    name: str = ""
    output_root: str = ""
    speed_tier: str = "safe"
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


class CoverageEnqueueBody(BaseModel):
    action: str
    confirm: bool = False
    start_url: str = ""
    template_id: str = ""
    template_path: str = ""
    speed_tier: str = "safe"
    start: bool = False
    notes: str = ""


def _auth_enabled() -> bool:
    return bool(str(os.environ.get("D2I_WEB_TOKEN", "") or "").strip())


def require_auth(authorization: Optional[str] = Header(default=None)) -> None:
    token = str(os.environ.get("D2I_WEB_TOKEN", "") or "").strip()
    if not token:
        return
    raw = str(authorization or "").strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    if raw != token:
        raise HTTPException(status_code=401, detail="unauthorized")


def create_app() -> FastAPI:
    jobs_db.init_db()
    app = FastAPI(title="D2I Cloud", version="0.1.0", description="NAS queue API for d2ilite")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

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

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"ok": True, "service": "d2i-cloud"}

    @app.get("/api/v1/health")
    def api_health() -> Dict[str, Any]:
        return {"ok": True, "service": "d2i-cloud"}

    @app.get("/api/v1/status", dependencies=[Depends(require_auth)])
    def status() -> Dict[str, Any]:
        payload = queue_service.status_payload()
        payload["data_root"] = str(cloud_data_root())
        payload["jobs_db"] = str(jobs_db_path())
        payload["auth_enabled"] = _auth_enabled()
        return payload

    @app.get("/api/v1/templates", dependencies=[Depends(require_auth)])
    def templates() -> Dict[str, Any]:
        items = queue_service.list_template_files()
        return {"count": len(items), "templates": items}

    @app.get("/api/v1/queues", dependencies=[Depends(require_auth)])
    def list_queues(
        limit: int = Query(default=200, ge=1, le=1000),
        auto_finalize: bool = Query(default=True),
    ) -> Dict[str, Any]:
        rows = queue_service.list_enriched_queues(limit=limit, auto_finalize=bool(auto_finalize))
        return {"count": len(rows), "queues": rows}

    @app.post("/api/v1/queues", dependencies=[Depends(require_auth)])
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

    @app.get("/api/v1/queues/{queue_id}", dependencies=[Depends(require_auth)])
    def get_queue(queue_id: str) -> Dict[str, Any]:
        queue = queue_service.get_enriched_queue(queue_id)
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

    @app.post("/api/v1/queues/{queue_id}/start", dependencies=[Depends(require_auth)])
    def start_queue(queue_id: str) -> Dict[str, Any]:
        return _control(queue_id, "start")

    @app.post("/api/v1/queues/{queue_id}/pause", dependencies=[Depends(require_auth)])
    def pause_queue(queue_id: str) -> Dict[str, Any]:
        return _control(queue_id, "pause")

    @app.post("/api/v1/queues/{queue_id}/resume", dependencies=[Depends(require_auth)])
    def resume_queue(queue_id: str, body: Optional[ControlBody] = None) -> Dict[str, Any]:
        return _control(queue_id, "resume", body)

    @app.post("/api/v1/queues/{queue_id}/retry", dependencies=[Depends(require_auth)])
    def retry_queue(queue_id: str, body: Optional[ControlBody] = None) -> Dict[str, Any]:
        return _control(queue_id, "retry", body)

    @app.post("/api/v1/queues/{queue_id}/cancel", dependencies=[Depends(require_auth)])
    def cancel_queue(queue_id: str) -> Dict[str, Any]:
        return _control(queue_id, "cancel")

    @app.post("/api/v1/queues/{queue_id}/finalize", dependencies=[Depends(require_auth)])
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

    @app.get("/api/v1/queues/{queue_id}/logs", dependencies=[Depends(require_auth)])
    def queue_logs(queue_id: str, lines: int = Query(default=80, ge=20, le=2000)) -> Dict[str, Any]:
        try:
            return queue_service.queue_logs(queue_id, lines=lines)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/v1/queues/{queue_id}/items", dependencies=[Depends(require_auth)])
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

    @app.get("/api/v1/queues/{queue_id}/items/{item_id}", dependencies=[Depends(require_auth)])
    def queue_item_detail(queue_id: str, item_id: str) -> Dict[str, Any]:
        try:
            payload = queue_service.queue_items(queue_id, limit=2000, offset=0, status="", progress_limit=2000)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        for row in payload.get("items") or []:
            if str(row.get("id") or "") == str(item_id or "").strip():
                return {"queue_id": queue_id, "item": row}
        raise HTTPException(status_code=404, detail=f"item not found: {item_id}")

    @app.get("/api/v1/queues/{queue_id}/items/{item_id}/preview", dependencies=[Depends(require_auth)])
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

    @app.get("/api/v1/library", dependencies=[Depends(require_auth)])
    def library(
        limit: int = Query(default=60, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        status: str = Query(default=""),
        q: str = Query(default=""),
        queue_id: str = Query(default=""),
        queue_limit: int = Query(default=40, ge=1, le=200),
    ) -> Dict[str, Any]:
        return queue_service.library_list(
            limit=limit,
            offset=offset,
            status=status,
            q=q,
            queue_id=queue_id,
            queue_limit=queue_limit,
            per_queue_limit=200,
        )

    # --- Coverage graph (C0 tree/geo, C1 map series, C2 confirm enqueue) ---

    @app.get("/api/v1/coverage/tree", dependencies=[Depends(require_auth)])
    def coverage_tree(domain: str = Query(default="")) -> Dict[str, Any]:
        return coverage_service.build_tree(domain=domain or None)

    @app.get("/api/v1/coverage/geo-summary", dependencies=[Depends(require_auth)])
    def coverage_geo() -> Dict[str, Any]:
        return coverage_service.build_geo_summary()

    @app.get("/api/v1/coverage/nodes/{node_id}", dependencies=[Depends(require_auth)])
    def coverage_node(node_id: str) -> Dict[str, Any]:
        detail = coverage_service.get_node(node_id)
        if not detail:
            raise HTTPException(status_code=404, detail=f"node not found: {node_id}")
        return detail

    @app.post("/api/v1/coverage/nodes/{node_id}/scout", dependencies=[Depends(require_auth)])
    def coverage_scout(node_id: str) -> Dict[str, Any]:
        try:
            return coverage_service.scout_stub(node_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/v1/coverage/nodes/{node_id}/enqueue", dependencies=[Depends(require_auth)])
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

    return app


app = create_app()
