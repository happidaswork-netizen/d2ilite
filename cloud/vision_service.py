# -*- coding: utf-8 -*-
"""Cloud-owned Grok vision: classify portraits into visual_* without touching source gender."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cloud.paths import default_tasks_base_root, people_db_path, portrait_root
from image_asset_safety import sha256_file
from llm_client import OpenAICompatibleClient
from visual_classifier import (
    PROMPT_VERSION,
    VisionRuntime,
    classify_image,
    normalize_visual_result,
    vision_runtime_available,
)

# people.sqlite visual columns we may write (only if present)
VISUAL_PEOPLE_COLS = (
    "person_count",
    "visual_gender",
    "visual_body_type",
    "visual_hairstyle",
    "visual_pose",
    "visual_age",
    "visual_attire",
    "visual_classification_source",
    "visual_classification_raw",
    "visual_warnings",
    "vision_verified",
    "vision_model",
    "vision_prompt_version",
    "vision_classified_at",
    "vision_image_sha256",
)

_HOST_PORTRAIT = Path("/vol1/1001/角色肖像")
_RUNTIME_PORTRAIT = Path("/runtime/portrait")


def _utc_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _norm(path: Any) -> str:
    return str(path or "").replace("\\", "/").strip()


# H5: 拦截语义与 cloud/queue_service._is_protected_path(预览路由防护)保持同步——
# 选角、repair_backup、__hdd_prebind 一律拒绝;vision 外呼另叠加"备份"通配,
# 因为这些图片会 base64 后发外部 Grok。改动任一处时请同步另一处。
_PROTECTED_VISION_MARKERS = ("选角", "备份")
_PROTECTED_VISION_MARKERS_LOWER = ("repair_backup", "__hdd_prebind")


def _is_protected_vision_path(path_value: Any) -> bool:
    """选角/备份/__hdd_prebind 永不允许经 vision 外呼离开 NAS。"""
    text = _norm(path_value)
    if not text:
        return False
    lowered = text.lower()
    if any(marker in text for marker in _PROTECTED_VISION_MARKERS):
        return True
    if any(marker in lowered for marker in _PROTECTED_VISION_MARKERS_LOWER):
        return True
    return False


def _vision_allowed_roots() -> List[Path]:
    """H5 白名单根:portrait 终落点根 + 任务输出根,其余一律不外呼。

    根的取法与 promote_service 落图一致:cloud.paths.portrait_root /
    default_tasks_base_root,再加 resolve_image_path 本就互映的宿主机/容器
    portrait 挂载点。
    """
    roots: List[Path] = [
        _HOST_PORTRAIT,
        _RUNTIME_PORTRAIT,
        Path("/vol3/1001/生成图片/角色肖像"),
    ]
    try:
        roots.append(portrait_root())
    except Exception:
        pass
    try:
        tasks_root = str(default_tasks_base_root() or "").strip()
        if tasks_root:
            roots.append(Path(tasks_root))
    except Exception:
        pass
    resolved: List[Path] = []
    for root in roots:
        try:
            resolved.append(root.resolve())
        except OSError:
            resolved.append(root)
    return resolved


def _under_any_root(path: Path, roots: List[Path]) -> bool:
    for root in roots:
        try:
            path.relative_to(root)
            return True
        except (ValueError, OSError):
            continue
    return False


def resolve_image_path(path_value: Any) -> Optional[Path]:
    """Map host/container portrait paths to a readable local file.

    H5: 只放行 portrait 终落点根 / 任务输出根内的真实路径(realpath 后校验,
    防 .. 与软链逃逸);含 选角/备份/__hdd_prebind 的路径直接拒绝。
    """
    raw = _norm(path_value)
    if not raw:
        return None
    if _is_protected_vision_path(raw):
        return None
    candidates: List[Path] = [Path(raw)]
    # Legacy scrape root → 角色肖像 (and 政府/ variant)
    if raw.startswith("/data/photos/originals/"):
        rel = raw[len("/data/photos/originals/") :]
        candidates.extend(
            [
                Path("/data/photos/originals") / rel,
                _HOST_PORTRAIT / rel,
                _HOST_PORTRAIT / "政府" / rel,
                Path("/vol3/1001/生成图片/角色肖像") / rel,
                Path("/vol3/1001/生成图片/角色肖像") / "政府" / rel,
                portrait_root() / rel,
                portrait_root() / "政府" / rel,
                _RUNTIME_PORTRAIT / rel,
                _RUNTIME_PORTRAIT / "政府" / rel,
            ]
        )
    if "山东公开官员" in raw:
        idx = raw.find("山东公开官员")
        rel = raw[idx + len("山东公开官员") :].lstrip("/")
        candidates.extend(
            [
                Path("/vol1/1001/山东公开官员") / rel,
                _HOST_PORTRAIT / rel,
                _HOST_PORTRAIT / "政府" / rel,
                portrait_root() / rel,
                portrait_root() / "政府" / rel,
            ]
        )
    if raw.startswith("/vol1/1001/角色肖像/"):
        rel = raw[len("/vol1/1001/角色肖像/") :]
        candidates.append(_RUNTIME_PORTRAIT / rel)
        candidates.append(portrait_root() / rel)
    if raw.startswith("/runtime/portrait/"):
        rel = raw[len("/runtime/portrait/") :]
        candidates.append(_HOST_PORTRAIT / rel)
        candidates.append(portrait_root() / rel)
    # bare relative under portrait root
    if not Path(raw).is_absolute():
        candidates.append(portrait_root() / raw)
        candidates.append(_HOST_PORTRAIT / raw)
        candidates.append(_HOST_PORTRAIT / "政府" / raw)
        candidates.append(_RUNTIME_PORTRAIT / raw)
    allowed_roots = _vision_allowed_roots()
    seen: set[str] = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        try:
            if not cand.is_file():
                continue
            # realpath 防 ../ 与软链逃逸,再做白名单/保护段校验
            resolved = cand.resolve()
        except OSError:
            continue
        if _is_protected_vision_path(resolved):
            continue
        if not _under_any_root(resolved, allowed_roots):
            continue
        return resolved
    return None


def status_payload() -> Dict[str, Any]:
    runtime = VisionRuntime.from_environment()
    key_ok = False
    try:
        key_ok = bool(runtime.api_key())
    except Exception:
        key_ok = False
    return {
        "ok": True,
        "enabled": bool(runtime.enabled),
        "available": bool(runtime.available()),
        "api_base": runtime.api_base,
        "model": runtime.model,
        "api_key_file": str(runtime.api_key_file),
        "api_key_file_exists": runtime.api_key_file.is_file(),
        "api_key_present": key_ok,
        "concurrency": runtime.concurrency,
        "timeout_seconds": runtime.timeout_seconds,
        "max_retries": runtime.max_retries,
        "prompt_version": PROMPT_VERSION,
        "fields": [
            "person_count",
            "visual_gender",
            "visual_body_type",
            "visual_hairstyle",
            "visual_pose",
        ],
    }


def _client_for(runtime: VisionRuntime) -> OpenAICompatibleClient:
    return OpenAICompatibleClient(
        api_base=runtime.api_base,
        api_key=runtime.api_key(),
        timeout_seconds=runtime.timeout_seconds,
        max_retries=runtime.max_retries,
    )


def classify_path(
    image_path: Path | str,
    *,
    expected_sha256: str = "",
    runtime: Optional[VisionRuntime] = None,
    client: Optional[OpenAICompatibleClient] = None,
) -> Dict[str, Any]:
    """Classify one image. Fail-open callers should catch exceptions."""
    path = Path(image_path)
    if not path.is_file():
        raise FileNotFoundError(f"image not found: {path}")
    rt = runtime or VisionRuntime.from_environment()
    if not rt.available():
        raise RuntimeError("vision_runtime_unavailable")
    cl = client or _client_for(rt)
    before = sha256_file(path)
    expected = str(expected_sha256 or "").strip().lower()
    if expected.startswith("sha256:"):
        expected = expected[7:]
    result = classify_image(
        path,
        expected_sha256=expected or before,
        runtime=rt,
        client=cl,
    )
    return {
        "ok": True,
        "path": str(path),
        "image_sha256": before,
        "model": rt.model,
        "prompt_version": PROMPT_VERSION,
        "result": result,
        "source": "vision_model",
        "classified_at": _utc_stamp(),
    }


def _open_people(db_path: Path) -> sqlite3.Connection:
    if not db_path.is_file():
        raise FileNotFoundError(f"people db not found: {db_path}")
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})")}


def _match_people_by_name(
    conn: sqlite3.Connection,
    cols: set[str],
    *,
    name: str,
    unit_name: str = "",
    city: str = "",
    province: str = "",
    select: str = "*",
) -> List[sqlite3.Row]:
    """M6: name 回退强制附加 unit/city/province 约束(有信息就带上)。

    最多取 2 行,让调用方区分"唯一命中"与"同名多行";绝不 ORDER BY rowid
    硬挑一行——同名官员写错人比不写更糟。
    """
    clauses = ["name=?"]
    params: List[Any] = [name]
    if unit_name and "unit_name" in cols:
        clauses.append("unit_name LIKE ?")
        params.append(f"%{unit_name}%")
    if city and "city" in cols:
        clauses.append("city=?")
        params.append(city)
    if province and "province" in cols:
        clauses.append("province=?")
        params.append(province)
    sql = (
        f"SELECT {select} FROM people WHERE {' AND '.join(clauses)} "
        "ORDER BY rowid DESC LIMIT 2"
    )
    return list(conn.execute(sql, tuple(params)))


def write_visual_to_people(
    conn: sqlite3.Connection,
    *,
    person_id: str = "",
    name: str = "",
    primary_image_path: str = "",
    result: Dict[str, Any],
    image_sha256: str = "",
    model: str = "",
    dry_run: bool = False,
    unit_name: str = "",
    city: str = "",
    province: str = "",
) -> Dict[str, Any]:
    """Write visual_* only. Never touches gender / source facts."""
    cols = _table_cols(conn, "people")
    row = None
    if person_id and "person_id" in cols:
        row = conn.execute("SELECT * FROM people WHERE person_id=?", (person_id,)).fetchone()
    if row is None and primary_image_path and "primary_image_path" in cols:
        row = conn.execute(
            "SELECT * FROM people WHERE primary_image_path=? ORDER BY rowid DESC LIMIT 1",
            (primary_image_path,),
        ).fetchone()
    if row is None and name:
        if primary_image_path and "primary_image_path" in cols:
            row = conn.execute(
                "SELECT * FROM people WHERE name=? AND primary_image_path=? ORDER BY rowid DESC LIMIT 1",
                (name, primary_image_path),
            ).fetchone()
        if row is None:
            # M6: name-only 回退带 unit/city 约束;约束后仍多行命中 → ambiguous,不写库
            matches = _match_people_by_name(
                conn, cols, name=name, unit_name=unit_name, city=city, province=province
            )
            if len(matches) == 1:
                row = matches[0]
            elif len(matches) > 1:
                return {
                    "action": "ambiguous",
                    "error": "ambiguous_name_match",
                    "name": name,
                    "person_id": person_id,
                    "candidates": len(matches),
                }
    if row is None:
        return {"action": "miss", "name": name, "person_id": person_id}

    pid = str(row["person_id"]) if "person_id" in row.keys() else person_id
    values: Dict[str, Any] = {}
    visual = result if isinstance(result, dict) else {}
    for key in (
        "person_count",
        "visual_gender",
        "visual_body_type",
        "visual_hairstyle",
        "visual_pose",
        "visual_age",
        "visual_attire",
    ):
        if key in cols and key in visual:
            values[key] = visual.get(key)
    if "visual_classification_source" in cols:
        values["visual_classification_source"] = "vision_model"
    if "visual_classification_raw" in cols:
        values["visual_classification_raw"] = json.dumps(visual, ensure_ascii=False)
    if "vision_verified" in cols:
        # vision is reference only; never mark human-verified
        values["vision_verified"] = 0
    if "vision_model" in cols and model:
        values["vision_model"] = model
    if "vision_prompt_version" in cols:
        values["vision_prompt_version"] = PROMPT_VERSION
    if "vision_classified_at" in cols:
        values["vision_classified_at"] = _utc_stamp()
    if "vision_image_sha256" in cols and image_sha256:
        values["vision_image_sha256"] = image_sha256
    if "last_checked_at" in cols:
        values["last_checked_at"] = _utc_stamp()

    if not values:
        return {
            "action": "no_visual_columns",
            "person_id": pid,
            "name": name or (row["name"] if "name" in row.keys() else ""),
        }
    if dry_run:
        return {
            "action": "dry_update",
            "person_id": pid,
            "name": name or (row["name"] if "name" in row.keys() else ""),
            "fields": sorted(values.keys()),
            "result": visual,
        }
    assigns = ", ".join(f"{k}=?" for k in values)
    conn.execute(f"UPDATE people SET {assigns} WHERE person_id=?", (*values.values(), pid))
    conn.commit()
    return {
        "action": "update",
        "person_id": pid,
        "name": name or (row["name"] if "name" in row.keys() else ""),
        "fields": sorted(values.keys()),
        "result": visual,
    }


def classify_and_write_person(
    *,
    image_path: str = "",
    person_id: str = "",
    name: str = "",
    primary_image_path: str = "",
    write_people: bool = True,
    dry_run: bool = False,
    force: bool = False,
    runtime: Optional[VisionRuntime] = None,
    client: Optional[OpenAICompatibleClient] = None,
    people_db: Optional[Path] = None,
    unit_name: str = "",
    city: str = "",
    province: str = "",
) -> Dict[str, Any]:
    path_hint = image_path or primary_image_path
    path = resolve_image_path(path_hint)
    if path is None:
        return {
            "ok": False,
            "error": "image_not_found",
            "path_hint": path_hint,
            "name": name,
            "person_id": person_id,
        }

    rt = runtime or VisionRuntime.from_environment()
    if not rt.available():
        return {
            "ok": False,
            "error": "vision_runtime_unavailable",
            "status": status_payload(),
            "path": str(path),
            "name": name,
            "person_id": person_id,
        }

    people_rec: Dict[str, Any] = {}
    db_path = Path(people_db) if people_db else people_db_path()
    if write_people and not force and db_path.is_file():
        # skip if visual_gender already filled (unless force).
        # F3: dry_run 同样走去重跳过——预览不该比真跑更费 Grok。
        try:
            conn = _open_people(db_path)
            try:
                cols = _table_cols(conn, "people")
                row = None
                if person_id and "person_id" in cols:
                    row = conn.execute(
                        "SELECT person_id, name, visual_gender, primary_image_path FROM people WHERE person_id=?",
                        (person_id,),
                    ).fetchone()
                if row is None and name and "visual_gender" in cols:
                    # M6: name 回退带 unit/city 约束;同名多行时不猜,交由写库层给 ambiguous
                    matches = _match_people_by_name(
                        conn,
                        cols,
                        name=name,
                        unit_name=unit_name,
                        city=city,
                        province=province,
                        select="person_id, name, visual_gender, primary_image_path",
                    )
                    row = matches[0] if len(matches) == 1 else None
                if row is not None and "visual_gender" in row.keys():
                    existing = str(row["visual_gender"] or "").strip()
                    if existing and existing not in {"", "不确定", "不适用"}:
                        return {
                            "ok": True,
                            "skipped": True,
                            "reason": "visual_gender_present",
                            "person_id": str(row["person_id"] or person_id),
                            "name": str(row["name"] or name),
                            "visual_gender": existing,
                            "path": str(path),
                        }
            finally:
                conn.close()
        except Exception:
            pass

    try:
        classified = classify_path(path, runtime=rt, client=client)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}:{exc}",
            "path": str(path),
            "name": name,
            "person_id": person_id,
            "fail_open": True,
        }

    if write_people:
        try:
            conn = _open_people(db_path)
            try:
                people_rec = write_visual_to_people(
                    conn,
                    person_id=person_id,
                    name=name,
                    primary_image_path=primary_image_path or _norm(path_hint),
                    result=classified.get("result") or {},
                    image_sha256=str(classified.get("image_sha256") or ""),
                    model=str(classified.get("model") or rt.model),
                    dry_run=dry_run,
                    unit_name=unit_name,
                    city=city,
                    province=province,
                )
            finally:
                conn.close()
        except Exception as exc:
            people_rec = {"action": "error", "error": f"{type(exc).__name__}:{exc}"}

    return {
        "ok": True,
        "path": str(path),
        "name": name or str(people_rec.get("name") or ""),
        "person_id": person_id or str(people_rec.get("person_id") or ""),
        "classification": classified,
        "people": people_rec,
        "dry_run": dry_run,
    }


def _candidate_rows_for_queue(queue_id: str, output_root: str) -> List[Dict[str, Any]]:
    """Build vision candidates from promote-style profile/download index + people paths."""
    from cloud import promote_service

    rows: List[Dict[str, Any]] = []
    try:
        candidates = promote_service.collect_promote_candidates(output_root)
    except Exception:
        candidates = []
    for item in candidates:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        local = str(
            item.get("src_path")
            or item.get("local_image_path")
            or item.get("src")
            or item.get("image_path")
            or ""
        ).strip()
        rows.append(
            {
                "name": name,
                "gender": str(item.get("gender") or ""),
                "detail_url": str(item.get("detail_url") or item.get("source_url") or ""),
                "local_image_path": local,
                "queue_id": queue_id,
            }
        )
    return rows


def _people_with_photos(
    *,
    names: Optional[List[str]] = None,
    unit_like: str = "",
    province: str = "",
    city: str = "",
    limit: int = 50,
    only_missing_visual: bool = True,
    people_db: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    db_path = Path(people_db) if people_db else people_db_path()
    if not db_path.is_file():
        return []
    conn = _open_people(db_path)
    try:
        cols = _table_cols(conn, "people")
        if "primary_image_path" not in cols or "name" not in cols:
            return []
        where = ["primary_image_path IS NOT NULL", "primary_image_path != ''"]
        params: List[Any] = []
        if only_missing_visual and "visual_gender" in cols:
            where.append("(visual_gender IS NULL OR visual_gender = '' OR visual_gender = '不确定')")
        if unit_like and "unit_name" in cols:
            where.append("unit_name LIKE ?")
            params.append(f"%{unit_like}%")
        if province and "province" in cols:
            where.append("province = ?")
            params.append(province)
        if city and "city" in cols:
            where.append("city = ?")
            params.append(city)
        if names:
            placeholders = ",".join("?" for _ in names)
            where.append(f"name IN ({placeholders})")
            params.extend(names)
        select_cols = ["person_id", "name", "gender", "unit_name", "primary_image_path"]
        for extra in (
            "visual_gender",
            "source_url",
            "source_page_image_status",
            "province",
            "city",
        ):
            if extra in cols:
                select_cols.append(extra)
        lim = max(1, min(int(limit or 50), 20000))
        sql = (
            f"SELECT {', '.join(select_cols)} FROM people WHERE {' AND '.join(where)} "
            "ORDER BY rowid DESC LIMIT ?"
        )
        params.append(lim)
        out: List[Dict[str, Any]] = []
        for row in conn.execute(sql, tuple(params)):
            out.append(dict(row))
        return out
    finally:
        conn.close()


def _lookup_people_row(
    *,
    name: str = "",
    person_id: str = "",
    unit_like: str = "",
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    db_path = Path(people_db) if people_db else people_db_path()
    if not db_path.is_file():
        return {}
    conn = _open_people(db_path)
    try:
        cols = _table_cols(conn, "people")
        select_cols = [c for c in (
            "person_id", "name", "gender", "unit_name", "primary_image_path",
            "visual_gender", "visual_body_type", "visual_pose", "visual_hairstyle",
            "source_url", "source_page_image_status", "person_count",
        ) if c in cols]
        if not select_cols:
            return {}
        row = None
        if person_id and "person_id" in cols:
            row = conn.execute(
                f"SELECT {', '.join(select_cols)} FROM people WHERE person_id=?",
                (person_id,),
            ).fetchone()
        if row is None and name:
            if unit_like and "unit_name" in cols:
                row = conn.execute(
                    f"SELECT {', '.join(select_cols)} FROM people "
                    "WHERE name=? AND unit_name LIKE ? ORDER BY rowid DESC LIMIT 1",
                    (name, f"%{unit_like}%"),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    f"SELECT {', '.join(select_cols)} FROM people WHERE name=? ORDER BY rowid DESC LIMIT 1",
                    (name,),
                ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}
    finally:
        conn.close()


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _issue_from_item(
    *,
    name: str,
    source_gender: str,
    detail_url: str,
    path_hint: str,
    item: Dict[str, Any],
    person_id: str = "",
) -> Dict[str, Any]:
    """Map one classify result into stage-2 issue codes for report / recrawl."""
    base = {
        "name": name,
        "person_id": person_id or str((item.get("people") or {}).get("person_id") or ""),
        "detail_url": detail_url,
        "path": path_hint or str(item.get("path") or ""),
        "source_gender": source_gender,
    }
    if item.get("skipped"):
        existing = str(item.get("visual_gender") or "").strip()
        return {
            **base,
            "severity": "ok",
            "codes": ["already_classified"],
            "reason": f"visual_gender already set: {existing}",
            "result": {"visual_gender": existing},
            "recrawl": False,
        }
    if not item.get("ok"):
        err = str(item.get("error") or "classify_failed")
        code = "image_not_found" if "image_not_found" in err or "image not found" in err.lower() else "classify_failed"
        return {
            **base,
            "severity": "must_recrawl",
            "codes": [code],
            "reason": err,
            "result": {},
            "recrawl": True,
            "fail_open": bool(item.get("fail_open")),
        }

    result = ((item.get("classification") or {}).get("result") or {})
    if not isinstance(result, dict):
        result = {}
    try:
        person_count = int(result.get("person_count"))
    except Exception:
        person_count = -1
    visual_gender = str(result.get("visual_gender") or "").strip()
    codes: List[str] = []
    severity = "ok"
    reason_parts: List[str] = []

    if person_count == 0 or visual_gender == "不适用":
        codes.append("no_person")
        severity = "must_recrawl"
        reason_parts.append("图中无可辨认人物")
    elif person_count >= 2 or visual_gender == "多人混合":
        codes.append("multi_person")
        severity = "must_recrawl"
        reason_parts.append(f"多人图 person_count={person_count}")
    elif visual_gender in {"", "不确定"}:
        codes.append("uncertain_visual")
        severity = "review"
        reason_parts.append("视觉性别不确定")

    src_g = str(source_gender or "").strip()
    if src_g in {"男", "女"} and visual_gender in {"男", "女"} and src_g != visual_gender:
        codes.append("gender_conflict")
        if severity == "ok":
            severity = "review"
        reason_parts.append(f"来源性别 {src_g} ≠ 视觉 {visual_gender}")

    if not codes:
        codes = ["ok"]
        reason_parts = ["视觉通过"]

    return {
        **base,
        "severity": severity,
        "codes": codes,
        "reason": "；".join(reason_parts),
        "result": result,
        "recrawl": severity == "must_recrawl",
        "people_action": str((item.get("people") or {}).get("action") or ""),
    }


def build_stage_roster(
    queue_id: str,
    *,
    names: Optional[List[str]] = None,
    limit: int = 0,
) -> Dict[str, Any]:
    """Stage-2 roster = promote candidates ∪ people photo rows for this queue unit."""
    from cloud import jobs_db, promote_service
    from services.task_service import normalize_public_task_root

    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    output_root = normalize_public_task_root(record.get("output_root")) or str(record.get("output_root") or "")
    ctx: Dict[str, str] = {}
    try:
        ctx = promote_service.resolve_admin_context(output_root)
    except Exception:
        ctx = {}
    unit = str(ctx.get("unit") or "")
    name_filter = {str(n).strip() for n in (names or []) if str(n).strip()}

    by_name: Dict[str, Dict[str, Any]] = {}
    for cand in _candidate_rows_for_queue(queue_id, output_root):
        name = str(cand.get("name") or "").strip()
        if not name:
            continue
        if name_filter and name not in name_filter:
            continue
        by_name[name] = {
            "name": name,
            "gender": str(cand.get("gender") or ""),
            "detail_url": str(cand.get("detail_url") or ""),
            "local_image_path": str(cand.get("local_image_path") or ""),
            "primary_image_path": "",
            "person_id": "",
            "source": "promote_candidate",
        }

    people_rows = _people_with_photos(
        names=list(name_filter) if name_filter else None,
        unit_like=unit or "",
        limit=int(limit or 500),
        only_missing_visual=False,
    )
    if not people_rows and name_filter:
        people_rows = _people_with_photos(
            names=list(name_filter),
            limit=int(limit or 500),
            only_missing_visual=False,
        )
    for row in people_rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        if name_filter and name not in name_filter:
            continue
        cur = by_name.get(name) or {
            "name": name,
            "gender": "",
            "detail_url": "",
            "local_image_path": "",
            "primary_image_path": "",
            "person_id": "",
            "source": "people",
        }
        cur["person_id"] = str(row.get("person_id") or cur.get("person_id") or "")
        cur["gender"] = str(row.get("gender") or cur.get("gender") or "")
        cur["primary_image_path"] = str(row.get("primary_image_path") or "")
        cur["detail_url"] = str(row.get("source_url") or cur.get("detail_url") or "")
        cur["visual_gender_existing"] = str(row.get("visual_gender") or "")
        cur["source_page_image_status"] = str(row.get("source_page_image_status") or "")
        cur["source"] = "people+candidate" if name in by_name else "people"
        by_name[name] = cur

    roster = list(by_name.values())
    roster.sort(key=lambda r: str(r.get("name") or ""))
    if limit and limit > 0:
        roster = roster[: int(limit)]
    return {
        "queue_id": queue_id,
        "output_root": output_root,
        "unit": unit,
        "admin": ctx,
        "template_id": str(record.get("template_id") or ""),
        "roster": roster,
        "record": record,
    }


def _classify_severity_bucket(issues: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets = {"must_recrawl": [], "review": [], "ok": []}
    for issue in issues:
        sev = str(issue.get("severity") or "ok")
        if sev not in buckets:
            sev = "review"
        buckets[sev].append(issue)
    return buckets


def build_recrawl_plan(
    queue_id: str,
    *,
    report: Optional[Dict[str, Any]] = None,
    include_review: bool = False,
) -> Dict[str, Any]:
    """Build a non-starting requeue plan from vision report for Hermes/AI."""
    from cloud import jobs_db

    record = jobs_db.get_queue(queue_id) or {}
    vision_report = report
    if vision_report is None:
        vision_report = load_vision_report(queue_id) or {}
    buckets = vision_report.get("buckets") if isinstance(vision_report.get("buckets"), dict) else {}
    must = list(buckets.get("must_recrawl") or [])
    review = list(buckets.get("review") or []) if include_review else []
    targets = must + review
    names = [str(x.get("name") or "") for x in targets if str(x.get("name") or "").strip()]
    detail_urls = [str(x.get("detail_url") or "") for x in targets if str(x.get("detail_url") or "").strip()]
    template_id = str(record.get("template_id") or vision_report.get("template_id") or "")
    plan = {
        "ok": True,
        "source_queue_id": queue_id,
        "at": _utc_stamp(),
        "template_id": template_id,
        "include_review": include_review,
        "must_recrawl_count": len(must),
        "review_count": len(review),
        "target_count": len(targets),
        "names": names,
        "detail_urls": detail_urls,
        "targets": targets,
        # Never auto-start. Hermes/operator confirms before create+start.
        "suggested_create": {
            "template_id": template_id,
            "name": f"vision-recrawl-{queue_id}-{datetime.now().strftime('%m%d%H%M')}",
            "speed_tier": "safe",
            "start": False,
            "notes": (
                f"stage2 vision recrawl from {queue_id}; "
                f"must={len(must)} review={len(review)}; names={','.join(names[:12])}"
            ),
            "auto_start_forbidden": True,
        },
        "policy": {
            "auto_recrawl": False,
            "default_targets": "must_recrawl_only",
            "review_needs_confirm": True,
        },
    }
    return plan


def load_vision_report(queue_id: str) -> Optional[Dict[str, Any]]:
    from cloud import jobs_db
    from services.task_service import normalize_public_task_root

    record = jobs_db.get_queue(queue_id)
    if not record:
        raise KeyError(f"queue not found: {queue_id}")
    root = normalize_public_task_root(record.get("output_root")) or str(record.get("output_root") or "")
    path = Path(root) / "reports" / "vision_report.json" if root else None
    if path and path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    last = meta.get("last_vision") if isinstance(meta.get("last_vision"), dict) else None
    return last


def run_queue_vision(
    queue_id: str,
    *,
    dry_run: bool = False,
    limit: int = 0,
    force: bool = False,
    write_people: bool = True,
    names: Optional[List[str]] = None,
    auto: bool = False,
) -> Dict[str, Any]:
    """Stage 2: classify queue portraits, write visual_*, emit vision report + recrawl plan."""
    from cloud import jobs_db
    from services.task_service import normalize_public_task_root

    stage = build_stage_roster(queue_id, names=names, limit=limit)
    record = stage["record"]
    output_root = stage["output_root"]
    unit = stage["unit"]
    roster = stage["roster"]
    runtime = VisionRuntime.from_environment()

    report: Dict[str, Any] = {
        "schema": "d2i-cloud-vision-report",
        "schema_version": 1,
        "stage": "vision",
        "ok": True,
        "queue_id": queue_id,
        "at": _utc_stamp(),
        "prompt_version": PROMPT_VERSION,
        "model": runtime.model,
        "available": runtime.available(),
        "dry_run": dry_run,
        "force": force,
        "auto": bool(auto),
        "unit": unit,
        "template_id": stage.get("template_id") or str(record.get("template_id") or ""),
        "output_root": output_root,
        "counts": {
            "roster": len(roster),
            "candidates": 0,
            "classified": 0,
            "skipped": 0,
            "failed": 0,
            "people_update": 0,
            "missing_image": 0,
            "must_recrawl": 0,
            "review": 0,
            "ok": 0,
            "cache_or_skip": 0,
        },
        "items": [],
        "issues": [],
        "buckets": {"must_recrawl": [], "review": [], "ok": []},
        "recrawl_plan": {},
    }

    # First pass: missing images (漏图) without calling the model
    to_classify: List[Dict[str, Any]] = []
    issues: List[Dict[str, Any]] = []
    for row in roster:
        name = str(row.get("name") or "").strip()
        path_hint = str(row.get("primary_image_path") or row.get("local_image_path") or "").strip()
        resolved = resolve_image_path(path_hint) if path_hint else None
        source_gender = str(row.get("gender") or "")
        detail_url = str(row.get("detail_url") or "")
        person_id = str(row.get("person_id") or "")
        page_status = str(row.get("source_page_image_status") or "")
        if resolved is None:
            code = "missing_image"
            if page_status == "no_photo" or not path_hint:
                code = "missing_image"
            else:
                code = "image_not_on_disk"
            issue = {
                "name": name,
                "person_id": person_id,
                "detail_url": detail_url,
                "path": path_hint,
                "source_gender": source_gender,
                "severity": "must_recrawl",
                "codes": [code],
                "reason": "下载/落盘阶段无可用肖像" if code == "missing_image" else f"路径不可读: {path_hint}",
                "result": {},
                "recrawl": True,
            }
            issues.append(issue)
            report["counts"]["missing_image"] += 1
            report["items"].append(
                {
                    "ok": False,
                    "name": name,
                    "person_id": person_id,
                    "error": code,
                    "path_hint": path_hint,
                    "issue": issue,
                }
            )
            continue
        to_classify.append({**row, "resolved_path": str(resolved), "path_hint": path_hint})

    report["counts"]["candidates"] = len(to_classify)

    if not runtime.available():
        # Still emit report for missing images; classification unavailable.
        report["ok"] = False
        report["error"] = "vision_runtime_unavailable"
        report["status"] = status_payload()
        for row in to_classify:
            issue = {
                "name": str(row.get("name") or ""),
                "person_id": str(row.get("person_id") or ""),
                "detail_url": str(row.get("detail_url") or ""),
                "path": str(row.get("path_hint") or ""),
                "source_gender": str(row.get("gender") or ""),
                "severity": "review",
                "codes": ["vision_unavailable"],
                "reason": "视觉运行时不可用，已有图待分类",
                "result": {},
                "recrawl": False,
            }
            issues.append(issue)
            report["items"].append({"ok": False, "name": row.get("name"), "error": "vision_runtime_unavailable", "issue": issue})
    else:
        client = _client_for(runtime)
        lock = threading.Lock()

        def _one(row: Dict[str, Any]) -> Dict[str, Any]:
            return classify_and_write_person(
                image_path=str(row.get("resolved_path") or row.get("path_hint") or ""),
                person_id=str(row.get("person_id") or ""),
                name=str(row.get("name") or ""),
                primary_image_path=str(row.get("primary_image_path") or row.get("path_hint") or ""),
                write_people=write_people,
                dry_run=dry_run,
                force=force,
                runtime=runtime,
                client=client,
                # M6: 队列是按单位建的,name 回退写库时带上单位约束
                unit_name=unit,
            )

        workers = max(1, min(runtime.concurrency, len(to_classify) or 1))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, row): row for row in to_classify}
            for fut in as_completed(futures):
                row = futures[fut]
                try:
                    item = fut.result()
                except Exception as exc:
                    item = {
                        "ok": False,
                        "error": f"{type(exc).__name__}:{exc}",
                        "name": row.get("name"),
                        "fail_open": True,
                    }
                issue = _issue_from_item(
                    name=str(row.get("name") or ""),
                    source_gender=str(row.get("gender") or ""),
                    detail_url=str(row.get("detail_url") or ""),
                    path_hint=str(row.get("path_hint") or ""),
                    item=item,
                    person_id=str(row.get("person_id") or ""),
                )
                item["issue"] = issue
                with lock:
                    report["items"].append(item)
                    issues.append(issue)
                    if item.get("skipped"):
                        report["counts"]["skipped"] += 1
                        report["counts"]["cache_or_skip"] += 1
                    elif item.get("ok"):
                        report["counts"]["classified"] += 1
                        action = str((item.get("people") or {}).get("action") or "")
                        if action == "update":
                            report["counts"]["people_update"] += 1
                    else:
                        report["counts"]["failed"] += 1

    buckets = _classify_severity_bucket(issues)
    report["issues"] = issues
    report["buckets"] = buckets
    report["counts"]["must_recrawl"] = len(buckets["must_recrawl"])
    report["counts"]["review"] = len(buckets["review"])
    report["counts"]["ok"] = len(buckets["ok"])
    report["recrawl_plan"] = build_recrawl_plan(queue_id, report=report, include_review=False)
    report["ok"] = report["counts"]["failed"] == 0 or report["counts"]["classified"] > 0 or report["counts"]["missing_image"] > 0

    # Persist under task reports/ (stage artifact)
    if output_root and not dry_run:
        try:
            report_dir = Path(normalize_public_task_root(output_root) or output_root) / "reports"
            _atomic_write_json(report_dir / "vision_report.json", report)
            _atomic_write_json(
                report_dir / "vision_recrawl_candidates.json",
                {
                    "queue_id": queue_id,
                    "at": report["at"],
                    "must_recrawl": buckets["must_recrawl"],
                    "review": buckets["review"],
                    "recrawl_plan": report["recrawl_plan"],
                },
            )
            report["report_path"] = str(report_dir / "vision_report.json")
            report["recrawl_path"] = str(report_dir / "vision_recrawl_candidates.json")
        except Exception as exc:
            report["report_write_error"] = str(exc)

    # queue meta summary for UI / Hermes
    try:
        jobs_db.update_queue(
            queue_id,
            meta={
                "last_vision": {
                    "at": report["at"],
                    "dry_run": dry_run,
                    "force": force,
                    "auto": bool(auto),
                    "counts": report["counts"],
                    "model": runtime.model,
                    "prompt_version": PROMPT_VERSION,
                    "ok": bool(report.get("ok")),
                    "must_recrawl": [x.get("name") for x in buckets["must_recrawl"]],
                    "review": [x.get("name") for x in buckets["review"]],
                    "report_path": report.get("report_path") or "",
                    "recrawl_path": report.get("recrawl_path") or "",
                }
            },
        )
    except Exception as exc:
        report["meta_error"] = str(exc)

    return report


def run_paths_vision(
    paths: List[str],
    *,
    dry_run: bool = False,
    write_people: bool = True,
    force: bool = False,
    names: Optional[List[str]] = None,
    person_ids: Optional[List[str]] = None,
    unit_names: Optional[List[str]] = None,
    cities: Optional[List[str]] = None,
    provinces: Optional[List[str]] = None,
) -> Dict[str, Any]:
    runtime = VisionRuntime.from_environment()
    report: Dict[str, Any] = {
        "ok": True,
        "at": _utc_stamp(),
        "model": runtime.model,
        "prompt_version": PROMPT_VERSION,
        "available": runtime.available(),
        "counts": {
            "candidates": len(paths),
            "classified": 0,
            "failed": 0,
            "skipped": 0,
            "people_update": 0,
            "missing": 0,
        },
        "items": [],
    }
    if not runtime.available():
        report["ok"] = False
        report["error"] = "vision_runtime_unavailable"
        report["status"] = status_payload()
        return report
    client = _client_for(runtime)
    work: List[Dict[str, Any]] = []

    def _at(seq: Optional[List[str]], idx: int) -> str:
        if seq and idx < len(seq):
            return str(seq[idx] or "")
        return ""

    for idx, raw in enumerate(paths):
        work.append(
            {
                "path": raw,
                "name": _at(names, idx),
                "person_id": _at(person_ids, idx),
                # M6: 省市单位随 item 带入,name 回退写库时作约束
                "unit_name": _at(unit_names, idx),
                "city": _at(cities, idx),
                "province": _at(provinces, idx),
            }
        )

    def _one(row: Dict[str, Any]) -> Dict[str, Any]:
        return classify_and_write_person(
            image_path=str(row.get("path") or ""),
            name=str(row.get("name") or ""),
            person_id=str(row.get("person_id") or ""),
            primary_image_path=str(row.get("path") or ""),
            write_people=write_people,
            dry_run=dry_run,
            force=force,
            runtime=runtime,
            client=client,
            unit_name=str(row.get("unit_name") or ""),
            city=str(row.get("city") or ""),
            province=str(row.get("province") or ""),
        )

    _TRANSIENT = ("http_429", "rate_limit", "timed out", "Read timeout", "Connection")

    def _one_with_retry(row: Dict[str, Any]) -> Dict[str, Any]:
        # Grok relay throttles bursts; retry transient errors with a short backoff
        # instead of burning the whole item.
        delays = (3.0, 8.0)
        result = _one(row)
        for delay in delays:
            if result.get("ok") or result.get("skipped"):
                return result
            err = str(result.get("error") or "")
            if not any(marker in err for marker in _TRANSIENT):
                return result
            time.sleep(delay)
            result = _one(row)
        return result

    workers = max(1, min(runtime.concurrency, len(work) or 1))
    if workers <= 1 or len(work) <= 1:
        results = [_one_with_retry(row) for row in work]
    else:
        results = [None] * len(work)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_one_with_retry, row): idx for idx, row in enumerate(work)}
            for fut in as_completed(futs):
                idx = futs[fut]
                seed = work[idx]
                try:
                    results[idx] = fut.result()
                except Exception as exc:
                    results[idx] = {
                        "ok": False,
                        "error": f"{type(exc).__name__}:{exc}",
                        "fail_open": True,
                        "name": seed.get("name") or "",
                        "person_id": seed.get("person_id") or "",
                        "path": seed.get("path") or "",
                    }
        results = [r if r is not None else {"ok": False, "error": "no_result"} for r in results]
    for item in results:
        report["items"].append(item)
        if item.get("skipped"):
            report["counts"]["skipped"] += 1
        elif item.get("ok"):
            report["counts"]["classified"] += 1
            if str((item.get("people") or {}).get("action") or "") == "update":
                report["counts"]["people_update"] += 1
        else:
            err = str(item.get("error") or "")
            if err == "image_not_found":
                report["counts"]["missing"] += 1
            report["counts"]["failed"] += 1
    return report


def inventory_unvisioned(
    *,
    province: str = "",
    city: str = "",
    unit_like: str = "",
    limit: int = 5000,
    only_resolvable: bool = False,
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    """People with primary_image_path but empty visual_gender; optional disk resolve."""
    rows = _people_with_photos(
        unit_like=unit_like,
        province=province,
        city=city,
        limit=max(1, min(int(limit or 5000), 20000)),
        only_missing_visual=True,
        people_db=people_db,
    )
    by_city: Dict[str, int] = {}
    by_unit: Dict[str, int] = {}
    resolvable: List[Dict[str, Any]] = []
    missing: List[Dict[str, Any]] = []
    for row in rows:
        db_path = _norm(row.get("primary_image_path"))
        prov = str(row.get("province") or "").strip() or "(空)"
        cit = str(row.get("city") or "").strip() or "(空)"
        unit = str(row.get("unit_name") or "").strip() or "(空)"
        city_key = f"{prov}/{cit}"
        unit_key = f"{prov}/{cit}/{unit}"
        by_city[city_key] = by_city.get(city_key, 0) + 1
        by_unit[unit_key] = by_unit.get(unit_key, 0) + 1
        resolved = resolve_image_path(db_path)
        item = {
            "person_id": str(row.get("person_id") or ""),
            "name": str(row.get("name") or ""),
            "gender": str(row.get("gender") or ""),
            "province": prov,
            "city": cit,
            "unit_name": unit,
            "primary_image_path": db_path,
            "resolved_path": str(resolved) if resolved else "",
            "on_disk": bool(resolved),
        }
        if resolved:
            resolvable.append(item)
        else:
            missing.append(item)
    items = resolvable if only_resolvable else (resolvable + missing)
    return {
        "ok": True,
        "schema": "d2i-cloud-vision-inventory",
        "at": _utc_stamp(),
        "total": len(rows),
        "resolvable": len(resolvable),
        "missing_on_disk": len(missing),
        "returned": len(items),
        "filters": {
            "province": province,
            "city": city,
            "unit_like": unit_like,
            "limit": limit,
            "only_resolvable": only_resolvable,
        },
        "by_city": dict(sorted(by_city.items(), key=lambda kv: -kv[1])[:40]),
        "by_unit": dict(sorted(by_unit.items(), key=lambda kv: -kv[1])[:30]),
        "items": items,
    }


def plan_vision_batches(
    *,
    batch_size: int = 40,
    province: str = "",
    city: str = "",
    unit_like: str = "",
    limit: int = 20000,
    only_resolvable: bool = True,
    people_db: Optional[Path] = None,
) -> Dict[str, Any]:
    inv = inventory_unvisioned(
        province=province,
        city=city,
        unit_like=unit_like,
        limit=limit,
        only_resolvable=only_resolvable,
        people_db=people_db,
    )
    size = max(5, min(int(batch_size or 40), 200))
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in inv.get("items") or []:
        if only_resolvable and not item.get("on_disk"):
            continue
        key = f"{item.get('province') or '(空)'}::{item.get('city') or '(空)'}"
        groups.setdefault(key, []).append(item)
    batches: List[Dict[str, Any]] = []
    for key in sorted(groups.keys(), key=lambda k: -len(groups[k])):
        chunk_items = groups[key]
        for offset in range(0, len(chunk_items), size):
            chunk = chunk_items[offset : offset + size]
            batches.append(
                {
                    "batch_id": f"vb_{len(batches) + 1:03d}",
                    "batch_key": key,
                    "province": chunk[0].get("province") or "",
                    "city": chunk[0].get("city") or "",
                    "count": len(chunk),
                    "offset": offset,
                    "items": [
                        {
                            "person_id": x.get("person_id") or "",
                            "name": x.get("name") or "",
                            "path": x.get("resolved_path") or x.get("primary_image_path") or "",
                            "primary_image_path": x.get("primary_image_path") or "",
                            "province": x.get("province") or "",
                            "city": x.get("city") or "",
                            "unit_name": x.get("unit_name") or "",
                        }
                        for x in chunk
                    ],
                }
            )
    return {
        "ok": True,
        "schema": "d2i-cloud-vision-batch-plan",
        "at": _utc_stamp(),
        "batch_size": size,
        "total_items": inv.get("resolvable") if only_resolvable else inv.get("total"),
        "resolvable": inv.get("resolvable"),
        "missing_on_disk": inv.get("missing_on_disk"),
        "batch_count": len(batches),
        "batches": batches,
        "by_city": inv.get("by_city") or {},
    }


def enqueue_unvisioned_batches(
    *,
    batch_size: int = 40,
    province: str = "",
    city: str = "",
    unit_like: str = "",
    limit: int = 20000,
    max_batches: int = 0,
    force: bool = False,
    write_people: bool = True,
    dry_run: bool = False,
    start: bool = False,
    max_running: int = 1,
) -> Dict[str, Any]:
    """Split unvisioned portraits into multiple vision_jobs (not scrape queues)."""
    from cloud import jobs_db

    plan = plan_vision_batches(
        batch_size=batch_size,
        province=province,
        city=city,
        unit_like=unit_like,
        limit=limit,
        only_resolvable=True,
    )
    batches = list(plan.get("batches") or [])
    if max_batches and max_batches > 0:
        batches = batches[: int(max_batches)]
    created: List[Dict[str, Any]] = []
    for idx, batch in enumerate(batches):
        job = jobs_db.create_vision_job(
            name=f"vision {batch.get('province')}/{batch.get('city')} #{batch.get('offset')}",
            batch_key=str(batch.get("batch_key") or ""),
            province=str(batch.get("province") or ""),
            city=str(batch.get("city") or ""),
            items=list(batch.get("items") or []),
            priority=100 + idx,
            force=force,
            write_people=write_people,
            dry_run=dry_run,
        )
        created.append(
            {
                "id": job.get("id"),
                "name": job.get("name"),
                "status": job.get("status"),
                "total": job.get("total"),
                "batch_key": job.get("batch_key"),
                "priority": job.get("priority"),
            }
        )
    pump = None
    if start and created:
        # full multi-batch start always background so HTTP returns
        pump = pump_vision_jobs(max_running=max_running, background=True)
    return {
        "ok": True,
        "created": len(created),
        "batch_size": plan.get("batch_size"),
        "planned_batches": plan.get("batch_count"),
        "enqueued": created,
        "inventory": {
            "resolvable": plan.get("resolvable"),
            "missing_on_disk": plan.get("missing_on_disk"),
            "total_items": plan.get("total_items"),
        },
        "pump": pump,
        "counts": jobs_db.vision_job_counts(),
    }


def _compact_vision_item_outcome(raw: Dict[str, Any], seed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Flatten classify_and_write_person output into a UI/DB-friendly per-person row."""
    base = dict(seed or {})
    classification = raw.get("classification") if isinstance(raw.get("classification"), dict) else {}
    visual = classification.get("result") if isinstance(classification.get("result"), dict) else {}
    if not visual and isinstance(raw.get("result"), dict):
        visual = raw.get("result") or {}
    people = raw.get("people") if isinstance(raw.get("people"), dict) else {}

    if raw.get("skipped"):
        status = "skipped"
    elif raw.get("ok"):
        status = "ok"
    else:
        status = "failed"
        if str(raw.get("error") or "") == "image_not_found":
            status = "missing"

    out = {
        "person_id": str(raw.get("person_id") or base.get("person_id") or people.get("person_id") or ""),
        "name": str(raw.get("name") or base.get("name") or ""),
        "path": str(
            raw.get("path")
            or raw.get("path_hint")
            or base.get("path")
            or base.get("primary_image_path")
            or ""
        ),
        "primary_image_path": str(base.get("primary_image_path") or ""),
        "province": str(base.get("province") or ""),
        "city": str(base.get("city") or ""),
        "unit_name": str(base.get("unit_name") or ""),
        "status": status,
        "ok": bool(raw.get("ok")),
        "skipped": bool(raw.get("skipped")),
        # M6: 写库层 ambiguous(同名多行未写)也算该 item 的错误,便于人工跟进
        "error": str(raw.get("error") or raw.get("reason") or people.get("error") or ""),
        "person_count": visual.get("person_count", raw.get("person_count")),
        "visual_gender": str(
            visual.get("visual_gender")
            or raw.get("visual_gender")
            or ""
        ),
        "visual_body_type": str(visual.get("visual_body_type") or ""),
        "visual_hairstyle": str(visual.get("visual_hairstyle") or ""),
        "visual_pose": str(visual.get("visual_pose") or ""),
        "visual_age": str(visual.get("visual_age") or ""),
        "visual_attire": str(visual.get("visual_attire") or ""),
        "people_action": str(people.get("action") or ""),
        "model": str(classification.get("model") or raw.get("model") or ""),
    }
    return out


def _people_visual_lookup(
    person_ids: List[str],
    names: Optional[List[str]] = None,
    people_db: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Map person_id (and fallback name) → visual_* from people.sqlite."""
    db_path = Path(people_db) if people_db else people_db_path()
    out: Dict[str, Dict[str, Any]] = {}
    if not db_path.is_file():
        return out
    ids = [str(x).strip() for x in person_ids if str(x or "").strip()]
    name_list = [str(x).strip() for x in (names or []) if str(x or "").strip()]
    try:
        conn = _open_people(db_path)
        try:
            cols = _table_cols(conn, "people")
            want = [
                c
                for c in (
                    "person_id",
                    "name",
                    "gender",
                    "primary_image_path",
                    "person_count",
                    "visual_gender",
                    "visual_body_type",
                    "visual_hairstyle",
                    "visual_pose",
                    "visual_age",
                    "visual_attire",
                    "vision_model",
                    "vision_classified_at",
                    "vision_verified",
                )
                if c in cols
            ]
            if not want:
                return out
            select = ", ".join(want)
            for pid in ids[:500]:
                if "person_id" not in cols:
                    break
                row = conn.execute(
                    f"SELECT {select} FROM people WHERE person_id=? LIMIT 1",
                    (pid,),
                ).fetchone()
                if row is not None:
                    out[pid] = {k: row[k] for k in row.keys()}
            # name fallback only for still-missing
            if "name" in cols and name_list:
                for nm in name_list[:200]:
                    already = any(str(v.get("name") or "") == nm for v in out.values())
                    if already:
                        continue
                    row = conn.execute(
                        f"SELECT {select} FROM people WHERE name=? ORDER BY rowid DESC LIMIT 1",
                        (nm,),
                    ).fetchone()
                    if row is not None:
                        key = str(row["person_id"] if "person_id" in row.keys() else nm)
                        out.setdefault(f"name:{nm}", {k: row[k] for k in row.keys()})
                        if key and key not in out:
                            out[key] = {k: row[k] for k in row.keys()}
        finally:
            conn.close()
    except Exception:
        return out
    return out


def enrich_vision_job(job: Dict[str, Any], *, people_db: Optional[Path] = None) -> Dict[str, Any]:
    """
    Attach per-item vision outcome for UI.
    Priority: item fields already on job items → result.item_results → people.sqlite visual_*.
    """
    data = dict(job or {})
    items = [dict(x) if isinstance(x, dict) else {} for x in (data.get("items") or [])]
    result = data.get("result") if isinstance(data.get("result"), dict) else {}
    stored = result.get("item_results") if isinstance(result.get("item_results"), list) else []
    by_pid: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}
    by_path: Dict[str, Dict[str, Any]] = {}
    for row in stored:
        if not isinstance(row, dict):
            continue
        pid = str(row.get("person_id") or "").strip()
        name = str(row.get("name") or "").strip()
        path = str(row.get("path") or row.get("primary_image_path") or "").strip()
        if pid:
            by_pid[pid] = row
        if name:
            by_name[name] = row
        if path:
            by_path[path] = row
    # Legacy jobs only kept compact errors — fold them in as failed outcomes.
    for row in result.get("errors") or []:
        if not isinstance(row, dict):
            continue
        err_row = {
            "person_id": str(row.get("person_id") or ""),
            "name": str(row.get("name") or ""),
            "path": str(row.get("path") or ""),
            "error": str(row.get("error") or ""),
            "status": "failed",
            "ok": False,
        }
        pid = err_row["person_id"]
        name = err_row["name"]
        path = err_row["path"]
        if pid and pid not in by_pid:
            by_pid[pid] = err_row
        if name and name not in by_name:
            by_name[name] = err_row
        if path and path not in by_path:
            by_path[path] = err_row

    need_people = []
    need_names = []
    for it in items:
        has_visual = bool(str(it.get("visual_gender") or "").strip()) or bool(it.get("status"))
        if not has_visual:
            pid = str(it.get("person_id") or "").strip()
            name = str(it.get("name") or "").strip()
            if pid and pid not in by_pid:
                need_people.append(pid)
            if name and name not in by_name:
                need_names.append(name)
    people_map = _people_visual_lookup(need_people, names=need_names, people_db=people_db) if (
        need_people or need_names
    ) else {}

    enriched: List[Dict[str, Any]] = []
    for it in items:
        row = dict(it)
        pid = str(row.get("person_id") or "").strip()
        name = str(row.get("name") or "").strip()
        path = str(row.get("path") or row.get("primary_image_path") or "").strip()
        extra = None
        if pid and pid in by_pid:
            extra = by_pid[pid]
        elif name and name in by_name:
            extra = by_name[name]
        elif path and path in by_path:
            extra = by_path[path]
        if extra:
            for k, v in extra.items():
                if k in {"person_id", "name", "path", "primary_image_path", "province", "city", "unit_name"}:
                    if not row.get(k) and v not in (None, ""):
                        row[k] = v
                elif v not in (None, ""):
                    row[k] = v
        people_row = None
        if pid and pid in people_map:
            people_row = people_map[pid]
        elif name and f"name:{name}" in people_map:
            people_row = people_map[f"name:{name}"]
        elif name and name in people_map:
            people_row = people_map[name]
        if people_row:
            for k in (
                "visual_gender",
                "visual_body_type",
                "visual_hairstyle",
                "visual_pose",
                "visual_age",
                "visual_attire",
                "person_count",
                "vision_model",
                "vision_classified_at",
                "gender",
            ):
                if not str(row.get(k) or "").strip() and people_row.get(k) not in (None, ""):
                    row[k] = people_row.get(k)
            if not row.get("status") and str(row.get("visual_gender") or "").strip():
                row["status"] = "ok"
                row["ok"] = True
            if not row.get("model") and people_row.get("vision_model"):
                row["model"] = people_row.get("vision_model")
        if not row.get("status"):
            if row.get("error"):
                row["status"] = "failed"
            elif str(data.get("status") or "") in {"queued", "running"}:
                row["status"] = "pending"
            else:
                row["status"] = "unknown"
        enriched.append(row)
    data["items"] = enriched
    return data


def _job_status_from_counts(counts: Dict[str, Any], report: Dict[str, Any]) -> str:
    """Honest terminal status: all-fail is failed, mixed is completed_with_errors."""
    ok_n = int(counts.get("classified") or 0)
    fail_n = int(counts.get("failed") or 0)
    if str(report.get("error") or "") == "vision_runtime_unavailable":
        return "failed"
    if fail_n > 0 and ok_n == 0:
        return "failed"
    if fail_n > 0:
        return "completed_with_errors"
    return "completed"


def requeue_failed_vision_items(
    *,
    job_id: str = "",
    batch_size: int = 20,
    start: bool = False,
    max_running: int = 1,
) -> Dict[str, Any]:
    """Create retry vision_jobs from failed/missing items only (never rescrapes).

    Re-checks people.sqlite first: anyone who has since gained a visual_gender
    is dropped instead of re-billed against the Grok relay.
    """
    from cloud import jobs_db

    if job_id:
        sources = [j for j in [jobs_db.get_vision_job(job_id)] if j]
        if not sources:
            raise KeyError(f"vision job not found: {job_id}")
    else:
        sources = [
            j
            for j in jobs_db.list_vision_jobs(limit=500)
            if int(j.get("failed_count") or 0) > 0
            and str(j.get("status") or "") in {"failed", "completed", "completed_with_errors"}
        ]

    candidates: List[Dict[str, Any]] = []
    seen: set = set()
    for job in sources:
        enriched = enrich_vision_job(job)
        for it in enriched.get("items") or []:
            if str(it.get("status") or "") not in {"failed", "missing"}:
                continue
            key = str(it.get("person_id") or "") or f"{it.get('name')}::{it.get('path')}"
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "person_id": str(it.get("person_id") or ""),
                    "name": str(it.get("name") or ""),
                    "path": str(it.get("path") or it.get("primary_image_path") or ""),
                    "primary_image_path": str(it.get("primary_image_path") or it.get("path") or ""),
                    "province": str(it.get("province") or ""),
                    "city": str(it.get("city") or ""),
                    "unit_name": str(it.get("unit_name") or ""),
                    "source_job": str(job.get("id") or ""),
                    "last_error": str(it.get("error") or "")[:200],
                }
            )

    # Drop anyone already visioned since the failure.
    lookup = _people_visual_lookup(
        [c["person_id"] for c in candidates if c["person_id"]],
        names=[c["name"] for c in candidates if not c["person_id"] and c["name"]],
    )
    todo: List[Dict[str, Any]] = []
    already = 0
    for cand in candidates:
        row = lookup.get(cand["person_id"]) or lookup.get(f"name:{cand['name']}")
        existing = str((row or {}).get("visual_gender") or "").strip()
        if existing and existing not in {"不确定", "不适用"}:
            already += 1
            continue
        todo.append(cand)

    size = max(5, min(int(batch_size or 20), 100))
    created: List[Dict[str, Any]] = []
    for offset in range(0, len(todo), size):
        chunk = todo[offset : offset + size]
        label = f"{chunk[0].get('province') or '?'}/{chunk[0].get('city') or '?'}"
        job = jobs_db.create_vision_job(
            name=f"retry {label} #{offset}",
            batch_key=f"retry::{label}",
            province=str(chunk[0].get("province") or ""),
            city=str(chunk[0].get("city") or ""),
            items=chunk,
            priority=50 + offset // size,
            force=False,
            write_people=True,
            dry_run=False,
        )
        created.append(
            {
                "id": job.get("id"),
                "name": job.get("name"),
                "total": job.get("total"),
                "priority": job.get("priority"),
            }
        )

    pump = None
    if start and created:
        pump = pump_vision_jobs(max_running=max_running, background=True)
    return {
        "ok": True,
        "source_jobs": len(sources),
        "failed_items_seen": len(candidates),
        "skipped_already_visioned": already,
        "requeued_items": len(todo),
        "created_jobs": len(created),
        "jobs": created,
        "pump": pump,
        "counts": jobs_db.vision_job_counts(),
    }


def run_vision_job(job_id: str) -> Dict[str, Any]:
    """Execute one vision_jobs row, sliced so progress and cancel stay honest.

    F4: 每跑完一片(5 个 item)回写一次 done/ok/failed 计数,进度条不再恒 0%。
    F2: 每片开始前重读 job status,遇 cancelled 立即停止后续 Grok 调用并保留
        已完成计数;终态写入走 CAS(expected_status='running'),写不中就保留
        cancelled,绝不"取消后又变完成"。
    """
    from cloud import jobs_db

    job = jobs_db.get_vision_job(job_id)
    if not job:
        raise KeyError(f"vision job not found: {job_id}")
    st = str(job.get("status") or "")
    if st == "queued":
        # CAS 起跑:status 仍是 queued 才置 running,防并发双跑
        jobs_db.update_vision_job(
            job_id, status="running", mark_started=True, expected_status="queued"
        )
        job = jobs_db.get_vision_job(job_id) or job
        if str(job.get("status") or "") != "running":
            return {"ok": False, "error": f"job status is {job.get('status')}", "job": job}
    elif st != "running":
        return {"ok": False, "error": f"job status is {st}", "job": job}

    items = list(job.get("items") or [])
    agg_counts: Dict[str, int] = {
        "candidates": len(items),
        "classified": 0,
        "failed": 0,
        "skipped": 0,
        "people_update": 0,
        "missing": 0,
    }
    report: Dict[str, Any] = {
        "ok": True,
        "at": _utc_stamp(),
        "model": "",
        "prompt_version": PROMPT_VERSION,
        "counts": agg_counts,
        "items": [],
    }
    outcomes: List[Dict[str, Any]] = []
    merged_items: List[Dict[str, Any]] = []
    processed = 0
    cancelled = False
    slice_size = 5  # F4: flush progress every 5 items
    try:
        for offset in range(0, len(items), slice_size):
            # F2: cooperative cancel — 每片开始前重读一次终态
            current = jobs_db.get_vision_job(job_id) or {}
            if str(current.get("status") or "") != "running":
                cancelled = True
                break
            chunk = items[offset : offset + slice_size]
            chunk_report = run_paths_vision(
                [str(x.get("path") or x.get("primary_image_path") or "") for x in chunk],
                dry_run=bool(job.get("dry_run")),
                write_people=bool(job.get("write_people")),
                force=bool(job.get("force")),
                names=[str(x.get("name") or "") for x in chunk],
                person_ids=[str(x.get("person_id") or "") for x in chunk],
                unit_names=[str(x.get("unit_name") or "") for x in chunk],
                cities=[str(x.get("city") or "") for x in chunk],
                provinces=[str(x.get("province") or "") for x in chunk],
            )
            counts = chunk_report.get("counts") or {}
            for key in ("classified", "failed", "skipped", "people_update", "missing"):
                agg_counts[key] += int(counts.get(key) or 0)
            report["at"] = str(chunk_report.get("at") or report["at"])
            report["model"] = str(chunk_report.get("model") or report.get("model") or "")
            report["prompt_version"] = str(
                chunk_report.get("prompt_version") or report.get("prompt_version") or ""
            )
            if chunk_report.get("error"):
                report["error"] = str(chunk_report.get("error") or "")
            raw_items = list(chunk_report.get("items") or [])
            report["items"].extend(raw_items)

            # Match outcomes back onto seed items. Thread pool may reorder; key by person_id/name/path.
            used = [False] * len(raw_items)

            def _take(pred) -> Optional[Dict[str, Any]]:
                for i, raw in enumerate(raw_items):
                    if used[i]:
                        continue
                    if pred(raw):
                        used[i] = True
                        return raw
                return None

            for seed in chunk:
                pid = str(seed.get("person_id") or "").strip()
                name = str(seed.get("name") or "").strip()
                path = str(seed.get("path") or seed.get("primary_image_path") or "").strip()
                raw = None
                if pid:
                    raw = _take(lambda r, p=pid: str(r.get("person_id") or "") == p)
                if raw is None and name:
                    raw = _take(lambda r, n=name: str(r.get("name") or "") == n)
                if raw is None and path:
                    raw = _take(
                        lambda r, p=path: str(r.get("path") or r.get("path_hint") or "") == p
                        or str(r.get("path") or "").endswith(Path(p).name)
                    )
                if raw is None:
                    raw = _take(lambda _r: True) or {"ok": False, "error": "no_outcome", "name": name, "person_id": pid}
                compact = _compact_vision_item_outcome(raw, seed=seed)
                outcomes.append(compact)
                merged_items.append(compact)

            # leftover raw outcomes (should be rare)
            for i, raw in enumerate(raw_items):
                if not used[i]:
                    compact = _compact_vision_item_outcome(raw)
                    outcomes.append(compact)
                    merged_items.append(compact)

            processed += len(chunk)
            # F4: 计数回写复用 update_vision_job(同一写路径,不另开连接风暴);
            # 只写计数不动 status,即使刚被 cancel 也不会把终态改活。
            try:
                jobs_db.update_vision_job(
                    job_id,
                    done_count=processed,
                    ok_count=agg_counts["classified"],
                    failed_count=agg_counts["failed"],
                    skipped_count=agg_counts["skipped"],
                    missing_count=agg_counts["missing"],
                )
            except Exception:
                pass
            if str(report.get("error") or "") == "vision_runtime_unavailable":
                # 运行时不可用时后续片只会同样失败,提前收尾
                break

        if cancelled:
            # F2: 保留已完成计数,不写终态——cancelled 就是终态
            job_out = jobs_db.get_vision_job(job_id) or {}
            return {
                "ok": True,
                "job_id": job_id,
                "cancelled": True,
                "processed": processed,
                "report": report,
                "job": enrich_vision_job(job_out),
            }

        jobs_db.update_vision_job(
            job_id,
            status=_job_status_from_counts(agg_counts, report),
            done_count=processed,
            ok_count=agg_counts["classified"],
            failed_count=agg_counts["failed"],
            skipped_count=agg_counts["skipped"],
            missing_count=agg_counts["missing"],
            items=merged_items,
            result={
                "at": report.get("at"),
                "model": report.get("model"),
                "prompt_version": report.get("prompt_version"),
                "counts": agg_counts,
                "item_results": outcomes,
                "errors": [
                    {
                        "name": it.get("name"),
                        "person_id": it.get("person_id"),
                        "error": it.get("error"),
                        "path": it.get("path"),
                        "status": it.get("status"),
                    }
                    for it in outcomes
                    if it.get("status") in {"failed", "missing"}
                ][:50],
            },
            error=str(report.get("error") or ""),
            finished=True,
            # F2: 终态 CAS——只覆盖仍在 running 的行;取消赢了就保持 cancelled
            expected_status="running",
        )
        job_out = jobs_db.get_vision_job(job_id) or {}
        return {
            "ok": True,
            "job_id": job_id,
            "cancelled": str(job_out.get("status") or "") == "cancelled",
            "report": report,
            "job": enrich_vision_job(job_out),
        }
    except Exception as exc:
        jobs_db.update_vision_job(
            job_id,
            status="failed",
            error=f"{type(exc).__name__}:{exc}",
            finished=True,
            # F2: 异常也不许把 cancelled 改写成 failed
            expected_status="running",
        )
        return {
            "ok": False,
            "job_id": job_id,
            "error": f"{type(exc).__name__}:{exc}",
            "job": enrich_vision_job(jobs_db.get_vision_job(job_id) or {}),
        }


_PUMP_LOCK = threading.Lock()
_PUMP_STATE: Dict[str, Any] = {
    "running": False,
    "started_at": "",
    "finished_at": "",
    "max_running": 1,
    "max_claim": 0,
    "ran_count": 0,
    "last_error": "",
    "last_job_id": "",
    "thread_name": "",
}


def pump_state() -> Dict[str, Any]:
    with _PUMP_LOCK:
        return dict(_PUMP_STATE)


def _set_pump_state(**kwargs: Any) -> None:
    with _PUMP_LOCK:
        _PUMP_STATE.update(kwargs)


def _pump_loop(*, max_running: int = 1, max_claim: int = 0) -> Dict[str, Any]:
    """
    Process queued vision jobs with a simple global concurrency cap.
    Default max_running=1 so batches run one-after-another (queue/排队 smoke).
    """
    from cloud import jobs_db

    cap = max(1, min(int(max_running or 1), 4))
    claim_budget = int(max_claim or 0)
    ran: List[Dict[str, Any]] = []
    skipped_busy = 0
    _set_pump_state(
        running=True,
        started_at=_utc_stamp(),
        finished_at="",
        max_running=cap,
        max_claim=claim_budget,
        ran_count=0,
        last_error="",
        last_job_id="",
        thread_name=threading.current_thread().name,
    )
    try:
        while True:
            counts = jobs_db.vision_job_counts()
            running = int(counts.get("running") or 0)
            queued = int(counts.get("queued") or 0)
            if queued <= 0:
                break
            if running >= cap:
                skipped_busy += 1
                break
            if claim_budget and len(ran) >= claim_budget:
                break
            job = jobs_db.claim_next_vision_job()
            if not job:
                break
            jid = str(job.get("id") or "")
            _set_pump_state(last_job_id=jid)
            result = run_vision_job(jid)
            ran.append(
                {
                    "id": job.get("id"),
                    "name": job.get("name"),
                    "ok": bool(result.get("ok")),
                    "counts": ((result.get("report") or {}).get("counts") if isinstance(result, dict) else None),
                    "error": result.get("error") if isinstance(result, dict) else None,
                }
            )
            _set_pump_state(ran_count=len(ran))
            # Serial drain is intentional: true multi-worker would need separate process workers.
            continue
        return {
            "ok": True,
            "ran": ran,
            "ran_count": len(ran),
            "max_running": cap,
            "skipped_busy": skipped_busy,
            "counts": jobs_db.vision_job_counts(),
        }
    except Exception as exc:
        _set_pump_state(last_error=f"{type(exc).__name__}:{exc}")
        raise
    finally:
        _set_pump_state(running=False, finished_at=_utc_stamp(), ran_count=len(ran))


def pump_vision_jobs(
    *,
    max_running: int = 1,
    max_claim: int = 0,
    background: bool = False,
    stale_after_seconds: float = 1800.0,
) -> Dict[str, Any]:
    """
    Drain vision_jobs. background=True starts a daemon thread and returns immediately
    (needed for full 80+ batch backfill without HTTP timeout).

    F1/M7: "检查+置位"在同一把 _PUMP_LOCK 临界区内完成,同步路径同样受
    already_running 守护;requeue_stale 只在本次调用真正拿到泵之后执行,
    且带 30 分钟 staleness 阈值——在跑的批绝不会被打回 queued 重复计费。
    """
    from cloud import jobs_db

    with _PUMP_LOCK:
        if _PUMP_STATE.get("running"):
            busy_state = dict(_PUMP_STATE)
        else:
            busy_state = None
            _PUMP_STATE.update(running=True, started_at=_utc_stamp(), finished_at="")
    if busy_state is not None:
        # 泵在跑:直接返回 pump_busy,不 requeue、不双跑
        return {
            "ok": True,
            "background": bool(background),
            "started": False,
            "already_running": True,
            "reason": "pump_busy",
            "requeued_stale": 0,
            "pump": busy_state,
            "counts": jobs_db.vision_job_counts(),
        }

    # Recover jobs left in running after process/container death so pump can claim
    # again. Only rows stale past the threshold move back (F1).
    requeued = 0
    try:
        requeued = int(
            jobs_db.requeue_stale_running_vision_jobs(
                reason="reset stale running before pump",
                stale_after_seconds=stale_after_seconds,
            )
            or 0
        )
    except Exception:
        requeued = 0

    if background:

        def _bg() -> None:
            try:
                _pump_loop(max_running=max_running, max_claim=max_claim)
            except Exception:
                pass

        try:
            t = threading.Thread(
                target=_bg,
                name="d2i-vision-pump",
                daemon=True,
            )
            t.start()
        except Exception:
            _set_pump_state(running=False, finished_at=_utc_stamp())
            raise
        # brief yield so state flips to running when possible
        time.sleep(0.05)
        return {
            "ok": True,
            "background": True,
            "started": True,
            "already_running": False,
            "requeued_stale": requeued,
            "pump": pump_state(),
            "counts": jobs_db.vision_job_counts(),
        }

    try:
        result = _pump_loop(max_running=max_running, max_claim=max_claim)
    except Exception:
        # _pump_loop 的 finally 已复位 running;这里只透传异常
        raise
    result["background"] = False
    result["requeued_stale"] = requeued
    result["pump"] = pump_state()
    return result
