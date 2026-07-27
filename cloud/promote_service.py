# -*- coding: utf-8 -*-
"""Promote Cloud queue images into 角色肖像 final root and write back people.sqlite.

Queue workspaces stay under d2i-cloud-tasks; this step copies/hardlinks durable
portraits and upserts D-layer people rows (primary_image_path / no_photo).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from cloud.paths import people_db_path, portrait_root
from services.task_service import normalize_public_task_root

FORBIDDEN_WRITE_MARKERS = (
    "山东公开官员",
    "/选角/",
    "\\选角\\",
    "人物整理",
    "角色肖像__hdd_prebind",
)

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
GENDER_DIRS = {"男", "女", "未知"}

# P1-4: promote-time image QA (min edge; placeholder filename fingerprints)
PROMOTE_MIN_EDGE = 40
PROMOTE_PLACEHOLDER_MARKERS = (
    "pub-user-portrait",
    "default_avatar",
    "default-avatar",
    "noavatar",
    "placeholder",
    "head_default",
)

_ADMIN_LEVELS = ("国家级", "省级", "市级", "区县级", "乡镇级", "县级", "区级")
# M12: county-level markers → task root is five-segment .../省/市/区县/级/单位
_COUNTY_LEVELS = ("区县级", "县级", "区级")


def _utc_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _norm(path: Any) -> str:
    return str(path or "").replace("\\", "/").strip()


def assert_safe_final_path(path: Path) -> None:
    text = _norm(path)
    for bad in FORBIDDEN_WRITE_MARKERS:
        if bad in text:
            raise RuntimeError(f"forbidden write path: {path}")
    if "/角色肖像/政府/" not in text and not text.rstrip("/").endswith("/角色肖像/政府"):
        # also allow when portrait root itself is mounted without the Chinese parent visible
        # only if relative parts still start with 政府
        try:
            rel = path.resolve().relative_to(portrait_root().resolve())
            parts = rel.parts
            if not parts or parts[0] != "政府":
                raise RuntimeError(f"must land under 角色肖像/政府: {path}")
        except Exception as exc:
            if "must land" in str(exc):
                raise
            raise RuntimeError(f"must land under 角色肖像/政府: {path}") from exc


def gender_dir(gender: str) -> str:
    g = str(gender or "").strip()
    if g in {"男", "女"}:
        return g
    if "女" in g:
        return "女"
    if "男" in g:
        return "男"
    return "未知"


def _safe_name(text: str) -> str:
    value = str(text or "").strip()
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value)
    value = value.strip(" .")
    return value or "unknown"


def person_id_for(*, province: str, city: str, unit: str, name: str, source_url: str) -> str:
    raw = f"{province}|{city}|{unit}|{name}|{source_url}".encode("utf-8")
    return "p_" + hashlib.sha256(raw).hexdigest()[:24]


def asset_id_for(person_id: str, image_url: str) -> str:
    raw = f"{person_id}|{image_url}".encode("utf-8")
    return "a_" + hashlib.sha256(raw).hexdigest()[:24]


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path, *, limit: int = 5000) -> List[Dict[str, Any]]:
    if not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if len(rows) >= limit:
                    break
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
    except OSError:
        return []
    return rows


def load_runtime_config(output_root: str) -> Dict[str, Any]:
    root = Path(normalize_public_task_root(output_root) or output_root)
    return _read_json(root / "state" / "runtime_config.json")


def resolve_admin_context(output_root: str, runtime_config: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Derive province/city/admin_level/unit from runtime rules + output_root segments."""
    root = Path(normalize_public_task_root(output_root) or output_root)
    cfg = runtime_config if isinstance(runtime_config, dict) else load_runtime_config(str(root))
    rules = cfg.get("rules") if isinstance(cfg.get("rules"), dict) else {}
    admin = rules.get("admin") if isinstance(rules.get("admin"), dict) else {}

    province = str(admin.get("province") or rules.get("output_region") or "").strip()
    city = str(admin.get("city") or "").strip()
    unit = str(rules.get("unit_name") or admin.get("unit") or "").strip()
    admin_level = str(admin.get("level") or admin.get("admin_level") or "").strip()
    county = str(admin.get("county") or admin.get("district") or "").strip()

    # Parse multi-level task root: .../省/市/级/单位 or .../省/市/区/级/单位
    parts = [p for p in _norm(root).split("/") if p]
    # Drop trailing technical dirs if any
    while parts and parts[-1] in {"state", "raw", "reports", "downloads", "snapshots"}:
        parts.pop()

    # Prefer trailing segment match against known shape
    if len(parts) >= 4:
        maybe_unit = parts[-1]
        maybe_level = parts[-2]
        if maybe_level in _ADMIN_LEVELS or maybe_level.endswith("级"):
            admin_level = admin_level or maybe_level
            unit = unit or maybe_unit
            if maybe_level in _COUNTY_LEVELS and len(parts) >= 5:
                # M12 five-segment .../省/市/区县/级/单位: county gets its own slot,
                # no longer shifted into city/province
                county = county or parts[-3]
                city = city or parts[-4]
                province = province or parts[-5]
            else:
                # four-segment .../省/市/级/单位 — city/province before level (unchanged)
                city = city or parts[-3]
                province = province or parts[-4]
        else:
            unit = unit or maybe_unit

    # output_subdir like 沧州市/市级/{unit}
    sub = str(rules.get("output_subdir") or "").strip()
    if sub and (not city or not admin_level):
        segs = [s for s in sub.replace("{unit}", unit or "").split("/") if s and not s.startswith("{")]
        for seg in segs:
            if seg in _ADMIN_LEVELS or seg.endswith("级"):
                admin_level = admin_level or seg
            elif seg.endswith("省") and not province:
                province = seg
            elif (seg.endswith("市") or seg.endswith("州") or seg.endswith("盟")) and not city:
                city = seg

    domain = "政府"
    return {
        "domain": domain,
        "province": _safe_name(province) if province else "unknown",
        "city": _safe_name(city) if city else "unknown",
        "admin_level": _safe_name(admin_level) if admin_level else "未知级",
        "unit": _safe_name(unit) if unit else _safe_name(root.name),
        "county": _safe_name(county) if county else "",
        "site_name": str(cfg.get("site_name") or "").strip(),
        "list_url": "",
    }


def _index_profiles(output_root: str) -> Dict[str, Dict[str, Any]]:
    root = Path(normalize_public_task_root(output_root) or output_root)
    by_key: Dict[str, Dict[str, Any]] = {}
    for row in _read_jsonl(root / "raw" / "profiles.jsonl"):
        name = str(row.get("name") or "").strip()
        detail = str(row.get("detail_url") or "").strip()
        payload = {
            "name": name,
            "gender": str(row.get("gender") or row.get("gender_raw") or "").strip(),
            "detail_url": detail,
            "list_url": str(row.get("list_url") or "").strip(),
            "image_url": str(row.get("image_url") or "").strip(),
            "summary": str(row.get("summary") or "").strip(),
            "full_content": str(row.get("full_content") or "").strip(),
            "position": "",
        }
        mapped = row.get("mapped") if isinstance(row.get("mapped"), dict) else {}
        if mapped:
            payload["summary"] = payload["summary"] or str(mapped.get("summary") or "").strip()
            payload["full_content"] = payload["full_content"] or str(mapped.get("full_content") or "").strip()
            payload["image_url"] = payload["image_url"] or str(mapped.get("image_url") or "").strip()
            if not payload["gender"]:
                payload["gender"] = str(mapped.get("gender") or "").strip()
        # crude position: first line-ish from summary "现任XXX"
        bio = payload["summary"] or payload["full_content"]
        m = re.search(r"现任([^，,。\n]{2,40})", bio)
        if m:
            payload["position"] = m.group(1).strip()
        if detail:
            by_key[f"url:{detail}"] = payload
        if name:
            # M5 exact (name, detail_url) key: same name + different detail_url are
            # two distinct people and must both survive indexing
            by_key[f"person:{name}|{detail}"] = payload
            # name-only fallback kept for download rows lacking detail_url
            # (exact-name lookup; ambiguous when duplicated, last one wins as before)
            by_key[f"name:{name}"] = payload
    return by_key


def _index_downloads(output_root: str) -> List[Dict[str, Any]]:
    root = Path(normalize_public_task_root(output_root) or output_root)
    rows = _read_jsonl(root / "downloads" / "image_downloads.jsonl")
    if rows:
        return rows
    # fallback: named images sitting on task root
    out: List[Dict[str, Any]] = []
    for path in sorted(root.iterdir()) if root.is_dir() else []:
        if not path.is_file():
            continue
        if path.suffix.lower() not in IMAGE_EXTS:
            continue
        out.append(
            {
                "name": path.stem,
                "saved_path": str(path),
                "named_path": str(path),
                "detail_url": "",
                "image_url": "",
            }
        )
    return out


def _resolve_host_style_path(path: Path, *, portrait: Path) -> str:
    """Prefer canonical host path form for people.primary_image_path."""
    text = _norm(path.resolve()) if path else ""
    # If running in container with alternate mount, rewrite to host canonical when possible.
    host_root = os.environ.get("D2I_PORTRAIT_HOST_ROOT", "/vol1/1001/角色肖像").strip()
    try:
        rel = path.resolve().relative_to(portrait.resolve())
        return _norm(Path(host_root) / rel)
    except Exception:
        return text


def _same_file(a: Path, b: Path) -> bool:
    try:
        if not a.is_file() or not b.is_file():
            return False
        return os.path.samefile(a, b)
    except OSError:
        return False


def inspect_promote_image(src: Path) -> Dict[str, Any]:
    """P1-4 pre-promote QA: geometry + placeholder fingerprint. No network."""
    info: Dict[str, Any] = {
        "ok": False,
        "path": str(src),
        "width": 0,
        "height": 0,
        "min_edge": 0,
        "size_bytes": 0,
        "error_code": "",
        "error": "",
        "placeholder": False,
    }
    name_blob = str(src).replace("\\", "/").lower()
    if any(m in name_blob for m in PROMOTE_PLACEHOLDER_MARKERS):
        info["placeholder"] = True
        info["error_code"] = "placeholder_avatar"
        info["error"] = "filename/path looks like placeholder avatar"
        return info
    if not src.is_file():
        info["error_code"] = "image_not_found"
        info["error"] = "source file missing"
        return info
    try:
        info["size_bytes"] = int(src.stat().st_size)
    except OSError as exc:
        info["error_code"] = "image_invalid"
        info["error"] = f"stat:{exc}"
        return info
    if info["size_bytes"] <= 0:
        info["error_code"] = "image_truncated"
        info["error"] = "0 bytes"
        return info
    try:
        from PIL import Image

        with Image.open(src) as im:
            im.load()
            w, h = im.size
        info["width"] = int(w)
        info["height"] = int(h)
        info["min_edge"] = min(int(w), int(h))
    except Exception as exc:  # noqa: BLE001
        msg = f"{type(exc).__name__}:{exc}"
        info["error"] = msg
        info["error_code"] = (
            "image_truncated" if "truncated" in msg.lower() else "image_invalid"
        )
        return info
    if info["min_edge"] < PROMOTE_MIN_EDGE:
        info["error_code"] = "image_too_small"
        info["error"] = f"min_edge {info['min_edge']} < {PROMOTE_MIN_EDGE}"
        return info
    info["ok"] = True
    return info


def place_image(src: Path, dest: Path, *, dry_run: bool = False) -> str:
    """Hardlink when possible, else copy. Returns action label."""
    assert_safe_final_path(dest)
    if dry_run:
        if dest.is_file():
            return "dry_exists"
        return "dry_copy"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file():
        if _same_file(src, dest):
            return "exists_same"
        # replace only if source is newer or dest tiny
        try:
            if dest.stat().st_size >= 400 and src.stat().st_mtime <= dest.stat().st_mtime:
                return "exists"
        except OSError:
            pass
    tmp = dest.with_suffix(dest.suffix + f".tmp.{os.getpid()}")
    try:
        os.link(src, tmp)
        action = "hardlink"
    except OSError:
        shutil.copy2(src, tmp)
        action = "copy"
    os.replace(tmp, dest)
    return action


def _open_people(db_path: Path) -> sqlite3.Connection:
    if not db_path.parent.is_dir():
        raise FileNotFoundError(f"people db parent missing: {db_path.parent}")
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})")}


def upsert_person(
    conn: sqlite3.Connection,
    *,
    ctx: Dict[str, str],
    name: str,
    gender: str,
    position: str,
    source_url: str,
    list_url: str,
    image_url: str,
    bio: str,
    path: Optional[Path],
    path_for_db: str,
    no_photo: bool,
    dry_run: bool,
    queue_id: str = "",
) -> Dict[str, Any]:
    province = ctx["province"]
    city = ctx["city"]
    unit = ctx["unit"]
    admin_level = ctx["admin_level"]
    pid = person_id_for(province=province, city=city, unit=unit, name=name, source_url=source_url or name)
    page_status = "no_photo" if no_photo else ("has_photo" if path or path_for_db else "unknown")
    photo_kind = "official_photo" if (path or path_for_db) and not no_photo else ""
    file_status = "present" if (path or path_for_db) and not no_photo else ("absent" if no_photo else "")
    image_status = "no_photo" if no_photo else ("official_photo" if (path or path_for_db) else "")
    now = _utc_stamp()
    domain = ""
    if source_url:
        try:
            domain = urlparse(source_url).netloc or ""
        except Exception:
            domain = ""
    raw = json.dumps(
        {
            "biography": bio or "",
            "queue_id": queue_id,
            "list_url": list_url,
            "promoted_at": now,
            "site_name": ctx.get("site_name") or "",
        },
        ensure_ascii=False,
    )

    existing = conn.execute(
        "SELECT person_id, primary_image_path FROM people WHERE name=? AND province=? AND city=? AND unit_name=?",
        (name, province, city, unit),
    ).fetchone()
    if existing is None and source_url:
        existing = conn.execute(
            "SELECT person_id, primary_image_path FROM people WHERE name=? AND source_url=?",
            (name, source_url),
        ).fetchone()

    if dry_run:
        return {
            "person_id": str(existing["person_id"]) if existing else pid,
            "action": "dry_update" if existing else "dry_insert",
            "path": path_for_db,
            "page_status": page_status,
        }

    cols = _table_cols(conn, "people")

    def set_if(d: Dict[str, Any], key: str, value: Any) -> None:
        if key in cols:
            d[key] = value

    values: Dict[str, Any] = {}
    set_if(values, "name", name)
    set_if(values, "gender", gender if gender in {"男", "女"} else gender_dir(gender) if gender else "")
    set_if(values, "province", province)
    set_if(values, "city", city)
    set_if(values, "county_or_district", ctx.get("county") or admin_level)
    set_if(values, "administrative_level", admin_level)
    set_if(values, "unit_name", unit)
    set_if(values, "department", unit)
    set_if(values, "position", position or "")
    set_if(values, "biography", bio or "")
    set_if(values, "full_public_text", bio or "")
    set_if(values, "source_url", source_url or "")
    set_if(values, "source_page_title", f"{unit} - {name}" if unit else name)
    set_if(values, "source_site_domain", domain)
    set_if(values, "source_unit_url", list_url or "")
    set_if(values, "has_official_photo", 0 if no_photo else 1)
    set_if(values, "has_news_photo", 0)
    set_if(values, "image_status", image_status)
    set_if(values, "primary_image_path", path_for_db if not no_photo else "")
    set_if(values, "raw_fields_json", raw)
    set_if(values, "crawled_at", now)
    set_if(values, "last_checked_at", now)
    set_if(values, "notes", f"cloud promote queue={queue_id or '-'}")
    set_if(values, "photo_kind", photo_kind)
    set_if(values, "file_status", file_status)
    set_if(values, "source_page_image_status", page_status)
    set_if(values, "source_image_status", page_status)

    if existing:
        pid = str(existing["person_id"])
        # Do not clobber a good final path with empty
        if not path_for_db and existing["primary_image_path"] and "primary_image_path" in values:
            values.pop("primary_image_path", None)
        # Do not overwrite an existing 男/女 with blank / 未知 (promote blank-gender guard).
        if "gender" in values and "gender" in cols:
            try:
                prev_g = conn.execute(
                    "SELECT gender FROM people WHERE person_id=?", (pid,)
                ).fetchone()
                prev = str(prev_g["gender"] if prev_g is not None else "") if prev_g is not None else ""
            except Exception:
                prev = ""
            new_g = str(values.get("gender") or "").strip()
            if prev in {"男", "女"} and new_g not in {"男", "女"}:
                values.pop("gender", None)
        assigns = ", ".join(f"{k}=?" for k in values)
        conn.execute(f"UPDATE people SET {assigns} WHERE person_id=?", (*values.values(), pid))
        action = "update"
    else:
        values["person_id"] = pid
        keys = ", ".join(values.keys())
        qs = ", ".join("?" for _ in values)
        conn.execute(f"INSERT INTO people({keys}) VALUES({qs})", tuple(values.values()))
        action = "insert"

    tables = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "image_assets" in tables and path_for_db and not no_photo:
        aid = asset_id_for(pid, image_url or path_for_db)
        acols = _table_cols(conn, "image_assets")
        conn.execute("DELETE FROM image_assets WHERE person_id=?", (pid,))
        asset: Dict[str, Any] = {}
        if "asset_id" in acols:
            asset["asset_id"] = aid
        if "person_id" in acols:
            asset["person_id"] = pid
        if "name" in acols:
            asset["name"] = name
        if "image_type" in acols:
            asset["image_type"] = "official_photo"
        if "image_index" in acols:
            asset["image_index"] = 1
        if "image_total" in acols:
            asset["image_total"] = 1
        if "image_path" in acols:
            asset["image_path"] = path_for_db
        if "image_url" in acols:
            asset["image_url"] = image_url or ""
        if "source_url" in acols:
            asset["source_url"] = source_url or ""
        if "source_page_title" in acols:
            asset["source_page_title"] = f"{unit} - {name}"
        if "source_site_domain" in acols:
            asset["source_site_domain"] = domain
        if "is_usable" in acols:
            asset["is_usable"] = 1
        if "crawled_at" in acols:
            asset["crawled_at"] = now
        if asset:
            keys = ", ".join(asset.keys())
            qs = ", ".join("?" for _ in asset)
            conn.execute(f"INSERT INTO image_assets({keys}) VALUES({qs})", tuple(asset.values()))

    conn.commit()
    return {
        "person_id": pid,
        "action": action,
        "path": path_for_db,
        "page_status": page_status,
    }


def _existing_src(path_value: Any, *, output_root: str) -> str:
    raw = _norm(path_value)
    if not raw:
        return ""
    candidates = [Path(raw)]
    # container vs host task root remap
    if raw.startswith("/runtime/d2i-cloud-tasks/"):
        candidates.append(Path("/vol4/1001/hermes-runtime/d2i-cloud-tasks") / raw[len("/runtime/d2i-cloud-tasks/") :])
    if raw.startswith("/vol4/1001/hermes-runtime/d2i-cloud-tasks/"):
        candidates.append(Path("/runtime/d2i-cloud-tasks") / raw[len("/vol4/1001/hermes-runtime/d2i-cloud-tasks/") :])
    root = normalize_public_task_root(output_root)
    if root and raw and not Path(raw).is_absolute():
        candidates.append(Path(root) / raw)
    for cand in candidates:
        try:
            if cand.is_file() and cand.suffix.lower() in IMAGE_EXTS:
                return str(cand.resolve())
        except OSError:
            continue
    return ""


def collect_promote_candidates(output_root: str) -> List[Dict[str, Any]]:
    root = normalize_public_task_root(output_root) or output_root
    profiles = _index_profiles(root)
    downloads = _index_downloads(root)
    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    # M5 exact dedup bookkeeping for the no_photo pass below
    seen_names: set[str] = set()  # every name emitted from downloads
    seen_names_no_detail: set[str] = set()  # download rows that had no detail_url

    for row in downloads:
        name = str(row.get("name") or "").strip()
        detail = str(row.get("detail_url") or "").strip()
        src = _existing_src(row.get("named_path") or row.get("saved_path") or row.get("image_path"), output_root=root)
        prof = profiles.get(f"url:{detail}") or profiles.get(f"name:{name}") or {}
        if not name:
            name = str(prof.get("name") or "").strip()
        if not name:
            continue
        key = f"{name}|{detail or src}"
        if key in seen:
            continue
        seen.add(key)
        seen_names.add(name)
        if not detail:
            seen_names_no_detail.add(name)
        gender = str(prof.get("gender") or "").strip()
        items.append(
            {
                "name": name,
                "gender": gender,
                "detail_url": detail or str(prof.get("detail_url") or ""),
                "list_url": str(prof.get("list_url") or ""),
                "image_url": str(row.get("image_url") or prof.get("image_url") or ""),
                "position": str(prof.get("position") or ""),
                "bio": str(prof.get("summary") or prof.get("full_content") or "")[:4000],
                "src_path": src,
                "has_image": bool(src),
            }
        )

    # profiles without download rows → no_photo candidates
    # M5: dedup by exact (name, detail_url); when detail_url is missing on either
    # side, degrade to exact name equality (NOT substring — 李小明/小明 are distinct)
    for key, prof in profiles.items():
        if not key.startswith("person:"):
            continue
        name = str(prof.get("name") or "").strip()
        if not name:
            continue
        detail = str(prof.get("detail_url") or "")
        marker = f"{name}|{detail}"
        if marker in seen:
            continue
        if detail:
            # downloads carrying a detail_url are matched above via the exact marker;
            # a same-name download without detail_url degrades to name equality
            if name in seen_names_no_detail:
                continue
        else:
            # profile itself has no detail_url → name equality is all we have
            if name in seen_names:
                continue
        seen.add(marker)
        items.append(
            {
                "name": name,
                "gender": str(prof.get("gender") or ""),
                "detail_url": detail,
                "list_url": str(prof.get("list_url") or ""),
                "image_url": str(prof.get("image_url") or ""),
                "position": str(prof.get("position") or ""),
                "bio": str(prof.get("summary") or prof.get("full_content") or "")[:4000],
                "src_path": "",
                "has_image": False,
            }
        )
    return items


def promote_queue_output(
    output_root: str,
    *,
    queue_id: str = "",
    dry_run: bool = False,
    limit: int = 0,
    portrait: Optional[Path] = None,
    people_db: Optional[Path] = None,
    write_people: bool = True,
    only_with_image: bool = False,
) -> Dict[str, Any]:
    root = normalize_public_task_root(output_root) or str(output_root or "").strip()
    if not root or not Path(root).is_dir():
        raise FileNotFoundError(f"output_root missing: {output_root}")

    cfg = load_runtime_config(root)
    ctx = resolve_admin_context(root, cfg)
    # list_url from start_urls
    starts = cfg.get("start_urls") if isinstance(cfg.get("start_urls"), list) else []
    if starts:
        ctx["list_url"] = str(starts[0] or "")

    por = Path(portrait) if portrait else portrait_root()
    final_base = por / "政府" / ctx["province"] / ctx["city"] / ctx["admin_level"] / ctx["unit"]
    assert_safe_final_path(final_base)

    candidates = collect_promote_candidates(root)
    if only_with_image:
        candidates = [c for c in candidates if c.get("has_image")]
    if limit and limit > 0:
        candidates = candidates[: int(limit)]

    db_path = Path(people_db) if people_db else people_db_path()
    conn: Optional[sqlite3.Connection] = None
    people_error = ""
    if write_people and not dry_run:
        try:
            if not db_path.is_file():
                raise FileNotFoundError(f"people db not found: {db_path}")
            conn = _open_people(db_path)
            cols = _table_cols(conn, "people")
            for required in ("person_id", "name", "unit_name", "primary_image_path"):
                if required not in cols:
                    raise RuntimeError(f"people table missing column: {required}")
        except Exception as exc:
            people_error = str(exc)
            conn = None
    elif write_people and dry_run:
        try:
            if db_path.is_file():
                conn = _open_people(db_path)
        except Exception as exc:
            people_error = str(exc)

    results: List[Dict[str, Any]] = []
    counts = {
        "candidates": len(candidates),
        "promoted": 0,
        "exists": 0,
        "no_photo": 0,
        "skipped": 0,
        "failed": 0,
        "rejected_qa": 0,
        "people_insert": 0,
        "people_update": 0,
        "people_dry": 0,
        "people_error": 0,
    }

    started = time.time()
    for item in candidates:
        name = str(item.get("name") or "").strip()
        rec: Dict[str, Any] = {
            "name": name,
            "src_path": item.get("src_path") or "",
            "detail_url": item.get("detail_url") or "",
        }
        try:
            gdir = gender_dir(str(item.get("gender") or ""))
            src_text = str(item.get("src_path") or "").strip()
            dest: Optional[Path] = None
            path_for_db = ""
            place_action = ""
            if src_text:
                src = Path(src_text)
                # P1-4: reject bad local assets before writing primary / people path
                qa = inspect_promote_image(src)
                rec["image_qa"] = {
                    k: qa.get(k)
                    for k in (
                        "ok",
                        "width",
                        "height",
                        "min_edge",
                        "size_bytes",
                        "error_code",
                        "placeholder",
                    )
                }
                if not qa.get("ok"):
                    counts["rejected_qa"] += 1
                    counts["skipped"] += 1
                    rec.update(
                        {
                            "dest": "",
                            "path_for_db": "",
                            "place_action": f"rejected_qa:{qa.get('error_code') or 'invalid'}",
                            "ok": False,
                            "no_photo": False,
                            "qa_rejected": True,
                            "error": qa.get("error") or qa.get("error_code"),
                        }
                    )
                    # Still optionally stamp people as unusable when writing
                    if write_people and conn is not None and not dry_run:
                        try:
                            from cloud import people_workflow

                            # Best-effort: locate by name+unit after upsert would run;
                            # here we upsert as no usable primary.
                            people_rec = upsert_person(
                                conn,
                                ctx=ctx,
                                name=name,
                                gender=str(item.get("gender") or ""),
                                position=str(item.get("position") or ""),
                                source_url=str(item.get("detail_url") or ""),
                                list_url=str(item.get("list_url") or ctx.get("list_url") or ""),
                                image_url=str(item.get("image_url") or ""),
                                bio=str(item.get("bio") or ""),
                                path=None,
                                path_for_db="",
                                no_photo=False,
                                dry_run=False,
                                queue_id=queue_id,
                            )
                            pid = str(people_rec.get("person_id") or "")
                            if pid:
                                people_workflow.mark_person(
                                    person_id=pid,
                                    action="unusable",
                                    reason=f"promote QA: {qa.get('error_code')}",
                                    clear_primary_path=False,
                                    people_db=db_path,
                                )
                            rec["people"] = {**people_rec, "qa": "unusable"}
                        except Exception as exc:  # noqa: BLE001
                            rec["people_error"] = f"qa_mark:{exc}"
                    results.append(rec)
                    continue
                ext = src.suffix.lower() if src.suffix.lower() in IMAGE_EXTS else ".jpg"
                dest = final_base / gdir / f"{_safe_name(name)}{ext}"
                place_action = place_image(src, dest, dry_run=dry_run)
                if place_action in {"exists", "exists_same"}:
                    counts["exists"] += 1
                elif place_action.startswith("dry_"):
                    pass
                else:
                    counts["promoted"] += 1
                if dry_run:
                    path_for_db = _resolve_host_style_path(dest, portrait=por)
                elif dest is not None and dest.is_file():
                    path_for_db = _resolve_host_style_path(dest, portrait=por)
                # P1-1: prefer container-stable form when writing people
                if path_for_db.startswith("/vol1/1001/角色肖像/"):
                    path_for_db = "/runtime/portrait/" + path_for_db[len("/vol1/1001/角色肖像/") :]
                rec.update(
                    {
                        "dest": str(dest) if dest else "",
                        "path_for_db": path_for_db,
                        "place_action": place_action,
                        "ok": True,
                        "no_photo": False,
                    }
                )
            else:
                counts["no_photo"] += 1
                rec.update(
                    {
                        "dest": "",
                        "path_for_db": "",
                        "place_action": "no_image",
                        "ok": True,
                        "no_photo": True,
                    }
                )

            if write_people and conn is not None:
                people_rec = upsert_person(
                    conn,
                    ctx=ctx,
                    name=name,
                    gender=str(item.get("gender") or ""),
                    position=str(item.get("position") or ""),
                    source_url=str(item.get("detail_url") or ""),
                    list_url=str(item.get("list_url") or ctx.get("list_url") or ""),
                    image_url=str(item.get("image_url") or ""),
                    bio=str(item.get("bio") or ""),
                    path=dest if dest and (dry_run or dest.is_file()) else None,
                    path_for_db=path_for_db,
                    no_photo=bool(rec.get("no_photo")),
                    dry_run=dry_run,
                    queue_id=queue_id,
                )
                rec["people"] = people_rec
                action = str(people_rec.get("action") or "")
                if action == "insert":
                    counts["people_insert"] += 1
                elif action == "update":
                    counts["people_update"] += 1
                elif action.startswith("dry_"):
                    counts["people_dry"] += 1
            elif write_people and people_error:
                rec["people_error"] = people_error
                counts["people_error"] += 1
        except Exception as exc:
            counts["failed"] += 1
            rec.update({"ok": False, "error": str(exc)})
        results.append(rec)

    # write report under task reports/
    # Keep promote_report even when scraper images_only_with_record cleanup
    # later wipes raw/ — reports/ is re-created here after promote.
    report = {
        "ok": counts["failed"] == 0,
        "schema": "d2i-cloud-promote-report",
        "queue_id": queue_id,
        "output_root": root,
        "portrait_root": str(por),
        "final_base": str(final_base),
        "people_db": str(db_path),
        "dry_run": dry_run,
        "admin": ctx,
        "counts": counts,
        "people_error": people_error,
        "elapsed_sec": round(time.time() - started, 3),
        "results": results,
        "promoted_at": _utc_stamp(),
        "note": (
            "promote_report is written after placement; if scraper cleanup "
            "removes raw/, re-run finalize only when candidates still exist"
        ),
    }
    if not dry_run:
        try:
            report_dir = Path(root) / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "promote_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    return report


def promote_queue_record(
    record: Dict[str, Any],
    *,
    dry_run: bool = False,
    limit: int = 0,
    write_people: bool = True,
) -> Dict[str, Any]:
    root = normalize_public_task_root(record.get("output_root"))
    if not root:
        raise ValueError("queue has no output_root")
    return promote_queue_output(
        root,
        queue_id=str(record.get("id") or ""),
        dry_run=dry_run,
        limit=limit,
        write_people=write_people,
    )
