#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T0 safe smoke: Hebei Cangzhou municipal leaders → 角色肖像 + people writeback.

WP-F style new-province G-crawl (not Shandong hospital/Dongying reuse).
Default speed: safe delays. No writes to 山东公开官员 / 选角.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import ssl

PROV = "河北省"
CITY = "沧州市"
UNIT = "沧州市人民政府"
ADMIN_LEVEL = "市级"
DOMAIN = "www.cangzhou.gov.cn"
LIST_URL = (
    "https://www.cangzhou.gov.cn/cangzhou/c116519e/202311/"
    "c8a74143818f4147be693259ed8a68a4.shtml"
)

# Fixed T0 roster (2026-07-26 site snapshot). Keep small (≤20).
ROSTER: List[Dict[str, str]] = [
    {
        "name": "刘勇",
        "gender": "男",
        "position": "代理市长",
        "source_url": "https://www.cangzhou.gov.cn/cangzhou/c116519e/202311/c8a74143818f4147be693259ed8a68a4.shtml",
        "image_url": "https://www.cangzhou.gov.cn/cangzhou/c116519e/202311/c8a74143818f4147be693259ed8a68a4/images/2ce4e3b7dd384165964440281ffaa88c.jpg",
        "bio": "刘勇，男，汉族，1973年1月出生，在职研究生，工商管理硕士、法学硕士，中共党员，现任沧州市委副书记、市政府党组书记、副市长、代理市长，沧州临港经济技术开发区党工委副书记、管委会主任、沧州高新技术产业开发区党工委书记。",
    },
    {
        "name": "戴树胜",
        "gender": "男",
        "position": "副市长",
        "source_url": "https://www.cangzhou.gov.cn/cangzhou/c116525/202504/7c067d8add9a4c0c9c8eb3673acd0ce1.shtml",
        "image_url": "https://www.cangzhou.gov.cn/cangzhou/c116525/202504/7c067d8add9a4c0c9c8eb3673acd0ce1/images/95dab47b29974467a3acf2be9f9cc7af.png",
        "bio": "戴树胜，男，回族，1968年7月生，省委党校在职研究生，中共党员，现任沧州市副市长。",
    },
    {
        "name": "尹卫江",
        "gender": "男",
        "position": "副市长",
        "source_url": "https://www.cangzhou.gov.cn/cangzhou/c116528/202504/9a90331490eb47809a1cb4fa94003f7f.shtml",
        "image_url": "https://www.cangzhou.gov.cn/cangzhou/c116528/202504/9a90331490eb47809a1cb4fa94003f7f/images/badd14b75a8749749036cfe4e8229917.png",
        "bio": "尹卫江，男，汉族，1967年7月生，中央党校在职研究生，中共党员，现任沧州市副市长。",
    },
    {
        "name": "王晓玲",
        "gender": "女",
        "position": "副市长",
        "source_url": "https://www.cangzhou.gov.cn/cangzhou/c116531/202504/c1ab8070b95e405b99e5d61d08612065.shtml",
        "image_url": "https://www.cangzhou.gov.cn/cangzhou/c116531/202504/c1ab8070b95e405b99e5d61d08612065/images/647d086d359c4017906ca568195380dc.png",
        "bio": "王晓玲，女，汉族，1973年12月生，在职研究生，工商管理硕士，无党派人士，现任沧州市副市长。",
    },
    {
        "name": "王少杰",
        "gender": "男",
        "position": "副市长",
        "source_url": "https://www.cangzhou.gov.cn/cangzhou/add101349/202412/b6351a35e2ee4c50b57183f3902bca1b.shtml",
        "image_url": "https://www.cangzhou.gov.cn/cangzhou/add101349/202412/b6351a35e2ee4c50b57183f3902bca1b/images/467b846f9a9d4d249cf58d8fee57c1c7.jpg",
        "bio": "王少杰，男，汉族，1970年7月出生，研究生学历，经济学硕士学位，中共党员，现任沧州市人民政府党组成员、副市长。",
    },
    {
        "name": "姜桂海",
        "gender": "男",
        "position": "副市长",
        "source_url": "https://www.cangzhou.gov.cn/cangzhou/add101346/202412/c5f660074af54439aefd3648f64b0baf.shtml",
        "image_url": "https://www.cangzhou.gov.cn/cangzhou/add101346/202412/c5f660074af54439aefd3648f64b0baf/images/084708a7f65e431fb20e78071a538cbc.jpg",
        "bio": "姜桂海，男，汉族，1970年7月出生，大学本科学历，法学学士学位，中共党员，现任沧州市人民政府党组成员、副市长，市公安局党委书记、局长、督察长，市委政法委员会委员。",
    },
]

FORBIDDEN_WRITE_MARKERS = (
    "山东公开官员",
    "/选角/",
    "\\选角\\",
    "人物整理",
    "角色肖像__hdd_prebind",
)

UA = "Mozilla/5.0 (compatible; D2ILiteArchiveBot/1.0; +local archival)"
SSL_CTX = ssl.create_default_context()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def person_id_for(name: str, source_url: str) -> str:
    raw = f"hebei-cangzhou|{name}|{source_url}".encode("utf-8")
    return "p_hb_cz_" + hashlib.sha256(raw).hexdigest()[:20]


def asset_id_for(person_id: str, image_url: str) -> str:
    raw = f"{person_id}|{image_url}".encode("utf-8")
    return "a_" + hashlib.sha256(raw).hexdigest()[:24]


def gender_dir(gender: str) -> str:
    g = str(gender or "").strip()
    if g in {"男", "女"}:
        return g
    return "未知"


def assert_safe_path(path: Path) -> None:
    text = str(path)
    for bad in FORBIDDEN_WRITE_MARKERS:
        if bad in text:
            raise RuntimeError(f"forbidden write path: {path}")
    # must land under 角色肖像/政府
    norm = text.replace("\\", "/")
    if "/角色肖像/政府/" not in norm and not norm.endswith("/角色肖像/政府"):
        raise RuntimeError(f"must land under 角色肖像/政府: {path}")


def download(url: str, dest: Path, *, dry_run: bool) -> Tuple[bool, str, int]:
    if dry_run:
        return True, "dry_run", 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 800:
        return True, "exists", dest.stat().st_size
    req = Request(url, headers={"User-Agent": UA, "Referer": LIST_URL})
    with urlopen(req, timeout=45, context=SSL_CTX) as resp:
        data = resp.read()
        ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if len(data) < 400:
        return False, f"too_small:{len(data)}", 0
    # fix extension from content-type if needed
    if ctype.startswith("image/"):
        ext = mimetypes.guess_extension(ctype) or dest.suffix
        if ext == ".jpe":
            ext = ".jpg"
        if ext and dest.suffix.lower() != ext.lower():
            dest = dest.with_suffix(ext)
    tmp = dest.with_suffix(dest.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(data)
    os.replace(tmp, dest)
    return True, "downloaded", len(data)


def ensure_columns(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(people)")}
    # only touch columns that already exist; do not migrate schema here
    required_min = {
        "person_id",
        "name",
        "province",
        "city",
        "unit_name",
        "position",
        "source_url",
        "primary_image_path",
    }
    missing = required_min - cols
    if missing:
        raise RuntimeError(f"people table missing columns: {sorted(missing)}")


def upsert_person(
    conn: sqlite3.Connection,
    *,
    row: Dict[str, str],
    path: Optional[Path],
    no_photo: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    name = row["name"]
    source_url = row["source_url"]
    pid = person_id_for(name, source_url)
    path_text = str(path) if path else ""
    page_status = "no_photo" if no_photo else ("has_photo" if path else "unknown")
    photo_kind = "official_photo" if path else ""
    file_status = "present" if path else ("absent" if no_photo else "")
    image_status = "no_photo" if no_photo else ("official_photo" if path else "")
    raw = json.dumps(
        {
            "biography": row.get("bio") or "",
            "t0_batch": "hebei_cangzhou_leadership_20260726",
            "list_url": LIST_URL,
            "speed_tier": "safe",
        },
        ensure_ascii=False,
    )
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    existing = conn.execute(
        "SELECT person_id FROM people WHERE name=? AND province=? AND city=? AND unit_name=?",
        (name, PROV, CITY, UNIT),
    ).fetchone()
    if dry_run:
        return {
            "person_id": existing["person_id"] if existing else pid,
            "action": "dry_update" if existing else "dry_insert",
            "path": path_text,
            "page_status": page_status,
        }

    cols = {r[1] for r in conn.execute("PRAGMA table_info(people)")}

    def set_if(d: Dict[str, Any], key: str, value: Any) -> None:
        if key in cols:
            d[key] = value

    values: Dict[str, Any] = {}
    set_if(values, "name", name)
    set_if(values, "gender", row.get("gender") or "")
    set_if(values, "province", PROV)
    set_if(values, "city", CITY)
    set_if(values, "county_or_district", ADMIN_LEVEL)
    set_if(values, "administrative_level", "市级")
    set_if(values, "unit_name", UNIT)
    set_if(values, "department", UNIT)
    set_if(values, "position", row.get("position") or "")
    set_if(values, "biography", row.get("bio") or "")
    set_if(values, "full_public_text", row.get("bio") or "")
    set_if(values, "source_url", source_url)
    set_if(values, "source_page_title", f"市政府领导 - {name}")
    set_if(values, "source_site_domain", DOMAIN)
    set_if(values, "source_unit_url", LIST_URL)
    set_if(values, "has_official_photo", 0 if no_photo else 1)
    set_if(values, "has_news_photo", 0)
    set_if(values, "image_status", image_status)
    set_if(values, "primary_image_path", path_text if path else "")
    set_if(values, "raw_fields_json", raw)
    set_if(values, "crawled_at", now)
    set_if(values, "last_checked_at", now)
    set_if(values, "notes", "T0 safe Hebei Cangzhou leadership 2026-07-26")
    set_if(values, "photo_kind", photo_kind)
    set_if(values, "file_status", file_status)
    set_if(values, "source_page_image_status", page_status)
    set_if(values, "source_image_status", page_status)

    if existing:
        pid = existing["person_id"]
        assigns = ", ".join(f"{k}=?" for k in values)
        conn.execute(
            f"UPDATE people SET {assigns} WHERE person_id=?",
            (*values.values(), pid),
        )
        action = "update"
    else:
        values["person_id"] = pid
        keys = ", ".join(values.keys())
        qs = ", ".join("?" for _ in values)
        conn.execute(f"INSERT INTO people({keys}) VALUES({qs})", tuple(values.values()))
        action = "insert"

    # image_assets if table exists
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "image_assets" in tables and path and not no_photo:
        aid = asset_id_for(pid, row["image_url"])
        acols = {r[1] for r in conn.execute("PRAGMA table_info(image_assets)")}
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
            asset["image_path"] = path_text
        if "image_url" in acols:
            asset["image_url"] = row["image_url"]
        if "source_url" in acols:
            asset["source_url"] = source_url
        if "source_page_title" in acols:
            asset["source_page_title"] = f"市政府领导 - {name}"
        if "source_site_domain" in acols:
            asset["source_site_domain"] = DOMAIN
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
        "path": path_text,
        "page_status": page_status,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--portrait-root",
        default=os.environ.get("D2I_PORTRAIT_ROOT", "/vol1/1001/角色肖像"),
    )
    ap.add_argument(
        "--people-db",
        default=os.environ.get(
            "D2I_PEOPLE_DB",
            "/vol4/1001/hermes-runtime/db/people.sqlite",
        ),
    )
    ap.add_argument(
        "--report-dir",
        default=os.environ.get(
            "D2I_T0_REPORT_DIR",
            "/vol1/1001/portrait_work/t0_cangzhou_20260726",
        ),
    )
    ap.add_argument("--delay", type=float, default=5.0, help="safe delay seconds")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    portrait_root = Path(args.portrait_root)
    final_base = portrait_root / "政府" / PROV / CITY / ADMIN_LEVEL / UNIT
    assert_safe_path(final_base)

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    batch = list(ROSTER)
    if args.limit and args.limit > 0:
        batch = batch[: args.limit]

    conn = sqlite3.connect(str(args.people_db))
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)

    results: List[Dict[str, Any]] = []
    ok_photo = 0
    no_photo = 0
    fail = 0

    print("=" * 60)
    print("T0 Cangzhou leadership | safe |", "dry" if args.dry_run else "apply")
    print("final_base", final_base)
    print("people_db", args.people_db)
    print("roster", len(batch))
    print("=" * 60)

    for i, row in enumerate(batch, 1):
        name = row["name"]
        print(f"\n[{i}/{len(batch)}] {row['position']} {name}")
        gdir = gender_dir(row.get("gender") or "")
        # prefer extension from URL
        url_path = urlparse(row["image_url"]).path
        ext = Path(url_path).suffix.lower() or ".jpg"
        if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
            ext = ".jpg"
        dest = final_base / gdir / f"{name}{ext}"
        assert_safe_path(dest)

        got, reason, size = download(row["image_url"], dest, dry_run=args.dry_run)
        if not got:
            print("  FAIL image", reason)
            # mark no_photo only if source truly has no usable image; here fail is transport
            rec = upsert_person(conn, row=row, path=None, no_photo=False, dry_run=args.dry_run)
            rec.update(
                {
                    "name": name,
                    "position": row["position"],
                    "ok": False,
                    "reason": reason,
                    "dest": str(dest),
                    "size": size,
                }
            )
            results.append(rec)
            fail += 1
        else:
            if args.dry_run:
                path_for_db = dest
            else:
                path_for_db = dest if dest.is_file() else None
                # if extension fixed inside download, re-detect
                if path_for_db is None:
                    for cand in dest.parent.glob(f"{name}.*"):
                        if cand.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
                            path_for_db = cand
                            break
            if path_for_db is None and not args.dry_run:
                print("  no file after download")
                rec = upsert_person(conn, row=row, path=None, no_photo=True, dry_run=args.dry_run)
                rec.update(
                    {
                        "name": name,
                        "position": row["position"],
                        "ok": False,
                        "reason": "missing_after_download",
                        "dest": str(dest),
                        "size": 0,
                    }
                )
                results.append(rec)
                no_photo += 1
            else:
                rec = upsert_person(
                    conn,
                    row=row,
                    path=path_for_db,
                    no_photo=False,
                    dry_run=args.dry_run,
                )
                rec.update(
                    {
                        "name": name,
                        "position": row["position"],
                        "ok": True,
                        "reason": reason,
                        "dest": str(path_for_db),
                        "size": size if size else (path_for_db.stat().st_size if path_for_db and path_for_db.is_file() else 0),
                    }
                )
                results.append(rec)
                ok_photo += 1
                print("  OK", rec["action"], rec["dest"], rec.get("size"))

        if i < len(batch) and args.delay > 0:
            time.sleep(args.delay)

    summary = {
        "batch": "t0_hebei_cangzhou_leadership_20260726",
        "speed_tier": "safe",
        "list_url": LIST_URL,
        "province": PROV,
        "city": CITY,
        "unit": UNIT,
        "final_base": str(final_base),
        "people_db": str(args.people_db),
        "dry_run": bool(args.dry_run),
        "roster_n": len(batch),
        "ok_photo": ok_photo,
        "no_photo": no_photo,
        "fail": fail,
        "finished_at": utc_now(),
        "results": results,
    }
    report_path = report_dir / ("summary_dry.json" if args.dry_run else "summary_apply.json")
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    lines_path = report_dir / ("results_dry.jsonl" if args.dry_run else "results_apply.jsonl")
    with lines_path.open("w", encoding="utf-8") as f:
        for item in results:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print("\n" + "=" * 60)
    print("roster", len(batch), "ok_photo", ok_photo, "no_photo", no_photo, "fail", fail)
    print("report", report_path)
    sample_paths = [r["dest"] for r in results if r.get("ok") and r.get("dest")][:3]
    for p in sample_paths:
        print("sample", p)
    print("=" * 60)
    conn.close()
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
