# -*- coding: utf-8 -*-
"""Coverage graph rollup: unit tree + geo heat for D2I Cloud UI (C0–C2)."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cloud.paths import PROJECT_ROOT, coverage_snapshot_path, people_db_path

_CACHE: Dict[str, Any] = {"at": 0.0, "payload": None}
_CACHE_TTL_SEC = 30.0


def _domain_for(unit_name: str, province: str) -> str:
    u = unit_name or ""
    pr = province or ""
    if any(k in u for k in ("医院", "卫生", "协和", "华西", "医学院")):
        return "医院"
    if any(k in u for k in ("公安", "警察", "民警", "交警")):
        return "公安"
    if "律师" in u:
        return "律师"
    if any(k in u for k in ("链家", "中介", "房产")):
        return "房产中介"
    if "院士" in u:
        return "院士"
    if "河南" in pr:
        return "河南政府/其他"
    return "政府/其他"


def _decide_action(domain: str, people_n: int, no_path_n: int) -> Tuple[str, str, bool]:
    """Return (gap_type, next_action, ban_redownload)."""
    with_path = people_n - no_path_n
    if people_n <= 0:
        return "empty", "scout", False
    if no_path_n == 0:
        # Lawyers with full path still may need disk-only index elsewhere; registry-complete → maintain
        if domain == "律师" and people_n >= 50:
            return "g_index", "g_index", True
        return "ok", "maintain", False
    if domain == "公安" and no_path_n >= 100:
        return "g_reconcile", "reconcile", False
    if domain == "律师" and with_path == people_n:
        return "g_index", "g_index", True
    if no_path_n / people_n > 0.5:
        return "g_photo", "g_photo", False
    if people_n < 30 and no_path_n > 0:
        return "g_crawl", "g_crawl", False
    return "g_photo", "g_photo", False


def _with_path_n(people_n: int, no_path_n: int) -> int:
    return max(0, int(people_n or 0) - int(no_path_n or 0))


def _strength_score(people_n: int, no_path_n: int) -> float:
    """0–100 campaign thickness: scale × completeness.

    Tiny complete sets stay mid/low; large complete sets go high;
    large incomplete sets stay mid and can drop under gap pressure.
    """
    n = int(people_n or 0)
    missing = int(no_path_n or 0)
    if n <= 0:
        return 0.0
    with_path = _with_path_n(n, missing)
    rate = (with_path / n) if n else 0.0
    # log volume so 5 images ≠ 500 images even at 100% rate
    vol = min(1.0, math.log1p(with_path) / math.log1p(3000.0))
    gap = min(1.0, missing / 200.0)
    score = 100.0 * (0.58 * vol + 0.32 * rate * vol + 0.10 * rate) * (1.0 - 0.38 * gap)
    return round(max(0.0, min(100.0, score)), 2)


def _missing_pressure(people_n: int, no_path_n: int) -> float:
    """0–100 higher = more missing-image pressure (absolute + ratio)."""
    n = int(people_n or 0)
    missing = int(no_path_n or 0)
    if n <= 0 and missing <= 0:
        return 0.0
    abs_part = min(1.0, math.log1p(missing) / math.log1p(500.0))
    rate_miss = (missing / n) if n else (1.0 if missing else 0.0)
    return round(100.0 * (0.65 * abs_part + 0.35 * rate_miss), 2)


def _heat(people_n: int, no_path_n: int) -> str:
    """Tree/map dot: never pure green just because a tiny set is 100% complete."""
    n = int(people_n or 0)
    missing = int(no_path_n or 0)
    if n <= 0:
        return "gray"
    with_path = _with_path_n(n, missing)
    rate = (with_path / n) if n else 0.0
    if with_path == 0:
        return "red" if n >= 8 else "gray"
    # absolute backlog dominates
    if missing >= 80 or (missing >= 30 and rate < 0.55):
        return "red"
    # thin campaign: complete-but-small is yellow, not green
    if with_path < 25:
        return "yellow"
    if rate >= 0.85 and with_path >= 80 and missing < 40:
        return "green"
    if rate >= 0.7 and with_path >= 40 and missing < 60:
        return "green" if rate >= 0.9 else "yellow"
    if rate < 0.45 or missing >= 50:
        return "red"
    return "yellow"


def _metric_bundle(people_n: int, no_path_n: int) -> Dict[str, Any]:
    n = int(people_n or 0)
    missing = int(no_path_n or 0)
    with_path = _with_path_n(n, missing)
    rate = round((with_path / n) if n else 0.0, 4)
    return {
        "people_n": n,
        "with_path_n": with_path,
        "no_path_n": missing,
        "path_rate": rate,
        "strength": _strength_score(n, missing),
        "missing_pressure": _missing_pressure(n, missing),
        "heat": _heat(n, missing),
    }


def _node_id(domain: str, province: str, city: str, unit: str) -> str:
    raw = f"{domain}|{province}|{city}|{unit}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:16]


def _load_units_from_people(db_path: Path) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT coalesce(province, ''), coalesce(city, ''), coalesce(unit_name, ''),
                   count(*) AS n,
                   sum(CASE WHEN primary_image_path IS NULL OR trim(primary_image_path) = '' THEN 1 ELSE 0 END) AS no_path
            FROM people
            GROUP BY 1, 2, 3
            """
        ).fetchall()
    finally:
        conn.close()

    units: List[Dict[str, Any]] = []
    for prov, city, unit, n, no_path in rows:
        n = int(n or 0)
        no_path = int(no_path or 0)
        domain = _domain_for(unit, prov)
        gap, action, ban = _decide_action(domain, n, no_path)
        with_path = n - no_path
        units.append(
            {
                "node_id": _node_id(domain, prov or "未知", city or "未知", unit or "(空单位)"),
                "kind": "unit",
                "domain": domain,
                "province": prov or "未知",
                "city": city or "未知",
                "unit_name": unit or "(空单位)",
                "people_n": n,
                "with_path_n": with_path,
                "no_path_n": no_path,
                "path_rate": round((with_path / n) if n else 0.0, 4),
                "gap_type": gap,
                "next_action": action,
                "ban_redownload": ban,
            }
        )
    return units


def _load_units_from_snapshot(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    units = list(data.get("unit_nodes") or [])
    meta = {
        "people_total": data.get("people_total"),
        "generated_from": data.get("generated_from") or "snapshot",
    }
    return units, meta


def load_unit_nodes(force: bool = False) -> Dict[str, Any]:
    now = time.time()
    if not force and _CACHE["payload"] and (now - float(_CACHE["at"])) < _CACHE_TTL_SEC:
        return _CACHE["payload"]  # type: ignore[return-value]

    source = "empty"
    units: List[Dict[str, Any]] = []
    people_total: Optional[int] = None
    db = people_db_path()
    snap = coverage_snapshot_path()

    if db.is_file():
        try:
            units = _load_units_from_people(db)
            people_total = sum(int(u.get("people_n") or 0) for u in units)
            source = f"people:{db}"
        except Exception as exc:
            source = f"people_error:{exc}"
            units = []

    if not units and snap.is_file():
        units, meta = _load_units_from_snapshot(snap)
        people_total = meta.get("people_total")
        if people_total is None:
            people_total = sum(int(u.get("people_n") or 0) for u in units)
        source = f"snapshot:{snap}"

    _apply_overrides_to_units(units)
    payload = {
        "ok": True,
        "source": source,
        "people_total": int(people_total or 0),
        "unit_count": len(units),
        "unit_nodes": units,
        "computed_at": int(now),
    }
    _CACHE["at"] = now
    _CACHE["payload"] = payload
    return payload


def build_tree(domain: Optional[str] = None) -> Dict[str, Any]:
    raw = load_unit_nodes()
    units = list(raw.get("unit_nodes") or [])
    if domain:
        units = [u for u in units if u.get("domain") == domain]

    # domain -> province -> city -> units
    tree: Dict[str, Any] = {}
    for u in units:
        d = u.get("domain") or "其他"
        p = u.get("province") or "未知"
        c = u.get("city") or "未知"
        tree.setdefault(d, {}).setdefault(p, {}).setdefault(c, []).append(u)

    def rollup_units(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        people_n = sum(int(x.get("people_n") or 0) for x in items)
        no_path = sum(int(x.get("no_path_n") or 0) for x in items)
        metrics = _metric_bundle(people_n, no_path)
        return {
            **metrics,
            "unit_n": len(items),
        }

    domains_out: List[Dict[str, Any]] = []
    for d_name in sorted(tree.keys(), key=lambda x: (-sum(int(u.get("people_n") or 0) for p in tree[x].values() for c in p.values() for u in c), x)):
        provinces = tree[d_name]
        prov_nodes: List[Dict[str, Any]] = []
        d_units: List[Dict[str, Any]] = []
        for p_name in sorted(provinces.keys()):
            cities = provinces[p_name]
            city_nodes: List[Dict[str, Any]] = []
            p_units: List[Dict[str, Any]] = []
            for c_name in sorted(cities.keys(), key=lambda cn: -sum(int(u.get("people_n") or 0) for u in cities[cn])):
                items = sorted(cities[c_name], key=lambda u: -int(u.get("people_n") or 0))
                p_units.extend(items)
                stats = rollup_units(items)
                city_nodes.append(
                    {
                        "node_id": _node_id(d_name, p_name, c_name, "*"),
                        "kind": "city",
                        "name": c_name,
                        "domain": d_name,
                        "province": p_name,
                        "city": c_name,
                        **stats,
                        "next_action": _city_next_action(items),
                        "children": [
                            {
                                "node_id": u["node_id"],
                                "kind": "unit",
                                "name": u["unit_name"],
                                **{
                                    k: u[k]
                                    for k in (
                                        "domain",
                                        "province",
                                        "city",
                                        "unit_name",
                                        "people_n",
                                        "with_path_n",
                                        "no_path_n",
                                        "path_rate",
                                        "gap_type",
                                        "next_action",
                                        "ban_redownload",
                                    )
                                    if k in u
                                },
                                **_metric_bundle(int(u.get("people_n") or 0), int(u.get("no_path_n") or 0)),
                            }
                            for u in items
                        ],
                    }
                )
            d_units.extend(p_units)
            p_stats = rollup_units(p_units)
            prov_nodes.append(
                {
                    "node_id": _node_id(d_name, p_name, "*", "*"),
                    "kind": "province",
                    "name": p_name,
                    "domain": d_name,
                    "province": p_name,
                    **p_stats,
                    "children": city_nodes,
                }
            )
        d_stats = rollup_units(d_units)
        domains_out.append(
            {
                "node_id": _node_id(d_name, "*", "*", "*"),
                "kind": "domain",
                "name": d_name,
                "domain": d_name,
                **d_stats,
                "children": prov_nodes,
            }
        )

    return {
        "ok": True,
        "source": raw.get("source"),
        "people_total": raw.get("people_total"),
        "unit_count": raw.get("unit_count"),
        "computed_at": raw.get("computed_at"),
        "tree": domains_out,
        "interaction": {
            "loop": "select_node → ai_scout_proposal → human_confirm → d2i_queue",
            "forbid": "click_without_confirm_full_crawl",
            "default_speed_tier": "safe",
        },
    }


def _city_next_action(items: List[Dict[str, Any]]) -> str:
    actions = [str(u.get("next_action") or "") for u in items]
    for preferred in ("reconcile", "g_photo", "g_crawl", "g_index", "scout", "maintain"):
        if preferred in actions:
            return preferred
    return "scout"


def build_geo_summary() -> Dict[str, Any]:
    raw = load_unit_nodes()
    units = list(raw.get("unit_nodes") or [])
    by_city: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
        lambda: {"people_n": 0, "no_path_n": 0, "unit_n": 0}
    )
    by_prov: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"people_n": 0, "no_path_n": 0, "unit_n": 0}
    )
    for u in units:
        prov = str(u.get("province") or "未知")
        city = str(u.get("city") or "未知")
        n = int(u.get("people_n") or 0)
        np = int(u.get("no_path_n") or 0)
        by_city[(prov, city)]["people_n"] += n
        by_city[(prov, city)]["no_path_n"] += np
        by_city[(prov, city)]["unit_n"] += 1
        by_prov[prov]["people_n"] += n
        by_prov[prov]["no_path_n"] += np
        by_prov[prov]["unit_n"] += 1

    cities = []
    for (prov, city), v in by_city.items():
        n, np = v["people_n"], v["no_path_n"]
        metrics = _metric_bundle(n, np)
        cities.append(
            {
                "province": prov,
                "city": city,
                "unit_n": v["unit_n"],
                **metrics,
            }
        )
    cities.sort(key=lambda x: (-int(x.get("with_path_n") or 0), -int(x.get("people_n") or 0)))

    provinces = []
    for prov, v in by_prov.items():
        n, np = v["people_n"], v["no_path_n"]
        metrics = _metric_bundle(n, np)
        provinces.append(
            {
                "province": prov,
                "unit_n": v["unit_n"],
                **metrics,
            }
        )
    provinces.sort(key=lambda x: (-int(x.get("with_path_n") or 0), -int(x.get("people_n") or 0)))

    return {
        "ok": True,
        "source": raw.get("source"),
        "people_total": raw.get("people_total"),
        "computed_at": raw.get("computed_at"),
        "provinces": provinces,
        "cities": cities,
        "map_note": (
            "默认「战役厚度」= 有主图规模 × 完成度；"
            "几张图 100% 不会再显示成浓绿。可切换：缺图压力 / 有主图率 / 登记规模。"
        ),
        "default_metric": "strength",
        "metrics": {
            "strength": {
                "label": "战役厚度",
                "unit": "分",
                "higher": "更好",
                "desc": "有主图数量（对数）× 完成度，并扣缺图压力。小而全 ≠ 厚。",
            },
            "missing": {
                "label": "缺图压力",
                "unit": "分",
                "higher": "更糟",
                "desc": "缺主图人数绝对值 + 缺图比例；越高越红。",
            },
            "rate": {
                "label": "有主图率",
                "unit": "%",
                "higher": "更好",
                "desc": "仅完成度，不含规模（易误导，仅作对照）。",
            },
            "people": {
                "label": "登记规模",
                "unit": "人",
                "higher": "更多",
                "desc": "人物库登记人数（不管有没有主图）。",
            },
        },
        "geojson": {
            "china": "/static/geo/china_100000_full.json",
            "shandong": "/static/geo/shandong_370000_full.json",
        },
        "color_legend": {
            "gray": "无登记 / 未开战",
            "red": "缺主图压力大，或几乎没图",
            "yellow": "有一定规模但未收口，或规模仍薄",
            "green": "有主图够厚且完成度高",
        },
        "province_series": [_series_item(p["province"], p) for p in provinces],
        "city_series_by_province": _city_series_by_province(cities),
    }


def _series_item(name: str, row: Dict[str, Any]) -> Dict[str, Any]:
    n = int(row.get("people_n") or 0)
    missing = int(row.get("no_path_n") or 0)
    with_path = int(row.get("with_path_n") or _with_path_n(n, missing))
    rate = float(row.get("path_rate") or 0.0)
    strength = float(row.get("strength") if row.get("strength") is not None else _strength_score(n, missing))
    missing_pressure = float(
        row.get("missing_pressure") if row.get("missing_pressure") is not None else _missing_pressure(n, missing)
    )
    return {
        "name": name,
        # default visual value = campaign strength (not pure rate)
        "value": strength,
        "strength": strength,
        "missing_pressure": missing_pressure,
        "rate_pct": round(rate * 100, 2),
        "people_n": n,
        "with_path_n": with_path,
        "no_path_n": missing,
        "path_rate": rate,
        "heat": row.get("heat") or _heat(n, missing),
        "metrics": {
            "strength": strength,
            "missing": missing_pressure,
            "rate": round(rate * 100, 2),
            "people": n,
        },
    }


def _city_series_by_province(cities: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in cities:
        out[str(c.get("province") or "未知")].append(_series_item(str(c.get("city") or "未知"), c))
    return dict(out)


def _overrides_path() -> Path:
    path = PROJECT_ROOT / "data" / "coverage" / "node_overrides.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _intents_path() -> Path:
    path = PROJECT_ROOT / "data" / "coverage" / "intents.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_overrides() -> Dict[str, Any]:
    path = _overrides_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_override(node_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    data = load_overrides()
    cur = data.get(node_id) if isinstance(data.get(node_id), dict) else {}
    cur.update(patch)
    cur["updated_at"] = int(time.time())
    data[node_id] = cur
    _overrides_path().write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return cur


def append_intent(record: Dict[str, Any]) -> Dict[str, Any]:
    rec = dict(record)
    rec.setdefault("intent_id", f"ci_{uuid.uuid4().hex[:12]}")
    rec.setdefault("created_at", int(time.time()))
    with _intents_path().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


def _apply_overrides_to_units(units: List[Dict[str, Any]]) -> None:
    overrides = load_overrides()
    if not overrides:
        return
    for u in units:
        ov = overrides.get(u.get("node_id") or "")
        if not isinstance(ov, dict):
            continue
        if "ban_redownload" in ov:
            u["ban_redownload"] = bool(ov["ban_redownload"])
        if ov.get("next_action"):
            u["next_action"] = ov["next_action"]
        if ov.get("gap_type"):
            u["gap_type"] = ov["gap_type"]


def get_node(node_id: str) -> Optional[Dict[str, Any]]:
    raw = load_unit_nodes()
    for u in raw.get("unit_nodes") or []:
        if u.get("node_id") == node_id:
            return {
                "ok": True,
                "node": _enrich_node_for_ui(u),
                "actions": _actions_for_node(u),
                "glossary": _ui_glossary(),
                "interaction": {
                    "loop": "选中节点 → AI 侦察提案 → 人确认 → 建队列",
                    "default_speed_tier": "safe",
                },
            }
    # synthetic city/domain ids not stored — search tree
    tree = build_tree()
    found = _find_in_tree(tree.get("tree") or [], node_id)
    if found:
        return {
            "ok": True,
            "node": _enrich_node_for_ui(found),
            "actions": _actions_for_node(found),
            "glossary": _ui_glossary(),
            "interaction": {
                "loop": "选中节点 → AI 侦察提案 → 人确认 → 建队列",
                "default_speed_tier": "safe",
            },
        }
    return None


def _ui_glossary() -> Dict[str, str]:
    return {
        "image_path": (
            "主图路径：人物库字段 primary_image_path，指向角色肖像盘上的图片文件。"
            "有路径≈库里认为这人有图；无路径≈库里还没挂上图（未必表示盘上真没有）。"
        ),
        "why_shown": (
            "覆盖图谱用「有主图 / 缺主图」衡量战役完成度，所以结果里会显示有图人数、缺图人数、有图率。"
        ),
    }


def _enrich_node_for_ui(node: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(node)
    # F7: unit dicts from legacy snapshots may predate the "kind" field;
    # tree nodes (domain/province/city) always carry their own kind
    out.setdefault("kind", "unit")
    with_path = int(out.get("with_path_n") or 0)
    no_path = int(out.get("no_path_n") or 0)
    people = int(out.get("people_n") or 0)
    out["with_image_n"] = with_path
    out["missing_image_n"] = no_path
    out["image_rate"] = out.get("path_rate")
    out["image_rate_pct"] = round(float(out.get("path_rate") or 0) * 100, 1) if people else 0.0
    if out.get("strength") is None:
        out["strength"] = _strength_score(people, no_path)
    if out.get("missing_pressure") is None:
        out["missing_pressure"] = _missing_pressure(people, no_path)
    if not out.get("heat"):
        out["heat"] = _heat(people, no_path)
    out["next_action_label"] = _action_label(out.get("next_action"))
    out["gap_type_label"] = {
        "ok": "已较完整",
        "empty": "尚无人物",
        "g_photo": "缺主图",
        "g_crawl": "覆盖偏薄",
        "g_index": "只需索引",
        "g_reconcile": "需对账",
        "ban": "禁止再下",
    }.get(str(out.get("gap_type") or ""), str(out.get("gap_type") or "—"))
    out["status_summary"] = (
        f"{people} 人登记 · 有主图 {with_path} · 缺主图 {no_path}"
        + f" · 厚度 {float(out.get('strength') or 0):.0f}"
        + (f" · 建议：{out['next_action_label']}" if out.get("next_action") else "")
    )
    return out


def _find_in_tree(nodes: List[Dict[str, Any]], node_id: str) -> Optional[Dict[str, Any]]:
    for n in nodes:
        if n.get("node_id") == node_id:
            return n
        kids = n.get("children") or []
        hit = _find_in_tree(kids, node_id)
        if hit:
            return hit
    return None


def _action_label(code: str) -> str:
    return {
        "scout": "先侦察入口",
        "g_photo": "补缺图",
        "g_crawl": "新开抓取",
        "g_index": "只入库索引",
        "reconcile": "先对账",
        "maintain": "维持观察",
        "ban": "禁止再下载",
        "unban": "允许再下载",
    }.get(str(code or "").strip(), str(code or "—"))


def _actions_for_node(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    ban = bool(node.get("ban_redownload"))
    action = str(node.get("next_action") or "scout")
    items = [
        {
            "id": "scout",
            "label": "先侦察入口",
            "enabled": True,
            "note": "让 AI 找网页入口与规模，只出方案，不会直接下载",
        },
        {
            "id": "g_photo",
            "label": "补缺图",
            "enabled": not ban and action in ("g_photo", "g_crawl", "reconcile", "scout"),
            "note": "人名已在库，但还没有主图路径 → 排队补图（默认安全档，确认后才建任务）",
        },
        {
            "id": "g_crawl",
            "label": "新开抓取",
            "enabled": not ban and action in ("g_crawl", "scout", "g_photo"),
            "note": "这个单位/地区还薄，需要从入口页重新抓名单和照片",
        },
        {
            "id": "g_index",
            "label": "只入库索引",
            "enabled": True,
            "note": "盘上图可能已有，只把库索引对齐；禁止再下载",
        },
        {
            "id": "reconcile",
            "label": "先对账",
            "enabled": action == "reconcile" or str(node.get("gap_type") or "") == "g_reconcile",
            "note": "库与盘不一致（如临沂公安）时，先核对再决定是否下载",
        },
        {
            "id": "ban",
            "label": "禁止再下载",
            "enabled": not ban,
            "note": "标记此节点：只许索引/对账，不许再开下载任务",
        },
        {
            "id": "unban",
            "label": "允许再下载",
            "enabled": ban,
            "note": "取消禁止标记，之后才能再派补图/新抓",
        },
    ]
    return items


def scout_stub(node_id: str) -> Dict[str, Any]:
    """C0/C3-prep: return a structured proposal without starting crawl."""
    detail = get_node(node_id)
    if not detail:
        raise KeyError(f"node not found: {node_id}")
    node = detail["node"]
    return {
        "ok": True,
        "status": "proposal_only",
        "node_id": node_id,
        "proposal": {
            "title": f"侦察：{node.get('name') or node.get('unit_name') or node_id}",
            "suggested_next_action": node.get("next_action") or "scout",
            "suggested_speed_tier": "safe",
            "estimated_people": node.get("people_n"),
            "no_path_n": node.get("no_path_n"),
            "ban_redownload": bool(node.get("ban_redownload")),
            "entry_url_candidates": [],
            "risks": [
                "C3 前为 stub：尚未接 Hermes 读站；确认后才可 enqueue",
                "若 next_action=reconcile，禁止盲下",
                "若 ban_redownload，只允许 g_index",
            ],
            "requires_human_confirm": True,
        },
        "interaction": detail.get("interaction"),
    }


def enqueue_from_node(
    node_id: str,
    *,
    action: str,
    confirm: bool = False,
    start_url: str = "",
    template_id: str = "",
    template_path: str = "",
    speed_tier: str = "safe",
    start: bool = False,
    notes: str = "",
) -> Dict[str, Any]:
    """C2: human-confirmed node action → intent and/or D2I queue (default not auto-start)."""
    detail = get_node(node_id)
    if not detail:
        raise KeyError(f"node not found: {node_id}")
    node = detail["node"]
    act = str(action or "").strip().lower()
    if act not in {"g_photo", "g_crawl", "g_index", "reconcile", "ban", "unban"}:
        raise ValueError(f"unsupported action: {action}")
    if not confirm:
        raise ValueError("confirm=true required (点节点→提案→确认→队列)")

    ban = bool(node.get("ban_redownload"))
    if act in {"g_photo", "g_crawl"} and ban:
        raise ValueError("ban_redownload: only g_index / ban actions allowed")

    label = node.get("name") or node.get("unit_name") or node_id
    base_notes = (
        f"[coverage] action={act} node={node_id} "
        f"domain={node.get('domain')} province={node.get('province')} "
        f"city={node.get('city')} unit={node.get('unit_name') or label}"
    )
    merged_notes = f"{base_notes}\n{notes}".strip() if notes else base_notes

    if act == "ban":
        ov = save_override(node_id, {"ban_redownload": True, "next_action": "g_index", "gap_type": "ban"})
        _CACHE["payload"] = None
        intent = append_intent(
            {
                "kind": "ban",
                "node_id": node_id,
                "node": {k: node.get(k) for k in ("domain", "province", "city", "unit_name", "people_n", "no_path_n")},
                "notes": merged_notes,
            }
        )
        return {
            "ok": True,
            "mode": "override",
            "action": act,
            "override": ov,
            "intent": intent,
            "queue": None,
            "message": "已标记禁下；不创建下载队列",
        }

    if act == "unban":
        ov = save_override(node_id, {"ban_redownload": False})
        _CACHE["payload"] = None
        intent = append_intent({"kind": "unban", "node_id": node_id, "notes": merged_notes})
        return {
            "ok": True,
            "mode": "override",
            "action": act,
            "override": ov,
            "intent": intent,
            "queue": None,
            "message": "已取消禁下标记",
        }

    if act in {"g_index", "reconcile"}:
        intent = append_intent(
            {
                "kind": act,
                "node_id": node_id,
                "node": {
                    k: node.get(k)
                    for k in (
                        "domain",
                        "province",
                        "city",
                        "unit_name",
                        "people_n",
                        "with_path_n",
                        "no_path_n",
                        "next_action",
                        "gap_type",
                    )
                },
                "notes": merged_notes,
                "status": "accepted",
            }
        )
        return {
            "ok": True,
            "mode": "intent_only",
            "action": act,
            "intent": intent,
            "queue": None,
            "message": (
                "已记录只索引意图（不下载）"
                if act == "g_index"
                else "已记录对账意图（先对账，禁止盲下）"
            ),
        }

    # g_photo / g_crawl → real queue, never auto-start unless start=true after confirm
    url = str(start_url or "").strip()
    if not url and not str(template_id or "").strip() and not str(template_path or "").strip():
        raise ValueError(
            "g_photo/g_crawl 需要 start_url 或 template_id；可先 AI 侦察再填入口"
        )

    from cloud import queue_service

    queue = queue_service.create_queue(
        start_url=url,
        template_id=template_id,
        template_path=template_path,
        name=f"coverage-{act}-{label}"[:80],
        speed_tier=speed_tier or "safe",
        speed_tier_reason=f"coverage node {node_id} action={act}",
        notes=merged_notes,
        start=bool(start),
        allow_turbo=False,
    )
    intent = append_intent(
        {
            "kind": act,
            "node_id": node_id,
            "queue_id": queue.get("id"),
            "start_url": url or queue.get("start_url"),
            "template_id": queue.get("template_id"),
            "started": bool(start),
            "notes": merged_notes,
            "status": "queued",
        }
    )
    return {
        "ok": True,
        "mode": "queue",
        "action": act,
        "intent": intent,
        "queue": queue,
        "message": (
            f"已创建队列 {queue.get('id')}（已启动）"
            if start
            else f"已创建队列 {queue.get('id')}（未启动，可在队列台 start）"
        ),
    }


def export_snapshot(dest: Optional[Path] = None) -> Path:
    """Write unit_nodes snapshot for offline / NAS-less dev."""
    raw = load_unit_nodes(force=True)
    path = dest or coverage_snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {
        "generated_from": raw.get("source"),
        "people_total": raw.get("people_total"),
        "unit_nodes": raw.get("unit_nodes"),
        "exported_at": int(time.time()),
    }
    path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    return path
