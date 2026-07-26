# -*- coding: utf-8 -*-
"""A5 模板库 lint(总纲 §2 M1+M3):校验 scraper/templates/ 全库达标。

校验点:
- 每个 *.json 可解析(UTF-8)且顶层为 dict;
- crawl.speed_tier 存在且 ∈ {safe, standard, turbo};
- rules.admin 若存在则结构合法:dict,province/city 均为非空字符串;
- rules.required_fields 不含 image_url("无图不丢人"),且至少含 name 与
  detail_url(模板抽取合同 §2);
- site_name 全库唯一(M3 加校验)。已知的英文名/中文名完全重复对暂列
  KNOWN_DUPLICATE_PAIRS 豁免(收敛方式待人工拍板,按总纲 merge+stub,
  本轮不删文件);豁免条件是两文件字节级完全一致,一旦漂移立即报错。

纯 python assert,可被 pytest 收集,也可直接 `python tests/test_a5_templates_lint.py`。
"""

import json
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "scraper" / "templates"

ALLOWED_SPEED_TIERS = {"safe", "standard", "turbo"}

# 完全重复模板对(字节级一致),等待人工决定留哪个后收敛;收敛完成后清空此表
KNOWN_DUPLICATE_PAIRS = [
    ("mps_yinglie_qingming2020.json", "公安英烈墙_2020清明祭扫英烈.json"),
    ("mps_yinglie_qingming2021.json", "公安英烈墙_2021清明祭扫英烈.json"),
]


def _template_files():
    files = sorted(TEMPLATES_DIR.glob("*.json"))
    assert files, f"no template json found under {TEMPLATES_DIR}"
    return files


def _load_all():
    loaded = {}
    for path in _template_files():
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        assert isinstance(data, dict), f"{path.name}: top-level must be a JSON object"
        loaded[path.name] = data
    return loaded


def test_templates_json_parse():
    """全部模板 JSON 可解析,顶层为 dict。"""
    loaded = _load_all()
    assert len(loaded) == len(_template_files())


def test_speed_tier_valid():
    """crawl.speed_tier 必须存在且取值合法(总纲 M3:缺省一律补 safe)。"""
    for name, data in _load_all().items():
        crawl = data.get("crawl")
        assert isinstance(crawl, dict), f"{name}: missing crawl block"
        tier = crawl.get("speed_tier")
        assert tier in ALLOWED_SPEED_TIERS, (
            f"{name}: crawl.speed_tier={tier!r} not in {sorted(ALLOWED_SPEED_TIERS)}"
        )


def test_admin_structure():
    """rules.admin 若存在则 province/city 必须为非空字符串(合同 §4)。"""
    for name, data in _load_all().items():
        rules = data.get("rules")
        assert isinstance(rules, dict), f"{name}: missing rules block"
        if "admin" not in rules:
            continue  # 全国性站点允许缺省,待人工补
        admin = rules["admin"]
        assert isinstance(admin, dict), f"{name}: rules.admin must be an object"
        for key in ("province", "city"):
            value = admin.get(key)
            assert isinstance(value, str) and value.strip(), (
                f"{name}: rules.admin.{key} must be a non-empty string, got {value!r}"
            )


def test_required_fields_contract():
    """required_fields 至少含 name+detail_url,且不得含 image_url(无图不丢人)。"""
    for name, data in _load_all().items():
        rules = data.get("rules")
        assert isinstance(rules, dict), f"{name}: missing rules block"
        required = rules.get("required_fields")
        assert isinstance(required, list), f"{name}: rules.required_fields must be a list"
        assert "image_url" not in required, (
            f"{name}: image_url must not be in required_fields (无图不丢人)"
        )
        for field in ("name", "detail_url"):
            assert field in required, (
                f"{name}: required_fields must contain {field!r}, got {required!r}"
            )


def test_site_name_unique():
    """site_name 全库唯一;已知重复对豁免,但必须保持字节级一致。"""
    known_pairs = {frozenset(pair) for pair in KNOWN_DUPLICATE_PAIRS}
    by_site = {}
    for name, data in _load_all().items():
        site_name = str(data.get("site_name", "") or "").strip()
        assert site_name, f"{name}: site_name must be non-empty"
        by_site.setdefault(site_name, []).append(name)
    for site_name, names in sorted(by_site.items()):
        if len(names) == 1:
            continue
        group = frozenset(names)
        assert group in known_pairs, (
            f"site_name {site_name!r} duplicated across {sorted(names)} "
            "(not an allowlisted known-duplicate pair)"
        )
        first, second = sorted(names)
        assert (TEMPLATES_DIR / first).read_bytes() == (TEMPLATES_DIR / second).read_bytes(), (
            f"known duplicate pair drifted apart: {first} vs {second}; "
            "merge them instead of editing one side"
        )


if __name__ == "__main__":
    for fn in (
        test_templates_json_parse,
        test_speed_tier_valid,
        test_admin_structure,
        test_required_fields_contract,
        test_site_name_unique,
    ):
        fn()
        print(f"PASS {fn.__name__}")
