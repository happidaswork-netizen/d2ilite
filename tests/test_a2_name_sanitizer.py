# -*- coding: utf-8 -*-
"""A2 姓名净化统一测试:严格版闸门 + 三入口共用同一模块对象。

纯 python assert 脚本,直接 `python tests/test_a2_name_sanitizer.py` 可跑,
也兼容 pytest 收集。仅本地导入,无网络 / 无抓取副作用。
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = REPO_ROOT / "scraper"
for _p in (str(REPO_ROOT), str(SCRAPER_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import name_sanitizer as ns  # noqa: E402


def test_reject_bare_title():
    # 合同 §7:裸职务不是人名(旧 browser 宽松版会放过 "市长")
    for bad in ("市长", "副市长", "常务副市长", "代理市长", "书记", "主任", "局长"):
        assert not ns._is_usable_person_name(bad), bad
        assert ns._prefer_person_name(bad, "") == "", bad


def test_reject_nav_chrome():
    # 沧州反例:导航词 "魅力狮城" 旧版会整词回落为人名
    assert not ns._is_usable_person_name("魅力狮城")
    assert ns._extract_person_name_token("魅力狮城") == ""
    assert ns._prefer_person_name("魅力狮城", "") == ""
    # detail/seed 都是垃圾时必须返回空串,不准回落原始文本
    assert ns._prefer_person_name("魅力狮城", "沧州市人民政府") == ""
    assert ns._prefer_person_name("首页", "网站地图") == ""


def test_extract_name_from_html_fragment():
    assert ns._prefer_person_name("<p>副市长 姜桂海</p>", "") == "姜桂海"


def test_extract_name_from_title_prefix_line():
    assert ns._prefer_person_name("副市长 姜桂海", "") == "姜桂海"
    assert ns._extract_person_name_token("副市长 姜桂海") == "姜桂海"
    assert ns._prefer_person_name("市委副书记、代理市长 刘勇", "") == "刘勇"


def test_accept_real_person_names():
    for good in ("姜桂海", "王少杰", "王晓玲", "尹卫江", "戴树胜", "刘勇"):
        assert ns._is_usable_person_name(good), good
        assert ns._prefer_person_name(good, "") == good, good


def test_seed_fallback_only_when_usable():
    # detail 名是垃圾、seed 名合法 → 用 seed;两边都垃圾 → 空串
    assert ns._prefer_person_name("政务公开", "王少杰") == "王少杰"
    assert ns._prefer_person_name("政务公开", "市长") == ""


def test_length_hard_constraint_2_to_4():
    assert not ns._is_usable_person_name("王")  # 1 字
    assert ns._prefer_person_name("王", "") == ""
    assert not ns._is_usable_person_name("欧阳娜娜娜")  # 5 字
    assert ns._prefer_person_name("欧阳娜娜娜", "") == ""
    assert ns._is_usable_person_name("刘勇")  # 2 字下界
    assert ns._is_usable_person_name("欧阳修文")  # 4 字上界


def test_same_module_object_across_entrances():
    import public_profile_spider as spider
    import run_public_scraper as runner

    for attr in ("_prefer_person_name", "_is_usable_person_name", "_extract_person_name_token", "_normalize_text"):
        assert getattr(spider, attr) is getattr(ns, attr), attr
        assert getattr(runner, attr) is getattr(ns, attr), attr
        assert getattr(spider, attr).__module__ == getattr(runner, attr).__module__, attr

    # llm_enricher 的第三份 _normalize_text 也已统一(可选依赖缺失时跳过)
    try:
        import llm_enricher
    except Exception:
        llm_enricher = None
    if llm_enricher is not None:
        assert llm_enricher._normalize_text is ns._normalize_text


if __name__ == "__main__":
    for _name in sorted(k for k in dir() if k.startswith("test_")):
        globals()[_name]()
        print(f"PASS {_name}")
    print("ALL PASS")
