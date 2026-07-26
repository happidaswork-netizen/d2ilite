# -*- coding: utf-8 -*-
"""
Long-running collector for official public profile pages.

This is intentionally registry-first: every discovered public person profile is
stored in SQLite even when no usable image is found. Downloaded images are a
separate asset table linked back to the person row and source URL.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import html as html_lib
import json
import mimetypes
import os
import random
import re
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import robotparser
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from lxml import html
from PIL import Image

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from metadata_writer import write_xmp_metadata
except Exception:  # pragma: no cover - metadata writing is best-effort.
    write_xmp_metadata = None  # type: ignore[assignment]


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"
)

PROFILE_KEYWORDS = (
    "领导",
    "政府领导",
    "机构领导",
    "领导信息",
    "领导之窗",
    "领导简介",
    "领导简历",
    "班子成员",
    "负责人",
    "分管",
    "分工",
    "简历",
    "人物库",
    "组织人事",
    "任免",
    "公示",
    "图/简历",
    "历任",
    "曾任",
    "原任",
)

STRONG_PROFILE_PAGE_KEYWORDS = (
    "政府领导",
    "市政府领导",
    "县政府领导",
    "区政府领导",
    "机构领导",
    "领导信息",
    "领导之窗",
    "领导简介",
    "领导简历",
    "领导成员",
    "班子成员",
    "班子信息",
    "个人简历",
    "工作分工",
    "党政领导人物库",
    "人物库",
    "组织人事",
    "任免",
    "任前公示",
    "图/简历",
    "历任",
    "曾任",
    "原任",
)

DISCOVERY_KEYWORDS = PROFILE_KEYWORDS + (
    "政府信息公开",
    "信息公开",
    "机构概况",
    "机构职能",
    "机构设置",
    "政府组成部门",
    "部门网站",
    "区县政府",
    "县市区",
    "街道",
    "乡镇",
    "派出所",
    "公安",
    "任免",
    "人事",
    "公示",
    "历任",
    "曾任",
    "原任",
)

HIGH_VALUE_LINK_KEYWORDS = (
    "政府领导",
    "市政府领导",
    "县政府领导",
    "区政府领导",
    "机构领导",
    "领导信息",
    "领导之窗",
    "领导简介",
    "领导简历",
    "领导成员",
    "班子成员",
    "领导班子",
    "领导分工",
    "工作分工",
    "个人简历",
    "简历",
    "图/简历",
    "人物库",
    "组织人事",
    "人事任免",
    "任免",
    "任前公示",
    "拟任",
    "干部任前",
)

LOW_VALUE_CONTEXT_KEYWORDS = (
    "领导活动",
    "政策文件",
    "政府公报",
    "政府规章",
    "发展规划",
    "专项规划",
    "区域规划",
    "统计公报",
    "意见征集",
    "会议直播",
    "会议内容",
    "市长信箱",
    "政府采购",
    "公共文化",
    "社会救助",
    "社会福利",
    "社会保险",
    "社会组织",
    "稳岗就业",
    "医疗卫生",
    "住房保障",
    "治安管理",
    "市管企业",
    "生态环境",
    "国资国企",
    "公共卫生",
    "市政建设",
    "安全生产",
    "房屋征收",
    "征地信息",
    "部门目录",
    "机构职能",
    "活动计划",
    "活动日历",
    "重点领域",
    "最新公开",
    "最新公报",
    "区县动态",
    "部门动态",
    "市级微博",
    "用户中心",
    "法人服务",
    "友情链接",
)

CITY_OPEN_SEARCH_PATTERNS = (
    "site:{host} {city} 政府领导 简历 照片",
    "site:{host} {city} 领导信息 姓名 职务 简历",
    "site:{host} {city} 领导班子 工作分工 简历",
    "site:{host} {city} 机构领导 分工 简历",
    "site:{host} {city} 任免 公示 图 简历",
    "site:{host} {city} 公安局 领导信息 工作分工",
)

SKIP_URL_PARTS = (
    "javascript:",
    "mailto:",
    "tel:",
    "/video/",
    "/shipin/",
    "/zt/",
    "/special/",
    "/hd/",
    "/hudong/",
    "/search",
    "/rss",
    "/icity/result",
    "/wza/",
    "ssoLoginOut",
    "jpaas-jis-sso-server",
    "tysfrz.isdapp.shandong.gov.cn",
    "zwfw.sd.gov.cn",
    "/zcwjk/",
    "/yhzx/",
    "/rightslist/",
    "ggzyjy.",
    "credit.",
    "wfdata.sd.gov.cn",
)

SKIP_IMAGE_PARTS = (
    "logo",
    "icon",
    "banner",
    "bg",
    "background",
    "weixin",
    "qrcode",
    "qr",
    "ewm",
    "spacer",
    "blank",
    "btn",
    "nav",
    "top",
    "footer",
    "menu",
    "unitstyle",
    "search",
    "dropdown",
    "default/images",
    "wzimages",
    "/template/",
    "wzdt",
    "wza",
    "yhzx",
    "conac",
    "counter",
    "/c.gif",
)

SKIP_IMAGE_ALT_PARTS = (
    "微信",
    "扫一扫",
    "二维码",
    "客户端",
    "人民网",
    "logo",
    "网站标识",
)

TRUSTED_PROFILE_SUPPLEMENT_DOMAINS = (
    "district.ce.cn",
    "renshi.people.com.cn",
)

SEARCH_RESULT_DOMAINS = (
    "www.bing.com",
    "cn.bing.com",
)

REGION_CONTEXT_TERMS = (
    "山东",
    "济南",
    "青岛",
    "淄博",
    "枣庄",
    "东营",
    "烟台",
    "潍坊",
    "济宁",
    "泰安",
    "威海",
    "日照",
    "临沂",
    "德州",
    "聊城",
    "滨州",
    "菏泽",
)

REGION_CITY_NAMES = (
    "济南市",
    "青岛市",
    "淄博市",
    "枣庄市",
    "东营市",
    "烟台市",
    "潍坊市",
    "济宁市",
    "泰安市",
    "威海市",
    "日照市",
    "临沂市",
    "德州市",
    "聊城市",
    "滨州市",
    "菏泽市",
)

TITLE_WORDS = (
    "省委书记",
    "省长",
    "副省长",
    "秘书长",
    "市委书记",
    "市委副书记",
    "市长",
    "副市长",
    "县委书记",
    "县长",
    "副县长",
    "区委书记",
    "区长",
    "副区长",
    "局长",
    "副局长",
    "主任",
    "副主任",
    "书记",
    "副书记",
        "政委",
        "副政委",
        "秘书长",
        "副秘书长",
        "支队长",
    "大队长",
    "所长",
    "副所长",
    "党组书记",
    "党组成员",
    "党委书记",
    "党委委员",
    "负责人",
)

REGION_CONFIGS: Dict[str, Dict[str, Any]] = {
    "shandong": {
        "province": "山东省",
        "project_name": "山东公开官员",
        "scope_domains": [
            "shandong.gov.cn",
            "sd.gov.cn",
            "jinan.gov.cn",
            "qingdao.gov.cn",
            "zibo.gov.cn",
            "zaozhuang.gov.cn",
            "dongying.gov.cn",
            "yantai.gov.cn",
            "weifang.gov.cn",
            "jining.gov.cn",
            "taian.gov.cn",
            "weihai.gov.cn",
            "rizhao.gov.cn",
            "linyi.gov.cn",
            "dezhou.gov.cn",
            "liaocheng.gov.cn",
            "binzhou.gov.cn",
            "heze.gov.cn",
            "district.ce.cn",
            "renshi.people.com.cn",
        ],
        "seeds": [
            ("山东省", "", "", "province", "山东省人民政府", "https://www.shandong.gov.cn/"),
            ("山东省", "济南市", "", "city", "济南市人民政府", "https://www.jinan.gov.cn/"),
            ("山东省", "青岛市", "", "city", "青岛市人民政府", "https://www.qingdao.gov.cn/"),
            ("山东省", "淄博市", "", "city", "淄博市人民政府", "https://www.zibo.gov.cn/"),
            ("山东省", "枣庄市", "", "city", "枣庄市人民政府", "https://www.zaozhuang.gov.cn/"),
            ("山东省", "东营市", "", "city", "东营市人民政府", "https://www.dongying.gov.cn/"),
            ("山东省", "烟台市", "", "city", "烟台市人民政府", "https://www.yantai.gov.cn/"),
            ("山东省", "潍坊市", "", "city", "潍坊市人民政府", "https://www.weifang.gov.cn/"),
            ("山东省", "济宁市", "", "city", "济宁市人民政府", "https://www.jining.gov.cn/"),
            ("山东省", "泰安市", "", "city", "泰安市人民政府", "https://www.taian.gov.cn/"),
            ("山东省", "威海市", "", "city", "威海市人民政府", "https://www.weihai.gov.cn/"),
            ("山东省", "日照市", "", "city", "日照市人民政府", "https://www.rizhao.gov.cn/"),
            ("山东省", "临沂市", "", "city", "临沂市人民政府", "https://www.linyi.gov.cn/"),
            ("山东省", "德州市", "", "city", "德州市人民政府", "https://www.dezhou.gov.cn/"),
            ("山东省", "聊城市", "", "city", "聊城市人民政府", "https://www.liaocheng.gov.cn/"),
            ("山东省", "滨州市", "", "city", "滨州市人民政府", "https://www.binzhou.gov.cn/"),
            ("山东省", "菏泽市", "", "city", "菏泽市人民政府", "https://www.heze.gov.cn/"),
            ("山东省", "济南市", "", "department", "济南市人民政府办公厅", "https://www.jinan.gov.cn/col98470/art/2025/art_98470_4986068.html"),
            ("山东省", "济南市", "", "agency", "济南市公安局", "https://jnga.jinan.gov.cn/col86386/art/2026/art_86386_4783913.html"),
            ("山东省", "", "", "media_profile", "中国经济网山东党政领导人物库", "http://district.ce.cn/zt/rwk/sf/sd/index.shtml"),
            ("山东省", "济南市", "", "media_profile", "人民网组织人事山东图简历", "http://renshi.people.com.cn/n1/2018/0514/c139617-29985562.html"),
        ],
        "open_search_queries": [
            'site:gov.cn 山东 领导信息 姓名 职务 简历 图片',
            'site:gov.cn 山东 领导班子 机构领导 分工 简历',
            'site:gov.cn 山东 任免 公示 简历 照片',
            'site:gov.cn 山东 政府领导 简历 照片',
            'site:gov.cn 山东 机构领导 工作分工 简历',
            'site:gov.cn 山东 公安局 领导信息 工作分工',
            'site:jinan.gov.cn 领导信息 姓名 职务 简历',
            'site:jnga.jinan.gov.cn 领导信息 领导简历 工作分工',
            'site:district.ce.cn 山东 党政领导 人物库 简历',
            'site:renshi.people.com.cn 山东 任 图/简历',
            '山东 历任 曾任 原任 领导 简历 照片',
        ],
    },
    "henan": {
        "province": "河南省",
        "project_name": "河南公开官员",
        "scope_domains": ["henan.gov.cn"],
        "seeds": [
            ("河南省", "", "", "province", "河南省人民政府", "https://www.henan.gov.cn/"),
            ("河南省", "郑州市", "", "city", "郑州市人民政府", "https://www.zhengzhou.gov.cn/"),
            ("河南省", "开封市", "", "city", "开封市人民政府", "https://www.kaifeng.gov.cn/"),
            ("河南省", "洛阳市", "", "city", "洛阳市人民政府", "https://www.luoyang.gov.cn/"),
            ("河南省", "平顶山市", "", "city", "平顶山市人民政府", "https://www.pds.gov.cn/"),
            ("河南省", "安阳市", "", "city", "安阳市人民政府", "https://www.anyang.gov.cn/"),
            ("河南省", "鹤壁市", "", "city", "鹤壁市人民政府", "https://www.hebi.gov.cn/"),
            ("河南省", "新乡市", "", "city", "新乡市人民政府", "https://www.xinxiang.gov.cn/"),
            ("河南省", "焦作市", "", "city", "焦作市人民政府", "https://www.jiaozuo.gov.cn/"),
            ("河南省", "濮阳市", "", "city", "濮阳市人民政府", "https://www.puyang.gov.cn/"),
            ("河南省", "许昌市", "", "city", "许昌市人民政府", "https://www.xuchang.gov.cn/"),
            ("河南省", "漯河市", "", "city", "漯河市人民政府", "https://www.luohe.gov.cn/"),
            ("河南省", "三门峡市", "", "city", "三门峡市人民政府", "https://www.smx.gov.cn/"),
            ("河南省", "南阳市", "", "city", "南阳市人民政府", "https://www.nanyang.gov.cn/"),
            ("河南省", "商丘市", "", "city", "商丘市人民政府", "https://www.shangqiu.gov.cn/"),
            ("河南省", "信阳市", "", "city", "信阳市人民政府", "https://www.xinyang.gov.cn/"),
            ("河南省", "周口市", "", "city", "周口市人民政府", "https://www.zhoukou.gov.cn/"),
            ("河南省", "驻马店市", "", "city", "驻马店市人民政府", "https://www.zhumadian.gov.cn/"),
            ("河南省", "济源示范区", "", "city", "济源示范区管委会", "https://www.jiyuan.gov.cn/"),
        ],
    },
}

CENTRAL_OR_OUT_OF_SCOPE_DOMAINS = {
    "www.gov.cn",
    "gov.cn",
    "www.npc.gov.cn",
    "npc.gov.cn",
    "www.cppcc.gov.cn",
    "cppcc.gov.cn",
    "www.12371.cn",
    "12371.cn",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_text(value: Any) -> str:
    text = html_lib.unescape(str(value or ""))
    text = re.sub(r"[\u00a0\u3000\t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ ]{2,}", " ", text)
    return text.strip()


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip()


def sanitize_filename(value: str, fallback: str = "未命名") -> str:
    value = compact_text(value) or fallback
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    value = value.strip(" ._")
    return value[:90] or fallback


def normalize_url(url: str, base: str = "") -> str:
    if not url:
        return ""
    absolute = urljoin(base, str(url).strip())
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""
    query_pairs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if not k.lower().startswith(("utm_", "spm", "from"))
    ]
    query = urlencode(query_pairs, doseq=True)
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    return urlunparse((parsed.scheme, parsed.netloc.lower(), path, "", query, ""))


def parent_candidate_urls(url: str) -> List[str]:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    if not parsed.netloc:
        return []
    path = parsed.path or "/"
    results: List[str] = []
    art_match = re.search(r"^(.*/art/\d{4}/)(?:art_[^/]+\.html|[^/]+\.html)$", path)
    if art_match:
        results.append(urlunparse((parsed.scheme, parsed.netloc, art_match.group(1), "", "", "")))
    col_art_match = re.search(r"^(.*/col/col\d+/)art/\d{4}/", path)
    if col_art_match:
        results.append(urlunparse((parsed.scheme, parsed.netloc, col_art_match.group(1) + "index.html", "", "", "")))
    segments = [seg for seg in path.split("/") if seg]
    for cut in range(len(segments) - 1, 0, -1):
        parent_path = "/" + "/".join(segments[:cut]) + "/"
        results.append(urlunparse((parsed.scheme, parsed.netloc, parent_path, "", "", "")))
        if len(results) >= 4:
            break
    return list(dict.fromkeys(url for url in results if url and url != normalized and not url_should_skip(url)))


def sibling_candidate_urls(url: str) -> List[str]:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    if not parsed.netloc:
        return []
    path = parsed.path or ""
    results: List[str] = []
    match = re.search(r"^(.*?/)(\d+)(\.html)$", path)
    if match:
        prefix, number, suffix = match.groups()
        value = int(number)
        width = len(number)
        for delta in (-3, -2, -1, 1, 2, 3):
            if value + delta <= 0:
                continue
            sibling = f"{prefix}{value + delta:0{width}d}{suffix}"
            results.append(urlunparse((parsed.scheme, parsed.netloc, sibling, "", "", "")))
    match = re.search(r"^(.*?/index_)(\d+)(\.html)$", path)
    if match:
        prefix, number, suffix = match.groups()
        value = int(number)
        width = len(number)
        for delta in (-2, -1, 1, 2):
            if value + delta <= 0:
                continue
            sibling = f"{prefix}{value + delta:0{width}d}{suffix}"
            results.append(urlunparse((parsed.scheme, parsed.netloc, sibling, "", "", "")))
    return list(dict.fromkeys(url for url in results if url and url != normalized and not url_should_skip(url)))


def same_profile_directory(source_url: str, candidate_url: str) -> bool:
    source = urlparse(normalize_url(source_url))
    candidate = urlparse(normalize_url(candidate_url))
    if source.netloc != candidate.netloc:
        return False
    source_parts = [part for part in source.path.split("/") if part]
    candidate_parts = [part for part in candidate.path.split("/") if part]
    if len(source_parts) < 3 or len(candidate_parts) < 2:
        return False
    common = 0
    for left, right in zip(source_parts, candidate_parts):
        if left != right:
            break
        common += 1
    if common >= 2 and any(token in "/".join(source_parts[:common]).lower() for token in ("col", "ld", "leader", "lingdao", "n")):
        return True
    source_parent = "/".join(source_parts[:-1])
    candidate_parent = "/".join(candidate_parts[:-1])
    return bool(source_parent and source_parent == candidate_parent)


def script_redirect_url(text: str, base_url: str) -> str:
    patterns = (
        r"(?:window\.)?location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]",
        r"(?:window\.)?location\.replace\(\s*['\"]([^'\"]+)['\"]\s*\)",
    )
    for pattern in patterns:
        match = re.search(pattern, text or "", re.IGNORECASE)
        if not match:
            continue
        url = normalize_url(match.group(1), base_url)
        if url and url != normalize_url(base_url):
            return url
    return ""


def unwrap_search_result_url(url: str) -> str:
    parsed = urlparse(url)
    if not search_result_domain(parsed.netloc):
        return url
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    target = params.get("url") or params.get("u") or ""
    if not target:
        return url
    if target.startswith(("http://", "https://")):
        return target
    if target.startswith("a1"):
        encoded = target[2:]
        padding = "=" * ((4 - len(encoded) % 4) % 4)
        try:
            decoded = base64.urlsafe_b64decode((encoded + padding).encode("ascii")).decode("utf-8", errors="ignore")
            if decoded.startswith(("http://", "https://")):
                return decoded
        except Exception:
            return url
    return url


def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()


def site_group_of(url: str) -> str:
    host = domain_of(url)
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) <= 3:
        return host
    if parts[-2:] == ["gov", "cn"]:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def url_in_allowed_domains(url: str, seed_hosts: Sequence[str], scope_domains: Sequence[str]) -> bool:
    host = domain_of(url)
    if not host:
        return False
    if host in CENTRAL_OR_OUT_OF_SCOPE_DOMAINS:
        return False
    for seed_host in seed_hosts:
        if host == seed_host or host.endswith("." + seed_host):
            return True
    for scope_domain in scope_domains:
        scope_domain = scope_domain.lower().strip()
        if scope_domain and (host == scope_domain or host.endswith("." + scope_domain)):
            return True
    return False


def trusted_profile_domain(host: str) -> bool:
    host = host.lower()
    return any(host == domain or host.endswith("." + domain) for domain in TRUSTED_PROFILE_SUPPLEMENT_DOMAINS)


def search_result_domain(host: str) -> bool:
    host = host.lower()
    return any(host == domain or host.endswith("." + domain) for domain in SEARCH_RESULT_DOMAINS)


def trusted_profile_context(text: str) -> bool:
    hay = compact_text(text)
    has_profile_signal = any(
        k in hay
        for k in (
            "党政领导人物库",
            "人物库",
            "组织人事",
            "任免",
            "公示",
            "图/简历",
            "简历",
            "履历",
            "任",
            "曾任",
            "原任",
            "历任",
        )
    )
    has_region_signal = any(k in hay for k in REGION_CONTEXT_TERMS)
    return has_profile_signal and has_region_signal


def possible_new_official_domain(
    url: str,
    context: str,
    source_url: str,
    seed_hosts: Sequence[str],
    scope_domains: Sequence[str],
) -> bool:
    host = domain_of(url)
    if not host or host in CENTRAL_OR_OUT_OF_SCOPE_DOMAINS:
        return False
    if search_result_domain(domain_of(source_url)):
        ctx = compact_text(f"{context} {source_url}")
        has_region_signal = any(k in ctx for k in REGION_CONTEXT_TERMS)
        has_profile_signal = any(k in ctx for k in DISCOVERY_KEYWORDS)
        if trusted_profile_domain(host):
            return trusted_profile_context(ctx)
        return host.endswith(".gov.cn") and has_region_signal and has_profile_signal
    if url_in_allowed_domains(url, seed_hosts, scope_domains):
        if host.endswith(".gov.cn"):
            return True
        if trusted_profile_domain(host):
            source_host = domain_of(source_url)
            return trusted_profile_context(f"{context} {source_url}") or trusted_profile_domain(source_host)
        return False
    if not host.endswith(".gov.cn"):
        return False
    if url_in_allowed_domains(url, seed_hosts, scope_domains):
        return True
    if not url_in_allowed_domains(source_url, seed_hosts, scope_domains):
        return False
    ctx = compact_text(context)
    return any(
        k in ctx
        for k in (
            "人民政府",
            "公安局",
            "司法局",
            "财政局",
            "教育局",
            "委员会",
            "街道办事处",
            "镇政府",
            "区政府",
            "县政府",
        )
    )


def profile_url_signal(url: str) -> bool:
    low = url.lower()
    tokens = (
        "leader",
        "lingdao",
        "ldxx",
        "ldzc",
        "ldzc",
        "zfld",
        "szfld",
        "xzfld",
        "qzfld",
        "jgld",
        "fld",
        "wfldxx",
        "xzc",
    )
    return any(token in low for token in tokens)


def profile_detail_text_signal(text: str) -> bool:
    hay = compact_text(text)
    if not hay:
        return False
    if any(k in hay for k in ("领导简介", "领导简历", "个人简历", "工作分工", "基本信息")):
        return True
    if any(k in hay for k in ("简介", "简历", "履历")) and any(
        k in hay for k in ("分工", "负责", "分管", "主持", "现任", "职务", "出生", "男，", "女，", "男,", "女,")
    ):
        return True
    if any(k in hay for k in ("现任", "职务", "负责", "分管", "主持")) and any(
        k in hay for k in ("男，", "女，", "男,", "女,", "出生", "汉族")
    ):
        return True
    return False


def name_from_page_title(title: str) -> str:
    title = compact_text(title)
    if looks_like_name(title):
        return normalize_person_name(title)
    for token in re.split(r"[\s\-＿_－—|｜·•]+", title):
        name = normalize_person_name(token)
        if looks_like_name(name):
            return name
    for match in re.finditer(r"([\u4e00-\u9fff·]{2,4})(?:同志)?(?:简介|简历|个人简历|工作分工|分工)?", title):
        name = normalize_person_name(match.group(1))
        if looks_like_name(name):
            return name
    return ""


def detail_title_profile_signal(title: str, text: str) -> bool:
    name = name_from_page_title(title)
    if not name:
        return False
    head = compact_text(text[:6000])
    if not head or not profile_detail_text_signal(head):
        return False
    if name in head:
        return True
    return bool(re.search(rf"{re.escape(name)}.{{0,80}}(?:男|女|现任|职务|负责|分管|主持|简介|简历|履历)", head))


def should_extract_people_from_page(item: "QueueItem", title: str, full_text: str) -> bool:
    title_context = compact_text(f"{title} {item.url}")
    if any(k in title_context for k in STRONG_PROFILE_PAGE_KEYWORDS):
        return True
    if trusted_profile_domain(domain_of(item.url)) and trusted_profile_context(f"{title_context} {full_text[:1000]}"):
        return True
    if profile_url_signal(item.url):
        return True
    if detail_title_profile_signal(title, full_text):
        return True
    if item.depth <= 0:
        return False
    head = compact_text(full_text[:2500])
    if "姓名" in head and "职务" in head and any(k in head for k in ("简历", "分工", "领导简历", "工作分工")):
        return True
    if item.priority >= 30 and looks_like_name(title) and any(k in head for k in ("简历", "分工", "负责", "分管", "出生", "男", "女")):
        return True
    if item.priority >= 35 and any(k in title_context for k in ("领导", "简历", "分工", "班子")):
        return True
    return False


def relevant_text(text: str) -> bool:
    hay = compact_text(text)
    return any(k in hay for k in DISCOVERY_KEYWORDS)


def high_value_link_text(text: str) -> bool:
    hay = compact_text(text)
    if not hay:
        return False
    if any(k in hay for k in LOW_VALUE_CONTEXT_KEYWORDS):
        return False
    return any(k in hay for k in HIGH_VALUE_LINK_KEYWORDS)


def profile_like_text(text: str) -> bool:
    hay = compact_text(text)
    return any(k in hay for k in PROFILE_KEYWORDS)


def infer_city_from_text(text: str) -> str:
    hay = compact_text(text)
    for city in REGION_CITY_NAMES:
        if city in hay or city.removesuffix("市") in hay:
            return city
    return ""


def infer_level_from_text(text: str) -> str:
    hay = compact_text(text)
    if "公安" in hay:
        return "agency"
    if any(k in hay for k in ("市政府", "政府领导", "市长", "副市长")):
        return "city"
    if any(k in hay for k in ("区政府", "县政府", "县长", "区长")):
        return "county"
    if trusted_profile_context(hay):
        return "media_profile"
    return "agency"


def infer_unit_from_text(title: str, snippet: str, fallback: str = "") -> str:
    hay = compact_text(f"{title} {snippet}")
    patterns = [
        r"([\u4e00-\u9fff]{2,12}市人民政府)",
        r"([\u4e00-\u9fff]{2,12}县人民政府)",
        r"([\u4e00-\u9fff]{2,12}区人民政府)",
        r"([\u4e00-\u9fff]{2,12}市公安局)",
        r"([\u4e00-\u9fff]{2,12}县公安局)",
        r"([\u4e00-\u9fff]{2,12}区公安分局)",
        r"([\u4e00-\u9fff]{2,16}(?:局|委|厅|办|中心))",
    ]
    for pattern in patterns:
        match = re.search(pattern, hay)
        if match:
            return sanitize_filename(match.group(1), fallback or "公开官员")
    city = infer_city_from_text(hay)
    if city and any(k in hay for k in ("市政府领导", "政府领导", "市长", "副市长")):
        return f"{city}人民政府"
    if "党政领导人物库" in hay:
        return "中国经济网山东党政领导人物库"
    if "组织人事" in hay or "图/简历" in hay:
        return "人民网组织人事山东图简历"
    return sanitize_filename(title, fallback or "公开官员")


def url_should_skip(url: str) -> bool:
    low = url.lower()
    return any(part.lower() in low for part in SKIP_URL_PARTS)


def image_should_skip(url: str, alt: str = "") -> bool:
    low = url.lower()
    if any(part in low for part in SKIP_IMAGE_PARTS):
        return True
    alt_low = compact_text(alt).lower()
    return any(part.lower() in alt_low for part in SKIP_IMAGE_ALT_PARTS)


def cn_gender(value: str) -> str:
    if value in {"male", "男"}:
        return "男"
    if value in {"female", "女"}:
        return "女"
    return "未知"


def infer_gender(text: str) -> str:
    text = compact_text(text)
    if re.search(r"(性别[:：]?\s*男|[,，]\s*男[,，]|男[,，]\s*汉族|男[,，]\s*\d{4}年)", text):
        return "男"
    if re.search(r"(性别[:：]?\s*女|[,，]\s*女[,，]|女[,，]\s*汉族|女[,，]\s*\d{4}年)", text):
        return "女"
    return ""


def normalize_person_name(value: str) -> str:
    value = compact_text(value)
    value = re.sub(r"(同志|先生|女士)$", "", value)
    value = re.sub(r"(?<=[\u4e00-\u9fff·])\s+(?=[\u4e00-\u9fff·])", "", value)
    return value.strip()


def looks_like_name(value: str) -> bool:
    value = normalize_person_name(value)
    value = re.sub(r"(同志|先生|女士)$", "", value)
    if not (2 <= len(value) <= 4):
        return False
    if not re.fullmatch(r"[\u4e00-\u9fff·]+", value):
        return False
    bad = {
        "领导",
        "政府",
        "首页",
        "更多",
        "简介",
        "简历",
        "分工",
        "领导分工",
        "工作分工",
        "职务",
        "姓名",
        "党委",
        "支队",
        "大队",
        "分局",
        "班子成员",
        "人事盘点",
        "省级任免",
        "男",
        "女",
        "主任",
        "书记",
        "市长",
        "省长",
        "县长",
        "区长",
        "局长",
        "所长",
        "信箱",
        "关闭",
        "详情",
        "详细",
        "查看",
        "专页",
        "现任",
        "中文",
        "英文",
        "日本",
        "韩国",
        "客户端",
        "小程序",
        "用户中心",
        "关于我们",
        "英文版",
        "中文版",
        "手机版",
        "无障碍",
        "长者模式",
        "活动",
        "显示时间",
        "市场监管",
        "关于我们",
        "网站地图",
        "联系我们",
        "机关",
        "机关简介",
        "公安局",
        "市公安局",
        "司法局",
        "财政局",
        "教育局",
        "办公室",
        "公司",
        "企业",
        "集团",
        "内设机构",
        "主要职责",
        "领导活动",
        "当前位置",
        "组织领导",
        "领导信息",
        "领导之窗",
        "政务公开",
        "山东",
        "济南",
        "青岛",
        "淄博",
        "枣庄",
        "东营",
        "烟台",
        "潍坊",
        "济宁",
        "泰安",
        "威海",
        "日照",
        "临沂",
        "德州",
        "聊城",
        "滨州",
        "菏泽",
        "习近平",
        "共中央",
    }
    if value in bad:
        return False
    if value in TITLE_WORDS:
        return False
    bad_parts = (
        "专题",
        "专栏",
        "首页",
        "公开",
        "政策",
        "服务",
        "办事",
        "要闻",
        "新闻",
        "公告",
        "通知",
        "网站",
        "栏目",
        "导航",
        "视频",
        "图片",
        "数字",
        "决策",
        "信息",
        "前往",
        "来到",
        "听了",
        "询问",
        "强调",
        "出席",
        "调研",
        "讲话",
        "重要",
        "总书记",
        "中央",
        "省委",
        "市委",
        "换届",
        "树立",
        "质量",
        "奖管",
        "解读",
        "相关",
        "介绍",
        "记者",
        "请问",
        "乡亲",
        "全国",
        "人民",
        "政府",
        "部门",
        "机构",
        "领导",
        "之窗",
        "负责",
        "协助",
        "办公",
        "公厅",
        "党组",
        "党委",
        "信访",
        "分局",
        "支队",
        "大队",
        "派出",
        "处长",
        "科长",
        "股长",
        "班子",
        "成员",
        "人事",
        "盘点",
        "任免",
        "区域",
        "区委",
    )
    if any(part in value for part in bad_parts):
        return False
    suspicious_prefixes = ("现任", "当前", "分管", "负责")
    suspicious_suffixes = ("政府", "潍坊", "济南", "烟台", "山东", "专页", "模式")
    if value.startswith(suspicious_prefixes) or value.endswith(suspicious_suffixes):
        return False
    return True


def extract_position_near_name(text: str, name: str) -> str:
    text = compact_text(text)
    idx = text.find(name)
    if idx < 0:
        span = text[:120]
    else:
        span = text[max(0, idx - 60) : min(len(text), idx + 90)]
    hits = [word for word in TITLE_WORDS if word in span]
    return "、".join(dict.fromkeys(hits))[:80]


def possible_names_from_text(text: str, max_names: int = 20) -> List[str]:
    text = compact_text(text)
    results: List[str] = []
    title_alt = "|".join(re.escape(t) for t in sorted(TITLE_WORDS, key=len, reverse=True))
    patterns = [
        rf"(?:{title_alt})[:：\s]*([\u4e00-\u9fff·]{{2,4}})",
        rf"(?<![\u4e00-\u9fff·])([\u4e00-\u9fff·]{{2,4}})(?:同志)?(?:[,，、:：\\s]{{0,3}})(?:{title_alt})",
        r"姓名[:：]\s*([\u4e00-\u9fff·\s]{2,12}?)(?=\s*(?:职务|简历|领导简历|工作分工)[:：])",
        r"([\u4e00-\u9fff·]{2,4})\s*(?:任|已任|拟任|当选|提名|兼任)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            name = normalize_person_name(match.group(1))
            if looks_like_name(name) and name not in results:
                results.append(name)
                if len(results) >= max_names:
                    return results
    return results


def explicit_profile_names_from_text(text: str, max_names: int = 20) -> List[str]:
    text = normalize_profile_label_text(text)
    results: List[str] = []
    patterns = [
        r"(?:姓名|领导姓名)[:：]\s*([\u4e00-\u9fff·\s]{2,12}?)(?=\s*(?:职务|简历|领导简历|工作分工|$)[:：]?)",
        r"(?:简介|简历)?\s*([\u4e00-\u9fff·]{2,4})[，,]\s*(?:男|女)[，,]",
        r"([\u4e00-\u9fff·]{2,4})\s*(?:同志)?\s*(?:简历|简介)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            name = normalize_person_name(match.group(1))
            if looks_like_name(name) and name not in results:
                results.append(name)
                if len(results) >= max_names:
                    return results
    return results


def extract_profile_names(text: str, max_names: int = 20) -> List[str]:
    text = compact_text(text)
    if not any(k in text for k in ("姓名", "简历", "分工", "职务", "领导", "负责", "分管", "性别", "出生")):
        return []
    return explicit_profile_names_from_text(text, max_names=max_names)


def image_block_names(block: str, alt: str = "") -> List[str]:
    results: List[str] = []
    alt = compact_text(alt)
    if looks_like_name(alt):
        results.append(normalize_person_name(alt))
    else:
        for token in re.split(r"[\s，,：:；;。|/\\-]+", alt):
            if looks_like_name(token):
                results.append(normalize_person_name(token))
                break
        for name in possible_names_from_text(alt, max_names=3):
            if name not in results:
                results.append(name)
    text = compact_text(block)
    if text:
        first_token = re.split(r"[\s，,：:；;。|/\\-]+", text, maxsplit=1)[0]
        if looks_like_name(first_token) and first_token not in results:
            results.append(normalize_person_name(first_token))
    for name in explicit_profile_names_from_text(text, max_names=5):
        if name not in results:
            results.append(name)
    if results:
        return results[:1]
    for name in possible_names_from_text(text[:260], max_names=2):
        if name not in results:
            results.append(name)
    return results


def nearest_text_block(node: html.HtmlElement, max_depth: int = 5) -> str:
    current: Optional[html.HtmlElement] = node
    for _ in range(max_depth):
        if current is None:
            break
        text = compact_text(current.text_content())
        if len(text) >= 8 and len(text) <= 500:
            return text
        current = current.getparent()
    return ""


def normalize_profile_label_text(text: str) -> str:
    text = clean_text(text)
    replacements = (
        (r"姓\s*名\s*[:：]", "姓名："),
        (r"职\s*务\s*[:：]", "职务："),
        (r"现\s*任\s*职\s*务\s*[:：]", "职务："),
        (r"基本\s*信\s*息\s*[:：]", "简历："),
        (r"领导\s*简\s*历\s*[:：]", "领导简历："),
        (r"简\s*历\s*[:：]", "简历："),
        (r"工作\s*分\s*工\s*[:：]", "工作分工："),
        (r"(?<!工作)分\s*工\s*[:：]", "工作分工："),
    )
    for pattern, repl in replacements:
        text = re.sub(pattern, repl, text)
    return compact_text(text)


def extract_between_label(text: str, start_label: str, end_labels: Sequence[str]) -> str:
    start = text.find(start_label)
    if start < 0:
        return ""
    start += len(start_label)
    end = len(text)
    for label in end_labels:
        idx = text.find(label, start)
        if idx >= 0:
            end = min(end, idx)
    return compact_text(text[start:end])


def labeled_profile_blocks(text: str, max_blocks: int = 120) -> List[Dict[str, str]]:
    normalized = normalize_profile_label_text(text)
    matches = list(
        re.finditer(
            r"姓名：\s*([\u4e00-\u9fff·\s]{2,12}?)(?=\s*(?:职务|简历|领导简历|工作分工)：)",
            normalized,
        )
    )
    blocks: List[Dict[str, str]] = []
    for idx, match in enumerate(matches[:max_blocks]):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(normalized)
        block = compact_text(normalized[start:end])
        name = normalize_person_name(match.group(1))
        if not looks_like_name(name):
            continue
        position = extract_between_label(block, "职务：", ("领导简历：", "简历：", "工作分工：", "姓名："))
        biography = extract_between_label(block, "领导简历：", ("工作分工：", "姓名："))
        if not biography:
            biography = extract_between_label(block, "简历：", ("工作分工：", "姓名："))
        division = extract_between_label(block, "工作分工：", ("姓名：",))
        blocks.append(
            {
                "name": name,
                "position": position,
                "biography": biography,
                "division_of_work": division,
                "block_text": block,
            }
        )
    return blocks


def historical_or_former_context(text: str) -> bool:
    hay = compact_text(text)
    return any(k in hay for k in ("历任", "曾任", "原任", "任免", "任前公示", "拟任", "免去", "图/简历"))


def has_position_signal(text: str) -> bool:
    hay = compact_text(text)
    return any(word in hay for word in TITLE_WORDS)


def names_from_short_position_block(text: str, max_names: int = 20) -> List[str]:
    hay = compact_text(text)
    results: List[str] = []
    title_alt = "|".join(re.escape(t) for t in sorted(TITLE_WORDS, key=len, reverse=True))
    patterns = [
        rf"(?:{title_alt})\s*([\u4e00-\u9fff·]{{2,4}})",
        rf"([\u4e00-\u9fff·]{{2,4}})\s*(?:{title_alt})",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, hay):
            name = normalize_person_name(match.group(1))
            if looks_like_name(name) and name not in results:
                results.append(name)
                if len(results) >= max_names:
                    return results
    return results


def extract_title(doc: html.HtmlElement) -> str:
    title = clean_text(doc.xpath("string(//title)"))
    if title:
        title = re.sub(r"[-_－—].*$", "", title).strip()
    h1 = clean_text(doc.xpath("string((//h1|//h2|//*[@class='title' or contains(@class,'tit')])[1])"))
    if title and looks_like_name(title):
        return title
    if h1 and h1 not in {"简介", "简历", "分工", "工作分工", "基本信息", "会议活动", "更多>>"}:
        return h1
    return title or h1


def body_text(doc: html.HtmlElement) -> str:
    candidates = doc.xpath(
        "//*[contains(@class,'content') or contains(@class,'article') or contains(@class,'main') "
        "or contains(@id,'content') or contains(@id,'article') or contains(@id,'main')]"
    )
    if candidates:
        texts = [clean_text(node.text_content()) for node in candidates]
        longest = max(texts, key=len, default="")
        if len(longest) >= 80:
            return longest[:20000]
    return clean_text(doc.text_content())[:20000]


def guess_unit_from_title(title: str, fallback: str) -> str:
    title = compact_text(title)
    for suffix in ("人民政府", "公安局", "司法局", "财政局", "教育局", "委员会", "办公室", "街道办事处", "镇政府"):
        idx = title.find(suffix)
        if idx > 0:
            start = max(0, idx - 18)
            candidate = title[start : idx + len(suffix)].strip(" -_—－")
            if fallback and candidate in {"市公安局", "县公安局", "区公安分局", "市人民政府", "县人民政府", "区人民政府"}:
                return fallback
            return candidate
    return fallback


@dataclass
class QueueItem:
    url: str
    province: str
    city: str
    county_or_district: str
    administrative_level: str
    unit_name: str
    source_unit_url: str
    depth: int
    parent_url: str
    priority: int = 0


class Registry:
    def __init__(self, root: Path, default_province: str):
        self.root = root
        self.default_province = default_province
        self.registry_dir = root / "registry"
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.registry_dir / "people.sqlite"
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init_db()

    def init_db(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS people (
                    person_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    gender TEXT,
                    province TEXT,
                    city TEXT,
                    county_or_district TEXT,
                    township_or_street TEXT,
                    administrative_level TEXT,
                    unit_name TEXT,
                    department TEXT,
                    position TEXT,
                    rank_or_grade TEXT,
                    division_of_work TEXT,
                    biography TEXT,
                    full_public_text TEXT,
                    source_url TEXT NOT NULL,
                    source_page_title TEXT,
                    source_site_domain TEXT,
                    source_unit_url TEXT,
                    publish_date TEXT,
                    updated_date TEXT,
                    has_official_photo INTEGER DEFAULT 0,
                    has_news_photo INTEGER DEFAULT 0,
                    image_status TEXT,
                    primary_image_path TEXT,
                    raw_fields_json TEXT,
                    crawled_at TEXT,
                    last_checked_at TEXT,
                    notes TEXT
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS image_assets (
                    asset_id TEXT PRIMARY KEY,
                    person_id TEXT,
                    name TEXT,
                    image_type TEXT,
                    image_index INTEGER,
                    image_total INTEGER,
                    image_path TEXT NOT NULL,
                    image_url TEXT,
                    source_url TEXT NOT NULL,
                    source_page_title TEXT,
                    source_site_domain TEXT,
                    caption_or_alt TEXT,
                    sha256 TEXT,
                    content_type TEXT,
                    width INTEGER,
                    height INTEGER,
                    is_usable INTEGER DEFAULT 1,
                    crawled_at TEXT,
                    notes TEXT
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_units (
                    unit_id TEXT PRIMARY KEY,
                    province TEXT,
                    city TEXT,
                    county_or_district TEXT,
                    township_or_street TEXT,
                    administrative_level TEXT,
                    unit_name TEXT NOT NULL,
                    official_site_url TEXT,
                    profile_list_url TEXT,
                    parent_unit_id TEXT,
                    crawl_status TEXT,
                    template_name TEXT,
                    last_crawled_at TEXT,
                    notes TEXT
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crawl_queue (
                    url TEXT PRIMARY KEY,
                    province TEXT,
                    city TEXT,
                    county_or_district TEXT,
                    administrative_level TEXT,
                    unit_name TEXT,
                    source_unit_url TEXT,
                    depth INTEGER DEFAULT 0,
                    parent_url TEXT,
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    retries INTEGER DEFAULT 0,
                    last_error TEXT,
                    http_status INTEGER,
                    enqueued_at TEXT,
                    updated_at TEXT
                )
                """
            )
            try:
                self.conn.execute("ALTER TABLE crawl_queue ADD COLUMN priority INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS page_visits (
                    url TEXT PRIMARY KEY,
                    source_site_domain TEXT,
                    status TEXT,
                    http_status INTEGER,
                    page_title TEXT,
                    people_found INTEGER DEFAULT 0,
                    images_found INTEGER DEFAULT 0,
                    links_found INTEGER DEFAULT 0,
                    crawled_at TEXT,
                    error TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_people_source_domain ON people(source_site_domain)"
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_person ON image_assets(person_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON crawl_queue(status)")

    def add_source_unit(
        self,
        province: str,
        city: str,
        county: str,
        level: str,
        unit_name: str,
        official_site_url: str,
        profile_list_url: str = "",
        status: str = "candidate",
        notes: str = "",
    ) -> str:
        unit_id = sha1_text("|".join([province, city, county, unit_name, official_site_url]))
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO source_units (
                    unit_id, province, city, county_or_district, administrative_level,
                    unit_name, official_site_url, profile_list_url, crawl_status,
                    last_crawled_at, notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(unit_id) DO UPDATE SET
                    profile_list_url=COALESCE(NULLIF(excluded.profile_list_url, ''), source_units.profile_list_url),
                    crawl_status=excluded.crawl_status,
                    last_crawled_at=excluded.last_crawled_at
                """,
                (
                    unit_id,
                    province,
                    city,
                    county,
                    level,
                    unit_name,
                    official_site_url,
                    profile_list_url,
                    status,
                    utc_now(),
                    notes,
                ),
            )
        return unit_id

    def enqueue(self, item: QueueItem) -> bool:
        url = normalize_url(item.url)
        if not url:
            return False
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO crawl_queue (
                    url, province, city, county_or_district, administrative_level,
                    unit_name, source_unit_url, depth, parent_url, status,
                    priority, enqueued_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    url,
                    item.province,
                    item.city,
                    item.county_or_district,
                    item.administrative_level,
                    item.unit_name,
                    item.source_unit_url,
                    item.depth,
                    item.parent_url,
                    item.priority,
                    utc_now(),
                    utc_now(),
                ),
            )
            return cur.rowcount > 0

    def reprioritize_or_enqueue(self, item: QueueItem) -> bool:
        url = normalize_url(item.url)
        if not url:
            return False
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE crawl_queue
                SET province=?, city=?, county_or_district=?, administrative_level=?,
                    unit_name=?, source_unit_url=?, depth=?, parent_url=?,
                    priority=MAX(COALESCE(priority, 0), ?), status='pending',
                    last_error='', http_status=0, updated_at=?
                WHERE url=?
                """,
                (
                    item.province,
                    item.city,
                    item.county_or_district,
                    item.administrative_level,
                    item.unit_name,
                    item.source_unit_url,
                    item.depth,
                    item.parent_url,
                    item.priority,
                    utc_now(),
                    url,
                ),
            )
            if cur.rowcount:
                return False
            self.conn.execute(
                """
                INSERT INTO crawl_queue (
                    url, province, city, county_or_district, administrative_level,
                    unit_name, source_unit_url, depth, parent_url, status,
                    priority, enqueued_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    url,
                    item.province,
                    item.city,
                    item.county_or_district,
                    item.administrative_level,
                    item.unit_name,
                    item.source_unit_url,
                    item.depth,
                    item.parent_url,
                    item.priority,
                    utc_now(),
                    utc_now(),
                ),
            )
            return True

    def pending_by_domain(self, blocked_domains: Dict[str, float], active_domains: set[str]) -> Optional[QueueItem]:
        now_ts = time.time()
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM crawl_queue WHERE status='pending' ORDER BY priority DESC, depth, enqueued_at LIMIT 500"
            ).fetchall()
            for row in rows:
                site_group = site_group_of(row["url"])
                if site_group in active_domains:
                    continue
                if blocked_domains.get(site_group, 0) > now_ts:
                    continue
                self.conn.execute(
                    "UPDATE crawl_queue SET status='running', updated_at=? WHERE url=?",
                    (utc_now(), row["url"]),
                )
                self.conn.commit()
                return QueueItem(
                    url=row["url"],
                    province=row["province"] or self.default_province,
                    city=row["city"] or "",
                    county_or_district=row["county_or_district"] or "",
                    administrative_level=row["administrative_level"] or "",
                    unit_name=row["unit_name"] or "",
                    source_unit_url=row["source_unit_url"] or "",
                    depth=int(row["depth"] or 0),
                    parent_url=row["parent_url"] or "",
                    priority=int(row["priority"] or 0),
                )
        return None

    def mark_queue(self, url: str, status: str, error: str = "", http_status: int = 0) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE crawl_queue
                SET status=?, last_error=?, http_status=?, updated_at=?
                WHERE url=?
                """,
                (status, error[:500], http_status, utc_now(), normalize_url(url)),
            )

    def record_visit(
        self,
        url: str,
        status: str,
        http_status: int,
        title: str,
        people_found: int,
        images_found: int,
        links_found: int,
        error: str = "",
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO page_visits (
                    url, source_site_domain, status, http_status, page_title,
                    people_found, images_found, links_found, crawled_at, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    status=excluded.status,
                    http_status=excluded.http_status,
                    page_title=excluded.page_title,
                    people_found=excluded.people_found,
                    images_found=excluded.images_found,
                    links_found=excluded.links_found,
                    crawled_at=excluded.crawled_at,
                    error=excluded.error
                """,
                (
                    normalize_url(url),
                    domain_of(url),
                    status,
                    http_status,
                    title,
                    people_found,
                    images_found,
                    links_found,
                    utc_now(),
                    error[:500],
                ),
            )

    def upsert_person(self, row: Dict[str, Any]) -> str:
        normalized_source = normalize_url(row.get("source_url", ""))
        person_id = row.get("person_id") or sha1_text("|".join([normalized_source, row.get("name", "")]))
        row["person_id"] = person_id
        columns = [
            "person_id",
            "name",
            "gender",
            "province",
            "city",
            "county_or_district",
            "township_or_street",
            "administrative_level",
            "unit_name",
            "department",
            "position",
            "rank_or_grade",
            "division_of_work",
            "biography",
            "full_public_text",
            "source_url",
            "source_page_title",
            "source_site_domain",
            "source_unit_url",
            "publish_date",
            "updated_date",
            "has_official_photo",
            "has_news_photo",
            "image_status",
            "primary_image_path",
            "raw_fields_json",
            "crawled_at",
            "last_checked_at",
            "notes",
        ]
        payload = {col: row.get(col, "") for col in columns}
        payload["crawled_at"] = payload.get("crawled_at") or utc_now()
        payload["last_checked_at"] = utc_now()
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(f"{col}=excluded.{col}" for col in columns if col != "person_id")
        with self.lock, self.conn:
            self.conn.execute(
                f"""
                INSERT INTO people ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(person_id) DO UPDATE SET {updates}
                """,
                [payload[col] for col in columns],
            )
        return person_id

    def upsert_asset(self, row: Dict[str, Any]) -> str:
        asset_id = row.get("asset_id") or sha1_text(
            "|".join(
                [
                    row.get("person_id", ""),
                    row.get("image_url", ""),
                    row.get("image_type", ""),
                    str(row.get("image_index", "")),
                    row.get("image_path", ""),
                ]
            )
        )
        row["asset_id"] = asset_id
        columns = [
            "asset_id",
            "person_id",
            "name",
            "image_type",
            "image_index",
            "image_total",
            "image_path",
            "image_url",
            "source_url",
            "source_page_title",
            "source_site_domain",
            "caption_or_alt",
            "sha256",
            "content_type",
            "width",
            "height",
            "is_usable",
            "crawled_at",
            "notes",
        ]
        payload = {col: row.get(col, "") for col in columns}
        payload["crawled_at"] = payload.get("crawled_at") or utc_now()
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(f"{col}=excluded.{col}" for col in columns if col != "asset_id")
        with self.lock, self.conn:
            self.conn.execute(
                f"""
                INSERT INTO image_assets ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(asset_id) DO UPDATE SET {updates}
                """,
                [payload[col] for col in columns],
            )
            if payload.get("is_usable"):
                self.conn.execute(
                    """
                    UPDATE people
                    SET has_official_photo=1, image_status='official_photo',
                        primary_image_path=COALESCE(NULLIF(primary_image_path, ''), ?)
                    WHERE person_id=?
                    """,
                    (payload.get("image_path"), payload.get("person_id")),
                )
        return asset_id

    def export_tables(self) -> None:
        self.reconcile_asset_paths()
        self.reconcile_image_statuses()
        tables = ("people", "image_assets", "source_units")
        with self.lock:
            for table in tables:
                rows = self.conn.execute(f"SELECT * FROM {table}").fetchall()
                if not rows:
                    continue
                columns = rows[0].keys()
                csv_path = self.registry_dir / f"{table}.csv"
                jsonl_path = self.registry_dir / f"{table}.jsonl"
                with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=columns)
                    writer.writeheader()
                    for row in rows:
                        writer.writerow({col: row[col] for col in columns})
                with jsonl_path.open("w", encoding="utf-8") as f:
                    for row in rows:
                        f.write(json.dumps({col: row[col] for col in columns}, ensure_ascii=False) + "\n")
            kpi = self.kpi_locked()
            (self.registry_dir / "kpi.json").write_text(
                json.dumps(kpi, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            lines = [
                f"# {self.root.name}采集 KPI",
                "",
                f"- 更新时间: {kpi['updated_at']}",
                f"- 人员入库: {kpi['people_total']}",
                f"- 可用图片: {kpi['usable_images_total']}",
                f"- 有资料且有图人员: {kpi['profile_with_image_people']}",
                f"- 男性有资料且有图人员: {kpi['male_profile_with_image_people']}",
                f"- 历史/旧任线索人员: {kpi['historical_people']}",
                f"- 历史/旧任线索图片: {kpi['historical_usable_images']}",
                f"- 正式头像: {kpi['official_photo_images']}",
                f"- 新闻图片: {kpi['news_photo_images']}",
                f"- 资料图片: {kpi['material_photo_images']}",
                f"- 男性可用图: {kpi['male_usable_images']}",
                f"- 女性可用图: {kpi['female_usable_images']}",
                f"- 未知性别可用图: {kpi['unknown_gender_usable_images']}",
                f"- 无图人员: {kpi['no_image_people']}",
                f"- 图片/人员比: {kpi['usable_images_per_person']}",
                f"- 官方单位候选: {kpi['source_units_total']}",
                f"- 已完成页面: {kpi['done_pages']}",
                f"- 待抓页面: {kpi['pending_pages']}",
                f"- 失败页面: {kpi['failed_pages']}",
            ]
            (self.registry_dir / "kpi.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def reconcile_asset_paths(self) -> int:
        image_suffixes = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
        fixed = 0
        with self.lock, self.conn:
            rows = self.conn.execute(
                "SELECT asset_id, image_path FROM image_assets WHERE is_usable=1"
            ).fetchall()
            for row in rows:
                image_path = str(row["image_path"] or "")
                if image_path and Path(image_path).exists():
                    continue
                replacement: Optional[Path] = None
                if image_path:
                    old_path = Path(image_path)
                    if old_path.parent.exists():
                        candidates = [
                            p
                            for p in old_path.parent.glob(f"{old_path.stem}.*")
                            if p.is_file() and p.suffix.lower() in image_suffixes
                        ]
                        if candidates:
                            candidates.sort(
                                key=lambda p: (
                                    0 if p.suffix.lower() in {".jpg", ".jpeg"} else 1,
                                    str(p),
                                )
                            )
                            replacement = candidates[0]
                if replacement is None:
                    continue

                width = height = 0
                try:
                    with Image.open(replacement) as im:
                        width, height = im.size
                except Exception:
                    pass
                content_type = "image/jpeg" if replacement.suffix.lower() in {".jpg", ".jpeg"} else ""
                self.conn.execute(
                    """
                    UPDATE image_assets
                    SET image_path=?, sha256=?, width=COALESCE(NULLIF(?, 0), width),
                        height=COALESCE(NULLIF(?, 0), height),
                        content_type=COALESCE(NULLIF(?, ''), content_type)
                    WHERE asset_id=?
                    """,
                    (
                        str(replacement),
                        sha256_file(replacement),
                        width,
                        height,
                        content_type,
                        row["asset_id"],
                    ),
                )
                fixed += 1

            people = self.conn.execute(
                """
                SELECT person_id, primary_image_path
                FROM people
                WHERE image_status='official_photo'
                """
            ).fetchall()
            for person in people:
                primary_path = str(person["primary_image_path"] or "")
                if primary_path and Path(primary_path).exists():
                    continue
                asset = self.conn.execute(
                    """
                    SELECT image_path
                    FROM image_assets
                    WHERE person_id=? AND is_usable=1
                    ORDER BY image_index
                    LIMIT 1
                    """,
                    (person["person_id"],),
                ).fetchone()
                if asset and asset["image_path"] and Path(str(asset["image_path"])).exists():
                    self.conn.execute(
                        """
                        UPDATE people
                        SET primary_image_path=?, has_official_photo=1, image_status='official_photo'
                        WHERE person_id=?
                        """,
                        (asset["image_path"], person["person_id"]),
                    )
                    fixed += 1
            return fixed

    def reconcile_image_statuses(self) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE people
                SET has_official_photo=0,
                    image_status='failed_image',
                    primary_image_path=''
                WHERE image_status='official_photo'
                  AND COALESCE(NULLIF(primary_image_path, ''), '')=''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM image_assets a
                      WHERE a.person_id=people.person_id
                        AND a.is_usable=1
                  )
                """
            )
            return int(cur.rowcount or 0)

    def kpi_locked(self) -> Dict[str, Any]:
        people_total = int(self.conn.execute("SELECT COUNT(*) FROM people").fetchone()[0])
        usable_images_total = int(
            self.conn.execute("SELECT COUNT(*) FROM image_assets WHERE is_usable=1").fetchone()[0]
        )
        male_usable_images = int(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM image_assets a
                JOIN people p ON p.person_id=a.person_id
                WHERE a.is_usable=1 AND p.gender='男'
                """
            ).fetchone()[0]
        )
        female_usable_images = int(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM image_assets a
                JOIN people p ON p.person_id=a.person_id
                WHERE a.is_usable=1 AND p.gender='女'
                """
            ).fetchone()[0]
        )
        unknown_gender_usable_images = int(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM image_assets a
                JOIN people p ON p.person_id=a.person_id
                WHERE a.is_usable=1 AND COALESCE(NULLIF(p.gender,''),'未知')='未知'
                """
            ).fetchone()[0]
        )
        no_image_people = int(
            self.conn.execute("SELECT COUNT(*) FROM people WHERE image_status='no_image'").fetchone()[0]
        )
        profile_with_image_people = int(
            self.conn.execute(
                """
                SELECT COUNT(DISTINCT p.person_id)
                FROM people p
                JOIN image_assets a ON a.person_id=p.person_id
                WHERE a.is_usable=1
                  AND COALESCE(NULLIF(p.full_public_text,''), NULLIF(p.biography,''), NULLIF(p.position,''), '')!=''
                """
            ).fetchone()[0]
        )
        male_profile_with_image_people = int(
            self.conn.execute(
                """
                SELECT COUNT(DISTINCT p.person_id)
                FROM people p
                JOIN image_assets a ON a.person_id=p.person_id
                WHERE a.is_usable=1
                  AND p.gender='男'
                  AND COALESCE(NULLIF(p.full_public_text,''), NULLIF(p.biography,''), NULLIF(p.position,''), '')!=''
                """
            ).fetchone()[0]
        )
        historical_people = int(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM people
                WHERE notes LIKE '%former_or_historical%'
                   OR raw_fields_json LIKE '%former_or_historical%'
                """
            ).fetchone()[0]
        )
        historical_usable_images = int(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM image_assets a
                JOIN people p ON p.person_id=a.person_id
                WHERE a.is_usable=1
                  AND (p.notes LIKE '%former_or_historical%' OR p.raw_fields_json LIKE '%former_or_historical%')
                """
            ).fetchone()[0]
        )
        official_photo_images = int(
            self.conn.execute("SELECT COUNT(*) FROM image_assets WHERE is_usable=1 AND image_type='official_photo'").fetchone()[0]
        )
        news_photo_images = int(
            self.conn.execute("SELECT COUNT(*) FROM image_assets WHERE is_usable=1 AND image_type='news_photo'").fetchone()[0]
        )
        material_photo_images = int(
            self.conn.execute("SELECT COUNT(*) FROM image_assets WHERE is_usable=1 AND image_type='material_photo'").fetchone()[0]
        )
        return {
            "updated_at": utc_now(),
            "people_total": people_total,
            "usable_images_total": usable_images_total,
            "profile_with_image_people": profile_with_image_people,
            "male_profile_with_image_people": male_profile_with_image_people,
            "historical_people": historical_people,
            "historical_usable_images": historical_usable_images,
            "official_photo_images": official_photo_images,
            "news_photo_images": news_photo_images,
            "material_photo_images": material_photo_images,
            "male_usable_images": male_usable_images,
            "female_usable_images": female_usable_images,
            "unknown_gender_usable_images": unknown_gender_usable_images,
            "no_image_people": no_image_people,
            "usable_images_per_person": round(usable_images_total / people_total, 4) if people_total else 0,
            "source_units_total": int(self.conn.execute("SELECT COUNT(*) FROM source_units").fetchone()[0]),
            "done_pages": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='done'").fetchone()[0]),
            "pending_pages": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='pending'").fetchone()[0]),
            "failed_pages": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='failed'").fetchone()[0]),
        }

    def counts(self) -> Dict[str, int]:
        with self.lock:
            return {
                "pending": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='pending'").fetchone()[0]),
                "running": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='running'").fetchone()[0]),
                "done_pages": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='done'").fetchone()[0]),
                "failed_pages": int(self.conn.execute("SELECT COUNT(*) FROM crawl_queue WHERE status='failed'").fetchone()[0]),
                "people": int(self.conn.execute("SELECT COUNT(*) FROM people").fetchone()[0]),
                "assets": int(self.conn.execute("SELECT COUNT(*) FROM image_assets WHERE is_usable=1").fetchone()[0]),
                "profile_with_image": int(
                    self.conn.execute(
                        """
                        SELECT COUNT(DISTINCT p.person_id)
                        FROM people p
                        JOIN image_assets a ON a.person_id=p.person_id
                        WHERE a.is_usable=1
                          AND COALESCE(NULLIF(p.full_public_text,''), NULLIF(p.biography,''), NULLIF(p.position,''), '')!=''
                        """
                    ).fetchone()[0]
                ),
                "historical_people": int(
                    self.conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM people
                        WHERE notes LIKE '%former_or_historical%'
                           OR raw_fields_json LIKE '%former_or_historical%'
                        """
                    ).fetchone()[0]
                ),
                "no_image": int(self.conn.execute("SELECT COUNT(*) FROM people WHERE image_status='no_image'").fetchone()[0]),
                "units": int(self.conn.execute("SELECT COUNT(*) FROM source_units").fetchone()[0]),
                "male_assets": int(
                    self.conn.execute(
                        """
                        SELECT COUNT(*) FROM image_assets a
                        JOIN people p ON p.person_id=a.person_id
                        WHERE a.is_usable=1 AND p.gender='男'
                        """
                    ).fetchone()[0]
                ),
                "female_assets": int(
                    self.conn.execute(
                        """
                        SELECT COUNT(*) FROM image_assets a
                        JOIN people p ON p.person_id=a.person_id
                        WHERE a.is_usable=1 AND p.gender='女'
                        """
                    ).fetchone()[0]
                ),
                "unknown_gender_assets": int(
                    self.conn.execute(
                        """
                        SELECT COUNT(*) FROM image_assets a
                        JOIN people p ON p.person_id=a.person_id
                        WHERE a.is_usable=1 AND COALESCE(NULLIF(p.gender,''),'未知')='未知'
                        """
                    ).fetchone()[0]
                ),
            }

    def reset_stale_running(self) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE crawl_queue
                SET status='pending', updated_at=?, last_error=COALESCE(NULLIF(last_error, ''), 'reset_stale_running')
                WHERE status='running'
                """,
                (utc_now(),),
            )
            return int(cur.rowcount or 0)


class RobotsCache:
    def __init__(self, obey: bool, timeout: int):
        self.obey = obey
        self.timeout = timeout
        self.cache: Dict[str, robotparser.RobotFileParser] = {}
        self.lock = threading.Lock()

    def allowed(self, url: str) -> bool:
        if not self.obey:
            return True
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        with self.lock:
            rp = self.cache.get(base)
            if rp is None:
                rp = robotparser.RobotFileParser()
                rp.set_url(urljoin(base, "/robots.txt"))
                try:
                    rp.read()
                except Exception:
                    # If robots cannot be fetched, do not assume prohibition.
                    pass
                self.cache[base] = rp
        try:
            return bool(rp.can_fetch(USER_AGENT, url))
        except Exception:
            return True


class OfficialCollector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.region_config = REGION_CONFIGS[args.region]
        self.default_province = str(self.region_config["province"])
        self.project_name = str(self.region_config["project_name"])
        self.seeds = list(self.region_config["seeds"])
        self.seed_hosts = [urlparse(row[5]).netloc.lower() for row in self.seeds]
        self.scope_domains = [str(item).lower() for item in self.region_config.get("scope_domains", [])]
        self.root = Path(args.output_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)
        self.registry = Registry(self.root, self.default_province)
        self.robots = RobotsCache(args.obey_robots, args.timeout)
        self.active_domains: set[str] = set()
        self.blocked_domains: Dict[str, float] = {}
        self.domain_next_time: Dict[str, float] = {}
        self.group_request_locks: Dict[str, threading.Lock] = {}
        self.state_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.log_path = self.root / "logs" / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def log(self, message: str, **fields: Any) -> None:
        payload = " ".join(f"{k}={v}" for k, v in fields.items() if v not in (None, ""))
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
        if payload:
            line += " " + payload
        print(line, flush=True)
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def seed_region(self) -> None:
        for province, city, county, level, unit_name, url in self.seeds:
            self.registry.add_source_unit(
                province=province,
                city=city,
                county=county,
                level=level,
                unit_name=unit_name,
                official_site_url=url,
                status="candidate",
                notes="seed",
            )
            self.registry.enqueue(
                QueueItem(
                    url=url,
                    province=province,
                    city=city,
                    county_or_district=county,
                    administrative_level=level,
                    unit_name=unit_name,
                    source_unit_url=url,
                    depth=0,
                parent_url="",
                    priority=10,
                )
            )
        if self.args.reseed_open_search:
            search_queries = list(self.region_config.get("open_search_queries", []))
            for province, city, _county, level, _unit_name, url in self.seeds:
                if level != "city" or not city:
                    continue
                host = domain_of(url).removeprefix("www.")
                search_queries.extend(pattern.format(host=host, city=city) for pattern in CITY_OPEN_SEARCH_PATTERNS)
            for query in dict.fromkeys(search_queries):
                query_text = str(query).strip()
                if not query_text:
                    continue
                for page_index in range(max(1, self.args.search_pages)):
                    first = page_index * 10 + 1
                    search_url = "https://www.bing.com/search?" + urlencode({"q": query_text, "first": str(first)})
                    self.registry.enqueue(
                        QueueItem(
                            url=search_url,
                            province=self.default_province,
                            city="",
                            county_or_district="",
                            administrative_level="search",
                            unit_name=f"开放搜索:{query_text[:60]}",
                            source_unit_url=search_url,
                            depth=0,
                            parent_url="",
                            priority=90 - page_index,
                        )
                    )

    def possible_official_url(self, url: str, context: str, source_url: str) -> bool:
        return possible_new_official_domain(url, context, source_url, self.seed_hosts, self.scope_domains)

    def make_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            }
        )
        session.verify = False
        return session

    def wait_domain(self, domain: str) -> None:
        while True:
            with self.state_lock:
                wait_until = self.domain_next_time.get(domain, 0)
            remain = wait_until - time.time()
            if remain <= 0:
                return
            time.sleep(min(remain, 1.0))

    def schedule_next_domain(self, domain: str, slow: bool = False) -> None:
        if slow:
            delay = random.uniform(self.args.slow_delay_min, self.args.slow_delay_max)
        else:
            delay = random.uniform(self.args.delay_min, self.args.delay_max)
        with self.state_lock:
            self.domain_next_time[domain] = time.time() + delay

    def request_lock_for(self, domain: str) -> threading.Lock:
        with self.state_lock:
            lock = self.group_request_locks.get(domain)
            if lock is None:
                lock = threading.Lock()
                self.group_request_locks[domain] = lock
            return lock

    def fetch(self, session: requests.Session, url: str, referer: str = "") -> Tuple[int, str, bytes, str]:
        headers = {}
        if referer:
            headers["Referer"] = referer
        resp = session.get(url, headers=headers, timeout=self.args.timeout, allow_redirects=True)
        content_type = resp.headers.get("Content-Type", "")
        if "text" in content_type or "html" in content_type or not content_type:
            resp.encoding = resp.apparent_encoding or resp.encoding or "utf-8"
            text = resp.text
        else:
            text = ""
        return resp.status_code, content_type, resp.content, text

    def fetch_html_grouped(self, session: requests.Session, url: str, site_group: str, referer: str = "") -> Tuple[int, str, bytes, str]:
        lock = self.request_lock_for(site_group)
        lock.acquire()
        try:
            self.wait_domain(site_group)
            try:
                result = self.fetch(session, url, referer)
            except Exception:
                self.schedule_next_domain(site_group, slow=True)
                raise
            status_code = result[0]
            self.schedule_next_domain(site_group, slow=status_code in {403, 429, 521} or status_code >= 500)
            return result
        finally:
            lock.release()

    def fetch_image_grouped(
        self,
        session: requests.Session,
        url: str,
        site_group: str,
        referer: str,
    ) -> Tuple[int, str, bytes]:
        lock = self.request_lock_for(site_group)
        lock.acquire()
        try:
            self.wait_domain(site_group)
            headers = {"Referer": referer, "User-Agent": USER_AGENT}
            try:
                resp = session.get(url, headers=headers, timeout=self.args.timeout, allow_redirects=True)
            except Exception:
                self.schedule_next_domain(site_group, slow=True)
                raise
            content_type = resp.headers.get("Content-Type", "")
            slow = resp.status_code in {403, 429, 521} or resp.status_code >= 400 or not content_type.lower().startswith("image/")
            self.schedule_next_domain(site_group, slow=slow)
            return resp.status_code, content_type, resp.content
        finally:
            lock.release()

    def process_one(self, item: QueueItem) -> None:
        domain = domain_of(item.url)
        site_group = site_group_of(item.url)
        session = self.make_session()
        status_code = 0
        title = ""
        people_found = 0
        images_found = 0
        links_found = 0
        try:
            if not self.robots.allowed(item.url):
                self.registry.mark_queue(item.url, "failed", "robots_disallowed", 0)
                self.registry.record_visit(item.url, "blocked", 0, "", 0, 0, 0, "robots_disallowed")
                self.log("robots disallowed", url=item.url)
                return

            status_code, content_type, _payload, text = self.fetch_html_grouped(session, item.url, site_group, item.parent_url)
            if status_code in {403, 429, 521}:
                with self.state_lock:
                    self.blocked_domains[site_group] = time.time() + self.args.block_backoff_minutes * 60
                self.registry.mark_queue(item.url, "failed", f"blocked_status_{status_code}", status_code)
                self.registry.record_visit(item.url, "blocked", status_code, "", 0, 0, 0, f"blocked_status_{status_code}")
                self.log("site group backoff", domain=domain, site_group=site_group, status=status_code, minutes=self.args.block_backoff_minutes)
                return
            if status_code >= 400:
                self.registry.mark_queue(item.url, "failed", f"http_{status_code}", status_code)
                self.registry.record_visit(item.url, "failed", status_code, "", 0, 0, 0, f"http_{status_code}")
                return
            if not text:
                self.registry.mark_queue(item.url, "failed", "non_html_or_empty", status_code)
                self.registry.record_visit(item.url, "failed", status_code, "", 0, 0, 0, "non_html_or_empty")
                return

            redirect_url = script_redirect_url(text, item.url)
            if (
                redirect_url
                and not url_should_skip(redirect_url)
                and self.possible_official_url(redirect_url, f"{item.unit_name} {item.url}", item.url)
            ):
                self.registry.reprioritize_or_enqueue(
                    QueueItem(
                        url=redirect_url,
                        province=item.province,
                        city=item.city,
                        county_or_district=item.county_or_district,
                        administrative_level=item.administrative_level,
                        unit_name=item.unit_name,
                        source_unit_url=item.source_unit_url or item.url,
                        depth=item.depth + 1,
                        parent_url=item.url,
                        priority=max(item.priority + 20, 55),
                    )
                )
                self.registry.mark_queue(item.url, "done", "", status_code)
                self.registry.record_visit(item.url, "done", status_code, "", 0, 0, 1, f"script_redirect:{redirect_url}")
                self.log("script redirect queued", from_url=item.url, to_url=redirect_url)
                return

            doc = html.fromstring(text)
            doc.make_links_absolute(item.url)
            title = extract_title(doc)
            full_text = body_text(doc)
            document_text = clean_text(doc.text_content())[:30000]

            if search_result_domain(domain):
                links_found = self.discover_search_results(doc, item, title, full_text)
                self.registry.mark_queue(item.url, "done", "", status_code)
                self.registry.record_visit(item.url, "done", status_code, title, 0, 0, links_found)
                self.log(
                    "search page done",
                    status=status_code,
                    links=links_found,
                    url=item.url,
                )
                return

            links_found = self.discover_links(doc, item, title, full_text)
            can_extract_people = should_extract_people_from_page(item, title, full_text) or bool(
                labeled_profile_blocks(document_text, max_blocks=1)
            )
            people, page_images = (
                self.extract_people(doc, item, title, full_text)
                if can_extract_people
                else ([], [])
            )
            people.sort(
                key=lambda row: (
                    0 if row.get("gender") == "男" else 1 if not row.get("gender") else 2,
                    0 if row.get("_image_urls") else 1,
                    row.get("name", ""),
                )
            )
            people_found = len(people)

            for person in people:
                person_id = self.registry.upsert_person(person)
                image_urls = list(dict.fromkeys(person.pop("_image_urls", []) or []))
                if not image_urls and len(people) == 1:
                    image_urls = page_images[: self.args.max_images_per_person]
                image_total = min(len(image_urls), self.args.max_images_per_person)
                if image_total:
                    downloaded = self.download_person_images(
                        session=session,
                        person_id=person_id,
                        person=person,
                        image_urls=image_urls[:image_total],
                        image_total=image_total,
                        referer=item.url,
                    )
                    images_found += downloaded
                    if downloaded <= 0:
                        person["person_id"] = person_id
                        person["image_status"] = "failed_image"
                        person["has_official_photo"] = 0
                        person["primary_image_path"] = ""
                        self.registry.upsert_person(person)
                else:
                    # Keep explicit no-image state.
                    person["person_id"] = person_id
                    person["image_status"] = "no_image"
                    person["has_official_photo"] = 0
                    person["primary_image_path"] = ""
                    self.registry.upsert_person(person)

            if people_found or images_found:
                links_found += self.discover_related_profile_pages(doc, item, title, full_text, people_found, images_found)

            self.registry.mark_queue(item.url, "done", "", status_code)
            self.registry.record_visit(item.url, "done", status_code, title, people_found, images_found, links_found)
            self.log(
                "page done",
                status=status_code,
                people=people_found,
                images=images_found,
                links=links_found,
                url=item.url,
            )
        except Exception as exc:
            self.schedule_next_domain(site_group, slow=True)
            self.registry.mark_queue(item.url, "failed", repr(exc), status_code)
            self.registry.record_visit(item.url, "failed", status_code, title, people_found, images_found, links_found, repr(exc))
            self.log("page failed", error=repr(exc), url=item.url)
        finally:
            with self.state_lock:
                self.active_domains.discard(site_group)

    def discover_related_profile_pages(
        self,
        doc: html.HtmlElement,
        item: QueueItem,
        title: str,
        full_text: str,
        people_found: int,
        images_found: int,
    ) -> int:
        if item.depth >= self.args.max_depth:
            return 0
        count = 0
        context = compact_text(f"{title} {full_text[:1200]} {item.unit_name}")
        if not (
            images_found
            or people_found >= 2
            or detail_title_profile_signal(title, full_text)
            or any(k in context for k in ("领导", "简历", "分工", "班子", "图/简历", "任免"))
        ):
            return 0

        candidate_rows: List[Tuple[str, str, int]] = []
        for url in parent_candidate_urls(item.url):
            candidate_rows.append((url, "上级栏目", 60 if images_found else 45))
        for url in sibling_candidate_urls(item.url):
            candidate_rows.append((url, "同目录邻近页", 55 if images_found else 40))
        for a in doc.xpath("//a[@href]"):
            href = normalize_url(a.get("href") or "", item.url)
            if not href or url_should_skip(href):
                continue
            text = compact_text(a.text_content() or a.get("title") or "")
            combined = compact_text(f"{text} {href} {context[:500]}")
            if looks_like_name(text) or high_value_link_text(combined) or detail_title_profile_signal(text, context):
                candidate_rows.append((href, text or "相关人物页", 70 if looks_like_name(text) else 55))

        seen: set[str] = set()
        for url, label, priority in candidate_rows:
            url = normalize_url(url, item.url)
            if not url or url in seen or url == normalize_url(item.url) or url_should_skip(url):
                continue
            seen.add(url)
            combined = f"{label} {url} {context[:800]}"
            if not self.possible_official_url(url, combined, item.url):
                continue
            if not (
                looks_like_name(label)
                or high_value_link_text(combined)
                or profile_url_signal(url)
                or same_profile_directory(item.url, url)
            ):
                continue
            if self.registry.reprioritize_or_enqueue(
                QueueItem(
                    url=url,
                    province=item.province,
                    city=item.city,
                    county_or_district=item.county_or_district,
                    administrative_level=item.administrative_level,
                    unit_name=item.unit_name,
                    source_unit_url=item.source_unit_url or item.url,
                    depth=item.depth + 1,
                    parent_url=item.url,
                    priority=priority,
                )
            ):
                count += 1
        return count

    def discover_search_results(self, doc: html.HtmlElement, item: QueueItem, title: str, full_text: str) -> int:
        if item.depth >= self.args.max_depth:
            return 0
        count = 0
        seen: set[str] = set()
        result_nodes = doc.xpath("//li[contains(concat(' ', normalize-space(@class), ' '), ' b_algo ')]")
        if not result_nodes:
            result_nodes = doc.xpath("//a[@href]/ancestor::*[self::li or self::div][1]")
        for node in result_nodes:
            links = node.xpath(".//a[@href]")
            if not links:
                continue
            href = links[0].get("href") or ""
            url = normalize_url(unwrap_search_result_url(href), item.url)
            if not url or url in seen or url_should_skip(url):
                continue
            seen.add(url)
            link_text = compact_text(links[0].text_content() or links[0].get("title") or "")[:200]
            snippet = compact_text(node.text_content())[:800]
            combined = f"{link_text} {snippet} {url} {title} {full_text[:800]}"
            if not self.possible_official_url(url, combined, item.url):
                continue
            if not relevant_text(combined) and not trusted_profile_context(combined):
                continue
            city = infer_city_from_text(combined)
            child_unit = infer_unit_from_text(link_text, snippet, item.unit_name)
            level = infer_level_from_text(combined)
            priority = 35
            if any(k in combined for k in STRONG_PROFILE_PAGE_KEYWORDS):
                priority += 50
            if any(k in combined for k in ("照片", "图片", "图/简历", "头像", "同志")):
                priority += 20
            if trusted_profile_domain(domain_of(url)):
                priority += 20
            self.registry.add_source_unit(
                province=item.province,
                city=city,
                county="",
                level=level,
                unit_name=child_unit,
                official_site_url=url,
                profile_list_url=url if profile_like_text(combined) else "",
                status="candidate",
                notes=f"open search: {item.unit_name}",
            )
            if self.registry.enqueue(
                QueueItem(
                    url=url,
                    province=item.province,
                    city=city,
                    county_or_district="",
                    administrative_level=level,
                    unit_name=child_unit,
                    source_unit_url=url,
                    depth=item.depth + 1,
                    parent_url=item.url,
                    priority=priority,
                )
            ):
                count += 1
        return count

    def discover_links(self, doc: html.HtmlElement, item: QueueItem, title: str, full_text: str) -> int:
        if item.depth >= self.args.max_depth:
            return 0
        count = 0
        for a in doc.xpath("//a[@href]"):
            href = a.get("href") or ""
            url = normalize_url(href, item.url)
            if not url or url_should_skip(url):
                continue
            link_text = compact_text(a.text_content())[:200]
            if not link_text:
                link_text = compact_text(a.get("title") or "")
            combined = f"{link_text} {url}"
            if not high_value_link_text(combined) and not looks_like_name(link_text):
                continue
            if not self.possible_official_url(url, combined, item.url):
                continue
            if trusted_profile_domain(domain_of(url)) and not trusted_profile_context(
                f"{combined} {title} {full_text[:800]}"
            ):
                continue
            same_domain = domain_of(url) == domain_of(item.url)
            should_follow = high_value_link_text(combined)
            priority = 1
            link_is_name = looks_like_name(link_text) or bool(extract_profile_names(link_text, max_names=1))
            if any(k in combined for k in ("领导", "简历", "分工", "班子", "负责人")):
                priority += 20
            if any(k in combined for k in STRONG_PROFILE_PAGE_KEYWORDS) or profile_url_signal(url):
                priority += 40
            if any(k in combined for k in ("照片", "图片", "头像", "同志")):
                priority += 10
            if link_is_name:
                priority += 30
                if profile_like_text(f"{title} {full_text[:1200]} {item.unit_name}"):
                    should_follow = True
            if not should_follow and item.depth == 0 and same_domain:
                # On seed homepages, allow one shallow hop through government-info
                # navigation even if the URL itself is not descriptive.
                should_follow = any(k in combined for k in ("领导", "机构领导", "领导信息", "领导之窗", "班子"))
            if not should_follow:
                continue

            child_unit = item.unit_name
            child_level = item.administrative_level
            child_city = item.city
            child_county = item.county_or_district
            if any(k in link_text for k in ("区政府", "县政府", "市政府", "人民政府", "公安局", "司法局", "财政局", "教育局")):
                child_unit = sanitize_filename(link_text, item.unit_name)
                if "县" in link_text or "区" in link_text:
                    child_level = "county"
                    child_county = child_county or re.sub(r".*(\S{2,8}[县区]).*", r"\1", link_text)
                self.registry.add_source_unit(
                    province=item.province,
                    city=child_city,
                    county=child_county,
                    level=child_level or "agency",
                    unit_name=child_unit,
                    official_site_url=url,
                    profile_list_url=url if profile_like_text(combined) else "",
                    status="candidate",
                    notes=f"discovered from {item.url}",
                )

            if self.registry.enqueue(
                QueueItem(
                    url=url,
                    province=item.province,
                    city=child_city,
                    county_or_district=child_county,
                    administrative_level=child_level,
                    unit_name=child_unit,
                    source_unit_url=item.source_unit_url or item.url,
                    depth=item.depth + 1,
                    parent_url=item.url,
                    priority=priority,
                )
            ):
                count += 1
        return count

    def content_images(self, doc: html.HtmlElement, base_url: str) -> List[Tuple[str, str]]:
        images: List[Tuple[str, str]] = []
        for img in doc.xpath("//img[@src]"):
            src = normalize_url(img.get("src") or "", base_url)
            alt = compact_text(img.get("alt") or img.get("title") or "")
            if not src or image_should_skip(src, alt):
                continue
            images.append((src, alt))
        return images

    def extract_people(
        self,
        doc: html.HtmlElement,
        item: QueueItem,
        page_title: str,
        full_text: str,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        people: Dict[str, Dict[str, Any]] = {}
        all_images = self.content_images(doc, item.url)
        document_text = clean_text(doc.text_content())[:30000]
        fallback_labeled_blocks = labeled_profile_blocks(document_text, max_blocks=self.args.max_people_per_page)
        if not should_extract_people_from_page(item, page_title, full_text) and not fallback_labeled_blocks:
            return [], []

        def add_person(
            name: str,
            block_text: str,
            method: str,
            image_urls: Optional[List[str]] = None,
            source_url: Optional[str] = None,
            position_override: str = "",
            division_override: str = "",
            biography_override: str = "",
            image_type_hint: str = "",
        ) -> None:
            name_clean = normalize_person_name(name)
            if not looks_like_name(name_clean):
                return
            confidence_text = compact_text(block_text or full_text)
            strong_person_signal = any(
                k in confidence_text
                for k in (
                    "姓名",
                    "简介",
                    "简历",
                    "履历",
                    "职务",
                    "分工",
                    "负责",
                    "分管",
                    "主持",
                    "任",
                    "历任",
                    "曾任",
                    "原任",
                    "公示",
                    "男，",
                    "女，",
                    "男,",
                    "女,",
                    "出生",
                )
            )
            if method not in {"image_block", "detail_title", "labeled_roster", "media_article", "media_index_photo"} and not strong_person_signal:
                return
            src_url = normalize_url(source_url or item.url, item.url)
            unit_name = guess_unit_from_title(page_title, item.unit_name or "")
            position = compact_text(position_override)[:160] or extract_position_near_name(block_text or full_text, name_clean)
            gender = infer_gender(block_text or full_text)
            division = compact_text(division_override)[:2000]
            biography = compact_text(biography_override or block_text)[:4000]
            is_historical = historical_or_former_context(f"{page_title} {block_text}")
            asset_hint = image_type_hint or ("news_photo" if method == "media_article" else "official_photo")
            raw = {
                "extraction_method": method,
                "page_url": item.url,
                "block_text": compact_text(block_text)[:2000],
                "source_unit_url": item.source_unit_url,
                "profile_status": "former_or_historical" if is_historical else "current_or_unspecified",
                "image_type_hint": asset_hint,
            }
            row = {
                "name": name_clean,
                "gender": gender,
                "province": item.province or self.default_province,
                "city": item.city,
                "county_or_district": item.county_or_district,
                "township_or_street": "",
                "administrative_level": item.administrative_level,
                "unit_name": unit_name,
                "department": "",
                "position": position,
                "rank_or_grade": "",
                "division_of_work": division,
                "biography": biography,
                "full_public_text": full_text,
                "source_url": src_url,
                "source_page_title": page_title,
                "source_site_domain": domain_of(src_url),
                "source_unit_url": item.source_unit_url or item.url,
                "publish_date": "",
                "updated_date": "",
                "has_official_photo": 1 if image_urls and asset_hint == "official_photo" else 0,
                "has_news_photo": 1 if image_urls and asset_hint == "news_photo" else 0,
                "image_status": "news_photo" if image_urls and asset_hint == "news_photo" else "official_photo" if image_urls else "no_image",
                "primary_image_path": "",
                "raw_fields_json": json.dumps(raw, ensure_ascii=False),
                "notes": "former_or_historical" if is_historical else "",
                "_image_urls": image_urls or [],
                "_image_type_hint": asset_hint,
            }
            key = f"{name_clean}|{src_url}"
            existing = people.get(key)
            if existing:
                existing_method = ""
                try:
                    existing_raw = json.loads(str(existing.get("raw_fields_json") or "{}"))
                    existing_method = str(existing_raw.get("extraction_method") or "")
                except Exception:
                    existing_method = ""
                existing_images = existing.get("_image_urls") or []
                if (
                    existing_method == "labeled_roster"
                    and method == "image_block"
                    and existing_images
                    and image_urls
                    and len(image_urls) > 1
                ):
                    merged_images = list(existing_images)
                else:
                    merged_images = list(dict.fromkeys(existing_images + (image_urls or [])))
                existing["_image_urls"] = merged_images
                if len(block_text) > len(existing.get("biography", "")):
                    existing["biography"] = biography
                if position and not existing.get("position"):
                    existing["position"] = position
                if division and not existing.get("division_of_work"):
                    existing["division_of_work"] = division
                if image_urls:
                    existing["image_status"] = row["image_status"]
                    existing["has_official_photo"] = max(int(existing.get("has_official_photo") or 0), int(row["has_official_photo"] or 0))
                    existing["has_news_photo"] = max(int(existing.get("has_news_photo") or 0), int(row["has_news_photo"] or 0))
            else:
                people[key] = row

        labeled_blocks = labeled_profile_blocks(full_text, max_blocks=self.args.max_people_per_page)
        if not labeled_blocks:
            labeled_blocks = fallback_labeled_blocks
        if labeled_blocks:
            ordered_images = [url for url, alt in all_images if not image_should_skip(url, alt)]
            for idx, block in enumerate(labeled_blocks):
                paired_images = []
                if idx < len(ordered_images):
                    paired_images = [ordered_images[idx]]
                add_person(
                    block["name"],
                    block["block_text"],
                    "labeled_roster",
                    paired_images,
                    item.url,
                    position_override=block.get("position", ""),
                    division_override=block.get("division_of_work", ""),
                    biography_override=block.get("biography", "") or block.get("block_text", ""),
                    image_type_hint="official_photo",
                )

        # Trusted media and people-library pages often expose a photo through
        # title/alt/context rather than a formal leadership roster.
        if trusted_profile_domain(domain_of(item.url)):
            media_candidates: List[Tuple[str, str, str]] = []
            for img in doc.xpath("//img[@src]"):
                img_url = normalize_url(img.get("src") or "", item.url)
                alt = compact_text(img.get("alt") or img.get("title") or "")
                if not img_url or image_should_skip(img_url, alt):
                    continue
                nearby = nearest_text_block(img)
                context_for_name = compact_text(f"{nearby} {alt}")
                if not context_for_name or not has_position_signal(context_for_name):
                    continue
                media_candidates.append((img_url, alt, nearby))

            grouped_by_nearby: Dict[str, List[Tuple[str, str]]] = {}
            for img_url, alt, nearby in media_candidates:
                grouped_by_nearby.setdefault(nearby, []).append((img_url, alt))

            assigned_media_images: set[str] = set()
            for nearby, entries in grouped_by_nearby.items():
                names = names_from_short_position_block(nearby, max_names=len(entries) + 5)
                if len(names) >= len(entries) and len(entries) > 1:
                    for idx, (img_url, alt) in enumerate(entries):
                        name = names[idx]
                        block = compact_text(f"{page_title} {nearby} {alt} {full_text[:2500]}")
                        add_person(
                            name,
                            block,
                            "media_index_photo",
                            [img_url],
                            item.url,
                            image_type_hint="official_photo",
                        )
                        assigned_media_images.add(img_url)

            for img_url, alt, nearby in media_candidates:
                if img_url in assigned_media_images:
                    continue
                names = image_block_names(nearby, alt)
                if not names and alt:
                    names = image_block_names(alt, alt)
                if not names and has_position_signal(page_title):
                    names = possible_names_from_text(page_title, max_names=3)
                if not names:
                    continue
                block = compact_text(f"{page_title} {nearby} {alt} {full_text[:2500]}")
                for name in names:
                    add_person(
                        name,
                        block,
                        "media_article",
                        [img_url],
                        item.url,
                        image_type_hint="news_photo" if "people.com.cn" in domain_of(item.url) else "official_photo",
                    )

        # Image-centered blocks are high confidence for roster pages.
        if not labeled_blocks and not trusted_profile_domain(domain_of(item.url)):
            for img in doc.xpath("//img[@src]"):
                src = normalize_url(img.get("src") or "", item.url)
                alt = compact_text(img.get("alt") or img.get("title") or "")
                if not src or image_should_skip(src, alt):
                    continue
                node = img
                block = ""
                for _ in range(4):
                    parent = node.getparent()
                    if parent is None:
                        break
                    node = parent
                    block = compact_text(node.text_content())
                    if len(block) < 8:
                        continue
                    if not any(k in block for k in ("分工", "简介", "简历", "男", "女", "负责", "分管", "主持")):
                        continue
                    if len(block) > 2500 and len(self.content_images(node, item.url)) > 3:
                        continue
                    names = image_block_names(block, alt)
                    if not names:
                        continue
                    for name in list(dict.fromkeys(names)):
                        add_person(name, block, "image_block", [src])
                    break

        # Links that are exactly a person name or position+name are usually detail pages.
        for a in doc.xpath("//a[@href]"):
            link_text = compact_text(a.text_content())
            href = normalize_url(a.get("href") or "", item.url)
            if not href or not self.possible_official_url(href, link_text, item.url):
                continue
            if trusted_profile_domain(domain_of(href)) and not trusted_profile_context(
                f"{link_text} {href} {page_title} {full_text[:800]}"
            ):
                continue
            names = []
            if looks_like_name(link_text) and profile_like_text(f"{link_text} {href}"):
                names = [link_text]
            elif profile_like_text(f"{link_text} {href}"):
                names = extract_profile_names(link_text, max_names=3)
            if not names:
                continue
            block = compact_text(a.getparent().text_content() if a.getparent() is not None else link_text)
            for name in names:
                add_person(name, block, "person_link", [], href)

        # Detail pages often have a name as h1/title and biography in body.
        title_name = name_from_page_title(page_title)
        if title_name and profile_detail_text_signal(full_text):
            detail_images = [url for url, _alt in all_images]
            add_person(title_name, full_text, "detail_title", detail_images[: self.args.max_images_per_person])

        # Text-only roster fallback.
        if profile_like_text(page_title) or any(k in item.url for k in ("leader", "lingdao", "ldxx", "ldzc", "jgld")):
            if not labeled_blocks and not trusted_profile_domain(domain_of(item.url)):
                for name in extract_profile_names(full_text, max_names=self.args.max_people_per_page):
                    add_person(name, full_text, "text_pattern", [])

        page_image_urls = [url for url, _alt in all_images]
        return list(people.values())[: self.args.max_people_per_page], page_image_urls

    def output_dir_for_person(self, person: Dict[str, Any]) -> Path:
        province = sanitize_filename(person.get("province") or self.default_province)
        city = sanitize_filename(person.get("city") or "省级")
        raw_county = compact_text(person.get("county_or_district") or "")
        county = sanitize_filename(raw_county) if raw_county else ""
        unit = sanitize_filename(person.get("unit_name") or "未知单位")
        gender_dir = cn_gender(person.get("gender") or "")
        if city == "省级":
            parts = [self.root, province, "省级", unit, gender_dir]
        elif county:
            parts = [self.root, province, city, county, unit, gender_dir]
        else:
            parts = [self.root, province, city, "市级", unit, gender_dir]
        out = Path(*parts)
        out.mkdir(parents=True, exist_ok=True)
        return out

    def image_extension(self, url: str, content_type: str) -> str:
        ext = Path(urlparse(url).path).suffix.lower()
        if ext in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
            return ".jpg" if ext == ".jpeg" else ext
        guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if guessed in {".jpe", ".jpeg"}:
            return ".jpg"
        if guessed in {".jpg", ".png", ".webp", ".gif", ".bmp"}:
            return guessed
        return ".jpg"

    def unique_image_path(self, directory: Path, name: str, image_index: int, ext: str) -> Path:
        base = sanitize_filename(name)
        if image_index <= 1:
            candidate = directory / f"{base}{ext}"
        else:
            candidate = directory / f"{base}-资料图-{image_index - 1}{ext}"
        if not candidate.exists():
            return candidate
        stem = candidate.stem
        for i in range(2, 999):
            retry = directory / f"{stem}_{i}{candidate.suffix}"
            if not retry.exists():
                return retry
        return directory / f"{stem}_{sha1_text(str(time.time()))[:8]}{candidate.suffix}"

    def download_person_images(
        self,
        session: requests.Session,
        person_id: str,
        person: Dict[str, Any],
        image_urls: Sequence[str],
        image_total: int,
        referer: str,
    ) -> int:
        ok_count = 0
        image_type_hint = str(person.get("_image_type_hint") or "").strip() or (
            "news_photo" if person.get("image_status") == "news_photo" else "official_photo"
        )
        for idx, image_url in enumerate(image_urls, start=1):
            if not image_url or image_should_skip(image_url):
                continue
            site_group = site_group_of(image_url)
            try:
                status_code, content_type, payload = self.fetch_image_grouped(session, image_url, site_group, referer)
                if status_code in {403, 429, 521}:
                    with self.state_lock:
                        self.blocked_domains[site_group] = time.time() + self.args.block_backoff_minutes * 60
                    self.log("image blocked", status=status_code, site_group=site_group, url=image_url)
                    continue
                if status_code >= 400 or not content_type.lower().startswith("image/"):
                    continue
                ext = self.image_extension(image_url, content_type)
                out_dir = self.output_dir_for_person(person)
                out_path = self.unique_image_path(out_dir, person["name"], idx, ext)
                out_path.write_bytes(payload)

                width = height = 0
                usable = 1
                try:
                    with Image.open(out_path) as im:
                        width, height = im.size
                    if width < self.args.min_image_width or height < self.args.min_image_height:
                        usable = 0
                    ratio = max(width / max(height, 1), height / max(width, 1))
                    if ratio > self.args.max_image_aspect_ratio:
                        usable = 0
                except Exception:
                    usable = 0

                if not usable:
                    try:
                        out_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue

                digest = sha256_file(out_path) if out_path.exists() else ""
                asset_id = sha1_text(f"{person_id}|{image_url}|official_photo|{idx}|{digest}")
                out_path = self.write_image_metadata(
                    out_path, person, person_id, asset_id, image_url, idx, image_total
                )
                rel_path = str(out_path)
                digest = sha256_file(out_path) if out_path.exists() else digest
                stored_content_type = (
                    "image/jpeg" if out_path.suffix.lower() in {".jpg", ".jpeg"} else content_type
                )
                self.registry.upsert_asset(
                    {
                        "asset_id": asset_id,
                        "person_id": person_id,
                        "name": person["name"],
                        "image_type": image_type_hint if idx == 1 else "material_photo",
                        "image_index": idx,
                        "image_total": image_total,
                        "image_path": rel_path,
                        "image_url": image_url,
                        "source_url": person.get("source_url") or referer,
                        "source_page_title": person.get("source_page_title", ""),
                        "source_site_domain": domain_of(person.get("source_url") or referer),
                        "caption_or_alt": "",
                        "sha256": digest,
                        "content_type": stored_content_type,
                        "width": width,
                        "height": height,
                        "is_usable": usable,
                        "notes": "" if usable else "too_small_or_unreadable",
                    }
                )
                ok_count += int(bool(usable))
            except Exception as exc:
                self.log("image failed", error=repr(exc), url=image_url)
        return ok_count

    def write_image_metadata(
        self,
        path: Path,
        person: Dict[str, Any],
        person_id: str,
        asset_id: str,
        image_url: str,
        image_index: int,
        image_total: int,
    ) -> Path:
        if write_xmp_metadata is None:
            return path
        description = person.get("full_public_text") or person.get("biography") or ""
        image_type = str(person.get("_image_type_hint") or "").strip() or (
            "news_photo" if person.get("image_status") == "news_photo" else "official_photo"
        )
        profile = {
            "person_id": person_id,
            "asset_id": asset_id,
            "name": person.get("name", ""),
            "gender": person.get("gender", ""),
            "province": person.get("province", ""),
            "city": person.get("city", ""),
            "county_or_district": person.get("county_or_district", ""),
            "unit": person.get("unit_name", ""),
            "unit_name": person.get("unit_name", ""),
            "department": person.get("department", ""),
            "position": person.get("position", ""),
            "source_url": person.get("source_url", ""),
            "source_page_title": person.get("source_page_title", ""),
            "source_site_domain": person.get("source_site_domain", ""),
            "source_unit_url": person.get("source_unit_url", ""),
            "image_url": image_url,
            "image_type": image_type if image_index == 1 else "material_photo",
            "image_index": image_index,
            "image_total": image_total,
            "full_content": description,
            "raw_fields": person.get("raw_fields_json", ""),
            "crawled_at": utc_now(),
        }
        metadata = {
            "title": person.get("name", ""),
            "person": person.get("name", ""),
            "name": person.get("name", ""),
            "description": description,
            "source": person.get("source_url", ""),
            "city": person.get("city", ""),
            "position": person.get("position", ""),
            "gender": person.get("gender", ""),
            "image_url": image_url,
            "titi_asset_id": asset_id,
            "d2i_profile": profile,
        }
        try:
            saved_path = write_xmp_metadata(str(path), metadata)
            return Path(saved_path) if saved_path else path
        except Exception as exc:
            self.log("metadata write failed", error=repr(exc), path=str(path))
            return path

    def run(self) -> None:
        reset_count = self.registry.reset_stale_running()
        fixed_paths = self.registry.reconcile_asset_paths()
        fixed_statuses = self.registry.reconcile_image_statuses()
        self.seed_region()
        self.log(
            "collector start",
            region=self.args.region,
            root=str(self.root),
            workers=self.args.max_workers,
            reset_running=reset_count,
            fixed_paths=fixed_paths,
            fixed_statuses=fixed_statuses,
        )
        workers: List[threading.Thread] = []
        pages_started = 0
        last_export = time.time()
        last_progress = 0.0

        while not self.stop_event.is_set():
            workers = [w for w in workers if w.is_alive()]
            while len(workers) < self.args.max_workers:
                item = self.registry.pending_by_domain(self.blocked_domains, self.active_domains)
                if item is None:
                    break
                domain = domain_of(item.url)
                site_group = site_group_of(item.url)
                with self.state_lock:
                    self.active_domains.add(site_group)
                thread = threading.Thread(target=self.process_one, args=(item,), daemon=True)
                thread.start()
                workers.append(thread)
                pages_started += 1
                if self.args.max_pages and pages_started >= self.args.max_pages:
                    self.stop_event.set()
                    break

            counts = self.registry.counts()
            now_ts = time.time()
            if now_ts - last_progress >= self.args.progress_interval:
                self.log("progress", **counts)
                last_progress = now_ts
            if now_ts - last_export >= self.args.export_interval:
                self.registry.export_tables()
                last_export = now_ts

            if not workers and counts["pending"] == 0:
                break
            time.sleep(0.8)

        for worker in workers:
            worker.join()
        self.registry.export_tables()
        self.log("collector done", **self.registry.counts(), db=str(self.registry.db_path))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect official public profile pages into d2ilite registry.")
    parser.add_argument("--region", choices=sorted(REGION_CONFIGS), default="shandong", help="Seed region to crawl.")
    parser.add_argument(
        "--output-root",
        default="",
        help="Project output root.",
    )
    parser.add_argument("--max-workers", type=int, default=4, help="Maximum parallel domains.")
    parser.add_argument("--max-depth", type=int, default=5, help="Discovery depth from seed portals.")
    parser.add_argument("--max-pages", type=int, default=0, help="Optional page cap; 0 means until queue is empty.")
    parser.add_argument("--max-people-per-page", type=int, default=80)
    parser.add_argument("--max-images-per-person", type=int, default=6)
    parser.add_argument("--min-image-width", type=int, default=120)
    parser.add_argument("--min-image-height", type=int, default=120)
    parser.add_argument("--max-image-aspect-ratio", type=float, default=3.0)
    parser.add_argument("--delay-min", type=float, default=1.2)
    parser.add_argument("--delay-max", type=float, default=3.0)
    parser.add_argument("--slow-delay-min", type=float, default=6.0)
    parser.add_argument("--slow-delay-max", type=float, default=15.0)
    parser.add_argument("--timeout", type=int, default=25)
    parser.add_argument("--block-backoff-minutes", type=int, default=45)
    parser.add_argument("--progress-interval", type=float, default=30)
    parser.add_argument("--export-interval", type=float, default=120)
    parser.add_argument("--obey-robots", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--reseed-open-search", action="store_true", help="Seed configured open-search result pages.")
    parser.add_argument("--search-pages", type=int, default=2, help="Search result pages per configured query when reseeding.")
    args = parser.parse_args(argv)
    if not args.output_root:
        args.output_root = str(PROJECT_ROOT / "data" / "public_archive" / REGION_CONFIGS[args.region]["project_name"])
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
    args = parse_args(argv)
    collector = OfficialCollector(args)
    try:
        collector.run()
    except KeyboardInterrupt:
        collector.log("collector interrupted")
        collector.registry.export_tables()
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
