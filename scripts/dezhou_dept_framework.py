"""
德州部门级采集体系框架 — 系统化采集德州部门子站领导数据

设计目标：
  1. 多 CMS 模式支持：不同部门使用不同的模板（列表+详情页、纯文本、直接图列）
  2. 子部门递归：自动发现并追踪子部门（甚至子部门的子部门）
  3. 可配置驱动：部门定义集中管理，新部门只需添加配置
  4. 统一 Registry：与现有 people/image_assets 数据库集成
  5. 可复用架构：可适配其他城市（通过更换域名和路径规则）

用法：
  # 全自动采集所有已配置部门
  python3 scripts/dezhou_dept_framework.py --all

  # 只采集指定部门
  python3 scripts/dezhou_dept_framework.py --departments 卫健委 公安局

  # 只探测模式（不写入 DB）
  python3 scripts/dezhou_dept_framework.py --all --dry-run

  # 从沙箱（Linux）运行：
  cd /sessions/determined-sweet-cannon/mnt/bugemini/d2ilite
  python3 scripts/dezhou_dept_framework.py --all

架构：
  DezhouDeptFramework
    ├── Config (部门定义)
    ├── discover_leadership_pages()     — 探测领导页入口
    ├── extract_leadership_list()       — 提取领导列表
    ├── extract_detail_pages()          — 提取详情页图文
    ├── download_photos()               — 下载照片
    ├── upsert_to_registry()            — 写入 SQLite Registry
    └── explore_sub_departments()       — 递归子部门

作者: Claude (d2ilite)
创建: 2026-06-28
"""

import sys, os, re, json, time, hashlib, sqlite3, io, copy
import urllib.request, ssl
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Any

# ── 路径配置 ──────────────────────────────────────────
BASE = Path(__file__).resolve().parents[1]  # d2ilite 项目根
ARCHIVE = BASE / "data" / "public_archive" / "山东公开官员"
REGISTRY_DB = ARCHIVE / "registry" / "people.sqlite"
PHOTO_BASE = ARCHIVE / "山东省" / "德州市" / "市级"

# ── SSL 兼容上下文（沙箱环境 HTTP 降级） ─────────────────
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


# ══════════════════════════════════════════════════════
#  1. 部门配置
# ══════════════════════════════════════════════════════

class DeptConfig:
    """部门配置：定义目标部门的所有采集参数"""

    def __init__(
        self,
        name: str,              # 部门中文名
        domain: str,            # 子站域名（不含协议）
        cms_pattern: str,       # CMS 类型: "table_detail" | "text_listing" | "photo_listing" | "hybrid"
        known_leadership_urls: list = None,  # 已知领导页路径（相对于 domain）
        detail_path_pattern: str = None,     # 详情页路径正则（如 c\d+/content\.html）
        photo_selector: str = None,          # 照片 CSS/XPath/正则
        name_selector: str = None,           # 姓名提取规则
        bio_selector: str = None,            # 简历提取规则
        photo_path_in_detail: str = "part",  # 详情页中照片所在子路径
        has_dbsource_photo: bool = False,    # 是否使用 dbsource 照片系统
        dbsource_prefix: str = None,         # dbsource URL 前缀
        sub_dept_domains: list = None,       # 子部门域名列表
        parent_n_number: str = None,         # CMS 父节点编号（用于尝试构造领导页路径）
    ):
        self.name = name
        self.domain = domain.rstrip("/")
        self.cms_pattern = cms_pattern
        self.known_leadership_urls = known_leadership_urls or []
        self.detail_path_pattern = detail_path_pattern
        self.photo_selector = photo_selector
        self.name_selector = name_selector
        self.bio_selector = bio_selector
        self.photo_path_in_detail = photo_path_in_detail
        self.has_dbsource_photo = has_dbsource_photo
        self.dbsource_prefix = dbsource_prefix
        self.sub_dept_domains = sub_dept_domains or []
        self.parent_n_number = parent_n_number

    @property
    def base_url(self):
        return f"http://{self.domain}"


# ── 已知的德州部门配置 ──────────────────

DEPT_CONFIGS: dict[str, DeptConfig] = {}

DEPT_CONFIGS["卫健委"] = DeptConfig(
    name="市卫生健康委员会",
    domain="dzwjw.dezhou.gov.cn",
    cms_pattern="table_detail",
    known_leadership_urls=[
        "/n24177433/n38407974/n38407976/n38408400/index.html",
    ],
    detail_path_pattern=r"c\d+/content\.html",
    photo_path_in_detail="part",
    has_dbsource_photo=True,
    dbsource_prefix="dbsource",
    parent_n_number="n24177433",
)

DEPT_CONFIGS["工信局"] = DeptConfig(
    name="市工业和信息化局",
    domain="gxj.dezhou.gov.cn",
    cms_pattern="table_detail",
    known_leadership_urls=[
        "/n53635284/n53635350/n53635352/n53635354/index.html",
    ],
    detail_path_pattern=r"c\d+/content\.html",
    photo_path_in_detail="part",
    parent_n_number="n53635284",
)

DEPT_CONFIGS["信访局"] = DeptConfig(
    name="市信访局",
    domain="dzxfj.dezhou.gov.cn",
    cms_pattern="text_listing",
    known_leadership_urls=[
        "/n54075412/n54075748/index.html",
    ],
    parent_n_number="n54075412",
)

DEPT_CONFIGS["公安局"] = DeptConfig(
    name="市公安局",
    domain="dzga.dezhou.gov.cn",
    cms_pattern="text_listing",  # 暂定，需探索确认
    parent_n_number="n1403",
)

DEPT_CONFIGS["发改委"] = DeptConfig(
    name="市发展和改革委员会",
    domain="fgw.dezhou.gov.cn",
    cms_pattern="unknown",  # 待发现
    parent_n_number="n1403",
)

DEPT_CONFIGS["司法局"] = DeptConfig(
    name="市司法局",
    domain="dzsfj.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["国资委"] = DeptConfig(
    name="市国资委",
    domain="gzw.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["人社局"] = DeptConfig(
    name="市人力资源和社会保障局",
    domain="hrss.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["自然资源局"] = DeptConfig(
    name="市自然资源局",
    domain="zrzyj.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["农业农村局"] = DeptConfig(
    name="市农业农村局",
    domain="nyncj.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["教育局"] = DeptConfig(
    name="市教育局",
    domain="dzedu.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["财政局"] = DeptConfig(
    name="市财政局",
    domain="czj.dezhou.gov.cn",
    cms_pattern="unknown",
)

DEPT_CONFIGS["大数据局"] = DeptConfig(
    name="市大数据局",
    domain="dsjj.dezhou.gov.cn",
    cms_pattern="unknown",
)


# ══════════════════════════════════════════════════════
#  2. HTTP 工具
# ══════════════════════════════════════════════════════

def fetch(url: str, timeout: int = 15) -> Optional[str]:
    """安全获取页面 HTML，失败返回 None"""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=CTX)
        return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None


def fetch_binary(url: str, referer: str = "", timeout: int = 15) -> Optional[bytes]:
    """下载二进制文件（照片），支持 Referer"""
    try:
        h = dict(HEADERS)
        if referer:
            h["Referer"] = referer
        req = urllib.request.Request(url, headers=h)
        resp = urllib.request.urlopen(req, timeout=timeout, context=CTX)
        return resp.read()
    except Exception as e:
        return None


from urllib.parse import urljoin


def resolve_url(base: str, path: str) -> str:
    """将相对路径解析为绝对 URL"""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return urljoin(base, path)


def get_image_dimensions(data: bytes) -> tuple:
    """从 JPEG 二进制数据解析宽高"""
    if data[:3] == b"\xff\xd8\xff":
        i = 2
        while i < len(data) - 9:
            if data[i] == 0xFF and data[i + 1] == 0xC0:
                return (data[i + 7] * 256 + data[i + 8],
                        data[i + 5] * 256 + data[i + 6])
            i += 1
    return (0, 0)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_filename(name: str) -> str:
    """安全文件名"""
    return re.sub(r'[\\/:*?"<>|]', "_", name).strip()


# ══════════════════════════════════════════════════════
#  3. HTML 解析工具
# ══════════════════════════════════════════════════════

def extract_links(html: str) -> list[tuple[str, str]]:
    """提取页面上所有链接 (text, href)"""
    links = re.findall(r'<a[^>]*href="([^"]*)"[^>]*>([^<]*)</a>', html)
    result = []
    for href, text in links:
        t = text.strip()
        if t:
            result.append((t, href.strip()))
    return result


def extract_tables(html: str) -> list[list[list[str]]]:
    """提取所有表格内容为 [[[cell_text, ..], ..], ..]"""
    tables = []
    for table_match in re.finditer(r'<table[^>]*>(.*?)</table>', html, re.DOTALL):
        table_html = table_match.group(1)
        rows = []
        for row_match in re.finditer(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL):
            row_html = row_match.group(1)
            cells = []
            for cell_match in re.finditer(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL):
                cell_html = cell_match.group(1)
                cell_text = re.sub(r'<[^>]+>', "", cell_html).strip()
                # Check for images
                img = re.search(r'<img[^>]*src="([^"]*)"', cell_html)
                cells.append({
                    "text": cell_text,
                    "img": img.group(1) if img else None,
                })
            if cells:
                rows.append(cells)
        if rows:
            tables.append(rows)
    return tables


def extract_text_content(html: str) -> list[str]:
    """提取页面上所有可见文本块"""
    cleaned = re.sub(r'<script[^>]*>.*?</script>', "", html, flags=re.DOTALL)
    cleaned = re.sub(r'<style[^>]*>.*?</style>', "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'&nbsp;', " ", cleaned)
    cleaned = re.sub(r'<br\s*/?>', "\n", cleaned)
    texts = re.findall(r'>([^<]{2,})<', cleaned)
    return [t.strip() for t in texts if t.strip()]


def find_content_area(html: str) -> str:
    """找到页面的主要内容区域（去除导航、页脚等）"""
    # Priority order: more specific patterns first
    content_patterns = [
        r'<div[^>]*class="[^"]*TRS_Editor[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*ewb-editor[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*article[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*main-content[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*text-con[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*nr[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*id="[^"]*(?:content|main|article|text|info|con|nr)[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*(?:content|main|article|text|info|con)[^"]*"[^>]*>(.*?)</div>',
    ]
    best_match = ("", 0)
    for pattern in content_patterns:
        for m in re.finditer(pattern, html, re.DOTALL):
            ct = m.group(1)
            if len(ct) > best_match[1]:
                best_match = (ct, len(ct))
    return best_match[0] if best_match[1] > 100 else html


# ══════════════════════════════════════════════════════
#  4. CMS 模式处理器
# ══════════════════════════════════════════════════════

class BaseHandler:
    """各 CMS 模式的基类"""

    def __init__(self, config: DeptConfig):
        self.config = config

    def discover(self) -> list[dict]:
        """探测领导页入口 → 返回 [{"name": ..., "position": ..., "detail_url": ..., "listing_url": ...}, ...]"""
        raise NotImplementedError

    def extract_detail(self, detail_url: str) -> Optional[dict]:
        """提取单个详情页 → 返回 {"bio": ..., "photo_url": ..., "position": ..., "name": ...} 或 None"""
        raise NotImplementedError


class TableDetailHandler(BaseHandler):
    """
    模式 A: 表格列表 + 详情页 (如 卫健委)
    listing: 表格每行有姓名链接
    detail: cXXXXX/content.html 内有 part/XXXXX.png 照片 + 简介
    """

    def discover(self) -> list[dict]:
        leaders = []
        for listing_url in self.config.known_leadership_urls:
            full_url = self.config.base_url + listing_url
            html = fetch(full_url)
            if not html:
                continue

            # Method 1: Extract from tables (卫健委 pattern)
            tables = extract_tables(html)
            for table in tables:
                for row in table:
                    for cell in row:
                        # Skip nav/template rows
                        if cell["text"] in ["无障碍浏览", "首页", "上页", "下页", "尾页"]:
                            continue
                        if len(cell["text"]) < 2 or len(cell["text"]) > 10:
                            continue
                        # If text looks like a Chinese name (2-4 chars) and has a link
                        # We need to check if this cell text is also a link
                        # Re-scan the HTML specifically for name links
                        pass

            # Method 2: Extract all links, filter by name patterns and path
            links = extract_links(html)
            detail_pat = re.compile(self.config.detail_path_pattern or r"c\d+/content\.html")
            for text, href in links:
                if len(text) >= 2 and len(text) <= 4 and re.match(r'^[一-鿿]+$', text):
                    # Looks like a Chinese name
                    full_href = resolve_url(full_url, href)
                    if detail_pat.search(full_href):
                        leaders.append({
                            "name": text,
                            "detail_url": full_href,
                            "listing_url": full_url,
                            "position": "",  # will be filled from detail
                        })

            # Method 3: Use saved HTML analysis - might have name links in non-standard tags
            # Re-scan for any link with 2-4 Chinese characters that goes to a content page
            if not leaders:
                for text, href in links:
                    if len(text) >= 2 and len(text) <= 4 and re.match(r'^[一-鿿]+$', text):
                        full_href = resolve_url(full_url, href)
                        if "content" in full_href.lower():
                            leaders.append({
                                "name": text,
                                "detail_url": full_href,
                                "listing_url": full_url,
                                "position": "",
                            })

        return leaders

    def extract_detail(self, detail_url: str) -> Optional[dict]:
        html = fetch(detail_url)
        if not html:
            return None

        result = {"detail_url": detail_url}

        # 1. Extract photos
        imgs = re.findall(r'<img[^>]*src="([^"]*)"', html)
        photos = []
        for src in imgs:
            full_src = resolve_url(detail_url, src)
            photos.append(full_src)

        portrait_candidates = []
        common_images = []

        for url in photos:
            if "/part/" in url and "/template/" not in url:
                portrait_candidates.append(url)
            elif "/dbsource/" in url and "/template/" not in url:
                # Only use dbsource if no part image found
                common_images.append(url)
            elif "/template/" in url or "/web_style/" in url:
                pass  # template/site images are not photos
            else:
                portrait_candidates.append(url)

        result["photo_urls"] = portrait_candidates if portrait_candidates else common_images
        result["common_images"] = common_images

        # 2. Extract bio text - try content area first, then full HTML
        content = find_content_area(html)
        if len(content) < 50:  # content area too small, use full HTML
            content = html
        texts = extract_text_content(content)

        # Also try fallback: extract from full HTML if content area fails
        if not any(("男" in t or "女" in t) for t in texts[:20]):
            texts = extract_text_content(html)

        bio_parts = []
        position = ""
        name = ""
        full_bio = ""

        for t in texts:
            # Skip boilerplate
            if any(k in t for k in ["无障碍", "网站首页", "首页", "导航", "友情链接", "版权所有",
                                     "ICP备", "您所在的"]):
                continue
            if len(t) < 4:
                continue

            # Bio line patterns
            if re.match(r'^[一-鿿]{2,4}[，,]\s*[男女]', t.strip()):
                name = t.split("，")[0].strip()
                full_bio = t
                bio_parts.append(t)
            elif re.match(r'^[一-鿿]{2,4}\s+现任', t.strip()):
                # "周春华　现任..." with full-width space
                name_match = re.match(r'^([一-鿿]{2,4})\s+', t)
                if name_match:
                    name = name_match.group(1)
                # Extract position after "现任"
                pos_match = re.search(r'现任\s*(.*)', t)
                if pos_match:
                    position = pos_match.group(1).strip()
                bio_parts.append(t)
            elif "现任" in t and any(k in t for k in ["书记", "局长", "主任", "会长", "院长", "校长"]):
                if not position:
                    position = t.replace("现任", "").strip()
                bio_parts.append(t)
            elif "工作分工" in t:
                bio_parts.append(t)
            elif any(k in t for k in ["负责", "分管", "主持"]):
                if len(t) > 10:  # meaningful text
                    bio_parts.append(t)
            elif any(k in t for k in ["中共党员", "九三学社", "研究生", "大学", "学士", "硕士",
                                       "汉族", "回族", "出生", "山东"]):
                if not full_bio:
                    full_bio = t
                bio_parts.append(t)

        result["bio"] = "\n".join(bio_parts) if bio_parts else ""
        result["position"] = position
        result["name"] = name
        result["full_bio"] = full_bio

        # 3. Extract gender
        gender = "未知"
        if "男" in full_bio:
            gender = "男"
        elif "女" in full_bio:
            gender = "女"
        result["gender"] = gender

        # 4. Extract ethnicity
        ethnicity_match = re.search(r'(汉族|回族|满族|蒙古族|壮族|苗族|藏族|维吾尔族)', full_bio)
        result["ethnicity"] = ethnicity_match.group(1) if ethnicity_match else ""

        return result


class TextListingHandler(BaseHandler):
    """
    模式 B: 纯文本列表 + 摘要简介 (如 信访局)
    listing: 页面直接包含领导姓名、职务、简介
    """

    def discover(self) -> list[dict]:
        leaders = []
        for listing_url in self.config.known_leadership_urls:
            full_url = self.config.base_url + listing_url
            html = fetch(full_url)
            if not html:
                continue

            # Use full HTML for text listings - bios are in non-standard containers
            texts = extract_text_content(html)

            for t in texts:
                # Split by Chinese semicolon (multiple bios in one text block)
                for seg in re.split(r'[；;]\s*', t.strip()):
                    seg = seg.strip()
                    if not seg:
                        continue
                    m = re.match(r'^([一-鿿]{2,4})[，,]\s*([男女])', seg)
                    if m:
                        name = m.group(1)
                        gender = m.group(2)
                        if not any(l["name"] == name for l in leaders):
                            leaders.append({
                                "name": name,
                                "gender": gender,
                                "bio_text": seg,
                                "listing_url": full_url,
                                "detail_url": "",
                            })

        return leaders

    def extract_detail(self, detail_url: str) -> Optional[dict]:
        # Text listing departments usually don't have detail pages
        return None


class PhotoListingHandler(BaseHandler):
    """
    模式 C: 直接照片列表 (如德州市主站领导页)
    listing: 页面直接显示领导照片和姓名
    """

    def discover(self) -> list[dict]:
        # Similar to the main dezhou city site collection
        leaders = []
        for listing_url in self.config.known_leadership_urls:
            full_url = self.config.base_url + listing_url
            html = fetch(full_url)
            if not html:
                continue

            # Find photo + name combinations
            # Name links pointing to detail pages
            links = extract_links(html)
            detail_pat = re.compile(self.config.detail_path_pattern or r"index\.html")

            # Find inline photos with adjacent names
            imgs = re.findall(r'<img[^>]*src="([^"]*)"[^>]*>', html)
            for src in imgs:
                if "/dbsource/" in src:
                    full_src = resolve_url(full_url, src)
                    leaders.append({
                        "name": "",  # need to find adjacent name
                        "photo_url": full_src,
                        "listing_url": full_url,
                    })

        return leaders

    def extract_detail(self, detail_url: str) -> Optional[dict]:
        html = fetch(detail_url)
        if not html:
            return None
        # Same as the main site detail extraction
        return extract_bio_from_generic_detail(html, detail_url)


class UnknownHandler(BaseHandler):
    """
    模式 ?: 尚未识别的 CMS 类型
    discovery: 尝试多种策略找到领导页
    """

    COMMON_LEADERSHIP_PATHS = [
        # 常见领导页路径模式
        "/n1403/n6862/index.html",     # 德州主站领导页
        "/col/col122/index.html",
        "/col/col563/index.html",
        # 机构概况下的领导信息
        "/n24177433/n38407974/n38407976/n38408400/index.html",
        # 政务公开-基础信息公开-机构职能-领导信息
        "/n53635284/n53635350/n53635352/n53635354/index.html",
    ]

    def discover(self) -> list[dict]:
        base = self.config.base_url
        html = fetch(base)
        if not html:
            return []

        leaders = []
        links = extract_links(html)

        # Strategy 1: Look for "领导" keyword links
        for text, href in links:
            if "领导" in text or "分工" in text:
                full_url = resolve_url(base, href)
                # Try this page
                sub_html = fetch(full_url)
                if sub_html:
                    sub_leaders = try_parse_leadership_page(sub_html, full_url)
                    leaders.extend(sub_leaders)

        # Strategy 2: Try common paths
        for path in self.COMMON_LEADERSHIP_PATHS:
            test_url = base + path
            test_html = fetch(test_url)
            if test_html and "领导" in test_html:
                sub_leaders = try_parse_leadership_page(test_html, test_url)
                leaders.extend(sub_leaders)

        # Strategy 3: Crawl nav links looking for leadership keywords
        nav_items = re.findall(r'<li[^>]*>.*?<a[^>]+href="([^"]*)"[^>]*>([^<]*)</a>', html, re.DOTALL)
        for href, text in nav_items:
            t = text.strip()
            if any(k in t for k in ["机构", "概况", "政务公开"]):
                full_url = resolve_url(base, href)
                sub_html = fetch(full_url)
                if sub_html:
                    sub_links = extract_links(sub_html)
                    for sub_text, sub_href in sub_links:
                        if "领导" in sub_text or "分工" in sub_text:
                            sub_full_url = resolve_url(full_url, sub_href)
                            sub_sub_html = fetch(sub_full_url)
                            if sub_sub_html:
                                sub_leaders = try_parse_leadership_page(sub_sub_html, sub_full_url)
                                leaders.extend(sub_leaders)

        return leaders

    def extract_detail(self, detail_url: str) -> Optional[dict]:
        html = fetch(detail_url)
        if not html:
            return None
        return extract_bio_from_generic_detail(html, detail_url)


def try_parse_leadership_page(html: str, page_url: str) -> list[dict]:
    """尝试解析任意领导页，返回领导列表"""
    leaders = []

    # Try table pattern
    tables = extract_tables(html)
    links = extract_links(html)

    for text, href in links:
        if len(text) >= 2 and len(text) <= 4 and re.match(r'^[一-鿿]+$', text):
            full_href = resolve_url(page_url, href)
            if "content" in full_href.lower() or re.search(r'c\d+/', full_href):
                leaders.append({
                    "name": text,
                    "detail_url": full_href,
                    "listing_url": page_url,
                    "position": "",
                })

    # Also try text pattern (inline bios)
    texts = extract_text_content(html)
    for t in texts:
        m = re.match(r'^([一-鿿]{2,4})[，,]\s*([男女])', t)
        if m:
            name = m.group(1)
            # Check if this name is already captured
            if not any(l["name"] == name for l in leaders):
                leaders.append({
                    "name": name,
                    "detail_url": "",
                    "listing_url": page_url,
                    "bio_text": t,
                })

    return leaders


def extract_bio_from_generic_detail(html: str, detail_url: str) -> Optional[dict]:
    """通用详情页提取（用于 UnknownHandler）"""
    result = {"detail_url": detail_url}

    # Extract images
    imgs = re.findall(r'<img[^>]*src="([^"]*)"', html)
    photos = []
    for src in imgs:
        full_src = resolve_url(detail_url, src)
        if "/template/" not in full_src and "/web_style/" not in full_src:
            photos.append(full_src)
    result["photo_urls"] = photos

    # Extract text
    content = find_content_area(html)
    texts = extract_text_content(content)

    bio_parts = []
    name = ""
    gender = "未知"

    for t in texts:
        if re.match(r'^[一-鿿]{2,4}[，,]\s*[男女]', t):
            name = t.split("，")[0].split(",")[0].strip()
            gender = "男" if "男" in t[:10] else "女"
            bio_parts.append(t)
        elif any(k in t for k in ["现任", "曾任", "负责", "分管", "主持", "中共党员",
                                   "大学", "研究生", "学士", "硕士", "出生"]):
            bio_parts.append(t)

    result["bio"] = "\n".join(bio_parts) if bio_parts else ""
    result["name"] = name
    result["gender"] = gender

    # Position
    for t in texts:
        pos_match = re.search(r'(党组书记|党组副书记|局长|副局长|主任|副主任|会长|副会长|主席|调研员)', t)
        if pos_match:
            result["position"] = t.strip()
            break

    return result


def get_handler(config: DeptConfig) -> BaseHandler:
    """工厂方法：根据 CMS 模式创建对应处理器"""
    handlers = {
        "table_detail": TableDetailHandler,
        "text_listing": TextListingHandler,
        "photo_listing": PhotoListingHandler,
    }
    handler_cls = handlers.get(config.cms_pattern, UnknownHandler)
    return handler_cls(config)


# ══════════════════════════════════════════════════════
#  5. Registry 操作（嵌入式 SQLite）
# ══════════════════════════════════════════════════════

class Registry:
    """嵌入式 Registry 操作（避免主采集器的导入问题）"""

    def __init__(self, db_path: Path, province: str = "山东省",
                 fallback_tmp: Optional[Path] = None):
        self._prod_db = db_path
        self.province = province
        self._tmp_db = Path(f"/tmp/dezhou_dept_registry.sqlite")

        # Check if a valid fallback /tmp copy exists (avoids FUSE disk I/O corruption)
        if fallback_tmp and fallback_tmp.exists():
            try:
                test_conn = sqlite3.connect(str(fallback_tmp))
                test_conn.execute("SELECT COUNT(*) FROM people")
                test_conn.close()
                # Fallback is valid, use it as source
                with open(fallback_tmp, "rb") as f:
                    db_bytes = f.read()
                print(f"  📦 Using fallback DB: {fallback_tmp}")
            except Exception:
                db_bytes = self._read_db_with_retry(db_path)
        else:
            db_bytes = self._read_db_with_retry(db_path)

        with open(self._tmp_db, "wb") as f:
            f.write(db_bytes)

        self.conn = sqlite3.connect(str(self._tmp_db))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_tables()

    def _read_db_with_retry(self, db_path: Path, retries: int = 3) -> bytes:
        """读取 DB 文件，带重试（处理 FUSE 临时错误）"""
        import time
        for attempt in range(retries):
            try:
                with open(db_path, "rb") as f:
                    return f.read()
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                # Last resort: try /tmp known-good backup
                for backup in [
                    Path("/tmp/dezhou_restored_base.sqlite"),
                    Path("/tmp/dezhou_people.sqlite"),
                ]:
                    if backup.exists():
                        print(f"  ⚠️  FUSE 读取失败 ({e})，使用 /tmp 备份: {backup}")
                        with open(backup, "rb") as f:
                            return f.read()
                raise

    def _ensure_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS people (
                person_id TEXT PRIMARY KEY,
                name TEXT, gender TEXT,
                province TEXT, city TEXT, county_or_district TEXT, township_or_street TEXT,
                administrative_level TEXT, unit_name TEXT, department TEXT,
                position TEXT, rank_or_grade TEXT, division_of_work TEXT,
                biography TEXT, full_public_text TEXT,
                source_url TEXT, source_page_title TEXT, source_site_domain TEXT,
                source_unit_url TEXT,
                publish_date TEXT, updated_date TEXT,
                has_official_photo INTEGER DEFAULT 0,
                has_news_photo INTEGER DEFAULT 0,
                image_status TEXT,
                primary_image_path TEXT,
                raw_fields_json TEXT,
                crawled_at TEXT, last_checked_at TEXT, notes TEXT
            );
            CREATE TABLE IF NOT EXISTS image_assets (
                asset_id TEXT PRIMARY KEY,
                person_id TEXT,
                name TEXT, image_type TEXT,
                image_index INTEGER, image_total INTEGER,
                image_path TEXT, image_url TEXT,
                source_url TEXT, source_page_title TEXT, source_site_domain TEXT,
                caption_or_alt TEXT,
                sha256 TEXT, content_type TEXT,
                width INTEGER, height INTEGER,
                is_usable INTEGER DEFAULT 1,
                crawled_at TEXT, notes TEXT
            );
            CREATE TABLE IF NOT EXISTS source_units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                province TEXT, city TEXT, county TEXT,
                level TEXT, unit_name TEXT,
                official_site_url TEXT, profile_list_url TEXT,
                status TEXT, notes TEXT
            );
        """)
        self.conn.commit()

    def get_existing_person(self, name: str, city: str = "德州市",
                            unit_name: str = "") -> Optional[dict]:
        """查找是否已存在同名同地区人员"""
        q = "SELECT * FROM people WHERE name=? AND city=?"
        params = [name, city]
        if unit_name:
            q += " AND unit_name=?"
            params.append(unit_name)
        row = self.conn.execute(q, params).fetchone()
        return dict(row) if row else None

    def upsert_person(self, row: dict) -> str:
        """插入或更新人员记录"""
        row.setdefault("person_id", "")
        if not row["person_id"]:
            # Generate deterministic ID
            raw = f"{row.get('name','')}|{row.get('city','')}|{row.get('unit_name','')}|{row.get('position','')}"
            row["person_id"] = hashlib.sha1(raw.encode()).hexdigest()[:16]
        row.setdefault("crawled_at", utc_now())
        row.setdefault("last_checked_at", row["crawled_at"])
        row.setdefault("gender", "未知")
        row.setdefault("province", self.province)
        row.setdefault("source_site_domain", "")

        cols = list(row.keys())
        placeholders = ":" + ", :".join(cols)
        update_set = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "person_id")

        sql = f"""
        INSERT INTO people ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(person_id) DO UPDATE SET {update_set}
        """
        self.conn.execute(sql, row)
        self.conn.commit()
        return row["person_id"]

    def upsert_image_asset(self, asset: dict) -> str:
        """插入或更新图片资产"""
        asset.setdefault("asset_id", "")
        if not asset["asset_id"]:
            raw = f"{asset.get('person_id','')}|{asset.get('image_url','')}"
            asset["asset_id"] = hashlib.sha1(raw.encode()).hexdigest()[:16]
        asset.setdefault("crawled_at", utc_now())
        asset.setdefault("is_usable", 1)

        cols = list(asset.keys())
        placeholders = ":" + ", :".join(cols)
        update_set = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "asset_id")

        sql = f"""
        INSERT INTO image_assets ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(asset_id) DO UPDATE SET {update_set}
        """
        self.conn.execute(sql, asset)
        self.conn.commit()
        return asset["asset_id"]

    def add_source_unit(self, province: str, city: str, county: str,
                        level: str, unit_name: str,
                        official_site_url: str = "",
                        profile_list_url: str = "",
                        status: str = "collected",
                        notes: str = ""):
        self.conn.execute(
            """INSERT OR IGNORE INTO source_units
               (province, city, county, level, unit_name, official_site_url, profile_list_url, status, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (province, city, county, level, unit_name,
             official_site_url, profile_list_url, status, notes)
        )
        self.conn.commit()

    def update_person_image_status(self, person_id: str, image_path: str):
        self.conn.execute(
            "UPDATE people SET image_status='official_photo', primary_image_path=? WHERE person_id=?",
            (image_path, person_id)
        )
        self.conn.commit()

    def copy_back(self):
        """将 /tmp 的 DB 复制回生产路径"""
        self.conn.close()
        with open(self._tmp_db, "rb") as f:
            db_bytes = f.read()
        with open(self._prod_db, "wb") as f:
            f.write(db_bytes)
        print(f"  ✅ DB 已写回: {self._prod_db}")

    def close(self):
        self.conn.close()

    def export_kpi(self) -> dict:
        row = self.conn.execute("""
            SELECT
              (SELECT COUNT(*) FROM people) AS people,
              (SELECT COUNT(*) FROM image_assets WHERE is_usable=1) AS usable_assets,
              (SELECT COUNT(DISTINCT person_id) FROM image_assets WHERE is_usable=1) AS people_with_assets,
              (SELECT COUNT(*) FROM people WHERE gender='男' AND COALESCE(primary_image_path,'')<>'') AS male_with_img,
              (SELECT COUNT(*) FROM people WHERE gender='女' AND COALESCE(primary_image_path,'')<>'') AS female_with_img,
              (SELECT COUNT(*) FROM people WHERE city='德州市') AS dezhou_people,
              (SELECT COUNT(*) FROM image_assets WHERE is_usable=1 AND image_path IN
                 (SELECT primary_image_path FROM people WHERE city='德州市')) AS dezhou_assets
        """).fetchone()
        return dict(row)


# ══════════════════════════════════════════════════════
#  6. 照片下载与元数据
# ══════════════════════════════════════════════════════

def download_photo(url: str, referer: str = "") -> Optional[bytes]:
    """下载照片，支持 Referer"""
    return fetch_binary(url, referer=referer)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def get_photo_path(photo_base: Path, unit_name: str, gender: str, name: str,
                   index: int = 0, total: int = 1) -> Path:
    """确定照片存储路径"""
    gender_dir = "男" if gender == "男" else ("女" if gender == "女" else "未知")
    unit_dir = sanitize_filename(unit_name)
    target_dir = photo_base / unit_dir / gender_dir
    ensure_dir(target_dir)

    if total == 1:
        filename = f"{name}.jpg"
    else:
        suffix = "" if index == 0 else f"-资料图-{index}"
        filename = f"{name}{suffix}.jpg"

    return target_dir / filename


def try_build_xmp(photo_path: Path, metadata: dict) -> bool:
    """尝试写入 XMP 元数据（使用 metadata_writer 模块）"""
    try:
        sys.path.insert(0, str(BASE))
        from metadata_writer import write_xmp_metadata, build_titi_json

        # Build the d2i_profile from metadata
        profile = {
            "person_id": metadata.get("person_id", ""),
            "asset_id": metadata.get("asset_id", ""),
            "name": metadata.get("name", ""),
            "gender": metadata.get("gender", ""),
            "province": metadata.get("province", "山东省"),
            "city": metadata.get("city", "德州市"),
            "county_or_district": metadata.get("county_or_district", ""),
            "unit_name": metadata.get("unit_name", ""),
            "department": metadata.get("department", metadata.get("unit_name", "")),
            "position": metadata.get("position", ""),
            "source_url": metadata.get("source_url", ""),
            "source_page_title": metadata.get("source_page_title", ""),
            "source_site_domain": metadata.get("source_site_domain", ""),
            "source_unit_url": metadata.get("source_unit_url", ""),
            "image_url": metadata.get("image_url", ""),
            "image_type": metadata.get("image_type", "official_photo"),
            "image_index": metadata.get("image_index", 0),
            "image_total": metadata.get("image_total", 1),
            "full_content": metadata.get("bio", ""),
            "raw_fields": json.dumps(metadata.get("raw_fields", {}), ensure_ascii=False),
            "crawled_at": metadata.get("crawled_at", utc_now()),
        }

        titi = build_titi_json({"d2i_profile": profile})
        write_xmp_metadata(str(photo_path), titi)
        return True
    except Exception as e:
        print(f"    ⚠️  XMP 写入失败: {e}")
        return False


# ══════════════════════════════════════════════════════
#  7. 主采集流程
# ══════════════════════════════════════════════════════

class DeptCollector:
    """部门采集器：运行完整的采集流水线"""

    def __init__(self, registry: Registry, dry_run: bool = False):
        self.registry = registry
        self.dry_run = dry_run
        self.stats = {"departments": 0, "leaders_found": 0, "photos_downloaded": 0,
                      "people_upserted": 0, "assets_upserted": 0, "errors": 0}

    def collect_department(self, config: DeptConfig):
        """采集单个部门的所有领导数据"""
        print(f"\n{'='*60}")
        print(f"📋 部门: {config.name} ({config.domain})")
        print(f"     CMS模式: {config.cms_pattern}")
        print(f"{'='*60}")

        handler = get_handler(config)
        self.stats["departments"] += 1

        # Step 1: Discover leadership pages
        print(f"\n  [1/4] 探测领导列表...")
        leaders = handler.discover()
        if not leaders:
            print(f"  ⚠️  未发现领导数据")
            # Try exploration mode for unknown CMS
            if config.cms_pattern == "unknown":
                print(f"  ↪ 尝试探索模式...")
                leaders = self._explore_department(config)
        print(f"  ✅ 发现 {len(leaders)} 位领导")

        if not leaders:
            return

        self.stats["leaders_found"] += len(leaders)

        # Step 2: Extract detail pages
        print(f"\n  [2/4] 提取详情页...")
        for li, leader in enumerate(leaders):
            detail_url = leader.get("detail_url", "")
            if detail_url:
                detail_data = handler.extract_detail(detail_url)
                if detail_data:
                    leader.update(detail_data)
                    print(f"    {li+1}. {leader.get('name','?')} "
                          f"| 照片: {len(leader.get('photo_urls', []))} "
                          f"| 性别: {leader.get('gender','?')}")
                else:
                    print(f"    {li+1}. {leader.get('name','?')} | ⚠️ 详情页提取失败")
            else:
                # For text listing, bio is already in leader
                print(f"    {li+1}. {leader.get('name','?')} | 纯文本 | {leader.get('bio_text','')[:30]}...")
            time.sleep(0.5)

        # Step 3: Download photos and register
        print(f"\n  [3/4] 下载照片 & 注册入库...")
        for leader in leaders:
            self._process_leader(config, leader)
            time.sleep(0.3)

        # Step 4: Register source unit
        if self.registry:
            self.registry.add_source_unit(
                province="山东省",
                city="德州市",
                county="",
                level="city",
                unit_name=config.name,
                official_site_url=config.base_url,
                profile_list_url=config.base_url + (config.known_leadership_urls[0] if config.known_leadership_urls else ""),
                status="collected",
                notes=f"CMS pattern: {config.cms_pattern}"
            )

        print(f"\n  [4/4] ✅ {config.name} 采集完成")

    def _process_leader(self, config: DeptConfig, leader: dict):
        """处理单个领导：入库、下载、注册图片"""
        name = leader.get("name", "")
        if not name:
            self.stats["errors"] += 1
            return

        # Determine unit_name for the folder structure
        unit_name = leader.get("unit_name", config.name)
        gender = leader.get("gender", "未知")
        bio = leader.get("bio", "")
        position = leader.get("position", "")
        full_public_text = leader.get("full_public_text",
                                      leader.get("bio", "") + "\n" + leader.get("bio_text", ""))
        photo_urls = leader.get("photo_urls", [])
        detail_url = leader.get("detail_url", "")

        # Prepare people row
        person_data = {
            "name": name,
            "gender": gender,
            "province": "山东省",
            "city": "德州市",
            "county_or_district": "",
            "township_or_street": "",
            "administrative_level": "city",
            "unit_name": unit_name,
            "department": unit_name,
            "position": position,
            "division_of_work": "",
            "biography": bio,
            "full_public_text": full_public_text,
            "source_url": detail_url or leader.get("listing_url", ""),
            "source_page_title": f"{name} - {config.name} - 德州市",
            "source_site_domain": config.domain,
            "source_unit_url": config.base_url,
            "image_status": "official_photo" if photo_urls else "no_photo",
        }

        if self.dry_run:
            print(f"    🏃 {name} ({gender}) - dry run, skipping DB")
            if photo_urls:
                for pu in photo_urls[:2]:
                    print(f"       照片: {pu[:80]}")
            return

        # Upsert person
        person_id = self.registry.upsert_person(person_data)
        self.stats["people_upserted"] += 1

        # Process photos
        total_photos = len(photo_urls)
        for idx, photo_url in enumerate(photo_urls):
            time.sleep(0.3)  # Be polite

            # Download
            img_data = download_photo(photo_url, referer=detail_url or leader.get("listing_url", ""))
            if not img_data:
                print(f"    ⚠️  {name} 照片下载失败: {photo_url[:60]}")
                continue

            # Determine path
            photo_path = get_photo_path(PHOTO_BASE, unit_name, gender, name, idx, total_photos)
            sha256 = sha256_bytes(img_data)
            width, height = get_image_dimensions(img_data)

            # Save file
            with open(photo_path, "wb") as f:
                f.write(img_data)
            print(f"    📸 {name} -> {photo_path.name} ({width}x{height}, {len(img_data)} bytes)")

            self.stats["photos_downloaded"] += 1

            # Prepare asset
            # Check content type
            ct = "image/jpeg"
            if photo_url.lower().endswith(".png"):
                ct = "image/png"

            asset = {
                "person_id": person_id,
                "name": name,
                "image_type": "official_photo" if idx == 0 else "profile_photo",
                "image_index": idx,
                "image_total": total_photos,
                "image_path": str(photo_path),
                "image_url": photo_url,
                "source_url": detail_url or leader.get("listing_url", ""),
                "source_page_title": f"{name} - {config.name} - 德州市",
                "source_site_domain": config.domain,
                "caption_or_alt": f"{name} {position}",
                "sha256": sha256,
                "content_type": ct,
                "width": width,
                "height": height,
                "is_usable": 1 if width > 100 else 0,  # filter tiny images
            }

            asset_id = self.registry.upsert_image_asset(asset)
            self.stats["assets_upserted"] += 1

            # Update person's primary_image_path
            if idx == 0:
                self.registry.update_person_image_status(person_id, str(photo_path))

            # Write XMP
            xmp_meta = dict(person_data)
            xmp_meta.update({
                "person_id": person_id,
                "asset_id": asset_id,
                "image_url": photo_url,
                "image_index": idx,
                "image_total": total_photos,
                "image_type": "official_photo",
                "bio": bio,
                "raw_fields": {"detail_url": detail_url, "listing_url": leader.get("listing_url", "")},
                "crawled_at": utc_now(),
            })
            try_build_xmp(photo_path, xmp_meta)

    def _explore_department(self, config: DeptConfig) -> list[dict]:
        """探索模式：尝试多种方法发现领导页"""
        base = config.base_url
        html = fetch(base)
        if not html:
            return []

        results = []
        links = extract_links(html)

        # Find links containing key navigation terms
        nav_targets = []
        for text, href in links:
            if any(k in text for k in ["机构", "概况", "政务公开", "信息公开"]):
                nav_targets.append((text, href))

        # Explore nav targets
        for nav_text, nav_href in nav_targets:
            nav_url = resolve_url(base, nav_href)
            nav_html = fetch(nav_url)
            if not nav_html:
                continue

            nav_links = extract_links(nav_html)
            for sub_text, sub_href in nav_links:
                if any(k in sub_text for k in ["领导", "分工", "班子", "机构"]):
                    sub_url = resolve_url(nav_url, sub_href)
                    sub_html = fetch(sub_url)
                    if sub_html:
                        sub_leaders = try_parse_leadership_page(sub_html, sub_url)
                        results.extend(sub_leaders)

        # Deduplicate by name
        seen = set()
        deduped = []
        for r in results:
            if r["name"] and r["name"] not in seen:
                seen.add(r["name"])
                deduped.append(r)

        return deduped


# ══════════════════════════════════════════════════════
#  8. CLI 入口
# ══════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="德州部门级采集体系框架")
    parser.add_argument("--all", action="store_true", help="采集所有已配置部门")
    parser.add_argument("--departments", nargs="+", default=[], help="指定部门名称，如 卫健委 公安局")
    parser.add_argument("--dry-run", action="store_true", help="仅探测，不写入 DB")
    parser.add_argument("--list", action="store_true", help="列出所有已配置部门")
    args = parser.parse_args()

    if args.list:
        print("已配置的德州部门:")
        for key, cfg in sorted(DEPT_CONFIGS.items()):
            print(f"  {key:12s} {cfg.domain:35s} CMS:{cfg.cms_pattern:15s} 领导页:{len(cfg.known_leadership_urls)}")
        return

    if not args.all and not args.departments:
        parser.print_help()
        print("\n请指定 --all 或 --departments，或 --list 查看可用部门")
        return

    # Select departments
    if args.all:
        selected = list(DEPT_CONFIGS.keys())
    else:
        selected = args.departments

    # Validate
    for key in selected:
        if key not in DEPT_CONFIGS:
            print(f"❌ 未知部门: {key}。可用: {list(DEPT_CONFIGS.keys())}")
            return

    # Init registry
    if not args.dry_run:
        print(f"\n📦 打开 Registry: {REGISTRY_DB}")
        print(f"📁 照片输出: {PHOTO_BASE}")
        reg = Registry(REGISTRY_DB, "山东省")
    else:
        reg = None
        print(f"\n🏃 DRY RUN 模式 — 不会写入数据库或下载照片")

    collector = DeptCollector(reg, dry_run=args.dry_run)

    # Collect each department
    for key in selected:
        config = DEPT_CONFIGS[key]
        collector.collect_department(config)

    # Summary
    print(f"\n{'='*60}")
    print(f"📊 采集统计")
    print(f"{'='*60}")
    print(f"  部门数:        {collector.stats['departments']}")
    print(f"  发现领导:      {collector.stats['leaders_found']}")
    print(f"  下载照片:      {collector.stats['photos_downloaded']}")
    print(f"  入库人员:      {collector.stats['people_upserted']}")
    print(f"  入库图片资产:  {collector.stats['assets_upserted']}")
    print(f"  错误:          {collector.stats['errors']}")

    # KPI
    if reg:
        kpi = reg.export_kpi()
        print(f"\n📈 全局 KPI")
        for k, v in kpi.items():
            print(f"  {k}: {v}")
        print(f"\n💾 写回 DB...")
        reg.copy_back()
        print(f"✅ 完成！")
    else:
        print(f"\n🏃 Dry run 完成（未实际写入）")

if __name__ == "__main__":
    main()
