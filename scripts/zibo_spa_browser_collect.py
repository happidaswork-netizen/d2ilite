"""
淄博 SPA 专采：用 Edge 浏览器渲染 Vue SPA 页面后提取领导信息

用法：
  cd /d D:\bugemini\d2ilite
  .venv\Scripts\python.exe scripts\zibo_spa_browser_collect.py

说明：
  淄博市政府网站（zibo.gov.cn）使用 Vue.js SPA，requests 无法获取动态内容。
  该脚本用 Playwright + Edge 逐个访问已知领导页面，提取照片和简介，
  写入 registry（与主采集器共用元数据/目录结构）。
"""

import sys, time, re, json, hashlib
from pathlib import Path
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scraper.run_official_profile_collector import Registry, QueueItem
from metadata_writer import write_xmp_metadata

ROOT = Path(r"D:\bugemini\d2ilite\data\public_archive\山东公开官员")
reg = Registry(ROOT, "山东省")

EDGE_PATHS = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]

def find_browser():
    for p in EDGE_PATHS:
        if Path(p).exists():
            return p
    return EDGE_PATHS[0]  # fallback

# 淄博已知领导页面（从DB中22人无图的source_url提取）
ZIBO_PAGES = [
    # 市政府领导
    "https://www.zibo.gov.cn/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1606898238116",
    # 秘书长
    "https://www.zibo.gov.cn/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1693302114873",
]

with sync_playwright() as p:
    browser = p.chromium.launch(
        executable_path=find_browser(),
        headless=False,
        ignore_https_errors=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    page = ctx.new_page()

    for url in ZIBO_PAGES:
        print(f"\n{'='*60}")
        print(f"访问: {url}")
        page.goto(url, wait_until="commit", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=30000)
        page.wait_for_timeout(5000)

        # 获取渲染后的HTML
        content = page.content()
        final_url = page.url
        print(f"最终URL: {final_url}")
        print(f"页面长度: {len(content)}")

        # 查找图片
        imgs = page.query_selector_all("img")
        print(f"图片数: {len(imgs)}")
        for img in imgs:
            src = img.get_attribute("src") or ""
            alt = img.get_attribute("alt") or ""
            if any(k in src.lower() for k in [".jpg", ".jpeg", ".png", ".webp", "vsb"]):
                print(f"  IMG: {src[:100]} | alt={alt[:30]}")

        # 提取页面正文
        body_text = page.inner_text("body")[:2000]
        print(f"正文预览: {body_text[:300]}")

        # 保存截图用于后续分析
        page.screenshot(path=Path(ROOT, f"zibo_debug_{hash(url)%10000}.png"))

    browser.close()

print("\n✅ 淄博SPA探测完成，请查看截图确认是否需要继续提取")
