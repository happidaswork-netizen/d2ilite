> **口径已更替(2026-07-26)**:本文的终落点(`data/public_archive/山东公开官员`)与队列口径已被 D2I Cloud 取代——终根只允许 `角色肖像`,禁写 `山东公开官员`;以 [`D2I_Cloud产品契约_2026-07-25.md`](D2I_Cloud产品契约_2026-07-25.md) 与 [`d2i_cloud_template_extract_contract.md`](d2i_cloud_template_extract_contract.md) 为准。本文仅保留站点经验与历史进度供参考。

# 淄博 SPA 专采方案 & 执行手册

> 创建：2026-06-28
> 执行人：Windows 用户（有 Edge + Playwright）
> 前置依赖：d2ilite 项目已克隆，虚拟环境已激活

---

## 问题

淄博市政府网站（zibo.gov.cn）使用 Vue.js SPA（单页应用），主采集器 `run_official_profile_collector.py` 基于
Python requests 库，无法执行 JavaScript，因此拿不到动态渲染的领导照片和简介。

**现状**：22 人已入库，但全部 0 图。

---

## 方案概述

分两步走：

### 第一阶段：页面探测（已完成）

脚本 `scripts/zibo_spa_browser_collect.py` 已写好，用 Playwright + Edge 有头浏览器：

1. 打开淄博领导页面
2. 等待 SPA 渲染完成
3. 截图 + 提取 HTML 和图片 URL
4. 供人工分析页面结构

执行命令：
```powershell
cd /d D:\bugemini\d2ilite
.venv\Scripts\python.exe scripts\zibo_spa_browser_collect.py
```

### 第二阶段：正式采集（需要人工确认页面结构后执行）

待第一阶段确认页面结构后，用以下逻辑写正式采集：

---

## 页面结构推测（基于已知信息）

淄博的 Vue SPA 页面可能结构如下：

```
https://www.zibo.gov.cn/gongkai/channel_5fc4b785f867d82dbc565b7e/
  └─ Vue 组件动态渲染
       ├─ .leader-card 或 .leader-item
       │   ├─ <img class="leader-photo" src="...">
       │   ├─ <h3 class="leader-name">赵庆文</h3>
       │   └─ <p class="leader-position">市长</p>
       └─ 更多条目...
```

关键 URL：
| 页面 | URL | 说明 |
|------|-----|------|
| 市政府领导 | `/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1606898238116` | 赵庆文等 |
| 秘书长 | `/gongkai/channel_5fc4b785f867d82dbc565b7e/?key=1693302114873` | 宗志坚 |
| 机构职能 | `/gongkai/channel_c1534/` | 部门列表 |
| 职能配置 | `/gongkai/channel_5ff26df1f44cee8ffa5a7f18/` | 配置信息 |

### 可能的 API 端点

SPA 通常会从后端 API 拉数据。打开浏览器 DevTools → Network 标签，查看 XHR/Fetch 请求，
可能发现以下模式之一：

```
/gongkai/channel_xxx/data.json
/gongkai/channel_xxx?page=1&size=20
/api/leader/list
```

在 Playwright 中监听所有 XHR 请求：
```python
page.on("request", lambda req: print(f"→ {req.url}"))
page.on("response", lambda resp: print(f"← {resp.status} {resp.url}"))
```

---

## 正式采集代码模板（待确认结构后使用）

### 步骤 1：注入种子（已执行）

运行 `scripts\batch_seed_new_cities_2026-06-28.py`。

### 步骤 2：Playwright 正式采集脚本

创建 `scripts/zibo_spa_formal_collect.py`，内容模板如下：

```python
"""
淄博 SPA 正式采集：用 Playwright 渲染后提取领导照片/简介，写入 registry
"""

import sys, hashlib, json
from pathlib import Path
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scraper.run_official_profile_collector import Registry, QueueItem

ROOT = Path(r"D:\bugemini\d2ilite\data\public_archive\山东公开官员")
reg = Registry(ROOT, "山东省")

EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
if not Path(EDGE_PATH).exists():
    EDGE_PATH = r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"

# === 配置区：根据页面探测结果填写 ===
LEADER_PAGES = [
    # (url, 列表容器CSS选择器, 图片CSS选择器, 姓名CSS选择器, 职务CSS选择器)
    ("https://www.zibo.gov.cn/gongkai/channel_...", ".leader-list", "img.leader-photo", "h3.leader-name", "p.leader-position"),
]

def extract_leader(page, url, container_css, img_css, name_css, pos_css):
    page.goto(url, wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(3000)
    
    items = page.query_selector_all(container_css + " > *")
    results = []
    for item in items:
        img_el = item.query_selector(img_css)
        name_el = item.query_selector(name_css)
        pos_el = item.query_selector(pos_css)
        if not img_el or not name_el:
            continue
        
        img_src = img_el.get_attribute("src")
        name = name_el.inner_text().strip()
        position = pos_el.inner_text().strip() if pos_el else ""
        
        # 下载图片
        if img_src and not img_src.startswith("data:"):
            resp = page.request.get(img_src)
            if resp.ok:
                img_bytes = resp.body()
                sha256 = hashlib.sha256(img_bytes).hexdigest()
                
                # 写入 registry
                # ... （参考 run_official_profile_collector.py 中的人名入库逻辑）
                
                results.append((name, position, img_src, len(img_bytes)))
    
    return results


with sync_playwright() as p:
    browser = p.chromium.launch(
        executable_path=EDGE_PATH, headless=False,
        ignore_https_errors=True,
    )
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    page = ctx.new_page()
    
    for url, container, img_css, name_css, pos_css in LEADER_PAGES:
        print(f"\n=== {url}")
        results = extract_leader(page, url, container, img_css, name_css, pos_css)
        for name, pos, src, size in results:
            print(f"  {name} | {pos} | {src[:80]} | {size} bytes")
    
    browser.close()

reg.export_tables()
reg.conn.close()
print("✅ 淄博 SPA 正式采集完成")
```

### 步骤 3：主采集器补跑

如果正式采集写入 registry 后，Zibo 还有未探索的链接，运行主采集器补充：

```powershell
.venv\Scripts\python.exe scraper\run_official_profile_collector.py ^
  --region shandong --max-workers 1 --max-pages 50 ^
  --delay-min 3.0 --delay-max 6.0
```

---

## 备用方案：淄博部门子站

如果市级 SPA 页难以提取，可以绕过市主站，直接抓部门子站：

| 子站 | 说明 |
|------|------|
| fgw.zibo.gov.cn | 发改委 |
| gaj.zibo.gov.cn | 公安局 |
| hrss.zibo.gov.cn | 人社局 |
| gtj.zibo.gov.cn | 自然资源局 |
| gxj.zibo.gov.cn | 工信局 |

这些子站可能不使用 SPA 架构，传统 HTML 可以直接用 requests 抓。先检查每个子站是否有 `领导信息`、`领导之窗`、`机构领导` 页面。

---

## 已知的 22 人（source_url 列表）

如果需要从这些人的原始页面重新提取，以下是他们的 source_url 模式（从 DB 导出）：

```sql
select name, position, source_url from people 
where city='淄博市' and (primary_image_path is null or primary_image_path='');
```

在 Windows 上执行：
```powershell
$env:PYTHONIOENCODING='utf-8'; @'
import sqlite3, json
conn = sqlite3.connect(r'D:\bugemini\d2ilite\data\public_archive\山东公开官员\registry\people.sqlite')
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "select name, position, source_url, source_page_title from people where city='淄博市'"
).fetchall()
for r in rows:
    print(json.dumps(dict(r), ensure_ascii=False))
conn.close()
'@ | D:\bugemini\d2ilite\.venv\Scripts\python.exe -
```

---

## 检查清单

### 页面探测阶段
- [ ] 运行 `zibo_spa_browser_collect.py`，看截图确认页面结构
- [ ] 确认领导照片在渲染后的 DOM 中是否以 `<img>` 出现
- [ ] 确认姓名/职务文字的 CSS 选择器
- [ ] 检查是否有 API 端点可直取 JSON 数据

### 正式采集阶段
- [ ] 填写 `LEADER_PAGES` 配置区
- [ ] 实现图片下载 + registry 写入
- [ ] 确认 XMP 元数据写入
- [ ] 运行主采集器补抓额外链接

### 验证
- [ ] DB 中淄博的 `has_primary` 从 0 变为 >0
- [ ] 图片文件存在于 `淄博市/市级/淄博市人民政府/男|女/`
- [ ] 元数据包含 `d2i_profile`
- [ ] KPI 导出已更新

---

## 参考文档

- 主采集器代码：`scraper\run_official_profile_collector.py`
- 浏览器兜底示例：`scraper\run_official_profile_browser_once.py`
- Playwright 文档：https://playwright.dev/python/docs/api/class-page
- DB 操作示例：`scripts\batch_seed_new_cities_2026-06-28.py`
