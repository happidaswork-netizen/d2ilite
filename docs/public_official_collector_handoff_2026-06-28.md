> **口径已更替(2026-07-26)**:本文的终落点(`data/public_archive/山东公开官员`)与队列口径已被 D2I Cloud 取代——终根只允许 `角色肖像`,禁写 `山东公开官员`;以 [`D2I_Cloud产品契约_2026-07-25.md`](D2I_Cloud产品契约_2026-07-25.md) 与 [`d2i_cloud_template_extract_contract.md`](d2i_cloud_template_extract_contract.md) 为准。本文仅保留站点经验与历史进度供参考。

# 公开官员采集交接运行手册 2026-06-28

本文给接手模型或接手脚本使用，不需要阅读此前聊天记录即可继续任务。

最后核对时间：2026-06-28 下午（更新：加入批量种子补种计划、淄博 SPA 专采脚本）

## 1. 当前任务目标

### 当前状态（2026-06-28 下午）

```
山东公开官员总进度：383人 / 343可用图片 / 338人有图
距 500 目标：差约 157 张
三大堵点：① 批量种子等执行（30个新入口就绪）② 淄博SPA浏览器专采（22人0图）
           ③ 未探索城市（青岛/济宁/泰安等已补种）
```

### 核心原则

长期采集公开页面中的政府、公安、部门、事业单位等公开人员资料，优先下载清晰人物照片，并将所有公开资料写入 registry 与图片元数据。

核心优先级：

1. 图片数量优先，尤其是男性公开头像/资料图。
2. 有公开资料的人都入库；没有图片也保留 registry 行，后续补图。
3. 当前、历任、旧任、曾任、任免、任前公示、图/简历新闻都可保留。
4. 只采集官方或机构控制的公开页面，或可信公开人物库补充源；不采集登录后、验证码后、私人信息或需要绕过限制的内容。

当前重点项目：

- `山东公开官员`
- `河南公开官员` 为副线，目前不建议盲跑低价值 pending 队列。

## 2. 项目位置

仓库：

```text
D:\bugemini\d2ilite
```

主脚本：

```text
D:\bugemini\d2ilite\scraper\run_official_profile_collector.py
```

可见 Edge 兜底脚本：

```text
D:\bugemini\d2ilite\scraper\run_official_profile_browser_once.py
```

山东输出：

```text
D:\bugemini\d2ilite\data\public_archive\山东公开官员
```

河南输出：

```text
D:\bugemini\d2ilite\data\public_archive\河南公开官员
```

核心数据库：

```text
D:\bugemini\d2ilite\data\public_archive\山东公开官员\registry\people.sqlite
D:\bugemini\d2ilite\data\public_archive\河南公开官员\registry\people.sqlite
```

导出文件：

```text
registry\people.csv
registry\people.jsonl
registry\image_assets.csv
registry\image_assets.jsonl
registry\source_units.csv
registry\source_units.jsonl
registry\kpi.md
registry\kpi.json
```

日志目录：

```text
data\public_archive\<项目名>\logs\
```

## 3. 当前权威数字

以 SQLite 中 `image_assets where is_usable=1` 为权威图片资产口径。磁盘图片文件数包含历史残留、重复、未登记或其他过程文件，不能直接当作完成数。

### 山东公开官员

数据库：

```text
D:\bugemini\d2ilite\data\public_archive\山东公开官员\registry\people.sqlite
```

当前状态（2026-06-28 下午，临沂 bmml 批量采集后）：

```text
pending: 0 (主采集器队列)
people: 383
usable image_assets: 343
people_with_assets: 338
male_with_img: 292
female_with_img: 31
no_primary_image mismatch: 0
403/429/521 page_visits: 0
```

城市分布：

```text
临沂市: total=258, has_primary=234 (90%), male_primary=211   ← 大丰收
济南市: total= 39, has_primary= 36, male_primary= 30
淄博市: total= 22, has_primary=  0, male_primary=  0         ← 攻坚中
烟台市: total= 18, has_primary= 18, male_primary= 15
潍坊市: total= 11, has_primary= 11, male_primary= 10
枣庄市: total= 11, has_primary= 10, male_primary= 10
菏泽市: total= 10, has_primary= 10, male_primary=  7
东营市: total=  9, has_primary=  9, male_primary=  7
德州市: total=  3, has_primary=  2, male_primary=  2
空city: total=  2, has_primary=  2
```

山东重点结论：

- **临沂大丰收**：通过 bmml（部门目录）批量采集40+部门VSB图片，从34人0图→258人234图。详见 `docs/山东政府网站bmml_VSB采集参考.md`。
- **淄博仍是瓶颈**：22人0图，Vue.js SPA架构，需要浏览器自动化。
- 剩余缺图约45个（临沂含重复24、淄博22、济南3、枣庄1、德州1）。
- 距离500目标还差约162张，需找新的高产城市或填补淄博。
- 0 WAF / 0 mismatch，风险可控。

### 河南公开官员

数据库：

```text
D:\bugemini\d2ilite\data\public_archive\河南公开官员\registry\people.sqlite
```

当前状态：

```text
pending: 211
running: 0
done_pages: 1249
failed_pages: 85
people: 20
usable image_assets: 10
people_with_assets: 10
male_with_img: 2
female_with_img: 0
unknown_with_img: 8
no_primary_image: 10
official_photo/news_photo but primary_image_path missing: 0
403/429/521 page_visits count: 0
actual image files under project root: 22
```

元数据审计：

```text
assets_total: 10
files_exist: 10
has_titi_json: 8
has_d2i_profile: 8
has_source: 8
has_full_content_or_description: 8
missing_files: 0
read_errors: 0
```

河南有 2 张图缺少 `d2i_profile`：

```text
D:\bugemini\d2ilite\data\public_archive\河南公开官员\河南省\商丘市\未命名\市政府领导\未知\孙起鹏.jpg
D:\bugemini\d2ilite\data\public_archive\河南公开官员\河南省\商丘市\未命名\市政府领导\未知\翁铁军.jpg
```

河南队列里有大量低价值政府信息公开年报、公开制度、PDF/DOC/WPS 附件，不要直接全速继续跑。先筛选高价值入口，再小批量跑。

## 4. 代码与功能状态

当前使用的是 Python 采集脚本，不是 GUI 里的传统 d2i 模板任务。它沿用 d2ilite/TITI 的目录、SQLite 和元数据规范。

主脚本职责：

- 建立 `people`、`image_assets`、`source_units`、`crawl_queue`、`page_visits` 表。
- 按区域配置种子页面。
- 对单个站点组串行请求，跨不同站点组可并行。
- 抓取公开页面中的人员、图片、来源、简介、分工、全文。
- 下载图片并写入 `image_assets`。
- 调用 `metadata_writer.write_xmp_metadata()` 写图片元数据。
- 导出 CSV/JSONL/KPI。

已支持的关键能力：

- `姓名/职务/简历/工作分工` 多人页面。
- 单人详情页，标题是姓名、正文有简介/分工/性别/现任等信号。
- 头像图、资料图、新闻图的分类。
- JS `window.location` 脚本跳转追踪。
- 高价值链接筛选：`领导信息`、`领导之窗`、`机构领导`、`政府领导`、`班子成员`、`分工`、`简历`、`任前公示`、`图/简历`、`历任`、`曾任`、`原任`。
- 低价值链接过滤：年报、公报、政策文件、政府采购、服务大厅、SSO、无障碍、PDF/DOC/WPS 附件等。
- 生产性页面之后追踪父级页面、兄弟页面、同目录相邻页、姓名链接。
- 山东官方域名和两个可信补充源：
  - `district.ce.cn`
  - `renshi.people.com.cn`

注意：`scraper/run_official_profile_collector.py` 目前是未跟踪/未提交状态，不要用旧版本覆盖。

## 5. 数据规范

必须保持三层同步：

1. `registry/people.sqlite`
2. `image_assets` 表与导出清单
3. 图片文件夹与图片内嵌元数据

人员入库最低字段：

```text
person_id
name
gender
province/city/county_or_district
unit_name
position
division_of_work
biography
full_public_text
source_url
source_page_title
source_site_domain
image_status
primary_image_path
raw_fields_json
crawled_at
```

图片资产最低字段：

```text
asset_id
person_id
name
image_type
image_index
image_total
image_path
image_url
source_url
source_page_title
source_site_domain
sha256
width
height
is_usable
crawled_at
```

图片目录结构：

```text
data/public_archive/<项目名>/
  <省>/
    省级/<单位>/男|女|未知/
    <市>/市级/<单位>/男|女|未知/
    <市>/<区县>/<单位>/男|女|未知/
```

命名：

- 主图：`姓名.jpg`
- 多图：`姓名-资料图-1.jpg`、`姓名-资料图-2.jpg`
- 同名冲突可追加 `_2`、`_3`
- 不允许用哈希或纯序号作为最终主命名

元数据要求：

- 每张最终图片都写入 `Xmp.titi.meta`。
- `Xmp.titi.meta.d2i_profile` 至少应包含：
  - `person_id`
  - `asset_id`
  - `name`
  - `gender`
  - `province/city/county_or_district`
  - `unit/unit_name`
  - `department`
  - `position`
  - `source_url`
  - `source_page_title`
  - `source_site_domain`
  - `source_unit_url`
  - `image_url`
  - `image_type`
  - `image_index`
  - `image_total`
  - `full_content`
  - `raw_fields`
  - `crawled_at`
- 长原文如果超过常规 XMP/EXIF 容量，应按 d2ilite 现有规则写入 JPEG COM 分块，不要只放 sidecar。

## 6. 安全与频率策略

不要绕过验证码、登录、WAF 或访问控制。

单个站点组：

- 永远只允许一个请求 worker。
- 页面和图片共用该站点组节流。
- 出现 403/429/521 或 5xx 时自动使用慢速 delay。

跨不同站点组：

- 可以谨慎并行。
- 山东建议 `--max-workers 2` 起步。
- 如果明确是小型普通政府站点，可临时 `--max-workers 3`，但必须看日志。

建议参数：

```text
--delay-min 3.0
--delay-max 6.0
--slow-delay-min 15
--slow-delay-max 35
--block-backoff-minutes 90
```

如果是很稳定、非敏感、小量普通政府站点，可降到：

```text
--delay-min 1.8
--delay-max 3.5
```

暂停条件：

- 同一站点短时间新增多个 403/429/521。
- 出现验证码、验证页面、WAF 提示。
- 521/403 数量持续增长。
- 页面频繁跳首页，且不是正常脚本跳转。
- 人名误识别明显扩散，例如把“领导信息”“市公安局”“政策解读”当作姓名。
- `official_photo/news_photo` 但 `primary_image_path` 为空的 mismatch 增长。
- 图片元数据写入失败大面积出现。

## 7. 状态检查命令

在 `D:\bugemini` 或 `D:\bugemini\d2ilite` 运行。

### 查看两个库状态

```powershell
$env:PYTHONIOENCODING='utf-8'; @'
import sqlite3, json
from pathlib import Path
base=Path(r'D:\bugemini\d2ilite\data\public_archive')
for label in ['山东公开官员','河南公开官员']:
    root=base/label
    db=root/'registry'/'people.sqlite'
    print('\n##', label)
    conn=sqlite3.connect(str(db)); conn.row_factory=sqlite3.Row
    row=conn.execute('''
    select
      (select count(*) from crawl_queue where status='pending') pending,
      (select count(*) from crawl_queue where status='running') running,
      (select count(*) from crawl_queue where status='done') done_pages,
      (select count(*) from crawl_queue where status='failed') failed_pages,
      (select count(*) from people) people,
      (select count(*) from image_assets where is_usable=1) assets,
      (select count(distinct person_id) from image_assets where is_usable=1) people_with_assets,
      (select count(*) from people where gender='男' and coalesce(primary_image_path,'')<>'') male_with_img,
      (select count(*) from people where image_status in ('official_photo','news_photo') and coalesce(primary_image_path,'')='') mismatch,
      (select count(*) from page_visits where http_status in (403,429,521)) waf
    ''').fetchone()
    print(json.dumps(dict(row), ensure_ascii=False, indent=2))
    print('actual_image_files', sum(1 for p in root.rglob('*') if p.is_file() and p.suffix.lower() in {'.jpg','.jpeg','.png','.webp'}))
    conn.close()
'@ | D:\bugemini\d2ilite\.venv\Scripts\python.exe -
```

### 查看最近日志

```powershell
Get-ChildItem -File 'D:\bugemini\d2ilite\data\public_archive\山东公开官员\logs' |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 10 FullName,Length,LastWriteTime

Get-Content -Tail 120 '<最新 run_*.log>'
```

### 审计元数据

```powershell
$env:PYTHONIOENCODING='utf-8'; @'
import sqlite3, json, sys
from pathlib import Path
sys.path.insert(0, r'D:\bugemini\d2ilite')
from metadata_manager import read_image_metadata

for label in ['山东公开官员','河南公开官员']:
    root=Path(r'D:\bugemini\d2ilite\data\public_archive')/label
    db=root/'registry'/'people.sqlite'
    conn=sqlite3.connect(str(db)); conn.row_factory=sqlite3.Row
    meta={'assets_total':0,'files_exist':0,'has_titi_json':0,'has_d2i_profile':0,'has_source':0,'has_full_content_or_description':0,'missing_files':0,'read_errors':0,'examples':[]}
    for r in conn.execute("select image_path from image_assets where is_usable=1"):
        meta['assets_total']+=1
        p=Path(r['image_path'])
        if not p.exists():
            meta['missing_files']+=1
            if len(meta['examples'])<5: meta['examples'].append({'path':str(p),'issue':'missing_file'})
            continue
        meta['files_exist']+=1
        try:
            info=read_image_metadata(str(p))
            if isinstance(info.titi_json, dict): meta['has_titi_json']+=1
            prof=(info.titi_json or {}).get('d2i_profile') if isinstance(info.titi_json, dict) else None
            if isinstance(prof, dict):
                meta['has_d2i_profile']+=1
                if prof.get('source_url') or info.source: meta['has_source']+=1
                if prof.get('full_content') or info.description: meta['has_full_content_or_description']+=1
            elif len(meta['examples'])<5:
                meta['examples'].append({'path':str(p),'issue':'no_d2i_profile'})
        except Exception as e:
            meta['read_errors']+=1
            if len(meta['examples'])<5: meta['examples'].append({'path':str(p),'issue':'read_error','error':repr(e)})
    print(label, json.dumps(meta, ensure_ascii=False, indent=2))
    conn.close()
'@ | D:\bugemini\d2ilite\.venv\Scripts\python.exe -
```

## 8. 如何补种入口 - 批量种子脚本

2026-06-28 新增批量种子脚本，可在 Windows 上一键注入 10 个新城市的 ~30 个入口：

脚本位置：

```text
D:\bugemini\d2ilite\scripts\batch_seed_new_cities_2026-06-28.py
```

覆盖城市：

| 城市 | 种子数 | 说明 |
|------|--------|------|
| 淄博市 | 8 | 市政府 SPA 页 + 5 个部门子站 |
| 青岛市 | 6 | 办公厅 + 3 个部门信息公开目录 |
| 济宁市 | 2 | 市政府 + 要闻动态 col |
| 泰安市 | 1 | 市政府首页 |
| 威海市 | 1 | 市政府首页 |
| 日照市 | 1 | 市政府首页 |
| 聊城市 | 1 | 市政府首页 |
| 滨州市 | 1 | 市政府首页 |
| 菏泽市 | 1 | 政务公开补充 |
| 东营市 | 1 | col38804 领导页补充 |

使用方法：

```powershell
cd /d D:\bugemini\d2ilite
.venv\Scripts\python.exe scripts\batch_seed_new_cities_2026-06-28.py
```

之后跑主采集器：

```powershell
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$root = 'D:\bugemini\d2ilite\data\public_archive\山东公开官员'
$out = Join-Path $root "logs\run_seed_$ts.out.log"
$err = Join-Path $root "logs\run_seed_$ts.err.log"

Start-Process -WindowStyle Hidden `
  -FilePath 'D:\bugemini\d2ilite\.venv\Scripts\python.exe' `
  -ArgumentList @(
    'D:\bugemini\d2ilite\scraper\run_official_profile_collector.py',
    '--region','shandong',
    '--max-workers','2',
    '--max-pages','150',
    '--delay-min','2.5',
    '--delay-max','5.0',
    '--progress-interval','30',
    '--export-interval','90'
  ) `
  -RedirectStandardOutput $out `
  -RedirectStandardError $err
```

注意：淄博种子是 Vue SPA 页面，主采集器的 requests 无法渲染动态内容，需配合浏览器脚本使用。

## 8b. 淄博 SPA 浏览器专采

淄博市政府网站（zibo.gov.cn）使用 Vue.js SPA 架构，主采集器的 requests 无法提取动态渲染的领导照片和简介。当前 22 人全部无图。

详细方案见 `docs/zibo_spa_collection_plan_2026-06-28.md`。

专采脚本位置：

```text
D:\bugemini\d2ilite\scripts\zibo_spa_browser_collect.py
```

详细执行方案：

```text
D:\bugemini\d2ilite\docs\zibo_spa_collection_plan_2026-06-28.md
```

运行方式（Windows 有头 Edge）：

```powershell
cd /d D:\bugemini\d2ilite
.venv\Scripts\python.exe scripts\zibo_spa_browser_collect.py
```

脚本行为：
- 自动探测 Edge/Chrome 路径
- 使用 Playwright 启动有头浏览器（headless=False）
- 访问已知的淄博领导页面（市政府领导、秘书长等）
- 提取渲染后的 HTML 和所有图片
- 截图保存到 `山东公开官员/` 目录供人工分析

注意：该脚本目前是探测模式（打印/截图），尚未实现全自动写入 registry。待确认页面结构后可升级为正式采集模式。

## 8c. 手工补种指南

不要盲目全网宽抓。优先人工/搜索确认高价值入口，再写入 `crawl_queue`。

优先关键词：

```text
领导信息
领导之窗
机构领导
政府领导
市政府领导
县政府领导
区政府领导
班子成员
领导班子
工作分工
个人简历
简历
图/简历
人事任免
任前公示
历任
曾任
原任
公安局 领导信息
```

搜索策略：

- 开放式搜索找到可能入口。
- 打开入口看页面形态，不要只看标题。
- 找父级栏目。
- 找同目录兄弟页。
- 找上级单位页面。
- 找同站点其他单位相同模板页面。
- 对有图页面优先补种详情页和同目录邻近页。

山东优先补的方向：

- 临沂：bmml 部门目录批量采集已完成（258人/234图）。见 `docs/山东政府网站bmml_VSB采集参考.md`。
- 淄博：`市政府领导`、公安局、部门领导。当前 22 人但 0 图。
- 青岛、东营、济宁、泰安、威海、日照、聊城、滨州、菏泽：当前图片不足，优先找市政府领导、公安局领导、部门领导入口。
- 可信补充源：`district.ce.cn`、`renshi.people.com.cn`，必须带山东上下文，避免扩散到全国泛任免新闻。

不要优先补：

```text
政府信息公开指南
政府信息公开制度
年度报告
政府公报
政策文件
政府采购
政务服务大厅
无障碍入口
SSO/用户中心
PDF/DOC/DOCX/WPS 附件
```

手工补种示例：

```powershell
$env:PYTHONIOENCODING='utf-8'; @'
import sys
from pathlib import Path
sys.path.insert(0, r'D:\bugemini\d2ilite')
from scraper.run_official_profile_collector import Registry, QueueItem

root = Path(r'D:\bugemini\d2ilite\data\public_archive\山东公开官员')
reg = Registry(root, '山东省')

seeds = [
    # province, city, county, level, unit_name, url, priority
    ('山东省', '临沂市', '', 'city', '临沂市人民政府', 'https://www.linyi.gov.cn/zf/szfld.htm', 120),
    ('山东省', '临沂市', '', 'city', '临沂市人民政府', 'https://www.linyi.gov.cn/zf/szfld/sz4/yst.htm', 130),
]

for province, city, county, level, unit_name, url, priority in seeds:
    reg.add_source_unit(
        province=province,
        city=city,
        county=county,
        level=level,
        unit_name=unit_name,
        official_site_url=url,
        profile_list_url=url,
        status='candidate',
        notes='manual handoff seed',
    )
    reg.reprioritize_or_enqueue(
        QueueItem(
            url=url,
            province=province,
            city=city,
            county_or_district=county,
            administrative_level=level,
            unit_name=unit_name,
            source_unit_url=url,
            depth=0,
            parent_url='',
            priority=priority,
        )
    )

reg.export_tables()
reg.conn.close()
'@ | D:\bugemini\d2ilite\.venv\Scripts\python.exe -
```

## 9. 如何启动小批量采集

推荐先小批量跑，确认产图和风险后再扩大。

```powershell
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$root = 'D:\bugemini\d2ilite\data\public_archive\山东公开官员'
$out = Join-Path $root "logs\collector_shandong_handoff_$ts.out.log"
$err = Join-Path $root "logs\collector_shandong_handoff_$ts.err.log"

Start-Process -WindowStyle Hidden `
  -FilePath 'D:\bugemini\d2ilite\.venv\Scripts\python.exe' `
  -ArgumentList @(
    'D:\bugemini\d2ilite\scraper\run_official_profile_collector.py',
    '--region','shandong',
    '--max-workers','2',
    '--max-pages','80',
    '--delay-min','3.0',
    '--delay-max','6.0',
    '--slow-delay-min','15',
    '--slow-delay-max','35',
    '--block-backoff-minutes','90',
    '--progress-interval','30',
    '--export-interval','90'
  ) `
  -RedirectStandardOutput $out `
  -RedirectStandardError $err

Write-Output $out
Write-Output $err
```

注意：

- 不要直接 `--reseed-open-search` 全量跑。此前 Bing/open search 在当前环境里低价值结果多，容易灌入年报/制度/附件。
- 若确实要用 open search，只跑少量页，跑后立刻看 `crawl_queue` 和 `page_visits`，发现低价值就停止/清理/降权。
- `--max-pages 80` 是小批量观察值，不是全量完成值。

## 10. 可见 Edge 兜底

当 requests 明显不稳定、但用户用 Edge 能打开页面时，用可见 Edge 脚本。不要用无头浏览器绕限制。

济南公安页面示例：

```powershell
cd /d D:\bugemini\d2ilite
.\.venv\Scripts\python.exe .\scraper\run_official_profile_browser_once.py `
  --region shandong `
  --url "https://jnga.jinan.gov.cn/col86386/art/2026/art_86386_4783913.html" `
  --city "济南市" `
  --administrative-level agency `
  --unit-name "济南市公安局" `
  --portrait-url-contains "/cms_files/filemanager/1568/picture/"
```

使用条件：

- 普通 requests 出现 502/空页面/协议问题。
- 人工 Edge 可正常打开。
- 页面较少、值得手工兜底。

不要把可见浏览器兜底当成大规模并发工具。

## 11. 监控规则

每 30 分钟检查一次即可。检查内容：

1. `crawl_queue` 的 `pending/running/done/failed`。
2. `page_visits` 中 403/429/521 是否新增。
3. 最新日志是否有 captcha、blocked、site group backoff、metadata write failed。
4. `people`、`image_assets`、男性图片数是否增长。
5. `official_photo/news_photo but no primary_image_path` 是否仍为 0。
6. 图片路径是否存在。
7. 抽样元数据是否包含 `d2i_profile`、`source_url`、`full_content`。
8. 是否出现大量低价值页面：年报、制度、PDF/DOC、服务大厅、用户中心。

如果任务跑完：

- 不要只报“完成”。
- 如果没有封禁/验证码/误识别风险，就继续搜索新入口、补种高价值页面、开下一轮小批量。
- 单个站点不要多并发；可以换其他城市/其他域名合理并行。

## 12. 当前已知问题与下一步

### 山东

1. 临沂：bmml/VSB 批量采集已完成（258人/234图）。
   - 详情见 `docs/山东政府网站bmml_VSB采集参考.md`。
   - 剩余缺图主要是重复入库/少量遗漏，真正的缺图很少。
   - 可补方向：检查个别bmml遗漏页面或追踪少数单位详情页。

2. **当前任务：批量补种新城市种子（30 种子，10 城市）**
   - 脚本：`scripts/batch_seed_new_cities_2026-06-28.py`
   - 覆盖：淄博(8)、青岛(6)、济宁(2)、泰安(1)、威海(1)、日照(1)、聊城(1)、滨州(1)、菏泽(1)、东营(1)
   - 主采集器参数：`--max-workers 2 --max-pages 150 --delay-min 2.5 --delay-max 5.0`
   - 执行流程：注入种子 → 跑主采集器 → 查看产图 → 淄博需浏览器兜底
   - 详情见上方 §8 / §8b

3. 淄博图片明显漏抓（22人，0图）。
   - 淄博使用 Vue.js SPA 系统，requests 无法直接获取动态内容。
   - 专采脚本：`scripts/zibo_spa_browser_collect.py`（Playwright + Edge 有头浏览器）
   - 当前为探测模式：打开 SPA 页面 → 提取渲染后 HTML/图片 → 截图存证
   - 待确认页面结构后可升级为全自动写入 registry 的正式采集模式
   - 已探索 col 范围，未找到明确领导详情页 API 端点

4. 青岛、济宁、泰安、威海、日照、聊城、滨州已通过 batch_seed 补种入口。
   - VSB 架构不适用于非临沂城市，各市使用独立CMS。
   - 待主采集器跑一轮后看产图效果，再决定是否继续深挖。

5. 德州、烟台、潍坊、枣庄已有产图入口，可继续追同类页面。

### 河南

1. Pending 队列 211，但低价值附件/制度/年报很多。
2. 商丘已有 8 图，其中 2 图元数据缺 `d2i_profile`，补修后再扩大。
3. 不建议直接跑河南全队列；先筛高价值入口再跑小批量。

## 13. 给接手模型的工作指令模板

可以把下面整段发给接手模型：

```text
你接手 D:\bugemini\d2ilite 的公开官员采集任务。先阅读：

1. D:\bugemini\d2ilite\AGENTS.md
2. D:\bugemini\d2ilite\docs\模板生成与使用SOP.md
3. D:\bugemini\d2ilite\docs\抓取页面与模板生成规范.md
4. D:\bugemini\d2ilite\docs\public_official_collector_handoff_2026-06-28.md

当前重点是山东公开官员。不要绕过验证码/登录/WAF。单个站点组只允许一个 worker，跨不同站点可以谨慎并行。图片优先，男性图片优先；所有公开资料人物都入 registry，无图也保留。历任、旧任、曾任、任免、图/简历新闻都可以保留。

先运行状态检查，确认 pending/running、WAF、mismatch、元数据状态。若没有风险且山东队列为空，继续开放式搜索高价值入口，优先领导信息、领导之窗、机构领导、政府领导、班子成员、分工、简历、图/简历、人事任免、任前公示、历任/曾任/原任。找到入口后追踪父级列表、兄弟网页、同目录详情页、上级单位页面；补种到 SQLite 队列后小批量运行。

不要盲跑河南 pending 队列，不要盲用 --reseed-open-search 全量灌队列。若发现 403/429/521 增长、验证码/封禁、同站点并发、严重误识别、元数据大面积缺失，立即暂停或降速并汇报。
```

## 14. 是否需要多模态

常规运行不需要多模态。HTML、正文、图片 URL、alt、标题、简介/分工文本足够支撑大部分采集。

多模态只用于抽检或疑难：

- 判断图片是否真实头像。
- 判断图片是否清晰。
- 判断是否 logo/二维码/横幅误抓。
- 判断是否多人合影或新闻图。
- 页面把资料做成图片或 PDF 时做 OCR。

不要用看脸来硬推私密信息。性别优先来自公开文本；无法确定就写 `未知`。

## 15. 完成口径

不要用“队列跑空”作为完成。完成至少要求：

- 目标区域高价值入口搜索过一轮。
- 高产页面的父级、兄弟页、同目录详情页追踪过。
- `pending=0` 且没有未处理高价值候选。
- 新增 WAF/验证码风险为 0 或已明确暂停。
- `mismatch=0`。
- 抽样图片元数据正常。
- 无图人员仍在 registry 中。
- 导出 CSV/JSONL/KPI 已刷新。

## 16. 沙箱环境可访问性说明

当前采集在两个环境中进行，各自能力不同。

### 沙箱环境（Linux Sandbox，即当前模型所在环境）

只能使用 Python requests（无浏览器渲染），且受 IP 限制：

| 城市 | 域名 | 可访问性 | 当前状态 |
|------|------|---------|---------|
| 临沂 | linyi.gov.cn | ✅ 完全可访问 | 258人/234图 ✅ 完成 |
| 潍坊 | weifang.gov.cn | ✅ 首页+领导页可访问 | 11人/11图 ✅ |
| 济宁 | jining.gov.cn | ⚠️ 首页可访问，子页云防护404 | 0人/0图 |
| 枣庄 | zaozhuang.gov.cn | 502 Bad Gateway | 11人/10图（种子由前序注入）|
| 淄博 | zibo.gov.cn | ⚠️ 首页可访问，SPA无渲染 | 22人/0图 ❌ |
| 其他山东城市 | *.gov.cn | ❌ 云防护 / SSL / WAF 403 | 0人/0图 |
| 济南 | jinan.gov.cn | ❌ 云防护 | 39人/36图（前序已产出）|
| 烟台 | yantai.gov.cn | ❌ 云防护 | 18人/18图（前序已产出）|
| 东营 | dongying.gov.cn | ❌ WAF 403 | 9人/9图（前序已产出）|
| 菏泽 | heze.gov.cn | ❌ TLS 协议版本不匹配 | 10人/10图（前序已产出）|
| 省站 | shandong.gov.cn | ❌ 所有 TLS 版本均拒绝 | 0 |
| 部门子站 | *.weifang.gov.cn | ❌ 连接被拒绝 | 未探索 |
| 部门子站 | *.zibo.gov.cn | ❌ 云防护 | 未探索 |

沙箱结论：**除临沂/潍坊外，大部分城市不可达**。340+ 张图片中绝大部分来自前序 Windows 采集。

### Windows 环境（用户本地机器）

- 有 Edge 浏览器（Playwright 有头模式）
- 无 IP 限制
- 可运行主采集器和浏览器脚本
- **所有需要 SSL、WAF 绕过、SPA 渲染的任务必须在 Windows 执行**

### 任务归属矩阵

| 任务 | 沙箱可做 | Windows 需做 |
|------|---------|-------------|
| 批量种子注入 (batch_seed) | ❌ 无法访问 SQLite FUSE | ✅ cd D:\bugemini\d2ilite; .venv\Scripts\python.exe scripts\batch_seed_new_cities_2026-06-28.py |
| 主采集器运行 | ❌ 大部分城市不可达 | ✅ Start-Process 跑 run_official_profile_collector.py |
| 淄博 SPA 专采 | ❌ 无浏览器 | ✅ .venv\Scripts\python.exe scripts\zibo_spa_browser_collect.py |
| 临沂 bmml 补充 | ❌ 已完成 | — |
| 潍坊部门子站领导页 | ❌ 子站被屏蔽 | ✅ 逐个访问 gaj.weifang.gov.cn 等找领导信息 |
| 新入口搜索 | ✅ 可有限搜索(linyi/weifang) | ✅ 无限制搜索 |
| 文档编写 | ✅ 擅长 | — |

### 自动记忆

以下信息已写入 auto memory，但值得在此重复：

- 大多数山东市政府网站使用云防护/安全防护，从沙箱 IP 无法访问
- VSB 图片系统仅临沂完整使用；其他城市使用独立 CMS
- SQLite DB 在 FUSE 挂载上有 disk I/O 问题，在沙箱需复制到 /tmp 操作
- 当前模型可以继续在沙箱中：搜索可访问城市的新入口、更新文档、写脚本
