> **口径已更替(2026-07-26)**:本文的终落点(`data/public_archive/山东公开官员`)与队列口径已被 D2I Cloud 取代——终根只允许 `角色肖像`,禁写 `山东公开官员`;以 [`D2I_Cloud产品契约_2026-07-25.md`](D2I_Cloud产品契约_2026-07-25.md) 与 [`d2i_cloud_template_extract_contract.md`](d2i_cloud_template_extract_contract.md) 为准。本文仅保留站点经验与历史进度供参考。

# Hermas 公开官员采集服务器执行规则

创建时间：2026-06-28  
适用对象：部署在国内服务器上的 Hermas/其他自动化模型或采集代理  
适用项目：`d2ilite` 公开官员、机构人员、公开资料与图片采集任务

本文是给服务器端执行代理看的硬规则。接手者不需要阅读此前聊天记录，但必须先阅读本文和同目录交接文档。

## 1. 接手前必须读取

在启动任何采集前，先读取：

```text
docs/public_official_collector_handoff_2026-06-28.md
docs/山东政府网站bmml_VSB采集参考.md
docs/zibo_spa_collection_plan_2026-06-28.md
docs/模板生成与使用SOP.md
docs/抓取页面与模板生成规范.md
```

同时理解以下代码中的元数据规范：

```text
metadata_writer.py
metadata_manager.py
desktop-next/config/exiftool-titi.config
```

如果本地技能目录也一并打包，读取：

```text
skills/public-official-profile-collector/SKILL.md
skills/public-official-profile-collector/references/schema.md
skills/public-official-profile-collector/references/workflow.md
```

如果这些 skill 文件没有被打包，则以本文第 4、5、6 节为最小规范。

## 2. 任务边界

只采集官方或机构控制的公开页面，以及明确可信的公开人物库补充源。允许范围：

- 政府门户、政府信息公开、部门官网、公安/警务/事业单位官网。
- 领导信息、机构领导、领导之窗、领导班子、工作分工、个人简历。
- 人事任免、任前公示、图/简历、历任、旧任、曾任、原任。
- 官方新闻稿中明确对应到同一人物的新闻图、资料图。
- 可信补充源，例如 `district.ce.cn`、`renshi.people.com.cn`，必须标记来源类型。

禁止范围：

- 不登录、不同意协议、不绕验证码、不破解 WAF。
- 不抓取登录后、内部系统、非公开 API、后台接口、个人社交账号、私人资料。
- 不使用代理池、不轮换 IP 来规避封禁、不伪装成多个真实用户。
- 不硬顶 `403/429/521`、验证码、跳首页、JS 挑战页。
- 不做高频目录扫描、漏洞扫描式探测、无边界参数枚举。

## 3. 服务器运行纪律

服务器与本地运行的合规边界相同，但服务器 IP 更容易被目标站识别为自动化，所以执行更保守。

默认参数：

```text
全局并发：2 起步，稳定后最多 3；没有人工确认不要超过 4。
单站点组并发：永远 1。
普通官方页面：3-8 秒/页，随机抖动。
小型领导列表：2-5 秒/页，随机抖动。
图片下载：2-5 秒/张，随机抖动。
敏感或不稳定站点：5-15 秒/页。
错误后慢速模式：15-35 秒/页。
封禁/风控退避：同域名至少暂停 90 分钟。
```

站点组不是简单 host，而是按同一个政府门户/同一个主域/同一个 WAF 归组。例如：

```text
www.linyi.gov.cn
zwgk.linyi.gov.cn
*.linyi.gov.cn
```

应视为同一站点组，不能同时跑多个 worker。不同地市、不同主域可以谨慎并行，但必须分别保持单站点组串行。

触发以下任一情况，立即暂停该站点组：

- 10 分钟内同站点出现 3 次以上 `403/429/521`。
- 最近 20 次请求中，错误率超过 20%。
- 出现验证码、访问过快、风控、WAF、Cloudflare/JS Challenge。
- 请求正常返回但反复跳到首页、无关页面或空白页。
- 同站点已有 worker 正在运行。
- 页面明确要求登录、身份校验或不可公开访问。

暂停一个站点组不影响其他安全站点继续运行，但必须写入日志。

## 4. 数据模型硬约束

保持三层同步：

```text
registry/people.sqlite
registry/people.csv
registry/people.jsonl

registry/image_assets.csv
registry/image_assets.jsonl

图片文件夹 + 图片内 TITI/d2i 元数据
```

每个公开人物都必须入库，即使没有图片。最小字段：

```text
person_id
name
gender
province
city
county_or_district
township_or_street
administrative_level
unit_name
department
position
rank_or_grade
division_of_work
biography
full_public_text
source_url
source_page_title
source_site_domain
source_unit_url
publish_date
updated_date
image_status
primary_image_path
raw_fields_json
crawled_at
last_checked_at
notes
```

每张图片都必须有资产记录：

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
caption_or_alt
sha256
content_type
width
height
is_usable
crawled_at
notes
```

允许的 `image_status`：

```text
official_photo
news_photo
no_image
failed_image
needs_review
unknown
```

允许的 `image_type`：

```text
official_photo
news_photo
material_photo
unknown_photo
```

不允许因为无图、女性、职务低、资料少而丢弃公开人物记录。

## 5. 图片与目录规则

图片优先级：

```text
1. 官方头像、领导详情页照片。
2. 同一官方页面中的资料图。
3. 官方新闻页面中能明确对应同一人物的新闻图。
4. 可信补充源中的人物图。
```

文件命名：

```text
姓名.jpg
姓名-资料图-1.jpg
姓名-资料图-2.jpg
姓名-新闻图-1.jpg
姓名-未确认-1.jpg
```

目录结构：

```text
data/public_archive/<项目名>/
  registry/
  <省>/
    省级/<单位>/男|女|未知|no_photo/
    <市>/市级/<单位>/男|女|未知|no_photo/
    <市>/<县区>/<单位>/男|女|未知|no_photo/
```

性别规则：

- 公开文本写明“男/女”时使用公开文本。
- 只能从姓名、职务、照片外观猜测时，不要强行写死，可用 `未知`。
- 女性不删除，按 `女` 文件夹保存。
- KPI 上男性图片优先，但数据完整性优先于筛选。

## 6. TITI/d2i 图片元数据规则

每张可用图片必须写入项目元数据。最低要求：

- 人类可读层：标题、描述、关键词、来源 URL、人物姓名。
- 机器可读层：`Xmp.titi.meta` JSON。
- `Xmp.titi.meta` 中必须包含或保留 `d2i_profile`。

`d2i_profile` 应尽量包含：

```json
{
  "name": "姓名",
  "gender": "男/女/未知",
  "province": "省",
  "city": "市",
  "county_or_district": "县区",
  "township_or_street": "乡镇街道",
  "unit_name": "单位",
  "department": "部门",
  "position": "职务",
  "rank_or_grade": "职级",
  "division_of_work": "分工",
  "biography": "公开简历",
  "full_public_text": "页面中可公开保留的原文",
  "source": "来源页面 URL",
  "source_page_title": "来源页面标题",
  "source_site_domain": "来源域名",
  "source_unit_url": "单位页面 URL",
  "image_url": "原始图片 URL",
  "image_type": "official_photo/news_photo/material_photo/unknown_photo",
  "person_id": "registry person_id",
  "asset_id": "image asset_id",
  "crawled_at": "采集时间"
}
```

如果图片元数据容量或写入库限制导致原文无法完整塞入图片，数据库和 JSONL 必须完整保存，并在图片元数据里写入 `person_id`、`asset_id`、`source_url` 和 registry 指针。

不要用自动摘要替代原文。可以额外生成摘要，但不能覆盖或丢弃 `full_public_text`。

## 7. 发现与抓取策略

开放式搜索关键词：

```text
领导
领导信息
领导之窗
机构领导
政府领导
领导班子
班子成员
工作分工
分管
简历
个人简历
图/简历
人事任免
任前公示
干部任前
拟任
历任
旧任
曾任
原任
部门目录
机构职能
机构设置
政府组成部门
派出所
公安局 领导信息
```

发现一个高价值页面后，必须继续追踪：

- 父级列表页。
- 兄弟页面。
- 同目录相邻详情页。
- 上级单位页面。
- 部门目录、区县政府、乡镇街道列表。
- 页面内姓名链接、头像链接、附件中的人物页面。

但不要盲跑低价值政府信息公开页面，例如：

- 年报、公报、政策文件、采购、意见征集、办事服务、统计公报、互动信箱。
- PDF/DOC/WPS 附件，除非标题明确是任免、公示、简历或名单。

遇到高产结构要沉淀成专项文档或脚本，例如临沂 `bmml/VSB`、淄博 `SPA`。

## 8. 有头浏览器规则

服务器可以部署有头浏览器，但 Linux 服务器一般没有真实显示器，需要二选一：

```text
方案 A：Playwright Chromium + Xvfb
方案 B：Playwright Chrome/Chromium + noVNC/远程桌面
```

推荐：

- 普通 requests 能抓到的页面，不启用浏览器。
- SPA、动态渲染、图片懒加载、复杂 JS 页面，用 Playwright。
- 调试阶段可用 `headless=false` + Xvfb/noVNC。
- 长期运行可用 Playwright headless，但若页面在 headless 下异常，再切有头虚拟显示。

浏览器只能用于正常渲染公开页面，不用于绕验证码、登录、WAF、反自动化挑战。

服务器安装参考：

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
python -m playwright install-deps chromium
```

如需虚拟有头：

```bash
sudo apt-get update
sudo apt-get install -y xvfb
xvfb-run -a python scripts/zibo_spa_browser_collect.py
```

如需人工远程观察，再部署 noVNC/桌面环境；不要把浏览器调试端口暴露到公网。

## 9. 应传给 Hermas 的本地信息

必须传：

```text
d2ilite/requirements.txt
d2ilite/scraper/run_official_profile_collector.py
d2ilite/scraper/run_official_profile_browser_once.py
d2ilite/metadata_writer.py
d2ilite/metadata_manager.py
d2ilite/text_parser.py
d2ilite/downloader.py
d2ilite/scripts/*.py
d2ilite/docs/public_official_collector_handoff_2026-06-28.md
d2ilite/docs/山东政府网站bmml_VSB采集参考.md
d2ilite/docs/zibo_spa_collection_plan_2026-06-28.md
d2ilite/docs/hermas_public_official_collector_rules_2026-06-28.md
d2ilite/docs/模板生成与使用SOP.md
d2ilite/docs/抓取页面与模板生成规范.md
d2ilite/desktop-next/config/exiftool-titi.config
```

如果要续跑而不是从零开始，必须传：

```text
d2ilite/data/public_archive/山东公开官员/registry/
d2ilite/data/public_archive/山东公开官员/**/*.jpg
d2ilite/data/public_archive/山东公开官员/**/*.jpeg
d2ilite/data/public_archive/山东公开官员/**/*.png
d2ilite/data/public_archive/河南公开官员/registry/    # 仅在要续跑河南时传
d2ilite/data/public_archive/河南公开官员/**/*.jpg      # 仅在要续跑河南时传
```

建议传：

```text
d2ilite/docs/official_profile_shandong_supplement_2026-06-28.md
d2ilite/docs/template_validation_*.md
d2ilite/scraper/templates/
```

不要传：

```text
d2ilite/.venv/
d2ilite/__pycache__/
d2ilite/.tmp/
d2ilite/scraper/_tmp_browser_*/
d2ilite/data/browser_profiles/
浏览器 Cookies、登录态、本地缓存
SSH 密钥、代理订阅、token、账号密码
```

特别注意：`run_official_profile_collector.py` 和部分专项脚本可能是本地未提交文件。只 `git clone` 可能拿不到当前工作版本，必须打包当前工作树或确认这些文件已经提交。

## 10. 启动前检查

启动前执行：

```text
1. 确认 Python 版本和依赖安装成功。
2. 确认 registry SQLite 可打开。
3. 导出当前 people/image_assets/source_units CSV/JSONL。
4. 统计 pending/running/done/failed、people、usable image_assets、people_with_assets。
5. 检查 official_photo/news_photo 但 primary_image_path 为空的不一致状态。
6. 抽样 5 张图片读取 TITI/d2i 元数据。
7. 检查最近日志是否有 403/429/521、验证码、跳首页。
8. 确认没有同站点组 worker 正在运行。
```

正式跑小批量：

```bash
python scraper/run_official_profile_collector.py \
  --region shandong \
  --max-workers 2 \
  --max-pages 80 \
  --delay-min 3.0 \
  --delay-max 6.0 \
  --slow-delay-min 15 \
  --slow-delay-max 35 \
  --block-backoff-minutes 90
```

首轮不要无人值守跑大批量。小批量通过后再持续运行。

## 11. 运行中 KPI

图片优先，但不能牺牲来源和元数据。

建议 KPI：

```text
usable image_assets 增量
people_with_assets 增量
male_with_img 增量
official_photo 增量
news_photo/material_photo 增量
no_image people 增量
metadata complete rate
source_url complete rate
primary_image mismatch count
403/429/521 count
captcha/block count
same-site concurrency violation count
```

硬性验收：

- `primary_image mismatch count = 0`。
- 可用图片必须有 `image_assets` 记录。
- 可用图片必须存在本地文件。
- 可用图片必须有 `Xmp.titi.meta` 和 `d2i_profile`。
- 图片元数据必须包含来源 URL 或可追溯 registry ID。
- 每个新增人物必须有 `source_url`。

## 12. 修改代码或规范时

允许修 bug、补模板、补专项脚本，但必须遵守：

- 修改前备份或确认 Git 状态。
- 不删除既有 registry 行和图片文件。
- 不重置用户已有数据。
- SQLite schema 变化必须写迁移说明，并同步更新文档。
- 采集逻辑变更后先用 1-3 个 URL 小样本验证。
- 至少运行 `python -m py_compile` 检查相关脚本。

任何输出规范改变，都要同步更新：

```text
docs/hermas_public_official_collector_rules_2026-06-28.md
docs/public_official_collector_handoff_2026-06-28.md
```

## 13. 报告格式

每次阶段报告至少包含：

```text
运行时间段：
运行区域/站点组：
新增 people：
新增 usable image_assets：
新增 people_with_assets：
新增 male_with_img：
新增 female_with_img：
新增 unknown_with_img：
无图但入库人数：
失败页：
403/429/521：
验证码/风控：
暂停的站点组：
metadata 完整率：
primary_image mismatch：
下一步高价值入口：
```

如果遇到封禁、验证码、投诉、同站点并发、严重误识别，必须先暂停相关站点并报告，不要自行加速、换 IP 或绕过。

