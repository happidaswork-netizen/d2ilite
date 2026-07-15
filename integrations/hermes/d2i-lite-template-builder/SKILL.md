---
name: d2i-lite-template-builder
description: 为 D2I Lite 生成、审改和验证公开人物资料站点抓取模板。用于用户给出律师、医院、政府、学校、机构人员名录或其他公开网页链接，要求 Hermes 分析页面、创建 scraper/templates/*.json、选择 requests_jsl 或 browser 模式、做三轮验证、解释 expected/discovered/downloaded/completed 差异或修复既有模板时。不得用于绕过登录、验证码、付费墙或访问控制，也不得直接写正式图库。
---

# D2I Lite Template Builder

把站点差异写入 JSON 模板，不为单个站点复制或修改抓取器代码。

## Required context

开始前读取：

1. D2I Lite `AGENTS.md`
2. `docs/模板生成与使用SOP.md`
3. `docs/抓取页面与模板生成规范.md`
4. `docs/TITI元数据规范_v2.md`
5. 本 Skill 的 `references/template-contract.md`

若 D2I Lite 仓库不可见，仍可在任务工作区生成模板草稿；真实抓取未运行时必须标记“未实机验收”。

## Workflow

1. 确认入口 URL、目标人物范围、期望字段、预期数量和运行时限。
2. 先回复：执行步骤、推荐模式及理由、验收口径。
3. 只读勘察列表页、详情页、分页、图片和字段位置，保存少量证据。
4. 优先复用最接近的现有模板；站点差异只改选择器、字段映射、headers、限速和输出策略。
5. 生成 `scraper/templates/<模板名>.json`，运行 `scripts/validate_template.py`。
6. 完成结构轮、模式轮、准生产轮验证。任一轮未运行，不得写“验证通过”。
7. 生成 `docs/template_validation_<模板名>.md`，包含计数、样本三元组、失败类型、推荐模式和切换条件。
8. 返回模板路径、验证文档路径、模板 SHA256 和最终计数。

## Mode selection

- 首选 `requests_jsl`：静态 HTML、document.write、可复现的请求响应或普通图片链接。
- 使用 `browser`：内容必须执行 JavaScript、真实浏览器才能稳定取到字段或图片。
- 两种模式都必须做小样实测后再推荐。browser 环境不可用时标记 blocked，不以推测替代验证。

## Validation rounds

### Structure

- 列表、姓名、详情链接、分页、详情姓名、详情图片命中。
- 选择器不得依赖页面公共导航或错误主体。
- 自定义字段和 `field_map` 与 TITI v2.3 分层一致。

### Mode

- 两种可行模式各跑小样，记录成功率、失败类型和耗时。
- 以图片成功率优先、速度次之选择推荐模式。

### Pre-production

- 抽样 8-20 个详情页。
- 核对“姓名 - 详情链接 - 本地图片路径”三元组。
- 记录 expected/discovered/downloaded/completed/review/failed。
- 元数据只能写 staging；正式 commit 由专用归档角色执行。

## Commands

验证模板：

```bash
python skills/d2i-lite-template-builder/scripts/validate_template.py <template.json> --json
```

D2I Lite 仓库可用时运行小样：

```bash
python scraper/run_public_scraper.py --config <template.json> --output-root <campaign-staging>
```

第一次验证可加 `--skip-metadata`；最终准生产轮也只能写 Campaign staging。

## Hard rules

- 默认遵守 robots.txt、同域限制、低并发和站点限速。
- 不绕过登录、验证码、付费墙、IP限制或其他访问控制。
- 模板不得包含 Cookie、token、Authorization、密码、代理订阅或 API key。
- 最终图片以人物姓名命名，冲突使用 `_2`、`_3`。
- 保留来源 URL、模板身份、模板 SHA256 和验证时间。
- 不把网页全文塞进图片；全文留在 D 层抓取记录。
- 不直接写 TITI 数据库、正式图库或 primary_image_path。
- 不把某个网站做成新 Skill；站点差异必须继续留在模板。
