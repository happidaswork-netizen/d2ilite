# 公安英烈墙 2022 模板验证记录

## 输入

- 入口链接: `https://www.mps.gov.cn/n2255079/n8310277/n8428023/index.html`
- 抓取范围: 仅当前 2022 清明祭公安英烈专题中的英烈人物列表。
- 目标字段: 姓名、详情链接、图片链接、地区/列表分组、简介、全文、页面标题、meta 信息。
- 输出要求: 按 `公安部英烈_2022` 建目录，只保留最终图片和抓取记录。
- 验收口径: 以入口页可见人物条目为 expected，运行后检查 discovered/downloaded/completed。

## 勘察结论

- 入口标题: `致敬·2022清明祭公安英烈`。
- 列表结构与 2021 模板同型，`div.main1 dl dd a[href*='/content.html']` 命中 474 个英烈人物条目。
- 地区分组位于 `div.main1 dl` 前后的分组文本中，可沿用 `list_region` 口径。
- 详情页结构与 2021 模板同型，抽样详情 `张天豪` 可命中:
  - 姓名: `.con_title::text`
  - 图片: `.touxiang img::attr(src)`
  - 简介/全文: `.content_con p::text` / `.content_con *::text`
- 反爬风险: 中。站点会返回 `__jsl_clearance_s` 挑战，沿用既有 JSL + Edge 浏览器模式。

## 推荐模式

- 推荐: `browser`
- 理由: 既有 2020/2021 公安部模板使用浏览器模式；2022 页面同样存在 JSL 挑战，浏览器模式对详情页和图片防盗链更稳。
- 备选: `requests_jsl`
- 切换条件: 浏览器模式异常慢或 Edge 启动失败，且 `requests_jsl` 小样本图片成功率接近 100% 时再切换。

## 频率

- 单并发。
- 页面/图片随机间隔: `5-8` 秒。
- 阻断状态: `403/429/521` 自动退避 6 小时。
- 性别处理: 全量保留，下载后按 `男/女/未知` 分目录整理；不因性别删除资料。

## 初始计数

- expected: 474
- discovered: 474
- downloaded: 324（当前退避暂停时的唯一图片记录数）
- completed: 324（当前退避暂停时的唯一下载清单数）

## 当前运行状态

- 状态: 暂停，未完成全量。
- 原因: 图片下载阶段收到 `521`，触发既有 backoff 保护。
- 退避到: `2026-06-13T01:38:37+00:00`，北京时间约 `2026-06-13 09:38:37`。
- 当前无硬失败: `raw/failures.jsonl` 不存在。
- 当前性别目录:
  - `男`: 312
  - `女`: 8
  - `未知`: 2
- 重复副本: 已移至 `_duplicates/`，未删除。
- 后续动作: 退避时间后继续运行同一命令，完成后再次执行 `scripts/split_public_archive_by_gender.py --apply`。

## 产物

- 英文模板: `scraper/templates/mps_yinglie_qingming2022.json`
- 中文模板: `scraper/templates/公安英烈墙_2022清明祭扫英烈.json`
- 命令行配置: `scraper/config.mps.qingming2022.json`
