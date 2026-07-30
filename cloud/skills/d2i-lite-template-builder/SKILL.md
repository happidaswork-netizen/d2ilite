---
name: d2i-lite-template-builder
description: 为 D2I Cloud 快速生成、修改、校验和入库公开网页抓取模板。用于用户给出政府、医院、学校、律所、机构人员页或其他公开网页，要求分析网页、复用已有模板、抓取图片并写入 TITI 元数据时。默认走个人项目快速路径：一次结构校验、一个小样队列，抓错可重抓，不做无收益的多轮测试。
---

# D2I Lite Template Builder

目标是尽快得到可运行模板。站点差异只放进 JSON 模板，不为每个网站复制爬虫代码。

## Current contract

开始前读取：

1. `docs/d2i_cloud_template_extract_contract.md`
2. `docs/模板生成与使用SOP.md`
3. `docs/抓取页面与模板生成规范.md`
4. 本 Skill 的 `references/template-contract.md`

Cloud 与旧文档冲突时，以 Cloud 合同为准：

- 必填只有 `name` 和 `detail_url`。
- `image_url` 应尽量提取，但不能设为必填；源站无图的人仍保留。
- 模板必须有 `crawl.speed_tier` 和简短理由。
- 运行和最终落盘只交给 D2I Cloud，不交给旧 `d2i-jobs` 或旧 image worker。

## Fast workflow

1. `d2i status`，确认 Cloud 可用。
2. `d2i templates search --url <URL>`，优先改编同域或相似模板。
3. 只读查看入口页、一个列表页和 2–5 个详情页；确认人物范围、分页、姓名、详情链接和头像位置。
4. 生成候选 JSON 到 `/runtime/d2i-template-work/`。
5. `d2i templates validate <模板.json>`。
6. 校验通过后：

   ```bash
   d2i templates import <模板.json> --notes "站点特点和已知风险"
   ```

7. 建一个 `safe` 小样队列。通常抓 5–10 人即可；站点本来只有 1–6 人时直接跑完整页。
8. 核对：

   - 发现的人是否真是人物，不是导航、栏目或职务。
   - 姓名、详情链接、图片是否对应。
   - 图片能打开。
   - 至少抽查一张图片的 TITI 元数据回读结果。

9. 小样正常就继续完整抓取。失败时修模板并重新抓，不为追求形式完整反复测试。
10. 跑完写回实际结果：

   ```bash
   d2i templates outcome <模板ID> \
     --stats '{"expected":6,"discovered":6,"downloaded":6,"completed":6}' \
     --mode requests_jsl --speed-tier safe --recommended \
     --notes "本次实际结果"
   ```

## Mode choice

- 默认先用 `requests_jsl`：快、稳定、资源少。
- 页面必须执行 JavaScript，或普通请求拿不到正确内容/图片时，直接改用 `browser`。
- 不再强制两个模式都跑一遍。只有当前模式失败或明显漏抓时才试另一个。

## Stop testing and proceed when

满足下面四项就进入正式抓取：

1. 列表命中的是人物。
2. 详情链接正确。
3. 小样图片能打开且与姓名对应。
4. 元数据能从成品图片回读。

抓取结果出错可以重抓，因此不要求为每个新站做三轮、20 人、双模式全套测试。

## Hard rules

- 不绕过登录、验证码、付费墙或访问控制。
- 模板不得包含 Cookie、token、Authorization、密码、代理订阅或 API key。
- 不把 `个人简历`、`要闻动态`、裸 `市长`、单位名或导航词当人物姓名。
- 最终图片按人物姓名命名。
- 不直接写 TITI 数据库或正式图库；Cloud 的 finalize 负责正式落盘。
- 默认继承模板速度档；`turbo` 仍需用户明确确认。
- Telegram 只报告已接收、关键进度和最终结果，不承载抓取器进程。

## Delivery

返回以下信息即可，不要求长篇报告：

- 模板 ID 和文件路径。
- 推荐模式与原因。
- `expected / discovered / downloaded / completed / failed`。
- 主要失败原因。
- 是否建议继续完整抓取。

