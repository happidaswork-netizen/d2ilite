# 模板验证记录：中国科学院力学研究所_人才队伍

- 模板文件：`scraper/templates/中国科学院力学研究所_人才队伍.json`
- 验证日期：2026-03-01
- 验证方式：Playwright 渲染 + 选择器抽样

## 1. 覆盖范围与层级

已纳入入口：

- `https://imech.cas.cn/people/na/`
- `https://imech.cas.cn/people/zgj/`
- `https://imech.cas.cn/people/fgj/`
- `https://imech.cas.cn/people/youth/ljrc/`
- `https://imech.cas.cn/people/youth/qnrc/`
- `https://imech.cas.cn/people/youth/qchy/`
- `https://imech.cas.cn/people/youth/yp/`

说明：

- `zgj/fgj` 为 Ajax 动态列表，必须浏览器渲染后才有完整条目。
- `cxtd` 为团队表格页（非个人卡片列表），本模板未纳入。

## 2. 列表计数（渲染后）

- `na`: 15
- `zgj`: 101
- `fgj`: 209
- `youth/ljrc`: 18
- `youth/qnrc`: 20
- `youth/qchy`: 44
- `youth/yp`: 21

合计发现：`428`

## 3. 详情抽样结果

抽样结论（每层取样）：

- `name/detail_url/image_url` 可稳定抽取。
- `na` 详情页可直接抽取图片与简介。
- `default_94777` 详情页（浏览器渲染后）可抽取：
  - `name/sex/title/education/phone/email/homepage`
  - `#jl` 等正文分区（部分人员为空）
- 部分 `sourcedb` 详情页正文为空（站点源数据即为空），但姓名与头像可抽取。

## 4. 推荐模式

- 推荐：`browser`
- 原因：
  - `zgj/fgj` 依赖前端 Ajax 渲染，`requests_jsl` 下会漏抓大部分人员。
  - 浏览器模式可统一覆盖多层栏目并提高详情字段完整度。

## 5. 计数口径（用于任务验收）

- 模板预期总数（当前页面状态）：`428`
- 验收建议：
  - `已发现` 接近 `428`（站点实时更新会有轻微浮动）
  - 重点检查 `name/detail_url/image_url` 缺失计数
  - 抽样点击“已完成条目”应可正常打开对应图片
