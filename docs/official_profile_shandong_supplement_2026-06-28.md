> **口径已更替(2026-07-26)**:本文的终落点(`data/public_archive/山东公开官员`)与队列口径已被 D2I Cloud 取代——终根只允许 `角色肖像`,禁写 `山东公开官员`;以 [`D2I_Cloud产品契约_2026-07-25.md`](D2I_Cloud产品契约_2026-07-25.md) 与 [`d2i_cloud_template_extract_contract.md`](d2i_cloud_template_extract_contract.md) 为准。本文仅保留站点经验与历史进度供参考。

# 山东公开官员补抓记录 2026-06-28

## 本次目标

针对山东公开官员图片偏少的问题，补充以下高价值公开页面：

- 济南市人民政府办公厅领导信息
- 济南市公安局领导信息
- 中国经济网山东党政领导人物库
- 人民网组织人事山东图/简历样本

执行口径：只要公开资料能与人物照片关联，即使是历任、曾任、原任、任免、图/简历新闻，也入库并下载图片；无法关联图片的人仍保留在 registry。

## 关键修正

- 支持 `姓名/职务/简历/工作分工` 单页多人结构，并按页面顺序配头像。
- 支持姓名中间有空格或全角空格，例如 `续  明`、`楚  波`、`谭　征`。
- 支持 `district.ce.cn`、`renshi.people.com.cn` 作为山东补充人物库/组织人事来源，但链接追踪必须带山东上下文，避免扩到全国泛任免页面。
- KPI 增加：
  - 有资料且有图人员
  - 男性有资料且有图人员
  - 历史/旧任线索人员
  - 历史/旧任线索图片
  - official/news/material 图片分布
- 市级目录修正为空县区时写入 `市级`，不再写入 `未命名`。

## 浏览器兜底

`https://jnga.jinan.gov.cn/col86386/art/2026/art_86386_4783913.html` 在 requests 与 in-app browser 中出现协议/代理错误，但可见 Edge 能正常打开。

已新增可复用脚本：

```powershell
.\.venv\Scripts\python.exe .\scraper\run_official_profile_browser_once.py `
  --region shandong `
  --url "https://jnga.jinan.gov.cn/col86386/art/2026/art_86386_4783913.html" `
  --city "济南市" `
  --administrative-level agency `
  --unit-name "济南市公安局" `
  --portrait-url-contains "/cms_files/filemanager/1568/picture/"
```

该脚本使用可见 Edge 读取页面，并在页面上下文中 fetch 图片，再走 d2ilite 现有 registry、目录和元数据写入逻辑。

## 本次结果

- 济南市人民政府办公厅领导信息：13 人 / 13 图
- 济南市公安局领导信息：15 人 / 15 图
- 中国经济网山东党政领导人物库：2 人 / 2 图
- 人民网组织人事山东图/简历：1 人 / 1 图

最终 KPI：

- 人员入库：89
- 可用图片：36
- 有资料且有图人员：34
- 男性有资料且有图人员：28
- 历史/旧任线索人员：6
- 历史/旧任线索图片：3
- 正式头像：33
- 新闻图片：1
- 资料图片：2
- `official_photo/news_photo` 但无 `primary_image_path`：0

输出位置：

- 数据库：`data/public_archive/山东公开官员/registry/people.sqlite`
- KPI：`data/public_archive/山东公开官员/registry/kpi.md`
- 济南公安图片：`data/public_archive/山东公开官员/山东省/济南市/市级/济南市公安局/男/`

## 风险状态

本次新增补抓未出现新的 403/429/521。库中历史 WAF 计数仍为 2，来自旧抓取记录。
