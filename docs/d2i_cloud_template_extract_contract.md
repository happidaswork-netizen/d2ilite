# D2I Cloud 模版抽取合同

> 本文是 **Cloud 队列 → finalize → 角色肖像 / people.sqlite** 路径上，模版「必须抽出什么」的硬规范。  
> 通用模版写法仍见 [`抓取页面与模板生成规范.md`](抓取页面与模板生成规范.md)；公开官员纪律见 [`hermas_public_official_collector_rules_2026-06-28.md`](hermas_public_official_collector_rules_2026-06-28.md)。  
> **冲突时以本文 Cloud 合同为准**（终落点与 people 回写依赖这里的字段形状）。

---

## 0. 一句话

模版不是「把页面文本尽量多抓下来」，而是稳定产出 **可指名的人** + **可溯源的页** + **可选的官方肖像**，并带上 **行政区 / 单位** 上下文，使 finalize 能落到：

```text
角色肖像/政府/{省}/{市}/{级}/{单位}/{性别}/{姓名}.ext
```

people 行至少能写：`name`、`unit_name`、`source_url`、`primary_image_path` 或 `source_page_image_status=no_photo`。

---

## 1. 对象边界

| 层级 | 谁负责 | 从哪来 |
| --- | --- | --- |
| 人（一条 profile） | 选择器 + 姓名净化 | 列表卡 / 详情页 |
| 图（可选） | `detail_image` 或列表图 | 详情主头像优先 |
| 文（简介/正文） | `detail_summary` / `detail_full_text` | 详情正文容器 |
| 行政区与单位 | **模版 rules，不是页面乱抽** | `rules.admin` + `unit_name` + `output_subdir` |
| 终落点路径 | promote | 上表拼出 |

禁止把导航、栏目名、机构全称截断、职务裸词当成 `name`。

---

## 2. 必抽字段（Must）

对 **每一个将写入 `profiles.jsonl` 的人**，模版链路必须最终得到：

| 字段 | 含义 | 合格标准 | 主要 selector / 规则 |
| --- | --- | --- | --- |
| **name** | 真实人名 | 去空白后 **2–4 个汉字**（少数民族姓名可放宽到约定白名单）；禁止导航词/栏目名/裸职务 | 列表 `name` → 详情 `detail_name` → `_prefer_person_name` |
| **detail_url** | 人物详情或唯一锚定页 | 绝对 URL；同站；不是 `listDisplaySelf` / 首页 / 栏目聚合页（除非该页就是该人唯一页且能抽出人名） | `detail_link` + `response.urljoin` |
| **list_url** | 发现来源列表页 | 可空但应尽量有；用于溯源 | spider 写入 |

`rules.required_fields` **至少**包含：

```json
["name", "detail_url"]
```

政府领导类若页面保证有图，可再加 `"image_url"`；**无图人物仍必须保留档案**（`no_photo`），不得因无图丢人。

### 2.1 姓名合格 / 不合格

**合格示例：** `刘勇`、`王晓玲`、`欧阳修`（≤4 字常规；更长姓名需显式允许）

**不合格（必须进 review，不得当人写入 / 不得 promote 成文件名）：**

- 栏目/导航：`个人简历`、`市政府领导`、`要闻动态`、`政民互动`、`魅力狮城`、`政务公开`、`首页`、`网站地图`
- 机构截断：`沧州市人`、`人民政府`、`…领导`
- 裸职务：`市长`、`副市长`、`书记`、`主任`、`局长`
- 页面壳：title 碎片、整段职务串且抽不出尾部人名

职务+姓名合一（如 `市委副书记、代理市长 刘勇`）**允许作为 detail_name 输入**，但落库 `name` 必须是净化后的 **`刘勇`**。

### 2.2 列表选择器硬约束

- `list_item` 必须落在 **人物列表容器**（如沧州 `jobPersons`、东营 `li:has(span.ldzch_name)`）。
- **禁止**仅用「href 含栏目 id」「正文含『市长』」这类全站宽匹配作为唯一 list 条件。
- 列表命中数应接近业务预期（领导页常为个位数到数十）；若上百且含导航，判模版失败。

---

## 3. 应抽字段（Should）

有则写、无则空；空不阻断建档，但影响 people 完整度与元数据。

| 字段 | 用途 | 合格标准 |
| --- | --- | --- |
| **image_url** | 官方肖像 | 人物头像/简历图；**禁止**站点轮播、logo、栏目装饰图；优先 avatar / 主图容器，慎用全站 `//img[contains(@src,'/images/')]` |
| **gender** | 目录 `男/女/未知` + people | `男`/`女`；可从正文「男，」「女，」推断；不明则 `未知`，不要瞎填 |
| **summary** | 短简介、现任职务抽取 | 首段简历；promote 用 `现任…` 正则抽 position |
| **full_content** | 完整公开简历/正文 | 详情正文容器，勿整页 header/footer |
| **position** | 职务 | 可来自 `detail_fields` 或 summary 解析；模版可 `field_map.position` |
| **list_url** | 发现页 | 已在 Must 鼓励 |

### 3.1 `detail_fields` / `list_fields`（按站点加）

政府领导常见可选：

- `ethnicity` / `title`（职务原文）
- `ethnicity_division`（分工，若页面有）
- `ethnicity`（民族）
- `birth`（公开出生信息，仅页面写明时）

律师/医院/科研等站点可增加 `department`、`title`、`email` 等，但 **不能替代** `name` + `detail_url`。

---

## 4. 模版 rules 必填上下文（不靠页面抽）

这些 **不从正文 xpath 碰运气**，写在模版 `rules`（及 `admin`）里：

| 键 | 作用 |
| --- | --- |
| `unit_name` | people.`unit_name`；路径中的单位段 |
| `admin.province` / `admin.city` | 省/市 |
| `output_subdir` 或等价 | `{市}/{级}/{unit}` 等；**级**建议写清 `省级|市级|县级|…` |
| `output_region` | 与 province 对齐 |
| `field_map.person` | 必须能回到姓名（通常 `["name"]`） |
| `image_naming` | 默认 `name` |
| `site_name` | 稳定英文 id |
| `allowed_domains` | 锁域 |
| `crawl.speed_tier` | Cloud 默认 `safe` |

**路径合同（finalize）：**

```text
政府/{province}/{city}/{admin_level}/{unit_name}/{性别}/{name}.ext
```

- 终根只允许 **角色肖像**（宿主 `/vol1/1001/角色肖像`）。
- 禁止写入 `山东公开官员`、`__hdd_prebind`；保护 `选角/`。
- `output_root` 历史若仍写 `data/public_archive/…`，只表示 **任务工作区**，不是终落点。

---

## 5. 落盘产物合同（队列工作区）

一次合格抓取至少形成：

| 产物 | 最小内容 |
| --- | --- |
| `profiles.jsonl` | 每行：`name`, `detail_url`, 可选 `gender/summary/full_content/image_url/mapped/fields` |
| `image_downloads.jsonl`（有图时） | 与人关联的本地 `named_path` / `saved_path` + `detail_url` 或 `name` |
| 命名图文件 | 文件名以 **姓名** 为中心（允许 `_2`） |
| 可选 `reports/` | promote 后 `promote_report.json` |

Cloud items / library 预览依赖「有真实图片路径」；people 回写依赖 profile +（有图则）下载记录。

---

## 6. promote / people 消费映射

| 来源 | 去向 |
| --- | --- |
| profile.`name` | 文件名、people.`name` |
| profile.`gender` | 目录性别段、people.`gender` |
| profile.`detail_url` | people.`source_url` |
| profile.`summary`/`full_content` | bio；`现任…` → people.`position` |
| rules.`unit_name` + admin | people.`unit_name` / province / city |
| 成功落盘图 | people.`primary_image_path`（**宿主路径**）、`source_page_image_status=has_photo` |
| 有人无图 | `source_page_image_status=no_photo`，仍建档 |

匹配键：优先 `name + province + city + unit_name`，其次 `name + source_url`。

---

## 7. 选择器设计优先级（Cloud 版）

1. **列表容器内人物节点**（最重要）  
2. **详情人名**：meta `ArticleTitle` / 头像旁姓名 / 正文首句姓名；禁止裸站点 `h1`/公共 title 唯一依赖  
3. **详情头像**：人物照片容器 only  
4. **正文**：简历/article 容器；summary 取首段有效 `<p>`  
5. **弱兜底**必须排在后面，且不能单独产生 list 命中  

沧州反例（已废）：

```text
//a[contains(@href,'c1165') or contains(.,'市长')]   ← 会吃导航
//img[contains(@src,'/images/')]                   ← 会吃轮播
name 失败时回落原始 anchor 文本                     ← 会写出「个人简历」
```

沧州正例：

```text
list_item: //div[contains(@class,'jobPersons')]//a[contains(@href,'.shtml') and not(contains(@href,'listDisplaySelf'))]
detail_name: meta ArticleTitle → avatar p
detail_image: //div[contains(@class,'avatar')]//img/@src
```

---

## 8. 验收清单（模版可交付）

新建或大改模版，**全部勾上**才可进 Cloud 默认库：

- [ ] `list_item` 抽样 100% 为人物链，无导航/栏目  
- [ ] 净化后 `name` 正确率 ≥ 95%（领导小样本应为 100%）  
- [ ] 无合格 `name` 的行进 review，不进 profiles 主成功集  
- [ ] `detail_url` 可打开且与该人对应  
- [ ] 有图时 `image_url` 为人像而非栏目图；无图人保留  
- [ ] `rules.unit_name` + `admin.province/city` 已填且与业务一致  
- [ ] 小样 dry-run finalize：路径落在 `角色肖像/政府/…/{单位}/{性别}/{姓名}`  
- [ ] 未把任务目录或 `山东公开官员` 当终根  
- [ ] 速度档默认 `safe`；turbo 需人工 `allow_turbo`

---

## 9. 最小模版骨架（政府领导）

```json
{
  "site_name": "province_city_gov_leadership",
  "start_urls": ["https://example.gov.cn/.../leaders..."],
  "allowed_domains": ["example.gov.cn"],
  "selectors": {
    "list_item": "xpath://*[@class='PERSON_LIST_CONTAINER']//a[...]",
    "name": ["xpath:normalize-space(string(.))"],
    "detail_link": ["xpath:./@href"],
    "detail_name": [
      "xpath:normalize-space(//meta[@name='ArticleTitle']/@content)",
      "xpath:normalize-space(string(//*[@class='avatar']//p[1]))"
    ],
    "detail_image": ["xpath://*[@class='avatar']//img/@src"],
    "detail_summary": ["xpath://*[contains(@class,'article') or contains(@class,'jobDetail')]//p[string-length(normalize-space())>20][1]"],
    "detail_full_text": ["xpath://*[contains(@class,'article') or contains(@class,'jobDetail')]"],
    "detail_fields": {},
    "next_page": []
  },
  "rules": {
    "required_fields": ["name", "detail_url"],
    "unit_name": "某某市人民政府",
    "admin": { "province": "某省", "city": "某市" },
    "output_subdir": "某市/市级/{unit}",
    "output_subdir_pattern": "{region}/{output_subdir}",
    "field_map": {
      "person": ["name"],
      "summary": ["summary"],
      "full_content": ["full_content"],
      "image_url": ["image_url"]
    },
    "image_naming": "name",
    "same_name_dedup": true
  },
  "crawl": {
    "speed_tier": "safe",
    "concurrent_requests": 1,
    "download_delay": 5
  }
}
```

---

## 10. AI / Hermes 怎么用本文

1. 写或改模版前先读本文 §2–§4。  
2. 用 Cloud API 建队跑小样 → `GET .../items` 人工看 `name` 是否像人。  
3. `finalize?dry_run=true` 看 `final_base` 与 candidates。  
4. 合格再 apply；不合格只改模版/选择器，**不要**用模型在热路径上「猜姓名」替代合同字段。  
5. 操作面见 [`cloud/skills/d2i-cloud/SKILL.md`](../cloud/skills/d2i-cloud/SKILL.md)。

---

## 11. 修订

| 日期 | 说明 |
| --- | --- |
| 2026-07-26 | 首版：对齐 promote/people 与沧州事故（宽 list + 导航名 + 宽 image） |
