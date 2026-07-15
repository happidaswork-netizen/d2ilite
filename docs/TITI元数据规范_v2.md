# TITI 元数据总规范（业务修订 v2.3）

规范修订版本：`2.3`
机器元数据版本：`schema: "titi-meta"`、`schema_version: 2`
修订日期：2026-07-15

日期：2026-07-06
状态：生效中，作为 TITI / D2I / D2I Lite / 资产库 / 酒馆导出的唯一元数据口径
适用范围：TITI 后端、前端资产页、队列输出、白板、提示词工作台、D2I/D2I Lite 采集层、真人资料库、Forge/D2I 回流、CLI/Agent、SillyTavern 角色卡导入导出

> 本文件是唯一主规范。任何单独的“真人库规范”“D2I 图片规范”“酒馆角色卡规范”“生成图元数据规范”都必须归并到本文件的分层模型中。历史 `schema_version: 1` 必须继续可读；从本规范生效后，新写入的 TITI 机器元数据默认使用 `schema_version: 2`。
>
> 2026-07-09 v2.2 更正：真人照片归档必须按性别分档，这是 TITI 的硬规则，不是可选偏好。`gender` / `visual_gender` 必须归并到 `archive_gender_bucket`，用于路径分档、抓取优先级、候选排序、主图候选和性别过滤。默认策略是男性优先抓取和保留；女性资料可根据质量、身份确认度、重复度和资源限制酌情降级或放弃。来源事实、视觉推断和归档分档必须分字段保存，视觉推断不得覆盖来源事实。
>
> 2026-07-15 v2.3 统一：业务规范修订版本与机器 `schema_version` 分开管理；新增单图 `photo_audit`、D2I Lite 采集运行、TITI 像素内容哈希、staging/commit 权限边界和 Hermes 模板生成契约。机器 schema 继续为 2，旧读取端必须忽略未知可选字段。

---

## 1. 总原则

1. **DB 是权威层**：图片内嵌元数据只是可携带副本，不能替代数据库、sidecar、导出 JSON 或原始资料归档。
2. **统一规范，分层存储**：真人采集、生成溯源、视觉分类、酒馆人格、新闻资料、提示词迭代都归入同一 schema，但必须放在不同字段层。
3. **全量搜集，轻量运行**：真人公开资料、新闻、网页、图片来源、截图、全文和备注必须全量保留；队列、最近生成结果、WebSocket、缩略图和普通前台热路径只读轻量索引。
4. **事实、推断、提示词、人格分离**：来源事实、视觉推断、AI 生图提示词、AI 改写结果、酒馆人格不能混写到同一个字段。
5. **不强迫迁移旧图库**：已有 JPG/PNG/WebP、`schema_version: 1`、旧 D2I 字段继续可读；只有新写入、显式修复、显式导出时升级。
6. **未知字段保留**：读写 `titi-meta` 时，任何未识别字段都要尽量原样保留，避免破坏 D2I/D2I Lite/插件私有扩展。
7. **图片文件不承载巨量资料**：JPG/PNG 内嵌层只放短摘要、索引和引用；长正文、完整角色卡、世界书、新闻全文、提示词迭代记录放 DB/sidecar/export bundle。
8. **不写秘密**：任何 API key、Cookie、Token、SSH 密钥、密码、私有代理订阅都不得写入图片元数据、sidecar 或普通文档。

---

## 2. 权威层级

### 2.1 D 层：数据库和资料归档，最高权威

D 层保存完整事实、全文、源页面、抓取记录、任务记录和大字段。

推荐承载：

| 内容 | 权威位置 |
| --- | --- |
| TITI 资产索引 | `assets` 表 |
| TITI 资产结构化元数据 | `assets.metadata` |
| 真人资料主体 | `people` 表 |
| 新闻、网页、采访、截图、全文 | `research_sources` / `research_documents` / 本地归档 |
| 生图任务与队列 | `task_groups` / `tasks` / `task_events` |
| 提示词迭代与验证 | 提示词工作台表、任务事件、导出 JSON |
| 酒馆角色卡草稿和多版本人格 | `assets.metadata.tavern_profile(s)` 或专门表 |

规则：

- D 层可以很大，可以全量。
- D 层不能被图片内嵌失败影响。
- 前台热路径默认不扫 D 层大字段。

### 2.2 A 层：标准图片展示字段

A 层写入通用 XMP/EXIF/IPTC，供 digiKam、Forge、文件管理器、人类查看。

| 逻辑字段 | 推荐位置 | 说明 |
| --- | --- | --- |
| `name` | `Xmp.iptcExt.PersonInImage[]` | 人名或主体名；真人图强烈推荐 |
| `display_description` | `Xmp.dc.description` / `XPComment` | 给人看的短简介，不能塞完整 JSON |
| `tags` | `Xmp.dc.subject[]` | 标签，至少包含姓名、城市、单位等关键索引 |
| `source_url` | `Xmp.dc.source` | 来源页面 URL |
| `city` | `Xmp.photoshop.City` | 地域 |
| `job_title` | `Xmp.photoshop.AuthorsPosition` | 职务/身份 |
| `rating` | `Xmp.xmp.Rating` | 0 到 5 星 |

### 2.3 B 层：TITI 机器字段

B 层是 `titi-meta` JSON，存放机器可读结构。

推荐写入位置：

1. XMP：`<titi:meta>`，namespace 为 `urn:titi:ns:1.0`
2. PNG text：key 为 `titi`
3. EXIF `UserComment(0x9286)`：只作为 JSON 回退
4. JPEG COM 分块：只用于兼容旧 D2I 长内容，不作为新大字段首选

### 2.4 C 层：sidecar 和导出文件

| 文件 | 用途 |
| --- | --- |
| `*.titi.json` | 单资产 sidecar，保存完整 `titi-meta` 或资产级资料 |
| `*.people.json` | 真人资料侧车，保存 people/profile/research 片段 |
| `*.tavern.json` | SillyTavern JSON 角色卡导出 |
| `*.tavern.png` | SillyTavern PNG 角色卡导出 |
| `*.research.json` | 完整资料包导出 |
| `archive_manifest.json` | 原网页、截图、全文、图片来源等归档清单 |

---

## 3. 版本策略

| 项目 | 规则 |
| --- | --- |
| 当前 schema | `schema: "titi-meta"` |
| 当前版本 | `schema_version: 2` |
| 可读历史版本 | `1`, `2` |
| 新写入版本 | 必须写 `2` |
| 批量修复 | 显式任务执行，不放进前台热路径 |
| 未知字段 | 读取和写回时尽量保留 |
| 旧图缺版本 | 按旧图处理，不立即判坏 |

`schema_version: 2` 的主变化：

1. 统一 D2I/D2I Lite/TITI/酒馆导出到同一 `titi-meta`。
2. 新增真人资料层 `people_profile` 和采集层 `d2i_profile` 的明确边界。
3. 新增 `tavern_profile(s)`、`research_sources`、`persona_reference`。
4. 明确 `prompt`、`prompt_base`、`prompt_context`、`identity_prompt` 的边界。
5. 明确 JPG 原图保留，酒馆 PNG/JSON 是导出产物。
6. 明确视觉推断字段不得覆盖来源事实字段。

### 3.1 扩展机制和变更治理

本规范必须允许扩容，但扩容必须发生在同一个规范内，不允许另开平行 schema。

新增情况处理流程：

1. 先判断新信息属于哪一层：D 层 DB/归档、A 层展示 XMP、B 层 `titi-meta`、C 层 sidecar/导出包。
2. 再判断它是事实、推断、提示词、人格、任务溯源、审核状态还是工具私有扩展。
3. 新字段优先作为 `schema_version: 2` 的可选字段加入；旧读取逻辑必须能忽略它。
4. 只有出现破坏性变化时才升级 `schema_version`，例如字段含义改变、旧代码会误读、核心结构不再兼容。
5. 不确定的新情况先进入 `raw_fields_json`、`extensions.<tool_name>` 或 `research_sources[].notes`，再由人工或迁移任务整理进正式字段。
6. 大字段默认进入 D 层或 C 层，不进入图片内嵌热路径。
7. 每次新增字段都必须补充字段登记表，说明用途、类型、所属层和热路径影响。

字段登记模板：

| 字段 | 类型 | 所属层 | 含义 | 是否可空 | 是否可入图片内嵌 | 是否进入热路径 | AI 是否可改写 | 迁移/兼容规则 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `example_field` | string | B | 示例字段 | 是 | 是，短文本 | 否 | 否 | 旧图缺失时按空处理 |

扩展命名规则：

| 场景 | 推荐写法 |
| --- | --- |
| 已确认会长期使用的通用字段 | 进入顶层或对应正式对象，例如 `people_profile.xxx` |
| D2I/D2I Lite 私有字段 | `extensions.d2i.xxx` 或 `d2i_profile.xxx` |
| TITI 运行期私有字段 | `extensions.titi.xxx`，稳定后再提升为正式字段 |
| 临时抓取原始字段 | `raw_fields_json` 或 `research_sources[].raw_*` |
| 模型/视觉分类临时输出 | `visual_classification_raw` 或 `extensions.vision_model.xxx` |
| 酒馆导出私有字段 | `tavern_profile.extensions.xxx` 或导出时写入 `data.extensions.titi` |

版本升级规则：

| 情况 | 处理 |
| --- | --- |
| 只新增可选字段 | 继续 `schema_version: 2` |
| 新增枚举值 | 继续 `schema_version: 2`，读取端未知枚举按 `unknown` 或 warning 处理 |
| 字段拆分但旧字段仍可读 | 继续 `schema_version: 2`，保留旧字段映射 |
| 字段含义改变 | 禁止直接改变；新增字段或升级 v3 |
| 删除旧字段 | 禁止；只能弃用并保留读取兼容 |
| 旧读取逻辑会误判/误生成 | 升级 v3 或增加明确兼容开关 |

---

## 4. v2 最小 JSON

新写入的 TITI 机器元数据至少应符合：

```json
{
  "schema": "titi-meta",
  "schema_version": 2,
  "app": "PWI",
  "component": "titi",
  "titi_asset_id": "uuid-v4",
  "titi_world_id": "default",
  "titi_content_hash": "sha256:...",
  "name": "角色名或资产名",
  "identity_prompt": "用于生图的视觉身份描述",
  "display_description": "给人看的短简介",
  "tags": ["tag1", "tag2"]
}
```

字段要求：

| 字段 | 类型 | 要求 |
| --- | --- | --- |
| `schema` | string | 必须为 `titi-meta` |
| `schema_version` | integer | 当前写入必须为 `2` |
| `app` | string | 推荐 `PWI` |
| `component` | string | 只允许 `titi` / `d2i`；表示数据归属层，不表示具体运行工具 |
| `titi_asset_id` | string | 入库资产必须有，推荐 UUID v4 |
| `titi_world_id` | string | 默认 `default`；真人库可用 `{省}_{市}` |
| `titi_content_hash` | string | 推荐，格式 `sha256:<hex>` |
| `name` | string | 角色资产、真人资产推荐填写 |
| `identity_prompt` | string | 视觉身份提示词，不等于酒馆人格 |
| `display_description` | string | 给人看的短简介 |
| `tags` | string[] | 标签，去空、去重 |

空值规则：

- 空字符串、空数组、空对象不要写入图片内嵌层。
- DB 可保留空字段供 UI 编辑，但导出时应清理。
- `null` 不作为有意义值写入图片内嵌层。

`component` 边界：

- `titi` 表示 TITI 管理、生成、队列、资产页、提示词工作台写入的数据。
- `d2i` 表示 D2I / D2I Lite / 采集器写入的数据；具体采集工具版本写入 `d2i_profile.collector_variant` 或 `extensions.d2i.variant`。
- Forge、CLI、插件、Agent 等只是写入工具或执行入口，应记录在 `titi_origin`、任务事件、导出日志或 `extensions.<tool_name>`，不得扩充 `component` 枚举。

---

## 5. 真人资料权威模型

真人资料是 TITI 的核心资产之一。真人资料必须统一进入本规范，不得另建一套和 TITI 无法互转的孤立 schema。

### 5.1 `people` 表推荐 schema

`people` 表是真人主体资料的权威表。它可以由 D2I/D2I Lite/爬虫/人工导入生成，也可以被 TITI 资产页和酒馆导出读取。

```sql
CREATE TABLE people (
    -- ===== 身份标识 =====
    person_id             TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    gender                TEXT,

    -- ===== 地域层级 =====
    province              TEXT,
    city                  TEXT,
    county_or_district    TEXT,
    township_or_street    TEXT,

    -- ===== 行政分类 =====
    administrative_level  TEXT,

    -- ===== 单位与职务 =====
    unit_name             TEXT,
    department            TEXT,
    position              TEXT,
    rank_or_grade         TEXT,

    -- ===== 文本资料 =====
    division_of_work      TEXT,
    biography             TEXT,
    full_public_text      TEXT,

    -- ===== 来源溯源 =====
    source_url            TEXT NOT NULL,
    source_page_title     TEXT,
    source_site_domain    TEXT,
    source_unit_url       TEXT,

    -- ===== 时间 =====
    publish_date          TEXT,
    updated_date          TEXT,
    crawled_at            TEXT,
    last_checked_at       TEXT,

    -- ===== 照片 =====
    has_official_photo    INTEGER DEFAULT 0,
    has_news_photo        INTEGER DEFAULT 0,
    primary_image_path    TEXT,

    -- ===== 照片状态：推荐拆分 =====
    photo_kind            TEXT,
    file_status           TEXT,
    source_image_status   TEXT,
    source_origin_status  TEXT,
    source_page_image_status TEXT,
    repair_status         TEXT,
    archive_decision      TEXT,
    identity_confidence   REAL,
    face_quality          TEXT,
    archive_gender_bucket TEXT,
    archive_decision_reason TEXT,
    quality_issues_json   TEXT,
    cleaning_action       TEXT,

    -- ===== 视觉审核 =====
    visual_age            TEXT,
    visual_gender         TEXT,
    visual_pose           TEXT,
    visual_attire         TEXT,
    visual_body_type      TEXT,
    visual_hairstyle      TEXT,

    -- ===== 原始数据 =====
    raw_fields_json       TEXT,
    notes                 TEXT
);
```

字段边界：

| 字段 | 规则 |
| --- | --- |
| `gender` | 来源站明确值或人工确认值；不得被视觉推断静默覆盖 |
| `visual_gender` | 视觉推断性别：`男` / `女` / `多人混合` / `不确定` |
| `visual_age` | 视觉年龄段，不等于真实年龄 |
| `biography` | 可放人物公开简历 |
| `full_public_text` | 可全量保留源页面正文，但不得进入前台热路径 |
| `raw_fields_json` | 采集原始字段，尽量完整 |
| `photo_kind` / `file_status` / `source_origin_status` / `source_page_image_status` / `repair_status` | 新系统唯一权威照片状态字段 |
| `archive_decision` | 图片归档用途决策，不等于文件可用状态 |
| `identity_confidence` | 该图确认为当前人物的置信度，0 到 1；来源文本、alt、标题、相邻说明和人工确认优先于视觉猜测 |
| `face_quality` | 脸部可用质量：`excellent` / `good` / `usable` / `poor` / `unusable` |
| `archive_gender_bucket` | 归档分档性别：由来源 `gender` 优先确定；来源缺失时可用 `visual_gender` 辅助，必须标记来源 |
| `archive_decision_reason` | 为什么给出该 `archive_decision`，用于记录身份、质量、性别分档和人工/规则来源 |

### 5.2 人口学和视觉筛选枚举

| 字段 | 可选值 |
| --- | --- |
| `gender` | `男` / `女` / `未知` |
| `visual_gender` | `男` / `女` / `多人混合` / `不确定` |
| `archive_gender_bucket` | `男` / `女` / `多人混合` / `未知` / `待审` |
| `visual_age` | `青年20-30` / `中30-45` / `中年45-55` / `中老55-70` / `老年70+` / `不确定` |
| `visual_attire` | `正装` / `工装` / `便装` / `白大褂` / `制服` / `其他` / `不确定` |
| `visual_pose` | `正面证件照` / `半身侧` / `站立` / `坐姿` / `工作场景` / `合影裁切` / `其他` / `不确定` |
| `visual_body_type` | `瘦削` / `标准` / `健壮` / `微胖` / `肥胖` / `多人` / `不确定` |
| `visual_hairstyle` | `光头` / `秃头` / `平头` / `短发` / `背头` / `偏分` / `中长发` / `盘发-扎发` / `长发披肩` / `其他` / `多人` / `不确定` |

视觉字段可用于筛选、辅助生图和归档分档，但不得覆盖来源事实。`gender` 是来源事实或人工事实；`visual_gender` 是视觉推断；`archive_gender_bucket` 是归档分档结果。三者冲突时不得互相静默覆盖，必须保留冲突并把图片标记为待审或在 `archive_decision_reason` 中说明。

### 5.3 行政区划和行政层级

| 字段 | 说明 |
| --- | --- |
| `province` | 省，例如 `山东省` |
| `city` | 市，例如 `济南市`、`临沂市` |
| `county_or_district` | 区/县；市级资料填 `市级` |
| `township_or_street` | 街道/乡镇，可暂不启用 |
| `administrative_level` | `省级` / `市级` / `县级` / `部门级` / `非官员` / `无照片` / `未知` |

### 5.4 单位和职务

| 字段 | 说明 |
| --- | --- |
| `unit_name` | 单位全称，例如 `潍坊市人民政府` |
| `department` | 内部部门，例如 `市公安局` |
| `position` | 职务，例如 `市长`、`局长` |
| `rank_or_grade` | 级别，例如 `一级高级警长` |

`unit_name`、`department`、`position` 不应混写。无法判断时保留原始文本到 `raw_fields_json` 或 `notes`。

### 5.5 照片路径规范

推荐路径：

```text
/data/photos/originals/{大类}/{省}/{市}/{区县级或"市级"}/{单位}/{性别}/{姓名}.jpg
```

大类建议：

| 大类 | 说明 |
| --- | --- |
| `山东省` | 政府官员 |
| `医院` | 医护人员 |
| `房产中介` | 经纪人 |
| `民警` | 公安民警 |
| `驾校教练` | 驾校教练 |

路径是辅助索引，不是唯一权威。DB 中仍必须保留 `person_id`、来源 URL、地域、单位等字段。

待审不是正式路径大类。待审状态应写入审计队列、临时 staging 目录或任务状态，不进入正式分类路径。

### 5.6 照片状态体系

v2.1 推荐拆分：

| 字段 | 可选值 | 说明 |
| --- | --- | --- |
| `photo_kind` | `official_photo` / `news_photo` / `activity_photo` / `portrait_secondary` / `image_asset` / `unknown` | 图片用途或语义类型；不等于主图决策 |
| `file_status` | `available` / `missing` / `unreadable` / `corrupted` / `reject` | 文件层可用状态；`available` 只表示文件可打开，不表示适合主图 |
| `source_origin_status` | `original` / `compressed` / `screenshot` / `damaged` / `unknown` | 源图形态和来源质量，例如原图、网页二压图、截图、损坏图 |
| `source_page_image_status` | `has_photo` / `no_photo_on_source` / `placeholder_avatar` / `unknown` | 来源页面是否有真实人物照片 |
| `repair_status` | `not_needed` / `optional_crop` / `optional_enhance` / `needs_repair` / `reject` | 后续处理建议，不得覆盖原图 |
| `archive_decision` | `keep_primary` / `keep_secondary` / `keep_asset` / `reject` | 归档用途决策；是否写 `primary_image_path` 只看此字段和主图规则 |
| `identity_confidence` | `0.0` 到 `1.0` | 该图确认为当前人物的置信度；身份确认不足时不得设为 primary |
| `face_quality` | `excellent` / `good` / `usable` / `poor` / `unusable` | 脸部可用质量；多人、远景、遮挡、模糊应降低质量 |
| `archive_gender_bucket` | `男` / `女` / `多人混合` / `未知` / `待审` | 归档分档；影响路径、抓取优先级、候选排序和性别过滤 |
| `archive_decision_reason` | string | 决策理由，可记录身份、质量、性别分档和规则来源 |
| `quality_issues_json` | JSON array | 质量问题，例如 `multi_person`、`far_view`、`watermark`、`low_resolution`、`screenshot_ui` |
| `cleaning_action` | string | 可执行清洗建议，例如保留原图、裁切预览、回源重下、转入素材 |

字段边界：

- `source_image_status` 是历史兼容字段，旧系统可继续读取；新写入优先使用 `source_origin_status` 和 `source_page_image_status`，避免把“来源页面是否有图”和“源图是否压缩/截图”混在一起。
- `file_status=available` 不能自动推出 `archive_decision=keep_primary`。
- `photo_kind=activity_photo` 或 `image_asset` 默认不进入 primary；如人工确认主体唯一且脸部质量足够，可以写 `keep_secondary` 或人工提升。
- `archive_gender_bucket` 是正式归档分档字段。它可来自来源 `gender`、人工确认或视觉推断，但必须可追溯；来源事实和视觉推断冲突时，优先保留事实并进入待审。
- 性别分档是抓取和归档决策硬规则：男性优先抓取、优先进入主图候选和复核；女性可根据质量、身份确认度、重复度和资源限制降为 `keep_secondary` / `keep_asset` / `reject`，或在采集队列中跳过。
- 所有裁切、增强、压缩都必须另存派生图；原始下载图不得被覆盖。

主图选择规则：

1. `archive_decision=keep_primary` 才能自动写入或替换 `primary_image_path`。
2. 自动 `keep_primary` 必须同时满足：身份已由来源文本/alt/标题/相邻说明/人工确认支撑，`identity_confidence >= 0.8`，`person_count=1` 或主体唯一，`face_quality` 为 `excellent` / `good` / `usable`。
3. `official_photo` 优先于 `portrait_secondary`，`portrait_secondary` 优先于单人清晰 `news_photo`。新闻照只能在缺少更好主图时作为临时主图，并必须在 notes 或 audit 中标明来源性质。
4. 多人合影、会议远景、海报、二维码、证书、网页截图、设备/车辆/场景占主导、身份无法确认、脸部不可用的图片不得自动设为 primary。
5. 必须支持按 `archive_gender_bucket` 分档；正式路径、检索筛选、采集队列和候选排序都必须能按性别过滤。
6. 默认硬规则：男性资料优先抓取、优先复核、优先进入主图候选；女性资料可在质量不足、身份证据不足、重复或资源紧张时酌情放弃，或降级为 `keep_secondary` / `keep_asset`。
7. 性别分档不得把低身份置信度、主体不唯一或脸部不可用的图片强行提升为 `keep_primary`；性别不能覆盖来源事实或人工确认事实。

兼容旧 `image_status`：

`image_status` 只作为历史字段读取和迁移输入，不作为新 DB schema 的推荐字段，不作为 UI/API 权威字段。现有数据库如果仍有 `image_status` 列，迁移时必须读出并映射到拆分字段；新写入只写 v2.1 字段。如确需兼容旧导出，可生成只读派生值，不得反向覆盖拆分字段。

| 旧状态码 | 映射建议 |
| --- | --- |
| `official_photo` | `photo_kind=official_photo`, `file_status=available` |
| `vision_classified` | 保留为审核标签，不等于文件状态 |
| `downloaded` | `file_status=available`, `repair_status=not_needed` 或按实际处理写 `optional_crop` / `optional_enhance` |
| `png_fixed` | `file_status=available`, `repair_status=not_needed`，并在 notes 记录旧修复动作 |
| `no_image` | `source_page_image_status=no_photo_on_source` |
| `file_missing` | `file_status=missing` |
| `placeholder_avatar` | `source_page_image_status=placeholder_avatar`, `archive_decision=reject` 或 `keep_asset` |
| `no_photo` | `source_page_image_status=no_photo_on_source` |
| `no_photo_on_source` | `source_page_image_status=no_photo_on_source` |
| `work_scene` | `photo_kind=activity_photo` |
| `portrait` | `photo_kind=portrait_secondary` |
| `corrupted` | 旧标记；应复核后改为 `file_status=corrupted`、`source_origin_status=damaged`、`repair_status=needs_repair` 或 `reject` |

---

## 6. 脏数据过滤规则

### 6.1 SKIP_NAMES

以下 `name` 值不应进入真人表：

```python
SKIP_NAMES = {
    '通知公告','机构概况','公共服务','减税降费','人事信息','减费降税',
    '建议提案','统计信息','审计信息','政府报告','会议图解','媒体报道',
    '视频点播','群众列席','健康科普','涉农补贴','创新举措','信息公开',
    '政策文件库','政策解读','政策法规','规划计划','财政信息','资金信息',
    '重点项目','重大建设项目','依申请公开','工作动态','通知公告栏',
    '部门信息公开','人事招考','人事招录','政务信息公开','政务信息',
    '领导简介','领导班子','领导分工','部门介绍','部门概况','部门职责',
    '主要职责','下属机构','直属机构','派出机构','地方戏',
    '医疗保障','生育保险','工伤保险','失业保险','社会保险','养老保险','医疗保险',
    '部门预算','部门决算','数据库','搜索引擎','政策法规栏目','机构概况下'
}
```

### 6.2 `clean_name`

```python
import re

def clean_name(name):
    name = re.sub(r'[\u3000\s]', '', str(name or ''))
    if not name:
        return None
    if len(name) < 2 or len(name) > 6:
        return None
    if name in SKIP_NAMES:
        return None
    if re.search(r'[0-9()（）<>《》]', name):
        return None
    if name.endswith('待确认') or '疑似' in name:
        return None
    return name
```

规则：

- 过滤结果必须记录到审计日志或 `raw_fields_json`，不要静默丢失可追溯信息。
- 长度规则适用于中文真实姓名；少数民族姓名、外文姓名、复姓长名需要人工复核入口。
- 不确定项进入待审队列或临时 staging 区，不要直接混入正式人物库，也不要把 `_待审` 当作正式路径分类。

---

## 7. 资产级 `titi-meta` 完整示例

```json
{
  "schema": "titi-meta",
  "schema_version": 2,
  "app": "PWI",
  "component": "d2i",
  "titi_asset_id": "uuid-v4",
  "titi_world_id": "山东省_济南市",
  "titi_content_hash": "sha256:file_sha256",
  "name": "姓名",
  "identity_prompt": "姓名，男，职务，正装半身照",
  "display_description": "短简介",
  "tags": ["姓名", "单位", "济南市", "市级", "男"],
  "people_profile": {
    "person_id": "sha1-or-custom-id",
    "name": "姓名",
    "gender": "男",
    "province": "山东省",
    "city": "济南市",
    "county_or_district": "市级",
    "administrative_level": "市级",
    "unit_name": "单位",
    "department": "部门",
    "position": "职务",
    "rank_or_grade": "级别",
    "division_of_work": "工作分工",
    "biography": "完整简历或摘要",
    "source_url": "https://...",
    "source_page_title": "页面标题",
    "source_site_domain": "example.gov.cn",
    "publish_date": "2026-01-01",
    "updated_date": "2026-01-02",
    "crawled_at": "2026-07-06T00:00:00Z",
    "photo_kind": "official_photo",
    "file_status": "available",
    "source_origin_status": "original",
    "source_page_image_status": "has_photo",
    "repair_status": "not_needed",
    "archive_decision": "keep_primary",
    "archive_gender_bucket": "男",
    "archive_decision_reason": "来源性别为男，身份确认充分，清晰官方照片，男性优先归档规则下进入主图候选",
    "identity_confidence": 0.95,
    "face_quality": "good",
    "quality_issues": [],
    "visual_age": "中年45-55",
    "visual_gender": "男",
    "visual_pose": "正面证件照",
    "visual_attire": "正装",
    "visual_body_type": "标准",
    "visual_hairstyle": "短发"
  },
  "d2i_profile": {
    "name": "姓名",
    "description": "完整简历或采集摘要",
    "keywords": ["keyword1"],
    "source": "https://...",
    "city": "济南市",
    "gender": "男",
    "unit": "单位",
    "unit_name": "单位",
    "position": "职务",
    "source_url": "https://..."
  },
  "research_source_refs": ["src_001"]
}
```

字段关系：

- `people_profile` 是真人资料结构化层，来自 DB。
- `d2i_profile` 是 D2I/D2I Lite 兼容层，保留旧工具读写习惯。
- `visual_age`、`visual_gender`、`visual_pose`、`visual_attire` 是唯一视觉筛选权威字段，使用中文枚举。
- 旧 `appearance_profile` 或模型英文分类结果只作为 legacy/raw 输入读取，必须映射到 `visual_*` 后再进入查询和筛选。
- `identity_prompt` 是生图视觉身份，不是酒馆人格。

---

## 8. 视觉身份和外观分类

### 8.1 视觉身份字段

```json
{
  "name": "张三",
  "identity_prompt": "张三，三十岁左右，刑警，短发，冷静克制，写实摄影风格",
  "display_description": "刑警张三，冷静理性。",
  "tags": ["刑警", "男性", "现代", "写实"],
  "role_aliases": [
    {
      "name": "李四",
      "identity_prompt": "李四，记者，纪实风格，半身像"
    }
  ]
}
```

约束：

- `identity_prompt` 是视觉描述，主要给生图模型使用。
- `display_description` 是人类简介，应短而干净。
- `role_aliases` 是同一视觉资产在不同任务中的别名或扮演身份，不等于酒馆 alternate greetings。
- AI 改写默认不能改写 `identity_prompt`、`people_profile`、`d2i_profile`、`role_aliases`，除非用户显式允许修改辅助提示词/事实层。

### 8.2 视觉筛选运行字段

TITI 前端、队列 Gate、智能图生图筛选和审计查询统一使用 `visual_*` 中文字段。不得再新增一套英文视觉枚举作为常规查询字段。

推荐字段：

| 字段 | 说明 |
| --- | --- |
| `visual_gender` | `男` / `女` / `多人混合` / `不确定` |
| `archive_gender_bucket` | `男` / `女` / `多人混合` / `未知` / `待审`；归档、队列筛选和智能图生图过滤使用 |
| `visual_age` | `青年20-30` / `中30-45` / `中年45-55` / `中老55-70` / `老年70+` / `不确定` |
| `visual_pose` | `正面证件照` / `半身侧` / `站立` / `坐姿` / `工作场景` / `合影裁切` / `其他` / `不确定` |
| `visual_attire` | `正装` / `工装` / `便装` / `白大褂` / `制服` / `其他` / `不确定` |
| `visual_classification_source` | `metadata_rules` / `metadata_text` / `vision_model` / `manual` / `unknown` |
| `visual_classification_raw` | 可选，保存模型原始输出或旧 `appearance_profile`，不进入常规查询 |
| `visual_warnings` | 分类警告，fail-open |

外观分类失败必须 fail-open：标记 `不确定` 或 warning，但不阻断生图。归档和采集任务必须仍能按 `archive_gender_bucket` 过滤；无法判断性别的资料进入 `未知` 或 `待审`，不得混入明确男女目录。

兼容旧 `appearance_profile`：

- 读取旧图时，可以把 `appearance_profile.appearance_gender` 映射到 `visual_gender`。
- 可以把 `appearance_profile.visual_age_band` / `metadata_age_band` 映射到 `visual_age`。
- 旧对象原文如需保留，写入 `visual_classification_raw` 或 `extensions.legacy.appearance_profile`。
- 新写入不得把 `appearance_profile` 作为常规字段保存。

---

## 9. A 层 XMP 写入规则

### 9.1 真人原始资产

真人原始资产新写入时，A 层推荐：

| XMP 字段 | 值 | 要求 |
| --- | --- | --- |
| `Xmp.iptcExt.PersonInImage[]` | `name` | 有姓名时必填 |
| `Xmp.dc.subject[]` | `[name, unit_name, city, county_or_district, gender, administrative_level]` | 至少含 name/city/unit 中已知项 |
| `Xmp.dc.description` | 短简介，建议不超过 200 字 | 有资料时必填 |
| `Xmp.dc.source` | `source_url` | 有来源 URL 时必填 |
| `Xmp.photoshop.City` | `city` | 有城市时必填 |
| `Xmp.photoshop.AuthorsPosition` | `position` | 有职务时必填 |

审核规则：

- 缺字段不是直接判坏，而是生成 audit issue。
- 医院、房产中介等旧照片如果 0% 符合，应进入补写任务，不应从资产库排除。
- `Xmp.dc.title` 不再作为姓名权威来源；优先 `PersonInImage`，再读 `titi-meta.d2i_profile.name` / `people_profile.name`。

### 9.2 TITI 生成图

生成图 A 层应轻量：

| XMP 字段 | 值 |
| --- | --- |
| `Xmp.dc.description` | `display_description` 或短生成摘要 |
| `Xmp.dc.subject[]` | 标签、提示词标签、任务类型 |
| `Xmp.dc.source` | 来源任务或原图来源 URL；没有时可空 |
| `Xmp.xmp.Rating` | 用户评分 |

完整任务、提示词、失败尝试、AI 改写记录不写入 A 层；进入 B 层/DB/事件表。

---

## 10. 生成溯源字段

普通生成图应保存本次生成的轻量溯源。

```json
{
  "workflow": "img2img",
  "task_id": "...",
  "group_id": "...",
  "group_name": "批次名",
  "group_type": "image2image",
  "api_name": "chatgpt2api",
  "model": "gpt-image-2",
  "prompt": "实际发送给引擎的完整提示词",
  "prompt_base": "用户原始提示词，不含自动元数据补充",
  "prompt_context": "从图片元数据构造的辅助提示词",
  "metadata_context_mode": "profile",
  "metadata_context_scope": "all",
  "negative_prompt": "负面提示词",
  "aspect_ratio": "1:1",
  "resolution": "2K",
  "format": "png",
  "source_image": "a.jpg",
  "source_images": ["a.jpg", "b.jpg"],
  "source_role_names": ["张三", "李四"],
  "image_no": 1,
  "image_total": 4,
  "titi_origin": {
    "task_id": "...",
    "group_id": "...",
    "api_config_id": "...",
    "api_name": "chatgpt2api",
    "model": "gpt-image-2"
  }
}
```

字段边界：

| 字段 | 含义 |
| --- | --- |
| `prompt` | 实际发送给生图接口的最终文本 |
| `prompt_base` | 用户输入或队列原始提示词 |
| `prompt_context` | TITI 自动从图片元数据拼出的辅助提示词 |
| `identity_prompt` | 资产事实/视觉身份，不等于本次生成 prompt |
| `display_description` | 展示简介，不等于 prompt |
| `tavern_profile` | 聊天人格，不等于 prompt |

---

## 11. 新闻、网页资料和真人图片

真人图片与其关联新闻、网页、采访、百科、社媒公开资料，必须全量保留、全量入库、全量可追溯，并且能够随角色卡一起导出为完整资料包。

### 11.1 资料证据层 `research_sources`

```json
{
  "research_sources": [
    {
      "source_id": "src_001",
      "kind": "news",
      "title": "新闻标题",
      "url": "https://example.com/news/1",
      "publisher": "媒体名",
      "published_at": "2026-01-01",
      "captured_at": "2026-07-06T12:00:00Z",
      "language": "zh-CN",
      "raw_text": "抓取到的原文或全文引用；很长时放到 research_documents 并在这里写 ref",
      "raw_text_ref": "research_documents/src_001.md",
      "snapshot_path": "archives/src_001.html",
      "screenshot_paths": ["archives/src_001_page.png"],
      "summary": "人工或 AI 整理后的摘要",
      "key_facts": ["公开事实 1", "公开事实 2"],
      "timeline_events": [
        {
          "date": "2026-01-01",
          "event": "事实事件",
          "source_quote_ref": "src_001#quote_001"
        }
      ],
      "notes": "作者自己的整理备注",
      "source_note": "资料来源、抓取方式、归档说明",
      "archive_scope": "full",
      "use_scopes": [
        "local_archive",
        "export_bundle",
        "prompt_context",
        "tavern_book"
      ],
      "confidence": "medium"
    }
  ]
}
```

字段约束：

| 字段 | 规则 |
| --- | --- |
| `url` | 能保留则必须保留 |
| `raw_text` / `raw_text_ref` | 能保留则必须保留；长正文优先 DB/sidecar/归档 |
| `snapshot_path` / `screenshot_paths` | 能抓取则保留本地归档路径 |
| `summary` | 摘要，用于快速浏览和酒馆短字段 |
| `key_facts` | 可复用事实点，和来源保持对应 |
| `timeline_events` | 人物经历、新闻事件、时间线事实 |
| `archive_scope` | `full` / `partial` / `link_only`；搜集层默认追求 `full` |
| `use_scopes` | `local_archive` / `export_bundle` / `prompt_context` / `tavern_book` / `tavern_profile_draft` |
| `confidence` | `low` / `medium` / `high` |

### 11.2 角色表现层 `persona_reference`

```json
{
  "persona_reference": {
    "basis": "fictionalized_from_public_sources",
    "summary": "基于公开资料整理出的可写作设定摘要",
    "confirmed_traits": ["公开资料能支撑的性格或经历特征"],
    "full_source_policy": "preserve_and_export",
    "fictionalization_notes": ["为了创作已虚构化处理的部分"],
    "source_ids": ["src_001"]
  }
}
```

规则：

1. 原始新闻、网页全文、长采访必须在 TITI 资料层保全。
2. `tavern_profile` 保存“可扮演、可聊天、经过整理的摘要”。
3. 大量背景资料导出到酒馆 `character_book`，并在 `extensions.titi.research_sources` 保留完整来源结构。
4. `creator_notes` 写来源说明、整理口径、虚构化边界和使用提醒。
5. 生图、AI 改写、酒馆导出都可以读取这些真人资料，但必须保留来源链路。

---

## 12. 酒馆角色卡字段

TITI 内部保存格式无关的角色资料；导出时再生成 SillyTavern PNG/JSON。

```json
{
  "tavern_profile": {
    "profile_id": "default",
    "profile_name": "默认角色卡",
    "source": "manual",
    "name": "角色名",
    "description": "角色设定与外观/背景摘要",
    "personality": "性格、说话方式、行为原则",
    "scenario": "默认聊天场景",
    "first_mes": "开场白",
    "alternate_greetings": ["备用开场白 1"],
    "mes_example": "示例对话",
    "creator_notes": "作者备注",
    "system_prompt": "高级系统提示词",
    "post_history_instructions": "历史对话之后追加的行为指令",
    "tags": ["tag1", "tag2"],
    "creator": "rpy",
    "character_version": "1.0"
  }
}
```

多人格版本：

```json
{
  "tavern_profiles": [
    {
      "profile_id": "default",
      "profile_name": "默认版",
      "is_default": true
    },
    {
      "profile_id": "npc-soft",
      "profile_name": "温和 NPC 版",
      "is_default": false
    }
  ]
}
```

约束：

- `tavern_profile.name` 缺省时可从顶层 `name` 补。
- `tavern_profile.description` 可从 `identity_prompt` 或 `display_description` 辅助生成，但不应直接等同。
- `personality`、`scenario`、`first_mes`、`mes_example` 属于聊天人格层，不能自动污染生图提示词。
- 普通生成图不默认复制完整 `tavern_profile`，只保存 `tavern_profile_ref`。

轻量引用：

```json
{
  "tavern_profile_ref": {
    "asset_id": "uuid-v4",
    "profile_id": "default",
    "name": "角色名"
  }
}
```

---

## 13. TITI 到 SillyTavern 的导出映射

推荐导出 JSON：

```json
{
  "spec": "chara_card_v2",
  "spec_version": "2.0",
  "data": {
    "name": "角色名",
    "description": "角色设定",
    "personality": "人格",
    "scenario": "场景",
    "first_mes": "开场白",
    "alternate_greetings": [],
    "mes_example": "示例对话",
    "creator_notes": "作者备注",
    "system_prompt": "系统提示词",
    "post_history_instructions": "后历史指令",
    "tags": [],
    "creator": "rpy",
    "character_version": "1.0",
    "extensions": {
      "titi": {
        "schema": "titi-meta",
        "schema_version": 2,
        "titi_asset_id": "uuid-v4",
        "titi_world_id": "default",
        "titi_content_hash": "sha256:...",
        "visual_identity_prompt": "视觉身份提示词",
        "people_profile_ref": "person_id",
        "research_source_refs": ["src_001"],
        "source_images": [],
        "role_aliases": []
      }
    }
  }
}
```

映射表：

| TITI | 酒馆卡 |
| --- | --- |
| `tavern_profile.name` / `name` | `data.name` |
| `tavern_profile.description` | `data.description` |
| `tavern_profile.personality` | `data.personality` |
| `tavern_profile.scenario` | `data.scenario` |
| `tavern_profile.first_mes` | `data.first_mes` |
| `tavern_profile.alternate_greetings` | `data.alternate_greetings` |
| `tavern_profile.mes_example` | `data.mes_example` |
| `tavern_profile.creator_notes` | `data.creator_notes` |
| `tavern_profile.system_prompt` | `data.system_prompt` |
| `tavern_profile.post_history_instructions` | `data.post_history_instructions` |
| `tavern_profile.tags` / `tags` | `data.tags` |
| `identity_prompt` | `data.extensions.titi.visual_identity_prompt` |
| `people_profile.person_id` | `data.extensions.titi.people_profile_ref` |
| `research_source_refs` | `data.extensions.titi.research_source_refs` |

导出粒度：

| 模式 | 内容 |
| --- | --- |
| `card_only` | 主卡字段 + TITI 来源引用 |
| `card_with_book` | 主卡 + 从资料层生成的 `character_book` 精炼条目 |
| `full_research_bundle` | 主卡 + `character_book` + `extensions.titi.research_sources` + 独立资料 JSON/归档清单 |

导出 PNG 卡时，TITI 可以从 JPG 原图生成 PNG 头像副本，再把酒馆 JSON 写入 PNG metadata。酒馆 PNG 卡是导出产物，不是 TITI 原图的替代品。

---

## 14. JPG / PNG / WebP 策略

| 格式 | TITI 原始资产 | TITI 内嵌元数据 | 酒馆角色卡 |
| --- | --- | --- | --- |
| JPG | 保持原样 | best-effort XMP/EXIF/COM | 导出时生成 PNG 副本 |
| PNG | 保持原样 | XMP/PNG text | 可直接作为酒馆 PNG 卡 |
| WebP | 保持原样 | best-effort，避免重编码风险 | 导出时生成 PNG 副本 |
| JSON | 不承载图片 | 完整可编辑源文件 | 推荐同步导出 |

JPG 不是无限元数据容器：

- JPEG APP/COM segment 由 16-bit 长度字段限制，单段通常约 64KB 上限。
- EXIF 和普通 XMP 常落在 APP1 segment 内，也会遇到 64KB 级限制。
- Extended XMP 或多 COM 分块通用支持不稳定。
- 元数据越长，读写图片时 IO 越重，越容易增加截断、损坏或性能抖动风险。

因此：

1. JPG 内嵌层放轻量字段：`titi_asset_id`、`titi_world_id`、`titi_content_hash`、短 `identity_prompt`、短摘要、来源 URL、`research_source_refs`。
2. 新闻全文、采访全文、长角色卡、世界书、完整提示词迭代记录放 DB/sidecar/归档文件/export JSON。
3. 图片里通过 `source_ids` 或 `research_source_refs` 指向完整资料。

---

## 15. 字段承载与性能边界

| 字段 | 建议承载 |
| --- | --- |
| `identity_prompt` | 可嵌入图片 |
| `display_description` | 可嵌入图片 |
| `people_profile` | 图片内嵌短结构；完整资料以 DB 为准 |
| `d2i_profile` | 可嵌入兼容字段 |
| `visual_*` | 可嵌入轻量结构；作为视觉筛选唯一权威字段 |
| `prompt` | 可嵌入；很长时 DB/sidecar 保留全量，图片内嵌摘要和引用 |
| `prompt_context` | 可嵌入；很长时 DB/sidecar 保留全量，图片内嵌摘要和引用 |
| `tavern_profile` | 角色资产和导出包可带全量；普通生成图写引用 |
| `character_book` | 导出包可带全量；普通生成图写引用 |
| `research_sources` | DB/sidecar/归档文件保留全量；普通生成图写 `research_source_refs` |
| `full_public_text` | DB/sidecar/归档文件保留全量；不进队列热路径 |
| 完整小说/长原文 | DB/sidecar/归档文件保留全量；图片内嵌层写引用 |

百万级图库性能规则：

1. 搜集层、资料层、归档层可以全量保存。
2. 前台热路径不扫描大图库来补全资料。
3. 队列状态和 WebSocket 不读取完整资料包。
4. 最近生成结果只读 TITI 自己生成的短期记录。
5. 普通生成图的内嵌 `titi:meta` 保持轻量索引。
6. 批量 audit、批量补写、全文索引、酒馆导出包生成应作为后台任务或离线任务执行。

---

## 16. 读写、审核和迁移规则

读取：

- v1/v2 都必须可读。
- 未知字段必须保留。
- 缺少 `schema_version` 时按旧图处理，不立即判坏。
- `schema` 不是 `titi-meta` 时，不应当作 TITI 权威元数据。
- `Xmp.dc.title` 不再作为姓名权威来源。

写入：

- 新写入必须为 `schema_version: 2`。
- 同文件修复时 merge 旧字段，不清空未知字段。
- v1 图片被显式写回时，可升级为 v2。
- 不在后台静默批量升级。
- AI 改写不得默认修改事实层字段。

审核：

- `schema_version` 为 `1` 或 `2` 都不应报错。
- 非支持版本给 warning，不自动修复。
- 缺少 `titi_asset_id` 是 issue，但允许导入流程后补。
- 真人资产缺 `source_url`、`PersonInImage`、`name/city/unit` 生成 audit issue，不直接删除。
- 医院、房产中介等旧资产即使缺 unit/position/source_url，也应进入补写队列，不应判为无效资产。

迁移：

1. 先读 DB/sidecar/图片内嵌三处，合并出候选资料。
2. 不覆盖人工字段。
3. 不覆盖来源事实字段。
4. 视觉推断字段只写 `visual_*` 层；旧 `appearance_profile` 只能读取并映射。
5. 长资料写 DB/sidecar，图片只写引用。
6. 每次批量修复必须输出修复报告。

---

## 17. UI 和功能落点

资产页：

- 显示 `name`、`identity_prompt`、`tags`、`role_aliases`。
- 显示并编辑 `people_profile` 的关键字段。
- 显示 `visual_*` 视觉审核结果。
- 显示 `research_sources` / `persona_reference`。
- 显示并编辑 `tavern_profile(s)`。
- 支持写入图片元数据、生成 sidecar、导出酒馆 PNG/JSON/资料包。

提示词工作台：

- 可以从成功生成结果提取 `prompt_base`、`prompt_context`、最终 `prompt`。
- 可以从角色资产提取 `identity_prompt`。
- 不默认修改 `people_profile`、`d2i_profile`、`tavern_profile`、`research_sources`。
- AI 改写结果必须记录，不应直接覆盖事实层。

队列和历史：

- 只展示轻量溯源和输出路径。
- 不加载完整 `research_sources` / `tavern_profile`。
- 导出最终提示词时保留任务 ID、输出图路径、`prompt_base`、`prompt_context`、最终 `prompt`。

D2I / D2I Lite：

- 采集层按本规范写 `people` 表和 `d2i_profile`。
- 图片内嵌层必须写 `schema: titi-meta`、`schema_version: 2`。
- 原始网页正文、截图、来源页和人工备注进入 D 层，不塞进 JPG 内嵌层。

CLI/Agent：

- 可以批量校检 v1/v2 差异。
- 可以批量导出酒馆角色卡。
- 可以批量生成 audit 报告。
- 不应在未经确认时批量改写原图。

---

## 18. 实施优先级

### P0：唯一规范落地

- 本文件作为唯一口径。
- 后端常量 `TITI_META_SCHEMA_VERSION = 2`。
- 新写入默认 v2。
- 批量校检接受 v1/v2。
- 文档中所有元数据引用指向本文件。

### P1：真人资料层合并

- 新增/对齐 `people` 表。
- 将 D2I/D2I Lite 采集字段映射到 `people_profile` 和 `d2i_profile`。
- 引入 `SKIP_NAMES` 和 `clean_name`。
- 引入照片状态拆分和旧 `image_status` 映射。
- 引入 `visual_*` 视觉审核字段，并提供旧 `appearance_profile` 只读映射。

### P2：资产页和元数据写入

- 资产页显示 people/profile/research/tavern 四层资料。
- 图片写入时按 A/B/C/D 层拆分。
- 批量 audit 输出缺字段报告。
- 补写任务不阻塞队列热路径。

### P3：酒馆导出

- 支持 TITI 资产导出 SillyTavern JSON。
- 支持 TITI 资产导出 SillyTavern PNG 卡。
- 支持 `card_only` / `card_with_book` / `full_research_bundle`。
- 支持 SillyTavern JSON/PNG 导入并合并到 `tavern_profile`。

### P4：后台 Agent 和离线索引

- CLI 支持批量导出角色卡。
- CLI 支持校检 v1/v2 差异。
- Agent 可以辅助生成酒馆人格草稿，但必须由用户确认后写入。
- 资料全文检索、百万图库统计、批量补写进入离线索引或后台任务。

---

## 19. 给外部 AI / 实现者的硬性要求

1. 不允许再创建和本文件冲突的独立元数据规范。
2. 所有新增字段必须说明属于 D/A/B/C 哪一层。
3. 真人公开资料必须全量保留，但不得塞进图片内嵌大字段。
4. `gender` 是来源/人工事实；`visual_gender` 是视觉推断；`archive_gender_bucket` 是正式归档分档。
5. 人物照片归档必须男性优先抓取和保留，女性资料可酌情降级或放弃；所有相关实现必须支持男女分文件夹和按性别过滤。
6. `identity_prompt` 是视觉身份；`tavern_profile` 是聊天人格；`prompt` 是生成任务文本。
7. `people_profile` / `d2i_profile` / `visual_*` 可以共存，不得互相覆盖；旧 `appearance_profile` 只能作为读取兼容输入。
8. JPG 原图不为酒馆强制转 PNG；PNG 角色卡是导出副本。
9. 前台热路径不得全库扫描、全文统计或批量读取大字段。
10. 任何批量修复必须可回滚、可审计、可报告。
11. 不写任何密钥、token、密码或私有凭据。
12. 新情况必须按“扩展机制和变更治理”登记，不得绕过本规范直接落库或写图。

---

## 20. 一句话口径

TITI v2 元数据以 DB 为权威，以 `titi-meta` JSON 为可携带机器层，以标准 XMP 为展示层；真人资料全量保留在资料层，生成和队列热路径保持轻量；JPG 原图继续保留，SillyTavern 兼容通过导出 PNG/JSON/资料包完成，不强制迁移旧图库。
---

## 21. v2.3 双版本与同步规则

### 21.1 两类版本号

| 版本 | 当前值 | 何时升级 |
| --- | --- | --- |
| 业务规范修订版本 | `2.3` | 字段登记、权限边界、处理流程或兼容口径变化时升级 |
| 图片机器 schema | `schema_version: 2` | 只有旧读取端会误读、字段含义破坏性变化或核心结构不兼容时才升级 |

v2.3 只增加可选对象、明确字段权威顺序和处理契约，因此机器 schema 继续为 2。任何实现不得把业务修订版本 `2.3` 写进 `schema_version`。

### 21.2 唯一主规范和镜像

- TITI 仓库 `docs/TITI元数据规范_v2.md` 是唯一主规范。
- D2I Lite 仓库保留同路径同名镜像，内容必须与 TITI 主规范字节一致。
- 两边实现变更必须同时运行各自规范测试；不得只复制文档而不修改代码。
- 独立照片归档、抓取模板、酒馆导出文档只能解释子流程，不得重新定义冲突字段。

## 22. 主体事实、单图审核和采集信息分层

### 22.1 `people_profile`：人物事实层

`people_profile` 保存来源页面或人工确认的人物事实，例如姓名、性别、地域、单位、部门、职务、简历摘要和来源。视觉模型不得覆盖这里的事实字段。

推荐姓名读取顺序：

1. `Xmp.iptcExt.PersonInImage[]`
2. `titi-meta.people_profile.name`
3. `titi-meta.d2i_profile.name`
4. `titi-meta.name`
5. 文件名兜底，仅用于提示，不自动写回

### 22.2 `photo_audit`：单张图片状态权威层

`photo_audit` 是资产级对象，保存单张图片的审核、质量、性别分档和归档决策。多个图片属于同一人物时，每张图片必须有自己的 `photo_audit`，不得只把状态放在人物表的一行中。

```json
{
  "schema": "titi-photo-audit",
  "schema_version": 1,
  "status": "pending",
  "file_status": "available",
  "source_page_image_status": "has_photo",
  "archive_gender_bucket": "男",
  "gender_source": "source",
  "audit_source": "d2ilite-collector",
  "audited_at": "2026-07-15T00:00:00Z"
}
```

`status` 可取：`pending` / `reviewed` / `approved` / `rejected`。

- `pending` 允许缺少视觉判断和归档决策，不得为了补齐字段而伪造 `face_quality`、`identity_confidence` 或 `archive_decision`。
- `reviewed` 必须包含审核者实际得出的字段，但不代表允许正式归档。
- `approved` 必须满足 archive-ready 校验，并携带审批或 Campaign 引用。
- `rejected` 必须有 `archive_decision=reject` 或明确拒绝原因。

照片状态权威顺序：

1. D 层照片/资产记录
2. B 层 `photo_audit`
3. `people_profile` 中的兼容镜像
4. 历史 `image_status` 派生值

v2.3 新写入时，`people_profile` 只保存人物事实；如为兼容旧程序镜像照片字段，必须由 `photo_audit` 单向派生，禁止反向覆盖。

### 22.3 `d2i_profile`：采集兼容层

D2I Lite 新写入至少可记录：

```json
{
  "collector_variant": "d2ilite",
  "collection_run_id": "run_...",
  "template_id": "site_template_name",
  "template_version": 1,
  "template_sha256": "sha256:...",
  "source_list_url": "https://...",
  "source_detail_url": "https://...",
  "image_url": "https://...",
  "collected_at": "2026-07-15T00:00:00Z"
}
```

姓名、性别、地域、单位、职务等稳定事实应同时映射到 `people_profile`；抓取器私有字段留在 `d2i_profile` 或 `extensions.d2i`。

## 23. 内容哈希和原始文件哈希

### 23.1 `titi_content_hash`

`titi_content_hash` 使用 TITI 规范化像素哈希 `titi-pixel-sha256-v1`，用于跨格式、跨元数据写入识别相同视觉内容：

1. 按 EXIF orientation 转正；
2. 转换为 RGBA；
3. 读取宽高和规范化像素字节；
4. 依次哈希：算法版本、NUL、`{width}x{height}`、NUL、RGBA 像素；
5. 写入格式为 `sha256:<hex>`。

图片写入 XMP/EXIF 后文件字节会变化，因此禁止把“写入前文件 SHA256”冒充 `titi_content_hash`。

### 23.2 `source_file_sha256`

原始下载文件字节 SHA256 写入 D 层 manifest/数据库字段 `source_file_sha256`，用于下载证据、损坏检测和不可变归档。它默认不进入图片内嵌热路径。

同一条采集记录应同时保留：

- `source_file_sha256`：原始下载字节证据；
- `titi_content_hash`：规范化像素身份；
- `titi_asset_id`：资产身份；
- `source_url/page_url`：来源身份。

## 24. D2I Lite 采集与元数据写入契约

### 24.1 采集运行 D 层产物

每个采集运行必须有稳定 `collection_run_id`，并保留：

```text
raw/list_records.jsonl
raw/profiles.jsonl
raw/review_queue.jsonl
raw/metadata_queue.jsonl
raw/metadata_write_results.jsonl
downloads/image_downloads.jsonl
state/jobdir/
state/image_url_index.json
state/image_sha_index.json
reports/image_download_report.json
reports/metadata_audit_report.json
crawl_record.json
```

完整网页正文、HTML 快照、模板快照、原始字段、失败原因和源文件 SHA256 属于 D 层。图片内嵌层只保存短摘要、稳定事实、来源引用和必要审核快照。

### 24.2 新图片写入要求

D2I Lite 新写入必须：

- 写 `schema: titi-meta`、`schema_version: 2`、`app: PWI`、`component: d2i`；
- 生成或保留 UUID v4 `titi_asset_id`；
- 使用 TITI 像素算法写 `titi_content_hash`；
- 写顶层 `name`、`display_description`、去重后的 `tags`；
- 把来源/人工事实映射到 `people_profile`；
- 把采集工具信息写入 `d2i_profile`；
- 初次下载可写 `photo_audit.status=pending`，不得自动声称 `keep_primary`；
- 保留旧 v1/v2 未知字段；显式写回时升级为机器 schema 2；
- 不把 `full_content`、网页全文、长角色卡或模型原始响应嵌入图片。

### 24.3 A 层写入要求

- 姓名写 `Xmp.iptcExt.PersonInImage[]`；
- 短展示说明写 `Xmp.dc.description`；
- 标签写 `Xmp.dc.subject[]`；
- 页面来源写 `Xmp.dc.source`；
- 城市和职务写既有标准字段；
- `Xmp.dc.title` 仅作标题展示，不作为姓名权威来源；
- 完整 `titi-meta` 写 TITI namespace，EXIF UserComment 只作回退。

## 25. 校验等级与正式提交门槛

### 25.1 `collected`

要求来源可追溯、下载记录存在、文件可读。允许 `photo_audit.status=pending`，只能位于运行目录或 staging。

### 25.2 `portable`

要求机器 schema 正确、`titi_asset_id`、`titi_content_hash`、姓名或明确非人物资产名、标准 XMP 与来源引用可读。portable 不等于可正式归档。

### 25.3 `archive-ready`

除 portable 外，还要求：

- `photo_audit.status` 为 `reviewed` 或 `approved`；
- 照片状态拆分字段完整；
- `archive_gender_bucket` 可追溯；
- `identity_confidence`、`face_quality`、`archive_decision` 来自真实审核；
- 原图未被裁切、增强、压缩覆盖；
- 审批或 Campaign 引用存在。

### 25.4 `primary-ready`

除 archive-ready 外，还必须满足本规范第5.6节主图规则。只有 `archive_decision=keep_primary` 且身份、主体唯一性和脸部质量门槛全部通过时，才能更新 `primary_image_path`。

### 25.5 写权限

- 采集器、模板生成器、普通 Hermes 长任务只能写运行目录和 staging；
- 元数据审核器可生成候选 sidecar、报告和补丁，不直接覆盖正式原图；
- 正式 commit 必须由专用归档角色执行，并记录审批、哈希、前后路径和回滚信息；
- `/archive`、只读扫描和模板验证不得获得正式归档写权限。

## 26. Hermes 生成 D2I Lite 模板契约

Hermes 负责“勘察、生成、验证、交付模板”，D2I Lite 负责执行模板。站点差异必须进入 `scraper/templates/*.json`，不得为每个站点创建 Skill 或复制抓取器代码。

### 26.1 输入

- 一个或多个公开入口 URL；
- 用户要求的人物范围、输出字段和预期数量；
- 可选已知单位、栏目、年份和运行时限。

### 26.2 必须先输出

1. 执行步骤；
2. 推荐模式 `requests_jsl` 或 `browser` 及理由；
3. 验收口径 `expected/discovered/downloaded/completed/review/failed`。

用户已明确要求直接继续时，可以在同一次回复中给出上述内容并开始执行。

### 26.3 必须产出

- `scraper/templates/<模板名>.json`；
- `docs/template_validation_<模板名>.md`；
- 页面勘察证据和抽样记录；
- 结构轮、模式轮、准生产轮三轮验证结果；
- 姓名、详情页、图片路径三元组抽样；
- 计数差异、失败类型、推荐模式和备选切换条件；
- 模板 SHA256 和验证时间。

### 26.4 安全边界

- 默认遵守 robots.txt、同域限制、低并发和站点限速；
- 不绕过登录、验证码、付费墙或访问控制；
- browser 模式不可用时明确标记 blocked，不伪造验证通过；
- 模板生成和验证只能写模板工作区、D2I Lite 模板目录和验证文档；
- 未经专用 commit 审批，不得写正式图库或修改 TITI 权威数据库；
- 模板内不得保存 Cookie、token、密码、代理订阅或其他凭据。

## 27. v2.3 字段登记与兼容

| 字段 | 类型 | 层 | 含义 | 图片内嵌 | AI默认可改 |
| --- | --- | --- | --- | --- | --- |
| `people_profile` | object | B/D镜像 | 人物来源/人工事实 | 短结构可 | 否 |
| `photo_audit` | object | B/D镜像 | 单张图片审核与归档状态 | 短结构可 | 仅审核流程 |
| `d2i_profile` | object | B | D2I Lite 采集兼容信息 | 短结构可 | 仅采集流程 |
| `research_source_refs` | string[] | B | 指向D层完整资料 | 可 | 否 |
| `titi_content_hash` | string | B/D | TITI像素内容哈希 | 必须/推荐 | 否 |
| `source_file_sha256` | string | D | 原始下载文件字节哈希 | 默认否 | 否 |
| `collection_run_id` | string | D/B引用 | 采集运行身份 | 可放d2i_profile | 否 |
| `template_id/version/sha256` | string/int | D/B引用 | 模板身份与可追溯性 | 可放d2i_profile | 否 |

兼容规则：

- 旧 `people_profile` 中的照片状态继续读取，并映射成 `photo_audit` 候选；未经显式审核不自动写回。
- 旧 `image_status` 只读迁移，不作为新权威字段。
- 旧 `component=forge` 读取时视为 D2I 兼容输入；新写入统一使用 `component=d2i`。
- v1 图片继续可读；显式写回升级到机器 schema 2；禁止后台无报告批量升级。
- 未知扩展字段必须保留；新工具私有字段先写 `extensions.<tool>`，稳定后登记到本规范。
