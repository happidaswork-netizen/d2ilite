# TITI 元数据规范 v2

日期：2026-07-01  
状态：生效中，作为后续 TITI 元数据的唯一口径  
适用范围：TITI 后端、前端资产页、白板、队列输出、Forge/D2I 回流、CLI/Agent、酒馆角色卡导入导出

> 本规范定义 `titi-meta` 的当前版本。历史 `schema_version: 1` 必须继续可读；从本规范生效后，TITI 新写入的机器元数据默认使用 `schema_version: 2`。

---

## 1. 设计目标

1. **DB 是权威层**：图片内嵌元数据只是可携带副本，不能替代数据库、sidecar 或导出 JSON。
2. **展示层和机器层分离**：给人看的简介、标签、来源写入标准 XMP；给系统读的结构化 JSON 写入 `titi:meta`。
3. **轻量优先**：队列、状态接口、最近生成结果和普通生成图不能依赖大字段、全量历史或全库扫描。
4. **不强迫迁移旧图库**：已有 JPG/PNG/WebP 和 `schema_version: 1` 继续可读；只有新写入或显式修复时写 v2。
5. **兼容酒馆角色卡**：TITI 内部保存格式无关的角色资料；导出时再生成 SillyTavern 可导入的 PNG/JSON。
6. **不污染提示词事实层**：视觉身份、辅助提示词、酒馆人格、生成提示词必须分层保存，避免 AI 改写误改固定事实。

---

## 2. 存储层

### 2.1 A 层：标准展示字段

A 层用于通用图片软件、文件管理器、digiKam/Forge 和人类查看。

推荐字段：

| 逻辑字段 | 推荐位置 | 说明 |
| --- | --- | --- |
| `name` | `Xmp.iptcExt.PersonInImage[]` | 人名或主体名 |
| `display_description` | `Xmp.dc.description` / `XPComment` | 给人看的说明，不能塞完整机器 JSON |
| `tags` | `Xmp.dc.subject[]` | 标签 |
| `source_url` | `Xmp.dc.source` | 来源页面 |
| `city` | `Xmp.photoshop.City` | 地域 |
| `job_title` | `Xmp.photoshop.AuthorsPosition` | 职务/身份 |
| `rating` | `Xmp.xmp.Rating` | 0 到 5 星 |

### 2.2 B 层：TITI 机器字段

B 层是 `titi-meta` JSON，存放结构化身份、溯源、生成参数和角色卡资料。

推荐写入位置：

1. XMP：`<titi:meta>`，namespace 为 `urn:titi:ns:1.0`
2. PNG text：key 为 `titi`
3. EXIF `UserComment(0x9286)`：只作为 JSON 回退，不再同时放普通简介

### 2.3 C 层：DB / sidecar / 导出文件

DB 是权威。图片内嵌失败、格式不支持或字段太大时，仍必须以 DB 或 sidecar 为准。

建议文件形态：

| 文件 | 用途 |
| --- | --- |
| SQLite `assets.metadata` | 当前权威资料 |
| `*.titi.json` | 单资产 sidecar，适合离线迁移 |
| `*.tavern.json` | 酒馆 JSON 卡导出 |
| `*.tavern.png` | 酒馆 PNG 角色卡导出 |

---

## 3. 版本策略

| 项目 | 规则 |
| --- | --- |
| 当前 schema | `schema: "titi-meta"` |
| 当前版本 | `schema_version: 2` |
| 可读历史版本 | `1`, `2` |
| 新写入版本 | 必须写 `2` |
| 批量修复 | 只在用户显式触发时执行，禁止后台偷偷扫描大图库 |
| 未知字段 | 读取和写回时尽量保留 |

`schema_version: 2` 相比 v1 的关键变化：

1. 明确加入 `tavern_profile` / `tavern_profiles`，用于酒馆角色卡互转。
2. 明确图片格式策略：JPG 继续做 TITI 原始资产，酒馆导出时生成 PNG/JSON。
3. 明确普通生成图不默认嵌入完整酒馆人格，只保存轻量引用。
4. 明确 `prompt`、`prompt_base`、`prompt_context`、`identity_prompt` 的边界。

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
  "tags": ["tag1", "tag2"]
}
```

字段要求：

| 字段 | 类型 | 要求 |
| --- | --- | --- |
| `schema` | string | 必须为 `titi-meta` |
| `schema_version` | integer | 当前写入必须为 `2` |
| `app` | string | 推荐为 `PWI` |
| `component` | string | `titi` / `forge` / `d2i` / `plugin` / `cli` |
| `titi_asset_id` | string | 已入库资产必须有，推荐 UUID v4 |
| `titi_world_id` | string | 默认 `default` |
| `titi_content_hash` | string | 推荐，格式可为 `sha256:<hex>` |
| `name` | string | 可选，但角色资产推荐填写 |
| `identity_prompt` | string | 视觉身份提示词，不等于酒馆人格 |
| `display_description` | string | 给人看的短简介 |
| `tags` | string[] | 标签，去空、去重 |

空值规则：

- 空字符串、空数组、空对象不要写入图片内嵌层。
- DB 可保留空字段供 UI 编辑，但导出时应清理。
- 不允许写入 API Key、Cookie、Token、SSH 密钥、用户密码。

---

## 5. 视觉身份字段

视觉身份用于生图、图生图、白板、参考图匹配。

```json
{
  "name": "张三",
  "identity_prompt": "张三，三十岁左右，刑警，短发，冷静克制，写实摄影风格",
  "display_description": "刑警张三，冷静理性。",
  "tags": ["刑警", "男性", "现代", "写实"],
  "d2i_profile": {
    "name": "张三",
    "description": "来自 Forge/D2I 的人物资料摘要"
  },
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
- `display_description` 是人类简介，应该短而干净。
- `role_aliases` 是同一视觉资产在不同任务中的别名或扮演身份，不等于酒馆 alternate greetings。
- AI 改写默认不能改写 `identity_prompt` 和 `role_aliases`，除非用户显式允许修改辅助提示词/事实层。

---

## 6. 生成溯源字段

普通生成图应保存本次生成的轻量溯源。

```json
{
  "workflow": "img2img",
  "task_id": "...",
  "group_id": "...",
  "group_name": "批次名",
  "group_type": "image2image",
  "api_name": "jimeng-main",
  "model": "jimeng-4.5",
  "prompt": "实际发送给引擎的完整提示词",
  "prompt_base": "用户原始提示词，不含自动元数据补充",
  "prompt_context": "从图片元数据构造的辅助提示词",
  "metadata_context_mode": "profile",
  "metadata_context_scope": "all",
  "negative_prompt": "负面提示词",
  "aspect_ratio": "1:1",
  "resolution": "2K",
  "format": "jpg",
  "source_image": "a.jpg",
  "source_images": ["a.jpg", "b.jpg"],
  "source_role_names": ["张三", "李四"],
  "image_no": 1,
  "image_total": 4
}
```

字段边界：

| 字段 | 含义 |
| --- | --- |
| `prompt` | 实际发送给生图接口的最终文本 |
| `prompt_base` | 用户输入或队列原始提示词 |
| `prompt_context` | TITI 自动从图片元数据拼出来的辅助提示词 |
| `identity_prompt` | 资产事实/视觉身份，不等于本次生成 prompt |
| `display_description` | 展示简介，不等于 prompt |

---

## 7. 酒馆角色卡字段

TITI v2 新增 `tavern_profile`，用于保存可导出为酒馆角色卡的默认人格资料。

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
    "alternate_greetings": [
      "备用开场白 1",
      "备用开场白 2"
    ],
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

如果同一视觉角色需要多个酒馆人格版本，可使用：

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
- 完整 `tavern_profile` 不应默认嵌入每一张普通生成图；普通生成图只保存引用，例如 `tavern_profile_ref`。

轻量引用示例：

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

## 8. 新闻、网页资料和真人图片

TITI 可以管理新闻、网页、采访、百科、社媒公开资料等外部来源，但这些资料不应直接等同于酒馆角色卡正文。尤其当图片对应真人时，必须把“资料证据层”和“角色表现层”分开。

### 8.1 资料证据层

外部资料建议存入 `research_sources`，作为可追溯资料，不默认进入生图 prompt 或酒馆人格。

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
      "captured_at": "2026-07-01T12:00:00Z",
      "language": "zh-CN",
      "summary": "人工或 AI 整理后的摘要",
      "key_facts": [
        "公开事实 1",
        "公开事实 2"
      ],
      "notes": "作者自己的整理备注",
      "rights_note": "只保存摘要和链接，不保存全文",
      "privacy_level": "public",
      "confidence": "medium"
    }
  ]
}
```

字段约束：

| 字段 | 规则 |
| --- | --- |
| `url` | 能保留则必须保留，方便回查 |
| `summary` | 摘要，不保存整篇转载新闻 |
| `key_facts` | 可复用事实点，禁止混入猜测 |
| `notes` | 作者备注，可以记录“不确定”“需复核” |
| `privacy_level` | `public` / `limited` / `private` / `sensitive` |
| `rights_note` | 记录版权/转载限制 |
| `confidence` | `low` / `medium` / `high` |

### 8.2 角色表现层

酒馆卡只应该使用整理后的角色表现资料，而不是直接塞原始新闻。

```json
{
  "persona_reference": {
    "basis": "fictionalized_from_public_sources",
    "summary": "基于公开资料整理出的可写作设定摘要",
    "confirmed_traits": [
      "公开资料能支撑的性格或经历特征"
    ],
    "fictionalization_notes": [
      "为了创作已虚构化处理的部分"
    ],
    "source_ids": ["src_001"]
  }
}
```

推荐规则：

1. 原始新闻、网页全文、长采访不要塞进 `tavern_profile.description`。
2. `tavern_profile` 只保存“可扮演、可聊天、经过确认的摘要”。
3. 需要大量背景时，导出到酒馆的 `character_book`，并只放精炼条目。
4. `creator_notes` 可以写来源说明、虚构化边界和使用提醒。
5. 真人资料默认不要自动进入 AI 改写、生图辅助提示词或角色人格，除非用户显式选择。

### 8.3 真人图片的限制

如果资产对应真实人物：

- 优先只保存公开、必要、可回查的资料摘要。
- 不把敏感个人信息写入图片内嵌元数据。
- 不把住址、联系方式、身份证件、家庭成员隐私、未经确认的传闻放进角色卡。
- 对非公众人物，默认只保留视觉身份和来源引用，不自动生成可扮演人格。
- 如果用于小说/虚构角色，应在 `persona_reference.basis` 标记为 `fictionalized_from_public_sources` 或 `fictional_character_inspired_by_sources`。

一句话：**新闻资料进 TITI 资料库，整理后的可用设定才进酒馆卡。**

---

## 9. TITI 到酒馆角色卡的导出映射

TITI 内部规范不直接等于 SillyTavern 文件格式。导出时由转换器生成酒馆卡。

推荐导出 JSON 结构：

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
| `titi_asset_id` 等 | `data.extensions.titi.*` |

导出要求：

1. 导出 PNG 卡时，TITI 可以从 JPG 原图生成 PNG 头像副本，再把酒馆 JSON 写入 PNG metadata。
2. 导出 JSON 卡时，不需要图片转换。
3. 导出压缩包时，建议包含 `角色名.tavern.png`、`角色名.tavern.json`、原始图片引用清单。

---

## 10. JPG / PNG / WebP 策略

| 格式 | TITI 原始资产 | TITI 内嵌元数据 | 酒馆角色卡 |
| --- | --- | --- | --- |
| JPG | 保持原样 | 支持 best-effort XMP/EXIF | 导出时生成 PNG 副本 |
| PNG | 保持原样 | 支持 XMP/PNG text | 可直接作为酒馆 PNG 卡 |
| WebP | 保持原样 | 只做 best-effort，避免重编码风险 | 导出时生成 PNG 副本 |
| JSON | 不承载图片 | 完整可编辑源文件 | 推荐同步导出 |

原则：

- 不把已有 JPG 批量转换成 PNG。
- 不为了酒馆兼容改写原始图库。
- 酒馆 PNG 卡是导出产物，不是 TITI 原图的替代品。
- 大字段优先放 DB/sidecar/export JSON，不塞进普通生成图。

### 10.1 JPG 元数据长度限制

JPG 不是无限元数据容器。

- JPEG APP/COM segment 由 16-bit 长度字段限制，单段通常只有约 64KB 上限。
- EXIF 和普通 XMP 常落在 APP1 segment 内，也会遇到 64KB 级别限制。
- Extended XMP 或多 COM 分块可以绕过一部分限制，但通用软件、NAS 索引器、缩略图服务和图片管理器支持不稳定。
- 元数据越长，读写图片时的 IO 越重；如果需要重写文件，还会增加失败、截断、损坏或性能抖动风险。

因此：

1. JPG 内嵌层只放轻量可携带字段：`titi_asset_id`、`titi_world_id`、`titi_content_hash`、短 `identity_prompt`、短摘要、来源 URL。
2. 新闻全文、采访全文、长角色卡、世界书、完整提示词迭代记录不写入 JPG 内嵌元数据。
3. 长资料放 DB / sidecar / 导出 JSON；图片里用 `source_ids` 或 `research_source_refs` 引用。

---

## 11. 字段大小和性能限制

为避免百万级图库变慢，必须遵守：

1. 热路径禁止扫描图片目录来补全元数据。
2. 队列状态和 WebSocket 禁止读取完整图片元数据。
3. 最近生成结果只读 TITI 自己生成的短期记录。
4. 普通生成图的内嵌 `titi:meta` 应保持轻量。
5. `tavern_profile`、长示例对话、世界书、完整原文等大字段默认放 DB/sidecar。
6. 如果必须写入图片，必须由用户显式点击“写入图片元数据”或“导出角色卡”。

建议限制：

| 字段 | 建议 |
| --- | --- |
| `identity_prompt` | 可嵌入 |
| `prompt` | 可嵌入，但普通任务不应无限长 |
| `prompt_context` | 可嵌入，建议截断或只保留摘要 |
| `tavern_profile` | 角色资产可嵌入，普通生成图不默认嵌入 |
| `character_book` | 默认不嵌入普通图片 |
| `research_sources` | 默认只放 DB/sidecar，不嵌入普通图片 |
| 完整小说/长原文 | 禁止直接嵌入普通生成图，使用外部引用 |

---

## 12. 读写和迁移规则

读取：

- v1/v2 都必须可读。
- 未知字段必须保留。
- 缺少 `schema_version` 时按旧图处理，不立即判坏。
- `schema` 不是 `titi-meta` 时，不应当作 TITI 权威元数据。

写入：

- 新写入必须为 `schema_version: 2`。
- 同文件修复时 merge 旧字段，不清空未知字段。
- v1 图片被显式写回时，可升级为 v2。
- 不在后台静默批量升级。

校检：

- `schema_version` 为 `1` 或 `2` 都不应报错。
- 非支持版本给 warning，不自动修复。
- 缺少 `titi_asset_id` 是 issue，但允许导入流程后补。

---

## 13. UI 和功能落点

资产页：

- 显示顶层身份字段：`name`、`identity_prompt`、`tags`、`role_aliases`
- 新增酒馆资料编辑区：`tavern_profile`
- 新增外部资料区：`research_sources` / `persona_reference`
- 支持导出：酒馆 PNG、酒馆 JSON、PNG+JSON 压缩包
- 支持导入：酒馆 PNG/JSON 补全 `tavern_profile`

提示词工作台：

- 可以从成功生成结果提取 `prompt_base`、`prompt_context`、`prompt`。
- 可以从角色资产提取 `identity_prompt`。
- 不默认修改 `tavern_profile` / `research_sources`，除非进入角色卡编辑、资料整理或 AI 补全流程。

队列和历史：

- 只展示轻量溯源和输出路径。
- 不加载完整 `tavern_profile`。
- 导出最终提示词时保留任务 ID、输出图路径、`prompt_base`、`prompt_context`、最终 `prompt`。

CLI/Agent：

- 可以读取 DB/sidecar 生成角色卡。
- 可以批量检查 v1/v2 元数据。
- 不应在未经确认时批量改写原图。

---

## 14. 实施方案

### P0：规范落地

- 新增本文件作为唯一口径。
- 后端新增 `TITI_META_SCHEMA_VERSION = 2`。
- 新写入的 TITI 机器元数据默认写 v2。
- 批量校检接受 v1/v2。

### P1：角色卡资料层

- DB `assets.metadata` 支持 `tavern_profile` 和 `tavern_profiles`。
- DB `assets.metadata` 支持 `research_sources` 和 `persona_reference`。
- 资产页增加酒馆资料编辑与预览。
- 资产页增加资料来源管理、摘要提取和隐私/版权标记。
- 普通生成图只保存 `tavern_profile_ref`，不默认复制完整人格。

### P2：导入导出

- `GET/POST /api/assets/tavern-card` 或等价接口。
- 支持 TITI 资产导出 SillyTavern JSON。
- 支持 TITI 资产导出 SillyTavern PNG 卡。
- 支持 SillyTavern JSON/PNG 导入并合并到 `tavern_profile`。

### P3：批量和 Agent

- CLI 支持批量导出角色卡。
- CLI 支持校检 v1/v2 差异。
- Agent 可以根据角色视觉身份辅助生成酒馆人格草稿，但必须由用户确认后写入。

---

## 15. 一句话口径

TITI v2 元数据以 DB 为权威，以 `titi-meta` JSON 为可携带机器层，以标准 XMP 为展示层；JPG 原图继续保留，酒馆兼容通过导出 PNG/JSON 完成，不强制迁移旧图库。
