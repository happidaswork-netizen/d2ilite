# D2I Cloud 迭代缺口交接（给迭代工 AI）

> 来源：nas-main Hermes 助手视角 · 2026-07-27  
> 主产品：**D2I Cloud**（Web + API + workers）  
> 助手：**Hermes** 只操作 Cloud，不另起平行采集主控  
> 目的：把「坏图/无图分诊」从人工手搓，收成 Cloud 一等能力

---

## 0. 产品边界（必须遵守）

- 终落点只认 **角色肖像**（容器常 `/runtime/portrait`）
- **禁止**写：`选角/`、`山东公开官员/**`、`__hdd_prebind*`
- **禁止**用 vision 覆盖源 `gender`；只写 `visual_*`
- 默认速度档 **safe**；turbo 须人工确认
- **禁止**对 20×20 / 截断 / 缺图 **反复 vision**
- 覆盖图谱：点节点只提案，确认后才入队
- 坏模板：修 `scraper/templates/` 后 **新开队列**，旧 profiles 不自愈
- 鉴权：以 `d2i status` → `auth_enabled` 为准；公网暴露必须开 token

### 已有能力（不要重复造）

| 能力 | 入口 |
| --- | --- |
| 抓取队列 | `d2i queues …` / `/api/v1/queues` |
| finalize → 肖像 + people | `d2i queues finalize` |
| vision 批处理 | inventory / plan / enqueue / pump |
| 失败分流 | `auto-route`、`requeue`、`recrawl-inbox` |
| 人档闸门 | `d2i people mark no_photo\|hold\|unusable\|resume` |
| 点路径 vision | `POST /api/v1/ai/vision`（paths + person_ids） |

### 明确不要加进 Cloud

1. Hermes 私有长爬 / 旁路 scraper 当二号主控  
2. 视觉结果覆盖源 gender  
3. 无确认全站自动重爬  
4. 写禁路径  
5. LLM 塞进列表页抽取热路径当主姓名源  

---

## 1. 实战背景（为什么提这些缺口）

2026-07-27 recrawl-inbox：**46 条 → 17 唯一人物**。

关键发现：

1. **多数不是 Cloud 队列产物**，是旧链路存量（`primary` 仍像 `/data/photos/originals/...`，无对应 `q_*`）。
2. 沂南「在线访谈」源站多为 **占位图**（`pub-user-portrait`），不是模板选错那么简单。
3. 同人常有 **20×20 假 primary + 另一目录真图**（如 `气象局局/` vs `气象局/`）。
4. `vision enqueue` 按省/市会 **误卷无关 resolvable**；应按人点修。
5. inbox 像 append-only：**人已 mark 后条目仍挂着**，运维噪声大。
6. 路径三套：`/data/photos/originals`、`/runtime/portrait`、宿主 `/mnt/.../角色肖像`，助手和 CLI 易踩坑。

已手工闭环示例（供回归）：

- `people mark no_photo` ×9（访谈占位/404/新闻合影）
- `people mark unusable` ×2（许如秀、韩俊庆：本地截断）
- 本地 rebind + vision 成功：庄启华、于昕光、禚海燕、高启民、张京峰、葛均波（院士路径）

---

## 2. P0（下个迭代必须）

### P0-1 收件箱生命周期

**问题**：`recrawl-inbox` 不能关闭/归档，已处理的人仍显示。

**交付**：

- 条目状态：`open | resolved | dismissed`
- 操作：确认无图 / 图不可用 / 已换好图 / 忽略
- 默认列表只显示 `open`
- 与 `people mark` 联动：mark 成功可自动 resolved
- CLI：`d2i vision recrawl-inbox --status open` 等
- API：list + patch status

**验收**：

- mark no_photo 后，该 person 从 open inbox 消失（或标 resolved）
- 历史日 JSONL 仍可查，但不污染默认视图

### P0-2 Primary 回绑

**问题**：本地已有好图时仍只能 raw 改 people 或靠助手。

**交付**：

- `POST /api/v1/people/{id}/rebind-primary`
- body：目标 path（容器路径）、可选 dry_run
- 服务端校验：文件存在、最小边长、非截断（PIL/完整读）、不在禁路径
- 成功：`primary_image_path` 规范化 + `source_page_image_status=has_photo`（或等价）+ workflow 恢复可 vision
- 可选只读：同名候选扫描 API（不自动 mv）
- CLI：`d2i people rebind --person-id … --path … [--dry-run]`

**验收**：

- 把于昕光从 20×20 路径 rebind 到地震办大图路径（或等价 fixture）后 inventory 可解析且可 vision
- 对 20×20 / truncated 目标 rebind **拒绝**

### P0-3 按人点修 vision

**问题**：enqueue 易全量卷市；`vision run --person-id` 不独立。

**交付**：

- `d2i vision run --person-id <id> [--force]`：自动 resolve path
- `d2i vision enqueue --person-ids a,b,c`（或 API 等价）
- UI：证据板/人档多选 → 只跑选中人
- **禁止**默认把整市 inventory 塞进「修这几人」操作

**验收**：

- 指定 1～3 个 person_id，jobs.total 等于选中可解析人数，不出现无关市批

### P0-4 源站探图（只读分诊）

**问题**：决定「修模板 vs no_photo」靠助手临时 curl。

**交付**：

- 对 `source_url` 只读探测（requests 优先，必要时 browser；遵守站规/不绕登录墙）
- 结论枚举：`has_single_portrait | placeholder_only | http_404 | login_wall | multi_person_news | unknown`
- 写回建议动作：`no_photo | fix_template | must_recrawl | blocked`
- **不**自动全站抓；**不**默认开队列
- UI/CLI：inbox 或 people 上「探测源站」按钮

**验收**：

- 沂南访谈类样例 → `placeholder_only` → 建议 `no_photo`
- 有清晰单人头像样例 → `has_single_portrait` → 建议 `fix_template` 或 rebind/recrawl（视本地文件）

---

## 3. P1（紧随其后）

### P1-1 路径真相统一

- API/CLI 统一暴露：`resolved_path`（容器）、`on_disk`、宽高、文件大小
- primary 写入规范化或稳定兼容映射
- 文档写清：宿主 `角色肖像` ↔ 容器 `/runtime/portrait`

### P1-2 来源分桶

- 标签：`cloud_queue | legacy_import`
- legacy 默认流程：rebind / mark；**不**默认「修模板新开队」
- Cloud 队列失败才进模板修复叙事

### P1-3 Inbox 批量工作流

- 多选 → batch `people mark`
- 失败 job → auto-route 后，在 inbox UI 直接操作人

### P1-4 Promote 前图片质检

- 最小边长、文件完整性、占位图指纹（如 `pub-user-portrait`）
- 不达标不进正式 primary，或标 unusable / 进 staging

### P1-5 人档轻量检索

- 省/市/单位/workflow 筛选
- 脏单位名、同名冲突提示（不做大 CRM）

---

## 4. P2（完整度，可后排）

- 覆盖图谱：缺口 → 提案 → 确认入队 一键流（确认门禁钉死）
- 视觉成本看板（次数/失败原因/每省）
- 通知：队列完成、inbox 新增、上游 503 → Telegram/页内
- 公网默认 `auth_enabled=true`
- TITI 只读消费态；TITI 不写主库
- 远程 worker 状态只读卡（不回写终库）

---

## 5. 建议迭代顺序

**Sprint A（最小可感）**

1. P0-1 inbox 生命周期  
2. P0-2 rebind-primary  
3. P0-3 按人 vision  

**Sprint B**

4. P0-4 源站探图  
5. P1-1 路径统一  
6. P1-2 legacy/cloud 分桶  

**Sprint C**

7. P1-3/4/5 + 选中的 P2  

---

## 6. 实现约束（给写代码的 AI）

- 改 Cloud 契约时同步：`d2i` CLI、Web UI、`skills/d2i-cloud/SKILL.md` + `AGENT_BRIEF.md`
- people 闸门字段约定（已有，勿另起表除非必要）：
  - no_photo → `source_page_image_status=no_photo_on_source` 等（见现 skill）
  - unusable → `source_image_status=source_too_small_or_corrupt` + `file_status=abandoned`
- inventory/plan/enqueue **必须**继续排除 gated people
- auto-route：retryable → retry-auto；must_recrawl → inbox；tiny/truncated 可 stamp unusable
- 任何 finalize / 写肖像路径：禁写选角与山东公开官员
- 测试优先：API 单测 + 1 条 CLI 冒烟 + 1 个 UI 手测清单

---

## 7. 一句话给迭代工

> 控制面已在；下一刀不是新爬虫，而是 **坏图闭环（inbox 可关、rebind、按人 vision、源站探图）** 和 **路径/旧存量治理**。做完后 Hermes 只处理异常升级与模板提案，不再手搓分诊。

---

## 8. 参考命令（现状）

```bash
d2i status
d2i people mark no_photo --person-id <id> --reason "..."
d2i people mark unusable --person-id <id> --reason "..."
d2i people marked --workflow no_photo
d2i vision recrawl-inbox
d2i vision auto-route <vj_id>
d2i vision requeue --policy retryable
```

API base: `http://127.0.0.1:8787`  
Skill: `d2i-cloud`（`AGENT_BRIEF.md` 含坏图分诊 SOP）
