# D2I Cloud — Hermes agent brief（nas-main）

> 一页纸。细节打开同目录 `SKILL.md` 与仓库 `docs/`。  
> 你是 **助手**，不是主控。主产品是 D2I Cloud Web + API。

## 角色

| 角色 | 是谁 | 你怎么配合 |
| --- | --- | --- |
| 主产品 | D2I Cloud（`:8787` / `d2i.517411.xyz`） | 读状态、建队、质检、finalize、vision |
| 你 | Hermes `nas-main` | 用 `d2i` CLI / HTTP；Telegram 只做通知与口令 |
| 下游 | TITI | 只消费 `visual_*` / 元数据，不指挥采集 |
| 模板 | 经验库 + `d2i-lite-template-builder` | 你生成/修补模板，不手写旁路入库脚本 |

## 入口

```bash
d2i status                          # 先看 auth_enabled / running / promoted
d2i templates list
d2i queues list | show | create | start | pause | logs | items | finalize
d2i vision status | inventory | plan | enqueue | jobs | job | pump
d2i vision requeue --policy retryable|must_recrawl|all
d2i vision recrawl-inbox [--status open|resolved|dismissed|all]
d2i vision recrawl-inbox --resolve-person-id <id>
d2i vision auto-route <vj_id>
d2i vision run --person-id <id> [--force]
d2i vision enqueue --person-ids a,b,c
d2i people get <person_id>
d2i people mark no_photo|hold|unusable|resume --person-id <id> [--reason …]
d2i people rebind --person-id <id> --path <portrait> [--dry-run]
d2i people marked [--workflow no_photo|hold|unusable]
```

- API 默认：`http://127.0.0.1:8787`（Hermes host 网络）
- Token：wrapper 自动读 `/runtime/d2i-cloud-data/web_token.txt`
- 公网须先过 Cloudflare Access；Access ≠ 免 Bearer（**以 `d2i status` → `auth_enabled` 为准**）

## 硬禁令

1. **不写** `选角/`、`山东公开官员/**`、`__hdd_prebind*`、`人物整理/` 当公开抓取终落点  
2. 终落点只认 **`角色肖像`**（容器常 `/runtime/portrait`）  
3. **不覆盖** 源 `gender`；只写 `visual_*`  
4. 默认速度档 **`safe`**；`turbo` 必须用户明确确认  
5. **不对** 图太小 / 截断图 / 缺图 **反复跑 vision**（白烧 Grok）  
6. 不双开遗留 `d2i-jobs` 与 Cloud 同一任务  
7. 不把 Hermes 全套 secret 挂进 D2I；视觉用 D2I 独立 key  
8. 覆盖图谱：点节点只 **提案**，确认后才 enqueue  

## 标准 SOP

### 下载 / 落盘

1. `d2i status` → `d2i templates list`  
2. `d2i queues create --template <id> --speed-tier safe`（需要再 `--start`）  
3. `d2i queues items <id>`：拒导航名（`个人简历`/`要闻`/裸`市长`…）  
4. `d2i queues finalize <id> --dry-run` → 确认 → 去掉 dry-run  
5. 坏模板：修 `scraper/templates/` 后 **新开队列**，别指望旧 profiles 自愈  
6. **人少也按 1 模板 1 队**（1–6 人常见）；不要 1 人 1 个下载队列  

### 视觉

1. `d2i vision status`（available / model / jobs 计数）  
2. `inventory` → `plan`（小库 `batch_size=10~15`）→ `enqueue` → `pump --background`  
3. 跑完看 job 的 `result.followup` 或详情「跑完分流」  
4. 失败分流：  
   - **可恢复**（限流/超时/5xx）→ `requeue --policy retryable` 或等 auto-route 建 `retry-auto`  
   - **需重抓**（图太小/损坏/缺图）→ `recrawl-inbox`（默认只列 **open**）；**先补图** 再入视觉  
5. 存量失败批可：`d2i vision auto-route <vj_id>`  
6. inventory **自动排除** 确认无图 / 暂挂 / 图不可用  
7. **按人点修（优先于全市 enqueue）**：本地已有好图 → `people rebind` → `vision run --person-id`；多人 → `enqueue --person-ids a,b,c`  
8. mark no_photo/unusable/hold 后该人从 open inbox **自动 resolved**；可 dismiss 手动忽略  

### 工作流标记（确认无图 / 暂挂）

| 动作 | CLI | 效果 |
| --- | --- | --- |
| 确认无图 | `d2i people mark no_photo --person-id …` | 源站无可用肖像终态；停下载+vision；图谱当已收口 |
| 暂挂 | `d2i people mark hold --person-id …` | 临时不入队；可 `--hold-until` 记在 notes |
| 图不可用 | `d2i people mark unusable --person-id …` | 本地坏图/过小；不入 vision，可再抓（auto-route 会自动戳） |
| 恢复 | `d2i people mark resume --person-id …` | 清闸门，重新进入开放流 |
| 回绑主图 | `d2i people rebind --person-id … --path …` | 校验边长/完整性后写 primary；拒绝 20×20/截断 |
| 列表 | `d2i people marked [--workflow no_photo\|hold\|unusable]` | 查看已标记 |

UI：视觉页证据板 / 收件箱有按钮；顶栏「工作流标记」。

字段约定（people.sqlite，不另起表）：

- 确认无图 → `source_page_image_status=no_photo_on_source` + `image_status=no_photo` + `repair_status=abandoned_no_usable_photo`（默认清空假 path）  
- 暂挂 → `repair_status=hold`  
- 图不可用 → `source_image_status=source_too_small_or_corrupt` + `file_status=abandoned`  

### 模板新站

1. 读 `docs/d2i_cloud_template_extract_contract.md`  
2. 用 `d2i-lite-template-builder` 出模板  
3. 小样 T0（常 1–6 人，safe）过再扩  

## 失败中文标签（UI/你汇报时用）

| 标签 | 含义 | 下一步 |
| --- | --- | --- |
| 图太小 | 20×20 等占位图 | 重抓原图或 **确认无图**；不重跑 vision |
| 图片损坏 | truncated / 0 字节 | 删坏图重抓或标 **图不可用** |
| 缺图 | path 无文件 | 重抓或 **确认无图** |
| 限流/超时/上游故障 | 瞬时 | retryable 重跑 |
| 同名歧义/性别冲突 | 需人 | 人工裁定；可 **暂挂** |

## 清理原则（nas-main）

- **可自动清：** 断链 skill 符号链接、明显过期的 `d2i-cloud` 短 skill 备份（装新版时已 backup）  
- **须你确认再动：** `d2i-image-worker`（旧视觉 worker 叙事）、其它非 D2I 的 hub skill  
- **不要删：** `titi-archive-doctor`、`titi-cli-operator`、`d2i-lite-template-builder`、本 skill  
- 长篇历史交接（7/15、7/16、7/23）**不必进 context**；需要时按路径打开知识库，不整本加载  

## 一句话自检

用户说「管 D2I」→ `d2i status` → 看队列/视觉计数 → 坏图进收件箱、可恢复才重跑 → finalize 先 dry-run → 不写禁路径。
