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
d2i vision recrawl-inbox
d2i vision auto-route <vj_id>
```

- API 默认：`http://127.0.0.1:8787`（Hermes host 网络）
- Token：wrapper 自动读 `/runtime/d2i-cloud-data/web_token.txt`
- 公网须先过 Cloudflare Access；Access ≠ 免 Bearer（以 `auth_enabled` 为准）

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

### 视觉

1. `d2i vision status`（available / model / jobs 计数）  
2. `inventory` → `plan` → `enqueue` → `pump --background`  
3. 跑完看 job 的 `result.followup` 或详情「跑完分流」  
4. 失败分流：  
   - **可恢复**（限流/超时/5xx）→ `requeue --policy retryable` 或等 auto-route 建 `retry-auto`  
   - **需重抓**（图太小/损坏/缺图）→ `recrawl-inbox`；**先补图** 再入视觉  
5. 存量失败批可：`d2i vision auto-route <vj_id>`  

### 模板新站

1. 读 `docs/d2i_cloud_template_extract_contract.md`  
2. 用 `d2i-lite-template-builder` 出模板  
3. 小样 T0（5–20 人，safe）过再扩  

## 失败中文标签（UI/你汇报时用）

| 标签 | 含义 | 下一步 |
| --- | --- | --- |
| 图太小 | 20×20 等占位图 | 重抓原图，不重跑 vision |
| 图片损坏 | truncated / 0 字节 | 删坏图重抓 |
| 缺图 | path 无文件 | 重抓或标 no_photo |
| 限流/超时/上游故障 | 瞬时 | retryable 重跑 |
| 同名歧义/性别冲突 | 需人 | 人工裁定，默认不自动入队 |

## 清理原则（nas-main）

- **可自动清：** 断链 skill 符号链接、明显过期的 `d2i-cloud` 短 skill 备份（装新版时已 backup）  
- **须你确认再动：** `d2i-image-worker`（旧视觉 worker 叙事）、其它非 D2I 的 hub skill  
- **不要删：** `titi-archive-doctor`、`titi-cli-operator`、`d2i-lite-template-builder`、本 skill  
- 长篇历史交接（7/15、7/16、7/23）**不必进 context**；需要时按路径打开知识库，不整本加载  

## 一句话自检

用户说「管 D2I」→ `d2i status` → 看队列/视觉计数 → 坏图进收件箱、可恢复才重跑 → finalize 先 dry-run → 不写禁路径。
