# D2I Lite 现代化技术路线实施清单（Tauri 2 + Rust + React）

更新时间：2026-03-04  
适用仓库：`d2ilite`

## 当前状态（2026-03-09）

1. `Phase 0` 已完成，Python 工程已完成核心服务分层。
2. `app.py` 当前主要保留 Tk UI、事件绑定和少量界面协调。
3. 契约回归可执行：`scripts/phase0_contract_smoke.py`、`scripts/bridge_cli_smoke.py`。
4. `desktop-next/` 已具备开发态最小闭环：目录浏览、图片预览、元数据读取与保存。
5. `desktop-next` 开发模式下已通过 `/api/bridge/*` 接入本地原生 metadata runtime 与共享 scraper runtime。
6. `desktop-next/src-tauri` 已初始化，并已接入最小 Tauri 命令桥接。
7. `npm run tauri:dev` 启动链路已验证可起。
8. 已补基础 smoke：provider 选择 + `tauri:dev` 启动链路。
9. 已确认 Tauri 壳内前端切到 `tauri` provider，并完成启动期 `ping`。
10. 已确认 Tauri 壳内 `ping/list/read/save/preview` 端到端 roundtrip 可跑通。
11. `desktop-next` 编辑区已支持 `Profile / TITI / XMP / EXIF / IPTC / Match` 元数据视图。
12. 路线已调整为“旧版冻结为规格基线，新版独立重构”，不再继续投资长期过渡层。
13. `desktop-next` 已支持单图角色元数据结构化编辑：`d2i_profile.name + role_aliases[]`。
14. `desktop-next` 已支持目录级角色工作流：原角色名 / 扮演角色名筛选、勾选集维护、批量设置 / 追加 / 替换 / 清空。
15. 当前已形成 `desktop-next` 重构稳定基线，下一阶段重点转为 `domain / infrastructure` 收敛。
16. `domain / infrastructure` 第一轮收敛已完成，`DesktopWorkspace` 已压薄为装配层。
17. 目录角色摘要索引 / 缓存和批量角色执行反馈已完成第一轮强化。
18. 已补统一 release gate，并已验证 `tauri:build:debug` 调试构建产物与切换边界。
19. 元数据读写运行时已切到原生 `ExifTool`，目录列表、图片预览和 metadata 都已由 Vite / Tauri 原生承接。
20. `desktop_metadata_backend.py` 与 `desktop_bridge_cli.py` 当前仅保留为兼容脚本 / 参考实现。
21. 新版公共抓取工作台已进入主路径可用阶段：`desktop-next` 已能读取模板、生成任务配置、启动新任务，并承接任务列表、进度表、日志尾部和已有任务的 `pause / continue / retry / rewrite`。

## 1. 目标与原则

目标：

1. 将当前 `Tkinter + ttkbootstrap` 桌面端重构为新的现代桌面应用：`Tauri 2 + Rust Core + React/TypeScript UI`。
2. 保持现有业务能力（看图、元数据读写、抓取结果复核）不回退。
3. 优先提升三项体验：界面稳定性、交互可见性、批量处理性能。

原则：

1. 旧版作为规格基线冻结，新版独立实现。
2. 每一阶段都要“可回滚、可验证、可交付”。
3. 高风险能力（批量写元数据）优先做双写校验与灰度切换。

## 2. 当前基线（Baseline）

当前版本事实：

1. UI 技术栈：`Tkinter + ttkbootstrap`。
2. 核心能力已可用：本地看图、结构化编辑、XMP/EXIF/IPTC 查看与写入、公共抓取流程。
3. 已做的近期修正：描述字段清洗、行断裂修复、编辑区布局与闪烁优化。

基线冻结要求：

1. 任何现代化迁移前，先在 `main` 保留一版稳定快照（本次已执行）。
2. 新功能迭代仅在新分支进行，不直接在 `main` 开发。

## 3. 分阶段实施清单

## Phase 0：工程分层（1 周）

目标：把现有 Python 工程拆成“UI 层 / 领域服务层 / I/O 适配层”。

状态：已完成（2026-03-06）

清单：

1. 抽离 `app.py` 中与 UI 无关逻辑到 `services/`（如元数据读写、文本清洗、任务状态转换）。
2. 统一服务接口（建议 `metadata_service.py`, `image_service.py`, `task_service.py`）。
3. 新增契约测试（输入图片 -> 输出结构化字段），固定黄金样本目录。

验收：

1. `app.py` 仅保留界面和事件绑定。
2. 关键流程有自动化回归测试（至少 20 张样本图）。
3. 最低限度契约冒烟脚本可执行：`python scripts/phase0_contract_smoke.py`。

## Phase 1：现代前端骨架（1 周）

目标：建立 `Tauri + React + TS` 新版主工程骨架，并完成最小可运行闭环。

状态：开发态闭环 + Tauri 壳骨架已完成（2026-03-09）

清单：

1. 新建 `desktop-next/` 前端工程，并初始化 `src-tauri/`。
2. 前端实现基础布局：顶部动作区、左预览、右元数据面板、底部状态栏。
3. 打通开发态最小闭环：打开目录 -> 读取元数据 -> 前端展示 -> 保存回图片。
4. 开发模式通过 Vite 中间件转发 `/api/bridge/*`，其中 metadata 由原生 `ExifTool` runtime 承接，scraper 由共享 `nativeScraperBackend.ts` 承接。
5. Tauri 壳已具备最小命令桥接：`bridge_ping / bridge_list_images / bridge_read_metadata / bridge_save_metadata / bridge_get_default_scraper_base_root / bridge_read_scraper_workspace / bridge_run_scraper_action`，其中 metadata 已原生化，scraper backend 已脱离 Python。

验收：

1. 新桌面端开发模式可读取、预览并保存本地图片元数据。
2. 旧版可继续作为规格基线独立使用。
3. `src-tauri` 已接入最小桥接骨架，可作为新版继续开发起点。

## Phase 2：Rust 核心迁移（2-3 周）

目标：把最耗时链路迁到 Rust，形成高性能本地核心。

清单：

1. Rust 实现目录扫描、文件索引、批量任务调度。
2. Rust 实现图片基础信息读取与缓存（尺寸、格式、hash）。
3. Python 元数据读写先保留，通过 Tauri Command/sidecar 调用。
4. 建立性能基准：`1k/10k` 图片目录加载时间、内存峰值、写入吞吐。

验收：

1. 大目录浏览和切图明显快于当前 Python UI。
2. 内存曲线稳定，无明显 UI 卡顿峰值。

## Phase 3：元数据写入内核升级（2 周）

目标：将高频写入链路逐步替换为 Rust 实现（或 Rust + 稳定库）。

清单：

1. 先做“读对齐”：Rust 读取结果与现有 Python 结果字段对齐。
2. 再做“写对齐”：双写同一张图，比较 XMP/EXIF 关键字段一致性。
3. 最后灰度切换：默认 Rust 写，保留 Python 回退开关。

验收：

1. 关键字段一致率达到 `>= 99.9%`（抽样 + 全量脚本校验）。
2. 写入失败自动回退，且不损坏图片像素。

## Phase 4：抓取与复核工作台整合（1-2 周）

目标：把公共抓取监控、失败重试、复核队列搬到新 UI。

状态：主路径已迁入（2026-03-10，新任务启动、监控面和已有任务控制已迁入 `desktop-next`）

清单：

1. 新 UI 提供新任务启动表单、任务列表、进度表、日志面板、失败重试。
2. 支持“发现进度/下载进度/写入进度”分开显示。
3. 统一中文日志映射与错误提示分级。

验收：

1. 新 UI 具备与当前抓取面板同等可用性。
2. 能在弱网下稳定恢复任务。

## Phase 5：发布切换（1 周）

目标：完成新版本主线切换。

清单：

1. 完成安装包产出（Windows 优先）。
2. 补齐迁移文档（配置、目录、日志、故障排查）。
3. 设置回滚机制（保留上一稳定版启动器）。

验收：

1. 默认启动新版本；旧版本可一键回退。
2. 关键用户操作路径全通过验收清单。

## 4. UI 设计规范（新版本）

硬性规范：

1. 禁止全局滚动壳，采用“固定操作区 + 局部滚动区”。
2. 主操作按钮常驻可见，不允许下拉后才可见。
3. 大列表必须虚拟滚动。
4. 拖动分栏和窗口缩放仅增量重绘，避免全量刷新。
5. 状态信息分层：运行状态、错误详情、可执行下一步。

视觉规范：

1. 统一 Design Tokens（间距、字号、颜色、边框、阴影、状态色）。
2. 使用密度分级（舒适/紧凑）适配大屏与小屏。
3. 保持中文优先的信息架构和术语一致性。

## 5. 分支与发布策略

分支约定：

1. `main`：稳定可发布。
2. `feature/tauri-modernization`：现代化主迭代分支。
3. 子任务分支：`feature/tauri-modernization-<topic>`。

提交与合并：

1. 每个 Phase 至少一个可运行里程碑。
2. 必须通过基础回归检查后再合并回 `feature/tauri-modernization`。
3. 阶段完成后再合并到 `main`。

## 6. 风险与回滚

主要风险：

1. 元数据写入实现差异导致字段丢失或编码异常。
2. 大目录/网络盘场景性能与稳定性不达标。
3. 新旧版本行为不一致导致使用成本上升。

回滚机制：

1. 写入链路保留 Python 回退开关。
2. 发布期保留旧版启动脚本。
3. 每阶段发布前自动备份关键配置与样本目录。

## 7. 下一步（立即执行）

1. 当前图片元数据工作台已经达到可交付、可受控切换状态。
2. 抓取工作台已完成新任务启动 + 监控面 + 已有任务控制迁移，且 desktop runtime backend 已不再依赖 Python；下一步决定是否继续迁移“复核队列 / 审计工作台”。
3. 正式 installer / 签名发布仍是后续独立工作。
