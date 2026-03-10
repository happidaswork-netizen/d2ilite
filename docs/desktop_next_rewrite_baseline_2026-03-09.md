# Desktop Next 重构稳定基线（2026-03-09）

适用仓库：`d2ilite`  
适用分支：`feature/tauri-modernization`

## 1. 基线结论

截至 2026-03-10，`desktop-next` 已形成一个可继续扩展的新版本稳定基线。

已经完成的七个大块：

1. 结构重整：`App.tsx -> DesktopWorkspace -> feature panes`
2. 单图角色元数据结构化编辑
3. 目录级角色筛选、勾选与批量编辑工作流
4. 回归矩阵、文档、长期边界整理
5. `domain / infrastructure` 第一轮收敛
6. 目录索引 / 缓存与批量工作流强化
7. 交付与切换准备

这意味着当前新版已经不是“临时壳”，而是具备明确结构、可回归、可继续扩展的主工程起点。

## 2. 当前稳定能力

1. 加载本地图片目录
2. 列表筛选、单图打开、图片预览
3. 读取并保存单图结构化元数据
4. 查看 `Profile / TITI / XMP / EXIF / IPTC / Match`
5. 编辑角色元数据：
   - 图片原角色名
   - 扮演角色名列表
   - 备注
   - 启用/停用
6. 目录级角色工作流：
   - 原角色名筛选
   - 扮演角色名筛选
   - 勾选集维护
   - 批量设置 / 追加 / 替换 / 清空
   - 按“原角色为空 / 扮演角色为空 / 任一角色为空”匹配
   - 批量进度、跳过统计、失败项反馈
7. 目录角色摘要索引 / 缓存已建立，可复用已读元数据摘要
8. `src-tauri` 已可承接最小桌面壳运行
9. Tauri 壳内 `ping/list/read/save/preview` 已通过端到端 roundtrip
10. 已补统一 release gate，并已验证 `tauri:build:debug` 调试构建产物
11. 当前 Python bridge 已收窄为元数据读写职责，目录列表和图片预览不再经它转发
12. 元数据读写运行时已切到专用 `desktop_metadata_backend.py`，`desktop_bridge_cli.py` 退回兼容层
13. 新版公共抓取工作台已完成第一轮迁移：任务列表、任务概览、进度表、日志尾部

## 3. 当前结构边界

当前推荐把现状理解为：

1. `app/`
   - 页面装配与工作台级协调 hook
2. `features/`
   - `preview`
   - `library`
   - `metadata`
   - `batch`
   - `scraper`
3. `domain/`
   - 元数据表单模型
   - 角色规则
   - 批量规则
   - 元数据显示格式化
4. `infrastructure/`
   - bridge provider
   - smoke roundtrip
   - 目录偏好读写
   - 目录角色摘要索引 / 缓存
   - 抓取工作台 bridge snapshot
5. `services/` 与 Python 写入链路
   - 仍作为当前字段语义和写入规则基线
   - 抓取工作台当前由 `desktop_scraper_backend.py` 承接监控 snapshot
6. `src-tauri/`
   - 作为当前桌面壳和最小命令桥接

当前仍然属于“临时但可用”的边界：

1. `scripts/desktop_bridge_cli.py`
2. `vite.config.ts` 中的 `/api/bridge/*`
3. `src-tauri` 中直接转调 Python CLI 的薄命令

这些可以继续用于开发与回归，但不再视为长期架构终点。

## 4. 当前固定回归矩阵

以下命令在当前机器上已全部通过：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite
.\.venv\Scripts\python.exe -m py_compile app.py services\metadata_service.py services\desktop_metadata_backend_service.py services\desktop_scraper_backend_service.py metadata_manager.py metadata_writer.py scripts\desktop_bridge_cli.py scripts\desktop_metadata_backend.py scripts\desktop_metadata_backend_smoke.py scripts\desktop_scraper_backend.py scripts\desktop_scraper_backend_smoke.py scripts\desktop_tauri_startup_smoke.py scripts\desktop_tauri_roundtrip_smoke.py scripts\desktop_vite_bridge_smoke.py
.\.venv\Scripts\python.exe scripts\phase0_contract_smoke.py
.\.venv\Scripts\python.exe scripts\bridge_cli_smoke.py
.\.venv\Scripts\python.exe scripts\desktop_metadata_backend_smoke.py
.\.venv\Scripts\python.exe scripts\desktop_scraper_backend_smoke.py
cargo check --manifest-path desktop-next/src-tauri/Cargo.toml
cd desktop-next
npm run lint
npm run build
npm run smoke:provider
npm run smoke:roles
cd ..
.\.venv\Scripts\python.exe scripts\desktop_vite_bridge_smoke.py
.\.venv\Scripts\python.exe scripts\desktop_tauri_startup_smoke.py
.\.venv\Scripts\python.exe scripts\desktop_tauri_roundtrip_smoke.py
```

补充说明：

1. `desktop_tauri_startup_smoke.py` 和 `desktop_tauri_roundtrip_smoke.py` 需要串行运行。
2. 两者并行会互抢 `1420` 端口和 `cargo` 锁。

## 5. 下一阶段不要再做的事

1. 不再回头深挖 Tk 侧小 wrapper。
2. 不再把 Python bridge 当长期正式接口继续扩写。
3. 不再把更多临时对接层塞回 `DesktopWorkspace`。

## 6. 下一阶段该做的事

下一阶段从这里接：

1. 在“图片元数据主工作流”范围内，当前已经具备可受控切换条件
2. Python metadata backend 的剩余职责已收窄到元数据读写，这也是下一步替换的明确边界
3. 抓取工作台当前已完成监控面迁移，下一决策点是是否继续迁移任务控制
4. 正式 installer / 签名发布仍是后续独立工作

## 7. 当前完成度判断

如果只看新版主工程：

1. 可继续开发的稳定基线：已完成
2. 长期结构第一轮收敛：已完成
3. 目录索引 / 缓存与批量反馈强化：已完成第一轮
4. 交付与切换准备：已完成
5. 替换临时 bridge：已缩到 metadata backend + scraper backend 两条 Python 运行时边界

更直接的判断：

1. `desktop-next` 的“能用且可继续扩展的基础盘”已经建立
2. 图片元数据主工作流现在已经达到“可交付、可受控切换”的状态
