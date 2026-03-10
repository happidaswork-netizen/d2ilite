# Desktop Next 启动说明（Phase 1 Tauri 壳已初始化）

目录：`desktop-next/`

## 当前状态

1. 已创建 `React + TypeScript + Vite` 前端骨架。
2. 已替换默认模板为 D2I Lite Next 工作台布局（固定操作栏 + 双栏主区 + 局部滚动）。
3. 已接入开发态 Python CLI bridge：
   - `GET /api/bridge/ping`
   - `GET /api/bridge/list`
   - `GET /api/bridge/read`
   - `POST /api/bridge/save`
   - `GET /api/bridge/preview`
   - `GET /api/bridge/scraper/default-root`
   - `GET /api/bridge/scraper/workspace`
   - `POST /api/bridge/scraper/action`
4. 已验证：
   - `npm run lint`
   - `npm run build`
   - 开发态 `ping/list/read/preview` 端到端联通
5. `src-tauri/` 已初始化，并已接入最小 Tauri bridge 命令：
   - `bridge_ping`
   - `bridge_list_images`
   - `bridge_read_metadata`
   - `bridge_save_metadata`
   - `bridge_get_default_scraper_base_root`
   - `bridge_read_scraper_workspace`
   - `bridge_run_scraper_action`
6. 已验证：
   - `cargo check --manifest-path desktop-next/src-tauri/Cargo.toml`
   - `npm run tauri:dev` 可完成启动链路（Vite + cargo run + src-tauri watch）
   - `npm run smoke:provider`
   - `npm run smoke:roles`
   - `.\.venv\Scripts\python.exe scripts/desktop_tauri_startup_smoke.py`
   - 已确认 Tauri 壳内前端切到 `tauri` provider，并完成启动期 `ping`
   - `.\.venv\Scripts\python.exe scripts/desktop_tauri_roundtrip_smoke.py`
   - 已确认 Tauri 壳内 `ping/list/read/save/preview` 全链路可跑通
   - `.\.venv\Scripts\python.exe scripts/desktop_scraper_backend_smoke.py`
   - `.\.venv\Scripts\python.exe scripts/desktop_scraper_control_smoke.py`
7. 编辑区已扩展为“结构化表单 + 原始元数据标签页”：
   - `Profile`
   - `TITI`
   - `XMP`
   - `EXIF`
   - `IPTC`
   - `Match`
8. 已补目录级角色工作流：
   - 原角色名筛选
   - 扮演角色名筛选
   - 勾选集维护
   - 批量设置 / 追加 / 替换 / 清空角色元数据
9. 已补目录角色摘要索引 / 缓存，以及批量执行进度、跳过统计和失败项反馈
10. 已补统一 release gate，并已验证 `tauri:build:debug` 调试构建产物
11. 当前 Python bridge 已收窄为元数据读写职责，目录列表和图片预览已由 Vite / Tauri 原生承接
12. 新版公共抓取工作台已接入：任务列表、任务概览、进度表、日志尾部和已有任务控制

## 本地运行

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite\desktop-next
npm install
npm run dev
```

启动后，开发态页面默认通过 Vite dev server 内的 `/api/bridge/*` 中间件承接目录列表、图片预览、抓取任务监控和已有任务控制，并调用本地 `scripts/desktop_metadata_backend.py` 与 `scripts/desktop_scraper_backend.py` 完成运行时读取，不依赖 Tauri runtime。

如果要起 Tauri 壳：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite\desktop-next
npm run tauri:dev
```

此时前端会优先切换到 `tauri` provider，调用 `src-tauri` 中的命令桥接。

## 进入 Tauri 之前的前置依赖

当前机器状态：`node/npm` 可用，`cargo` 可用。

进入 Tauri 2 需要先安装 Rust 工具链（Windows）：

1. 安装 Rustup（含 cargo）。
2. 安装 Visual Studio C++ Build Tools（Desktop development with C++）。
3. 验证：`cargo -V` 可执行。

随后执行（下一轮）：

1. 已在 `desktop-next` 初始化 `src-tauri`（Tauri 2）。
2. 已将现有开发态 bridge 命令映射到 Tauri `invoke`。
3. 当前已完成交付与切换准备：统一 gate、调试构建路径和切换边界都已固定。

## 一键脚本

已提供引导脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_tauri2.ps1
```

功能：

1. 检查 `node/npm`。
2. 检查 `cargo`（缺失则提示安装命令）。
3. 安装 `@tauri-apps/cli`。
4. 初始化 `src-tauri`（若不存在）。

## 当前边界

1. 当前已具备 React 开发态工作台和已初始化的 Tauri 壳，且 `tauri:dev` 启动链路、`tauri` provider 切换、完整 roundtrip 都已验证。
2. 当前目录级角色工作流已可用，但仍建立在 Python bridge + 本地文件读写之上。
3. 真实桥接在 Tauri 模式下仍依赖本地仓库中的 Python CLI 与 `.venv`。
4. 当前下一阶段的重点不再是打包准备，而是决定先继续迁移抓取新任务启动表单 / 复核台，还是先替换剩余的 Python metadata backend。
