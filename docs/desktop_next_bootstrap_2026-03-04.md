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
4. 已验证：
   - `npm run lint`
   - `npm run build`
   - 开发态 `ping/list/read/preview` 端到端联通
5. `src-tauri/` 已初始化，并已接入最小 Tauri bridge 命令：
   - `bridge_ping`
   - `bridge_list_images`
   - `bridge_read_metadata`
   - `bridge_save_metadata`
6. 已验证：
   - `cargo check --manifest-path desktop-next/src-tauri/Cargo.toml`
   - `npm run tauri:dev` 可完成启动链路（Vite + cargo run + src-tauri watch）
   - `npm run smoke:provider`
   - `npm run smoke:roles`
   - `.\.venv\Scripts\python.exe scripts/desktop_tauri_startup_smoke.py`
   - 已确认 Tauri 壳内前端切到 `tauri` provider，并完成启动期 `ping`
   - `.\.venv\Scripts\python.exe scripts/desktop_tauri_roundtrip_smoke.py`
   - 已确认 Tauri 壳内 `ping/list/read/save/preview` 全链路可跑通
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

## 本地运行

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite\desktop-next
npm install
npm run dev
```

启动后，开发态页面默认通过 Vite dev server 内的 `/api/bridge/*` 中间件调用本地 `scripts/desktop_bridge_cli.py`，不依赖 Tauri runtime。

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
3. 下一步不是继续补最小读写链路，而是收敛长期 `domain/infrastructure` 边界，并逐步引入更完整的目录索引 / 缓存能力。

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
4. 当前下一阶段的重点是整理稳定基线、收敛长期 `domain/infrastructure` 边界，并评估更完整的目录索引 / 缓存能力。
