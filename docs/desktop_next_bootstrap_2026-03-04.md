# Desktop Next 启动说明（Phase 1 开发态闭环）

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

## 本地运行

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite\desktop-next
npm install
npm run dev
```

启动后，开发态页面默认通过 Vite dev server 内的 `/api/bridge/*` 中间件调用本地 `scripts/desktop_bridge_cli.py`，不依赖 Tauri runtime。

## 进入 Tauri 之前的前置依赖

当前机器状态：`node/npm` 可用，`cargo` 未安装。

进入 Tauri 2 需要先安装 Rust 工具链（Windows）：

1. 安装 Rustup（含 cargo）。
2. 安装 Visual Studio C++ Build Tools（Desktop development with C++）。
3. 验证：`cargo -V` 可执行。

随后执行（下一轮）：

1. 在 `desktop-next` 初始化 `src-tauri`（Tauri 2）。
2. 将现有开发态 bridge 命令映射到 Tauri `invoke`。
3. 通过 Tauri 壳运行当前 React 工作台。

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

1. 当前已验证的是 React 开发态工作台，不是完整 Tauri 应用。
2. 真实桥接已可访问本地 Python 服务层，但仍通过开发服务器中转。
3. 下一阶段的重点不是继续改 Tk，而是把这套桥接接入 `src-tauri`。
