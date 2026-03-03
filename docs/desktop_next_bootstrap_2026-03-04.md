# Desktop Next 启动说明（Phase 1 起步）

目录：`desktop-next/`

## 当前状态

1. 已创建 `React + TypeScript + Vite` 前端骨架。
2. 已替换默认模板为 D2I Lite Next 工作台布局（固定操作栏 + 双栏主区 + 局部滚动）。
3. 已验证：
   - `npm run lint`
   - `npm run build`

## 本地运行

```powershell
cd d:\bugemini\d2ilite\desktop-next
npm install
npm run dev
```

## 进入 Tauri 之前的前置依赖

当前机器状态：`node/npm` 可用，`cargo` 未安装。

进入 Tauri 2 需要先安装 Rust 工具链（Windows）：

1. 安装 Rustup（含 cargo）。
2. 安装 Visual Studio C++ Build Tools（Desktop development with C++）。
3. 验证：`cargo -V` 可执行。

随后执行（下一轮）：

1. 在 `desktop-next` 初始化 `src-tauri`（Tauri 2）。
2. 将前端页面通过 Tauri 壳运行。
3. 接入 Python 服务层桥接（先走命令行/HTTP，本地回环）。

