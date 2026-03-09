# Desktop Next 重构稳定基线（2026-03-09）

适用仓库：`d2ilite`  
适用分支：`feature/tauri-modernization`

## 1. 基线结论

截至 2026-03-09，`desktop-next` 已形成一个可继续扩展的新版本稳定基线。

已经完成的四个大块：

1. 结构重整：`App.tsx -> DesktopWorkspace -> feature panes`
2. 单图角色元数据结构化编辑
3. 目录级角色筛选、勾选与批量编辑工作流
4. 回归矩阵、文档、长期边界整理

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
7. `src-tauri` 已可承接最小桌面壳运行
8. Tauri 壳内 `ping/list/read/save/preview` 已通过端到端 roundtrip

## 3. 当前结构边界

当前推荐把现状理解为：

1. `app/`
   - 页面装配和工作台级状态
2. `features/`
   - `preview`
   - `library`
   - `metadata`
   - `batch`
3. `services/` 与 Python 写入链路
   - 作为当前字段语义和规则基线
4. `src-tauri/`
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
.\.venv\Scripts\python.exe -m py_compile app.py services\metadata_service.py metadata_manager.py metadata_writer.py scripts\desktop_bridge_cli.py scripts\desktop_tauri_startup_smoke.py scripts\desktop_tauri_roundtrip_smoke.py
.\.venv\Scripts\python.exe scripts\phase0_contract_smoke.py
.\.venv\Scripts\python.exe scripts\bridge_cli_smoke.py
cargo check --manifest-path desktop-next/src-tauri/Cargo.toml
cd desktop-next
npm run lint
npm run build
npm run smoke:provider
npm run smoke:roles
cd ..
.\.venv\Scripts\python.exe scripts\desktop_tauri_startup_smoke.py
.\.venv\Scripts\python.exe scripts\desktop_tauri_roundtrip_smoke.py
```

补充说明：

1. `desktop_tauri_startup_smoke.py` 和 `desktop_tauri_roundtrip_smoke.py` 需要串行运行。
2. 两者并行会互抢 `1420` 端口和 `cargo` 锁。

## 5. 下一阶段不要再做的事

1. 不再回头深挖 Tk 侧小 wrapper。
2. 不再把 Python bridge 当长期正式接口继续扩写。
3. 不再把更多临时对接层塞进 `DesktopWorkspace`。

## 6. 下一阶段该做的事

下一阶段从这里接：

1. 把 `features/metadata/model.ts` 中的模型、格式化、批量规则继续下沉到 `domain/`
2. 把 bridge / preview / 本地 I/O 从 `workspace` 与 feature 中抽到 `infrastructure/`
3. 继续压薄 `DesktopWorkspace`，让它只做装配和跨 feature 协调
4. 再决定是否开始替换 Python bridge 或引入目录索引 / 缓存层

## 7. 当前完成度判断

如果只看新版主工程：

1. 可继续开发的稳定基线：已完成
2. 长期结构收敛：刚开始
3. 更高阶目录性能与批量能力：未开始
4. 替换临时 bridge：未开始

更直接的判断：

1. `desktop-next` 的“能用且可继续扩展的基础盘”已经建立
2. 下一阶段不再是补闭环，而是做长期架构收敛
