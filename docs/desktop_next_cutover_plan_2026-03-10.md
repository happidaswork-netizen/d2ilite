# Desktop Next 切换与交付边界（2026-03-10）

适用仓库：`d2ilite`  
适用分支：`feature/tauri-modernization`

## 1. 当前切换结论

截至 2026-03-10，`desktop-next` 已可作为“图片元数据工作台”的新主入口使用，但不是整个 `d2ilite` 的完整替代品。

当前建议的切换边界：

1. 新版负责：
   - 图片目录浏览
   - 图片预览
   - 单图元数据读取与保存
   - 原始元数据查看
   - 单图角色结构化编辑
   - 目录级角色筛选、勾选和批量编辑
   - 公共抓取任务目录浏览、任务概览、进度表、日志尾部
   - 已有抓取任务的 `pause / continue / retry / rewrite`
2. 旧版继续负责：
   - 公共抓取新任务启动与任务配置表单
   - 复核队列与审计工作台

这意味着当前可以切换的是“图片元数据主工作流”，不是“整个桌面应用全量切换”。

当前 bridge 责任边界：

1. Vite dev / Tauri 原生负责：
   - 目录列表
   - 本地图片预览
2. Python backend 当前负责：
   - 元数据读取
   - 元数据保存
   - 抓取任务目录、监控 snapshot 与已有任务控制

当前 Python backend 入口：

`d:\soft\gemini-business2api-workspace\d2ilite\scripts\desktop_metadata_backend.py`

`d:\soft\gemini-business2api-workspace\d2ilite\scripts\desktop_scraper_backend.py`

## 2. 交付门槛

每次准备给其他机器或操作者使用前，先执行统一 gate：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite
.\.venv\Scripts\python.exe scripts\desktop_next_release_gate.py
```

Gate 当前会顺序执行：

1. Python `py_compile`
2. `phase0_contract_smoke.py`
3. `bridge_cli_smoke.py`
4. `cargo check`
5. `desktop-next npm run lint`
6. `desktop-next npm run build`
7. `desktop-next npm run smoke:provider`
8. `desktop-next npm run smoke:roles`
9. `desktop_vite_bridge_smoke.py`
10. `desktop_metadata_backend_smoke.py`
11. `desktop_scraper_backend_smoke.py`
12. `desktop_scraper_control_smoke.py`
13. `desktop_tauri_startup_smoke.py`
14. `desktop_tauri_roundtrip_smoke.py`
15. `desktop-next npm run tauri:build:debug`

通过后会生成报告：

`d:\soft\gemini-business2api-workspace\d2ilite\.tmp\desktop-next\release-gate-report.json`

## 3. 当前可验证的交付产物

当前已验证的本地桌面产物路径：

`d:\soft\gemini-business2api-workspace\d2ilite\desktop-next\src-tauri\target\debug\d2i-lite-next.exe`

对应命令：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite\desktop-next
npm run tauri:build:debug
```

说明：

1. 这是“可运行桌面壳”的调试构建，不是正式安装包。
2. 当前正式 bundle 目录仍遵循 Tauri 默认输出：
   - `desktop-next/src-tauri/target/release/bundle/nsis/`
   - `desktop-next/src-tauri/target/release/bundle/msi/`
3. 正式 installer 还没有作为当前阶段验收的一部分。

## 4. 操作建议

当前推荐操作方式：

1. 图片元数据工作流默认用 `desktop-next`
2. 抓取监控和已有任务控制优先用 `desktop-next`；新任务启动表单、复核与审计继续用旧版 `app.py`
3. 不做“单机只留新版”的激进切换

启动命令：

新版：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite\desktop-next
npm run tauri:dev
```

旧版：

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite
.\.venv\Scripts\python.exe app.py
```

## 5. 回退方式

如果新版在图片元数据工作流上出现问题，当前回退方式很简单：

1. 停用 `desktop-next`
2. 继续使用旧版 `app.py`
3. 由于当前写入语义仍由 Python 侧规则兜底，不需要单独迁移图片数据格式

## 6. 当前阶段完成度判断

如果只看“图片元数据主工作流”：

1. 结构、批量能力、Tauri 壳、回归 gate：已完成
2. 可控切换边界：已完成
3. 临时 Python bridge 的责任面已缩小，但还未被完全替换
4. 正式 installer / 抓取新任务启动表单 / 复核台迁移：未完成

更直接地说：

1. `desktop-next` 现在已经达到“可交付、可受控切换”的阶段
2. 但还没有达到“整个 d2ilite 全量替代”的阶段
