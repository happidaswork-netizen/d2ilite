# D2I Lite 跨电脑接力文档（2026-03-06）

适用分支：`feature/tauri-modernization`

## 1. 当前结论

1. `Phase 0` 已完成，Python 工程已完成主服务分层。
2. `Phase 1` 已完成第一个可交付阶段：`desktop-next` 在开发模式下已接入真实 Python bridge，不再只是 mock UI。
3. `desktop-next/src-tauri` 已初始化，并已接入最小 Tauri bridge 命令骨架。
4. `npm run tauri:dev` 启动链路已验证可起。
5. 已补基础 smoke：provider 选择 + `tauri:dev` 启动链路。
6. 已确认 Tauri 壳内前端切到 `tauri` provider，并完成启动期 `ping`。
7. 已确认 Tauri 壳内 `ping/list/read/save/preview` 端到端 roundtrip 可跑通。
8. `desktop-next` 编辑区已扩展为“结构化表单 + 原始元数据标签页”，可直接查看 `Profile / TITI / XMP / EXIF / IPTC / Match`。
9. 当前路线已切到“旧版冻结为规格基线，新版独立重构”。
10. 当前最值得继续的方向，不是再深挖 `app.py`，而是直接把 `desktop-next` 整理成新版主工程。
11. `desktop-next` 已支持单图角色元数据结构化编辑：图片原角色名 + 扮演角色名列表。
12. `desktop-next` 已支持目录级角色工作流：原角色名 / 扮演角色名筛选、勾选集维护、批量设置 / 追加 / 替换 / 清空角色元数据。
13. 当前已补“稳定基线”文档，可直接作为下一阶段架构收敛的起点。
14. 目录角色摘要索引 / 缓存与批量执行反馈已完成第一轮强化，下一阶段应直接进入交付与切换准备。

## 2. 本轮累计完成内容

### 2.1 Python 侧 `Phase 0` 收口

已完成的核心下沉范围：

1. 抓取进度表 ViewModel、选择联动、错误筛选、右键菜单 plan。
2. 失败详情批量重试的 URL 归一化、JSONL 清理、图片 URL 索引回收、反馈文案。
3. 抓取任务生命周期编排：
   - `start`
   - `continue`
   - `retry`
   - `poll`
   - `exit`
   - `app close`
4. 公共抓取启动前配置：
   - 模板读取
   - 默认值推导
   - setup dialog submit plan
   - runtime config 覆写
5. 审核缺字段判定与 `review_queue.jsonl` 同步。
6. `app.py` 清理掉最后一批低价值 wrapper，保留 UI 协调层。

### 2.2 `desktop-next` 开发态最小闭环

已完成的前端与桥接工作：

1. 重写 `desktop-next` 工作台界面，形成“固定操作栏 + 左侧列表/预览 + 右侧元数据编辑”的可用布局。
2. 新增开发态 bridge provider：`vite-python-cli`。
3. 在 `vite.config.ts` 中加入 `/api/bridge/*` 中间件，直接转发到 `scripts/desktop_bridge_cli.py`。
4. 已支持：
   - `ping`
   - `list`
   - `read`
   - `save`
   - `preview`
5. 前端已支持：
   - 本地目录加载
   - 图片列表筛选
   - 图片预览
   - 元数据读取
   - 元数据保存
   - 未保存修改拦截
   - 当前文件重载
6. `src-tauri` 已具备最小命令桥接：
   - `bridge_ping`
   - `bridge_list_images`
   - `bridge_read_metadata`
   - `bridge_save_metadata`
7. 编辑区已支持：
   - 结构化摘要卡片（状态 / 文件大小 / 修改时间 / 关键词数）
   - `Profile`
   - `TITI`
   - `XMP`
   - `EXIF`
   - `IPTC`
   - `Match`
8. 角色元数据已支持：
   - 图片原角色名（对应 `d2i_profile.name`）
   - 扮演角色名列表（对应 `role_aliases[]`）
   - 备注
   - 启用/停用
9. 目录级角色工作流已支持：
   - 原角色名筛选
   - 扮演角色名筛选
   - 当前筛选结果勾选
   - 对勾选项或筛选结果批量设置 / 追加 / 替换 / 清空
10. 目录性能与批量强化已支持：
   - 目录角色摘要索引 / 缓存
   - 批量匹配条件（所有目标项 / 原角色为空 / 扮演角色为空 / 任一为空）
   - 批量进度、跳过统计、失败项反馈

## 3. 本次修改文件清单

### 3.1 Python / 服务层

1. `app.py`
2. `services/task_service.py`
3. `services/task_orchestration_service.py`
4. `services/scraper_monitor_service.py`
5. `services/public_scraper_config_service.py`（新增）
6. `scripts/phase0_contract_smoke.py`

### 3.2 新前端 `desktop-next`

1. `desktop-next/vite.config.ts`
2. `desktop-next/src/types.ts`
3. `desktop-next/src/bridge/desktopBridge.ts`
4. `desktop-next/src/App.tsx`
5. `desktop-next/src/App.css`
6. `desktop-next/src-tauri/`
7. `desktop-next/README.md`

### 3.3 文档

1. `docs/phase0_refactor_status_2026-03-04.md`
2. `docs/tauri_modernization_checklist_2026-03-04.md`
3. `docs/desktop_next_bootstrap_2026-03-04.md`
4. `docs/cross_machine_handoff_2026-03-06.md`（本文档，新增）
5. `docs/desktop_next_rewrite_baseline_2026-03-09.md`

### 3.4 其他

1. `AGENTS.md` 仅有结尾换行差异，不包含业务逻辑变化。

## 4. 当前验证状态

Python 侧：

```powershell
.\.venv\Scripts\python.exe -m py_compile app.py services\task_service.py services\task_orchestration_service.py services\scraper_monitor_service.py services\public_scraper_config_service.py scripts\phase0_contract_smoke.py
.\.venv\Scripts\python.exe scripts\phase0_contract_smoke.py
.\.venv\Scripts\python.exe scripts\bridge_cli_smoke.py
```

预期 / 当前结果：

1. `py_compile` 通过
2. `phase0_contract_smoke.py` 通过，当前 `28 tests`
3. `bridge_cli_smoke.py` 通过

前端侧：

```powershell
cd desktop-next
npm run lint
npm run build
```

预期 / 当前结果：

1. `lint` 通过
2. `build` 通过
3. `cargo check --manifest-path desktop-next/src-tauri/Cargo.toml` 通过
4. `npm run tauri:dev` 启动链路通过（Vite + cargo run + src-tauri watch）
5. `npm run smoke:provider` 通过
6. `npm run smoke:roles` 通过
7. `.\.venv\Scripts\python.exe scripts/desktop_tauri_startup_smoke.py` 通过
8. 已确认 Tauri 壳内前端切到 `tauri` provider，并完成启动期 `ping`
9. `.\.venv\Scripts\python.exe scripts/desktop_tauri_roundtrip_smoke.py` 通过
10. 已确认 Tauri 壳内 `ping/list/read/save/preview` roundtrip 可跑通
11. 当前 `desktop-next` 编辑区已能展示 bridge 返回的原始元数据分组，不再只有表单字段

开发态桥接已额外做过端到端检查：

1. `/api/bridge/ping` 通过
2. `/api/bridge/list` 通过
3. `/api/bridge/read` 通过
4. `/api/bridge/preview` 通过

## 5. 换电脑后最小恢复步骤

```powershell
cd d:\soft
git clone https://github.com/happidaswork-netizen/d2ilite.git
cd d2ilite
git checkout feature/tauri-modernization
git pull --ff-only
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
cd desktop-next
npm install
cd ..
```

## 6. 换电脑后建议先跑的检查

```powershell
.\.venv\Scripts\python.exe scripts\phase0_contract_smoke.py
.\.venv\Scripts\python.exe scripts\bridge_cli_smoke.py
cd desktop-next
npm run build
```

如果需要起界面：

老 UI：

```powershell
.\.venv\Scripts\python.exe app.py
```

新 UI（开发态）：

```powershell
cd desktop-next
npm run dev
```

## 7. 当前边界

1. `desktop-next` 现在已同时具备 React 开发态工作台和已初始化的 Tauri 壳。
2. Tauri 模式下的 bridge 仍依赖仓库内的 Python CLI 与 `.venv`。
3. 真实桥接当前已同时支持 Vite dev server 中转和 Tauri 自定义命令两条链路。
4. Tk 老界面仍可独立使用，并且仍是完整功能入口。

## 8. 下一步接力点

1. 直接进入交付与切换准备：固定打包路径、切换边界和最终回归矩阵。
2. 然后再决定是先替换 Python bridge，还是先推进更高阶工作台能力。
3. 不再回头扩写 Tk 侧或继续堆新的临时过渡层。
