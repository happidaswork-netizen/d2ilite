# D2I Lite 跨电脑接力文档（2026-03-06）

适用分支：`feature/tauri-modernization`

## 1. 当前结论

1. `Phase 0` 已完成，Python 工程已完成主服务分层。
2. `Phase 1` 已完成第一个可交付阶段：`desktop-next` 在开发模式下已接入真实 Python bridge，不再只是 mock UI。
3. 当前最值得继续的方向，不是再深挖 `app.py`，而是初始化 `desktop-next/src-tauri`，把现有 bridge 命令映射到 Tauri `invoke`。

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
6. `desktop-next/README.md`

### 3.3 文档

1. `docs/phase0_refactor_status_2026-03-04.md`
2. `docs/tauri_modernization_checklist_2026-03-04.md`
3. `docs/desktop_next_bootstrap_2026-03-04.md`
4. `docs/cross_machine_handoff_2026-03-06.md`（本文档，新增）

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

1. `desktop-next` 现在是 React 开发态工作台，不是完整 Tauri 应用。
2. `src-tauri/` 还没有初始化。
3. 真实桥接当前通过 Vite dev server 中转到 Python CLI。
4. Tk 老界面仍可独立使用，并且仍是完整功能入口。

## 8. 下一步接力点

1. 初始化 `desktop-next/src-tauri`。
2. 把当前 `/api/bridge/*` 对应的命令映射到 Tauri `invoke`。
3. 为 `desktop-next` 增加稳定的端到端冒烟检查，固定 `ping/list/read/save/preview` 这条链路。
4. 在新前端继续扩展结构化/XMP/EXIF/IPTC 视图，而不是回头继续大拆 `app.py`。
