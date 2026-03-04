# D2I Lite 跨电脑接力文档（2026-03-04）

适用分支：`feature/tauri-modernization`

## 1. 当前进度快照

最近核心提交（按时间倒序）：

1. `76644f2` `refactor: move public task summary and sorting into service`
2. `833628d` `refactor: move scraper row status helpers to task service`
3. `888307b` `refactor: extract scraper progress aggregation service`
4. `d4d2840` `refactor: extract jsonl count cache into task service`
5. `2d26b6b` `refactor: move scraper task utilities into task service`
6. `a7a1f16` `feat: add desktop bridge protocol and wire desktop-next workflow`
7. `bd7ff9d` `feat: bootstrap desktop-next react workspace`

当前结论：

1. `app.py` 已显著减薄，任务统计/模板状态/进度聚合等纯逻辑已经下沉到 `services/task_service.py`。
2. 任务“继续/重试/重写元数据”的参数编排已独立到 `services/task_orchestration_service.py`。
3. `desktop-next` 已可通过 `scripts/desktop_bridge_cli.py` 读写图片元数据（桥接协议已稳定）。
4. 代码可在当前分支直接继续迭代，不需要从旧版本重新开始。

## 2. 跨电脑继续开发（最小步骤）

在新电脑上执行：

```powershell
cd d:\bugemini
git clone https://github.com/happidaswork-netizen/d2ilite.git
cd d2ilite
git checkout feature/tauri-modernization
git pull --ff-only
```

安装 Python 依赖：

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

安装前端依赖（仅新界面需要）：

```powershell
cd desktop-next
npm install
cd ..
```

## 3. 一键自检（先跑再改）

```powershell
python -m py_compile app.py services\task_service.py scripts\phase0_contract_smoke.py
python scripts\phase0_contract_smoke.py
python scripts\bridge_cli_smoke.py
```

预期：

1. `phase0_contract_smoke` 通过（当前为 `23 tests`）。
2. `bridge_cli_smoke` 通过。

## 4. 运行方式

老 UI（Tk）：

```powershell
.\.venv\Scripts\python app.py
```

新 UI（React，开发态）：

```powershell
cd desktop-next
npm run dev
```

桥接手动检查：

```powershell
python scripts\desktop_bridge_cli.py ping
```

## 5. 继续迭代建议（接力点）

1. 把抓取进度表选择联动/错误筛选的 UI 逻辑继续下沉为 ViewModel 纯函数。
2. 为任务管理窗口补第二套契约测试（目录样本 -> 任务列表行模型）。
3. 在 `desktop-next` 对接任务列表与进度表接口，为后续 Tauri2 UI 迁移做准备。

## 6. 说明

1. 本仓库当前存在未跟踪运行目录（如 `d2ilite/`、`data/`），不属于代码提交内容。
2. 提交时请仅 `git add` 需要跟踪的源码/文档文件，避免把运行时数据误推送。
