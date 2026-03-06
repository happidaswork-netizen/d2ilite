# Phase 0 重构状态

最后更新：`2026-03-06`  
分支：`feature/tauri-modernization`

## 结论

`Phase 0` 已收口。

当前 Python 工程已经基本完成“`UI 层 / 领域服务层 / I/O 工具层`”分层，`app.py` 仍保留 Tk 界面、事件绑定和少量 UI 协调代码，符合该阶段目标。下一步应转入 `Phase 1`，不建议继续在 `Phase 0` 深挖小块 helper。

## 已完成项

1. 服务层已形成稳定边界：
   - `services/image_service.py`
   - `services/metadata_service.py`
   - `services/editor_text_service.py`
   - `services/viewer_load_service.py`
   - `services/runtime_service.py`
   - `services/settings_service.py`
   - `services/scraper_monitor_service.py`
   - `services/task_service.py`
   - `services/task_orchestration_service.py`
   - `services/public_scraper_config_service.py`

2. `app.py` 中已完成服务化接入的核心链路：
   - 图片扫描、预览与基础信息读取
   - URL/文本/Profile 规范化
   - XMP/EXIF/IPTC 读写兜底
   - 抓取监控日志解析、JSONL 读写、状态文案映射
   - 抓取进度表视图模型、选择联动、右键菜单、错误筛选
   - 任务列表汇总、状态推导、活动任务切换
   - 任务生命周期编排：`start / continue / retry / poll / exit / app close`
   - 公共抓取启动配置：模板读取、表单默认值、runtime config 生成与覆写
   - 失败详情重试清理链：JSONL 清理、URL 索引回收、反馈文案
   - 审核缺字段判定与 `review_queue.jsonl` 同步

3. 契约冒烟已覆盖当前拆出的主链路：
   - `scripts/phase0_contract_smoke.py`
   - `scripts/bridge_cli_smoke.py`

## 当前边界

1. `app.py` 仍保留 Tk 控件构建、窗口布局、事件绑定、弹框与线程调度。
2. 公共抓取面板中的按钮行为、窗口切换、Treeview 实例读写仍属于 UI 协调层。
3. 这部分保留在 `app.py` 是刻意的，不再属于 `Phase 0` 要继续下沉的范围。

## 验证结果

1. `python -m py_compile app.py services/task_service.py services/task_orchestration_service.py services/scraper_monitor_service.py services/public_scraper_config_service.py scripts/phase0_contract_smoke.py` 通过。
2. `python scripts/phase0_contract_smoke.py` 通过，当前 `28 tests`。
3. `python scripts/bridge_cli_smoke.py` 通过。

## 阶段产出

1. Python 侧已具备面向新前端的较清晰服务边界。
2. 现有 Tk 界面仍可继续独立使用，未要求一次性切换。
3. 继续推进时，应优先消费现有 service，而不是再回到 `app.py` 做深层拆分。

## 下一步

1. 进入 `Phase 1`：以 `desktop-next/` 为主线，开始把现有 service 边界接到 `Tauri + React + TypeScript`。
2. Python 侧只做配套性补充，不再把 `Phase 0` 作为主工作面。
