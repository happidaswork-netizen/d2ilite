# Phase 0 重构状态（2026-03-04）

分支：`feature/tauri-modernization`

## 已完成

1. 建立 `services/` 目录并完成第一批服务下沉：
   - `services/image_service.py`
   - `services/metadata_service.py`
   - `services/scraper_monitor_service.py`
   - `services/editor_text_service.py`
   - `services/viewer_load_service.py`
   - `services/runtime_service.py`
   - `services/settings_service.py`
   - `services/task_service.py`（任务编排核心逻辑）

2. `app.py` 已完成对以下能力的服务化接入：
   - 图片列表扫描与基础信息读取
   - URL 归一化 / 关键词解析
   - 原始 XMP/EXIF/IPTC 读写兜底
   - 抓取监控日志解析、JSONL 读写、原因文案归一化
   - 编辑器文本与 profile 规范化、LLM JSON 提取
   - 主界面加载流程（预览 + 元数据快照）服务化
   - 结构化保存 payload 组装服务化

3. 新增契约冒烟脚本：
   - `scripts/phase0_contract_smoke.py`
   - 执行命令：`python scripts/phase0_contract_smoke.py`（当前 13 项）

4. 新增任务服务能力并接入 `app.py`：
   - 路径与文件工具：`normalize_existing_path`、`read_json_file`、`safe_positive_int`
   - 抓取任务工具：`estimate_scraper_total_target`、`retry_requires_crawl_phase`
   - 任务发现与状态：`discover_public_task_roots`、`count_latest_metadata_status`、`derive_public_task_status`
   - 模板状态管理：模板目录、模板状态读写、模板列表排序
   - 运行时工具：`normalize_public_task_root`、`is_process_running`、`public_scraper_pause_flag_path`

## 验证结果

1. `python -m py_compile app.py` 通过。
2. `python -m py_compile services/*.py`（逐文件）通过。
3. `python scripts/phase0_contract_smoke.py` 通过（13 项）。
4. `python -c "import app; print('ok')"` 通过。

## 当前边界

1. `app.py` 仍保留 UI 控制流和事件绑定（符合 Phase 0 目标）。
2. 抓取面板中的 UI 事件编排（按钮行为、窗口切换）仍主要在 `app.py`。
3. 抓取进度行组装与树表渲染相关逻辑仍在 `app.py`，下一轮继续下沉。

## 下一阶段建议（Phase 0 收尾）

1. 新增 `services/task_orchestration_service.py`，承接抓取任务状态流转与汇总逻辑。
2. 把 `_collect_scraper_progress_rows` 的纯计算部分进一步下沉。
3. 为抓取任务状态增加第二套契约测试（JSONL 样本输入 -> UI 视图模型输出）。
