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
   - `services/task_service.py`（骨架）

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
   - 执行命令：`python scripts/phase0_contract_smoke.py`（当前 9 项）

## 验证结果

1. `python -m py_compile app.py` 通过。
2. `python -m py_compile services/*.py`（逐文件）通过。
3. `python scripts/phase0_contract_smoke.py` 通过（8 项）。
4. `python -c "import app; print('ok')"` 通过。

## 当前边界

1. `app.py` 仍保留 UI 控制流和事件绑定（符合 Phase 0 目标）。
2. 抓取面板中的流程编排（任务生命周期控制）仍主要在 `app.py`。
3. `task_service.py` 当前仅提供状态模型，下一轮扩展为任务编排服务。

## 下一阶段建议（Phase 0 收尾）

1. 新增 `services/task_orchestration_service.py`，承接抓取任务状态流转与汇总逻辑。
2. 把 `_collect_scraper_progress_rows` 的纯计算部分进一步下沉。
3. 为抓取任务状态增加第二套契约测试（JSONL 样本输入 -> UI 视图模型输出）。
