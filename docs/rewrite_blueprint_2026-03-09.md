# D2I Lite 全新重构蓝图（旧版冻结，新版独立实现）

更新时间：2026-03-09  
适用仓库：`d2ilite`

## 1. 决策

从本日开始，`d2ilite` 不再继续沿“Tk -> Python bridge -> Tauri shell”做长期渐进迁移，而是切换为：

1. 旧版冻结为可运行规格基线。
2. 新版作为独立产品重构。
3. 中间过渡层只保留为参考与短期校验工具，不再作为最终架构目标。

这意味着：

1. 旧 Tk 界面不再作为未来主线继续演进。
2. 现有 `desktop-next` 不再只是迁移壳，而是直接转成新版主产品入口。
3. 现有 Python bridge、smoke、契约脚本保留，但角色变为“规格校验 / 回归工具”，不是最终依赖边界。

## 2. 旧版保留什么

旧版保留内容：

1. `app.py` 与现有 Tk 工作流，作为行为参考。
2. `services/` 分层后的 Python 逻辑，作为业务规则来源。
3. `scripts/phase0_contract_smoke.py`
4. `scripts/bridge_cli_smoke.py`
5. 样本图片、任务目录、现有文档和字段定义。

旧版不再继续投入的方向：

1. 不再继续深挖 `app.py` 残余小 wrapper。
2. 不再继续把 Python CLI bridge 当长期正式接口扩写。
3. 不再为了“平滑迁移”继续建设更多临时中转层。

## 3. 新版目标

新版主目标：

1. 直接构建新的桌面主程序，而不是一层层包裹旧界面。
2. 把“图片浏览 + 元数据编辑 + 批量工作流 + 角色元数据 + 后续抓取复核”统一到一个新 UI 中。
3. 让新版本的目录结构、状态管理、领域模型从一开始就按长期可维护方式组织。

新版第一阶段只覆盖高价值主路径：

1. 目录加载
2. 单图浏览
3. 单图元数据读取
4. 单图元数据保存
5. 原始元数据查看
6. 基础批量操作

新版第二阶段继续补：

1. 角色元数据结构化编辑
2. 批量角色编辑
3. 目录筛选、查询、分组
4. 更完整的元数据工作流

新版第三阶段再评估：

1. 抓取与复核工作台是否并入新版
2. Rust 目录索引 / 缓存 / 性能核心是否提前落地
3. 打包与发布路径

## 4. 新版架构原则

1. `desktop-next` 直接作为新版主工程，不再视为“过渡前端”。
2. 业务模型先行，UI 只是领域对象的消费端。
3. 视图、状态、领域规则、I/O 适配分层明确。
4. 所有新能力优先在新版实现，不再默认 Tk 与新版双写。
5. 回归以样本图、契约脚本、端到端 smoke 为准，而不是以旧代码复用率为目标。

## 5. 新版最小架构边界

建议以 `desktop-next` 作为主工作区，逐步形成四层：

1. `app`
   - 路由、壳层、全局布局、应用级状态
2. `features`
   - `library`
   - `metadata`
   - `roles`
   - `batch`
   - `review`
3. `domain`
   - 元数据模型
   - 角色模型
   - 目录与筛选模型
   - 编辑命令与校验规则
4. `infrastructure`
   - 文件访问
   - 图片预览
   - 元数据读写
   - 后续索引 / 缓存 / sidecar 接口

## 6. 当前能直接复用的资产

可直接复用：

1. `desktop-next` 的现有 UI 壳、列表、预览、编辑器
2. `desktop-next/src-tauri`
3. Tauri smoke 脚本
4. Python 侧元数据字段定义与读写规则
5. `role_aliases`、`d2i_profile` 等现有字段语义

需要逐步替换或降级为参考的资产：

1. `scripts/desktop_bridge_cli.py`
2. `vite.config.ts` 里的开发态 Python bridge 路由
3. 任何仅为迁移过渡存在的 adapter

## 7. 执行顺序

### Stage A：冻结与清线

1. 冻结旧 Tk 线，不再继续大改。
2. 整理并提交当前 `desktop-next + src-tauri + smoke` 成果。
3. 明确 `desktop-next` 为新版主入口。

### Stage B：新版 UI 主路径

1. 重整 `desktop-next/src/` 目录结构。
2. 把当前单体 `App.tsx` 切成：
   - library pane
   - preview pane
   - metadata editor
   - raw metadata inspector
3. 建立稳定的页面级状态与领域模型。

### Stage C：角色与批量工作流

1. 角色元数据结构化编辑器
2. 批量角色写入 / 清空 / 替换 / 追加
3. 按原角色名 / 扮演角色名筛选

### Stage D：底层替换

1. 评估并逐步替换 Python CLI bridge
2. 引入更稳定的本地读写接口
3. 评估 Rust 索引 / 缓存层

## 8. 新版验收方式

每个阶段都必须满足：

1. 可运行
2. 可回归
3. 可替代旧版的一个清晰主路径

当前固定回归基线：

1. `.\.venv\Scripts\python.exe scripts\phase0_contract_smoke.py`
2. `.\.venv\Scripts\python.exe scripts\bridge_cli_smoke.py`
3. `cd desktop-next && npm run lint`
4. `cd desktop-next && npm run build`
5. `cd desktop-next && npm run smoke:provider`
6. `cd desktop-next && npm run smoke:roles`
7. `.\.venv\Scripts\python.exe scripts\desktop_tauri_startup_smoke.py`
8. `.\.venv\Scripts\python.exe scripts\desktop_tauri_roundtrip_smoke.py`

## 9. 当前完成度判断

如果按“渐进迁移”路线算，当前整体约 `35%~40%`。  
如果按“新版独立重构”路线算，当前更准确地应理解为：

1. 规格基线与验证资产：已就绪
2. 新版主工程骨架：已就绪
3. 新版正式产品结构：已完成第一轮拆分、稳定基线整理和 `domain / infrastructure` 第一轮收敛

也就是说，真正的“新版本重构工程”现在才算进入可正式开工阶段。

## 10. 下一步

立即执行顺序：

1. 先基于当前稳定基线整理并提交工作区成果。
2. 然后进入目录性能与批量工作流强化。
3. 再评估目录索引 / 缓存层与替换临时 bridge 的时机。
