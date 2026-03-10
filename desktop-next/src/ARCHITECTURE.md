# Desktop Next 长期结构说明

本目录从现在开始不再按“过渡前端”扩写，而是按新版主工程组织。

## 目标结构

1. `app/`
   - 应用壳
   - 顶层布局
   - 页面装配
   - 页面级协调 hook
2. `features/`
   - `library`
   - `metadata`
   - `roles`
   - `batch`
   - `scraper`
3. `domain/`
   - 元数据模型
   - 角色模型
   - 筛选与查询模型
   - 编辑命令与校验规则
4. `infrastructure/`
   - bridge 适配
   - 运行时偏好
   - 文件访问
   - 预览 URL
   - 后续索引 / 缓存 / sidecar 调用
5. `shared/`
   - 通用组件
   - 样式 token
   - 工具函数

## 当前状态

当前已经完成的拆分：

1. `App.tsx` 已变成薄入口
2. `DesktopWorkspace.tsx` 当前承担顶层模式切换壳
3. 图片元数据工作台已收进 `app/MetadataWorkspace.tsx + app/useDesktopWorkspace.ts`
4. `preview pane` 已拆到 `features/preview`
5. `library list` 已拆到 `features/library`
6. `metadata editor` 已拆到 `features/metadata/components/MetadataEditorPane.tsx`
7. `raw metadata inspector` 已拆到 `features/metadata/components/MetadataInspectorPane.tsx`
8. 元数据表单、角色规则、批量规则、格式化 helper 已下沉到 `domain/metadata`
9. bridge provider、smoke roundtrip、目录偏好读写已收进 `infrastructure/`
10. 目录角色摘要索引 / 缓存已收进 `infrastructure/cache`
11. 目录级角色筛选、勾选和批量编辑已补匹配条件、进度、跳过 / 失败反馈
12. 公共抓取工作台已收进 `app/ScraperWorkspace.tsx + app/useScraperWorkspace.ts`
13. `features/scraper` 已承接任务列表、进度表、日志尾部与已有任务控制面
14. `services/desktop_scraper_backend_service.py + scripts/desktop_scraper_backend.py` 已承接抓取目录扫描、监控 snapshot 与 `pause / continue / retry / rewrite`

## 下一步拆分顺序

1. 下一步不再补已有任务控制，而是决定是否继续迁移“新任务启动表单 + 任务配置”
2. 如果继续迁移抓取台，优先把 `start` 表单和复核 / 审计队列暴露给 `features/scraper`
3. 如果不继续迁抓取台，则回到 Python metadata backend 替换和正式发布链路
