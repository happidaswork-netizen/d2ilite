# Desktop Next 长期结构说明

本目录从现在开始不再按“过渡前端”扩写，而是按新版主工程组织。

## 目标结构

1. `app/`
   - 应用壳
   - 顶层布局
   - 全局状态入口
   - 页面装配
2. `features/`
   - `library`
   - `metadata`
   - `roles`
   - `batch`
   - `review`
3. `domain/`
   - 元数据模型
   - 角色模型
   - 筛选与查询模型
   - 编辑命令与校验规则
4. `infrastructure/`
   - bridge 适配
   - 文件访问
   - 预览 URL
   - 后续索引 / 缓存 / sidecar 调用
5. `shared/`
   - 通用组件
   - hooks
   - 样式 token
   - 工具函数

## 当前状态

当前已经完成的拆分：

1. `App.tsx` 已变成薄入口
2. 工作台状态已收进 `app/DesktopWorkspace.tsx`
3. `preview pane` 已拆到 `features/preview`
4. `library list` 已拆到 `features/library`
5. `metadata editor` 已拆到 `features/metadata/components/MetadataEditorPane.tsx`
6. `raw metadata inspector` 已拆到 `features/metadata/components/MetadataInspectorPane.tsx`
7. `metadata` 的本地模型与格式化 helper 已收进 `features/metadata/model.ts`
8. 单图角色元数据结构化编辑已收进 `features/metadata`
9. 目录级角色筛选、勾选和批量编辑已补到 `features/library + features/batch`
10. startup smoke hooks 仍在 `DesktopWorkspace`

## 下一步拆分顺序

1. 下一步把类型、格式化、校验规则从 `features/metadata/model.ts` 下沉到 `domain`
2. 再把 bridge / preview / 本地 I/O 收敛到 `infrastructure`
3. 最后把 `DesktopWorkspace` 继续压薄成纯装配层
