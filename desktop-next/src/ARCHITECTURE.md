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
   - `review`
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
2. `DesktopWorkspace.tsx` 已压薄为装配层
3. 工作台状态与交互 orchestration 已收进 `app/useDesktopWorkspace.ts`
4. `preview pane` 已拆到 `features/preview`
5. `library list` 已拆到 `features/library`
6. `metadata editor` 已拆到 `features/metadata/components/MetadataEditorPane.tsx`
7. `raw metadata inspector` 已拆到 `features/metadata/components/MetadataInspectorPane.tsx`
8. 元数据表单、角色规则、批量规则、格式化 helper 已下沉到 `domain/metadata`
9. bridge provider、smoke roundtrip、目录偏好读写已收进 `infrastructure/`
10. 目录角色摘要索引 / 缓存已收进 `infrastructure/cache`
11. 目录级角色筛选、勾选和批量编辑已补匹配条件、进度、跳过 / 失败反馈
12. 目录级角色工作流仍由 `app/useDesktopWorkspace.ts` 统一协调

## 下一步拆分顺序

1. 下一步进入交付与切换准备
2. 固化打包路径、切换边界和回归矩阵
3. 然后再决定是先替换 Python bridge，还是先推进更高阶工作台能力
