import type {
  BatchAliasMode,
  BatchOriginalRoleMode,
  BatchRoleScope,
} from '../../../domain/metadata'

type BatchRolePanelProps = {
  aliasMode: BatchAliasMode
  aliasText: string
  busy: boolean
  canApply: boolean
  filteredCount: number
  indexedCount: number
  indexBusy: boolean
  originalRoleMode: BatchOriginalRoleMode
  originalRoleName: string
  scope: BatchRoleScope
  selectedCount: number
  totalCount: number
  onAliasModeChange: (value: BatchAliasMode) => void
  onAliasTextChange: (value: string) => void
  onApply: () => void
  onOriginalRoleModeChange: (value: BatchOriginalRoleMode) => void
  onOriginalRoleNameChange: (value: string) => void
  onScopeChange: (value: BatchRoleScope) => void
}

export function BatchRolePanel({
  aliasMode,
  aliasText,
  busy,
  canApply,
  filteredCount,
  indexedCount,
  indexBusy,
  originalRoleMode,
  originalRoleName,
  scope,
  selectedCount,
  totalCount,
  onAliasModeChange,
  onAliasTextChange,
  onApply,
  onOriginalRoleModeChange,
  onOriginalRoleNameChange,
  onScopeChange,
}: BatchRolePanelProps) {
  const targetCount = scope === 'selected' ? selectedCount : filteredCount

  return (
    <div className="batch-panel">
      <div className="batch-head">
        <div>
          <p className="batch-title">批量角色编辑</p>
          <span className="batch-subtitle">
            当前目录已索引 {indexedCount}/{totalCount}
            {indexBusy ? '，仍在读取角色元数据' : ''}
          </span>
        </div>
        <button
          type="button"
          className="primary"
          onClick={onApply}
          disabled={busy || targetCount === 0 || !canApply}
        >
          应用到 {targetCount} 项
        </button>
      </div>

      <div className="batch-grid">
        <label className="input-stack">
          <span>作用范围</span>
          <select value={scope} onChange={(event) => onScopeChange(event.target.value as BatchRoleScope)}>
            <option value="selected">当前勾选项（{selectedCount}）</option>
            <option value="filtered">当前筛选结果（{filteredCount}）</option>
          </select>
        </label>

        <label className="input-stack">
          <span>原角色名操作</span>
          <select
            value={originalRoleMode}
            onChange={(event) => onOriginalRoleModeChange(event.target.value as BatchOriginalRoleMode)}
          >
            <option value="ignore">不修改</option>
            <option value="set">设置为</option>
            <option value="clear">清空</option>
          </select>
        </label>

        <label className="input-stack batch-span-2">
          <span>图片原角色名</span>
          <input
            value={originalRoleName}
            onChange={(event) => onOriginalRoleNameChange(event.target.value)}
            placeholder="仅在“设置为”时生效"
            disabled={originalRoleMode !== 'set'}
          />
        </label>

        <label className="input-stack">
          <span>扮演角色操作</span>
          <select value={aliasMode} onChange={(event) => onAliasModeChange(event.target.value as BatchAliasMode)}>
            <option value="ignore">不修改</option>
            <option value="append">追加</option>
            <option value="replace">替换为</option>
            <option value="clear">清空</option>
          </select>
        </label>

        <label className="input-stack batch-span-2">
          <span>扮演角色名列表</span>
          <input
            value={aliasText}
            onChange={(event) => onAliasTextChange(event.target.value)}
            placeholder="用逗号分隔多个角色名；追加/替换时生效"
            disabled={aliasMode !== 'append' && aliasMode !== 'replace'}
          />
        </label>
      </div>
    </div>
  )
}
