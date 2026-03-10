import type {
  BatchExecutionProgress,
  BatchExecutionReport,
  BatchAliasMode,
  BatchMatchMode,
  BatchOriginalRoleMode,
  BatchRoleScope,
} from '../../../domain/metadata'
import { getFileName } from '../../../shared/path'

type BatchRolePanelProps = {
  aliasMode: BatchAliasMode
  aliasText: string
  busy: boolean
  canApply: boolean
  filteredCount: number
  indexedCount: number
  indexBusy: boolean
  matchMode: BatchMatchMode
  originalRoleMode: BatchOriginalRoleMode
  originalRoleName: string
  progress: BatchExecutionProgress
  report: BatchExecutionReport | null
  scope: BatchRoleScope
  selectedCount: number
  totalCount: number
  onAliasModeChange: (value: BatchAliasMode) => void
  onAliasTextChange: (value: string) => void
  onApply: () => void
  onMatchModeChange: (value: BatchMatchMode) => void
  onOriginalRoleModeChange: (value: BatchOriginalRoleMode) => void
  onOriginalRoleNameChange: (value: string) => void
  onScopeChange: (value: BatchRoleScope) => void
}

function formatProgress(progress: BatchExecutionProgress): string {
  if (!progress.active || progress.total <= 0) {
    return '等待批量操作'
  }
  return `处理中 ${progress.processed}/${progress.total} · 改写 ${progress.changed} · 跳过 ${progress.skipped} · 失败 ${progress.failed}`
}

export function BatchRolePanel({
  aliasMode,
  aliasText,
  busy,
  canApply,
  filteredCount,
  indexedCount,
  indexBusy,
  matchMode,
  originalRoleMode,
  originalRoleName,
  progress,
  report,
  scope,
  selectedCount,
  totalCount,
  onAliasModeChange,
  onAliasTextChange,
  onApply,
  onMatchModeChange,
  onOriginalRoleModeChange,
  onOriginalRoleNameChange,
  onScopeChange,
}: BatchRolePanelProps) {
  const targetCount = scope === 'selected' ? selectedCount : filteredCount
  const progressPercent = progress.total > 0 ? Math.min(100, Math.round((progress.processed / progress.total) * 100)) : 0

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
          <span>应用条件</span>
          <select value={matchMode} onChange={(event) => onMatchModeChange(event.target.value as BatchMatchMode)}>
            <option value="all">所有目标项</option>
            <option value="missing_original">仅原角色名为空</option>
            <option value="missing_alias">仅扮演角色为空</option>
            <option value="missing_any">仅任一角色信息为空</option>
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

      <div className="batch-progress-panel">
        <div className="batch-progress-head">
          <span>{formatProgress(progress)}</span>
          <span>{progressPercent}%</span>
        </div>
        <div className="batch-progress-bar" aria-hidden="true">
          <span style={{ width: `${progressPercent}%` }} />
        </div>
      </div>

      {report ? (
        <div className="batch-report">
          <div className="batch-report-head">
            <strong>最近一次批量结果</strong>
            <span>
              改写 {report.changed} · 跳过 {report.skipped} · 失败 {report.failed}
            </span>
          </div>
          {report.failures.length > 0 ? (
            <ul className="batch-report-list">
              {report.failures.slice(0, 3).map((failure) => (
                <li key={`${failure.path}:${failure.error}`}>
                  <span>{getFileName(failure.path)}</span>
                  <span>{failure.error}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="batch-report-empty">本次批量执行没有失败项。</p>
          )}
        </div>
      ) : null}
    </div>
  )
}
