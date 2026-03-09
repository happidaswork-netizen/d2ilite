import type { RoleMetadataSummary } from '../../metadata/model'

type LibraryPaneProps = {
  aliasRoleFilter: string
  busy: boolean
  filterText: string
  filteredItems: string[]
  indexedCount: number
  indexBusy: boolean
  items: string[]
  originalRoleFilter: string
  roleSummaryByPath: Record<string, RoleMetadataSummary>
  selectedPath: string
  selectedPaths: string[]
  onAliasRoleFilterChange: (value: string) => void
  onClearSelection: () => void
  onFilterTextChange: (value: string) => void
  onOpenItem: (path: string) => void
  onOriginalRoleFilterChange: (value: string) => void
  onSelectFiltered: () => void
  onToggleSelection: (path: string) => void
  getFileName: (path: string) => string
}

function renderRoleSummary(summary: RoleMetadataSummary | undefined, indexBusy: boolean): string {
  if (!summary) {
    return indexBusy ? '角色信息索引中...' : '未读取角色信息'
  }

  const parts: string[] = []
  if (summary.originalRoleName) {
    parts.push(`原角色：${summary.originalRoleName}`)
  }
  if (summary.roleAliasNames.length > 0) {
    parts.push(`扮演：${summary.roleAliasNames.join('、')}`)
  }

  return parts.length > 0 ? parts.join(' | ') : '未填写角色信息'
}

export function LibraryPane({
  aliasRoleFilter,
  busy,
  filterText,
  filteredItems,
  indexedCount,
  indexBusy,
  items,
  originalRoleFilter,
  roleSummaryByPath,
  selectedPath,
  selectedPaths,
  onAliasRoleFilterChange,
  onClearSelection,
  onFilterTextChange,
  onOpenItem,
  onOriginalRoleFilterChange,
  onSelectFiltered,
  onToggleSelection,
  getFileName,
}: LibraryPaneProps) {
  const selectedSet = new Set(selectedPaths)

  return (
    <>
      <div className="list-toolbar">
        <div className="list-filters">
          <label className="input-stack">
            <span>名称筛选</span>
            <input
              value={filterText}
              onChange={(event) => onFilterTextChange(event.target.value)}
              placeholder="按文件名或路径过滤"
            />
          </label>
          <label className="input-stack">
            <span>原角色名筛选</span>
            <input
              value={originalRoleFilter}
              onChange={(event) => onOriginalRoleFilterChange(event.target.value)}
              placeholder="按图片原角色名过滤"
            />
          </label>
          <label className="input-stack">
            <span>扮演角色筛选</span>
            <input
              value={aliasRoleFilter}
              onChange={(event) => onAliasRoleFilterChange(event.target.value)}
              placeholder="按扮演角色名过滤"
            />
          </label>
        </div>

        <div className="list-side">
          <div className="list-meta">
            <span>显示 {filteredItems.length}/{items.length}</span>
            <span>勾选 {selectedPaths.length}</span>
            <span>
              角色索引 {indexedCount}/{items.length}
              {indexBusy ? '（进行中）' : ''}
            </span>
          </div>
          <div className="list-actions">
            <button type="button" onClick={onSelectFiltered} disabled={busy || filteredItems.length === 0}>
              勾选筛选结果
            </button>
            <button type="button" onClick={onClearSelection} disabled={busy || selectedPaths.length === 0}>
              清空勾选
            </button>
          </div>
        </div>
      </div>

      <div className="list-wrap">
        {filteredItems.length === 0 ? (
          <div className="empty-list">{items.length === 0 ? '暂无条目' : '筛选后无结果'}</div>
        ) : (
          <ul className="image-list">
            {filteredItems.map((path) => {
              const summary = roleSummaryByPath[path]
              const checked = selectedSet.has(path)
              return (
                <li key={path} className={checked ? 'item-row item-row-selected' : 'item-row'}>
                  <label className="item-select">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => onToggleSelection(path)}
                      disabled={busy}
                    />
                    <span>批量</span>
                  </label>
                  <button
                    className={path === selectedPath ? 'item-btn active' : 'item-btn'}
                    onClick={() => onOpenItem(path)}
                    disabled={busy}
                  >
                    <span className="item-name">{getFileName(path)}</span>
                    <span className="item-path">{path}</span>
                    <span className={summary ? 'item-summary' : 'item-summary item-summary-muted'}>
                      {renderRoleSummary(summary, indexBusy)}
                    </span>
                  </button>
                </li>
              )
            })}
          </ul>
        )}
      </div>
    </>
  )
}
