import { BatchRolePanel } from '../features/batch/components/BatchRolePanel'
import { LibraryPane } from '../features/library/components/LibraryPane'
import { MetadataEditorPane } from '../features/metadata/components/MetadataEditorPane'
import { PreviewPane } from '../features/preview/components/PreviewPane'
import { getFileName } from '../shared/path'

import { useDesktopWorkspace } from './useDesktopWorkspace'

export function DesktopWorkspace() {
  const workspace = useDesktopWorkspace()

  return (
    <div className="workspace">
      <header className="topbar">
        <div className="titleblock">
          <p className="eyebrow">D2I Lite Next</p>
          <h1>本地看图与元数据工作台</h1>
          <p className="subtitle">Phase 1 最小闭环：目录浏览、图片预览、元数据读取与保存。</p>
        </div>

        <div className="bridge-strip">
          <span className="chip chip-strong">Provider {workspace.provider}</span>
          <span className="chip">Bridge {workspace.bridgeVersion}</span>
          <span className={workspace.isDirty ? 'chip chip-warn' : 'chip'}>
            {workspace.isDirty ? '有未保存修改' : '已同步'}
          </span>
        </div>

        <div className="toolbar">
          <label className="input-stack input-stack-wide">
            <span>图片目录</span>
            <input
              className="path-input"
              value={workspace.folder}
              onChange={(event) => workspace.setFolder(event.target.value)}
              placeholder="输入目录路径"
            />
          </label>
          <div className="action-group">
            <button onClick={() => void workspace.onLoadFolder()} disabled={workspace.busy}>
              加载目录
            </button>
            <button onClick={() => void workspace.onReloadCurrent()} disabled={workspace.busy || !workspace.selectedPath}>
              重新读取
            </button>
            <button
              className="primary"
              onClick={() => void workspace.onSave()}
              disabled={workspace.busy || !workspace.selectedPath || !workspace.form || !workspace.isDirty}
            >
              保存元数据
            </button>
          </div>
        </div>
      </header>

      <main className="main">
        <section className="preview shell">
          <PreviewPane
            itemCount={workspace.items.length}
            previewFailed={workspace.previewFailed}
            previewUrl={workspace.previewUrl}
            selectedName={workspace.selectedName}
            selectedPath={workspace.selectedPath}
            onPreviewError={() => workspace.setPreviewFailed(true)}
          />
          <LibraryPane
            aliasRoleFilter={workspace.roleFilterAlias}
            busy={workspace.busy}
            filterText={workspace.filterText}
            filteredItems={workspace.filteredItems}
            indexedCount={workspace.indexedCount}
            indexBusy={workspace.indexBusy}
            items={workspace.items}
            originalRoleFilter={workspace.roleFilterOriginal}
            roleSummaryByPath={workspace.roleSummaryByPath}
            selectedPath={workspace.selectedPath}
            selectedPaths={workspace.selectedPaths}
            onAliasRoleFilterChange={workspace.setRoleFilterAlias}
            onClearSelection={workspace.onClearSelection}
            onFilterTextChange={workspace.setFilterText}
            onOpenItem={(path) => void workspace.onOpenItem(path)}
            onOriginalRoleFilterChange={workspace.setRoleFilterOriginal}
            onSelectFiltered={workspace.onSelectFiltered}
            onToggleSelection={workspace.onToggleSelection}
            getFileName={getFileName}
          />
          <BatchRolePanel
            aliasMode={workspace.batchAliasMode}
            aliasText={workspace.batchAliasText}
            busy={workspace.busy}
            canApply={workspace.hasBatchOperation}
            filteredCount={workspace.filteredItems.length}
            indexedCount={workspace.indexedCount}
            indexBusy={workspace.indexBusy}
            matchMode={workspace.batchMatchMode}
            originalRoleMode={workspace.batchOriginalRoleMode}
            originalRoleName={workspace.batchOriginalRoleName}
            progress={workspace.batchProgress}
            report={workspace.lastBatchReport}
            scope={workspace.batchScope}
            selectedCount={workspace.selectedPaths.length}
            totalCount={workspace.items.length}
            onAliasModeChange={workspace.setBatchAliasMode}
            onAliasTextChange={workspace.setBatchAliasText}
            onApply={() => void workspace.onApplyBatchRoleChanges()}
            onMatchModeChange={workspace.setBatchMatchMode}
            onOriginalRoleModeChange={workspace.setBatchOriginalRoleMode}
            onOriginalRoleNameChange={workspace.setBatchOriginalRoleName}
            onScopeChange={workspace.setBatchScope}
          />
        </section>

        <section className="editor shell">
          <MetadataEditorPane
            activeMetaTab={workspace.activeMetaTab}
            activeTabPayload={workspace.activeTabPayload}
            currentItem={workspace.currentItem}
            form={workspace.form}
            keywordCount={workspace.keywordCount}
            selectedName={workspace.selectedName}
            selectedPath={workspace.selectedPath}
            onFieldChange={workspace.onFieldChange}
            onRoleAliasAdd={workspace.onRoleAliasAdd}
            onRoleAliasRemove={workspace.onRoleAliasRemove}
            onRoleAliasChange={workspace.onRoleAliasChange}
            onTabChange={workspace.setActiveMetaTab}
          />
        </section>
      </main>

      <footer className="statusbar">
        <span>状态：{workspace.status}</span>
        <span>Bridge：{workspace.provider}</span>
        <span>版本：{workspace.bridgeVersion}</span>
        <span>条目：{workspace.items.length}</span>
        <span>勾选：{workspace.selectedPaths.length}</span>
        <span>索引：{workspace.indexedCount}/{workspace.items.length}</span>
      </footer>
    </div>
  )
}

