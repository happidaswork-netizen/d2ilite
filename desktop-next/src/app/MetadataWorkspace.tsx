import { listen, TauriEvent, type Event as TauriEventPayload } from '@tauri-apps/api/event'
import { useEffect, useMemo, useState } from 'react'

import { BatchRolePanel } from '../features/batch/components/BatchRolePanel'
import { LibraryPane } from '../features/library/components/LibraryPane'
import { CurrentImageSummaryPane } from '../features/metadata/components/CurrentImageSummaryPane'
import { MetadataEditorPane } from '../features/metadata/components/MetadataEditorPane'
import { MetadataInspectorPane } from '../features/metadata/components/MetadataInspectorPane'
import { CurrentImageToolsPane } from '../features/preview/components/CurrentImageToolsPane'
import { PreviewPane } from '../features/preview/components/PreviewPane'
import { AppSettingsPanel } from '../features/settings/components/AppSettingsPanel'
import { getFileName } from '../shared/path'

import {
  closeMetadataInspectorWindow,
  METADATA_INSPECTOR_WINDOW_LABEL,
  openMetadataInspectorWindow,
  syncMetadataInspectorWindow,
  type MetadataInspectorPlacement,
} from './metadataInspectorWindowBridge'
import { useDesktopWorkspace } from './useDesktopWorkspace'

type MetadataWorkspaceProps = {
  initialPath?: string
}

export function MetadataWorkspace(props: MetadataWorkspaceProps) {
  const workspace = useDesktopWorkspace({
    initialPath: props.initialPath,
  })
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [detailsPlacement, setDetailsPlacement] = useState<MetadataInspectorPlacement>('side')
  const [metadataWindowActive, setMetadataWindowActive] = useState(false)
  const [workbenchOpen, setWorkbenchOpen] = useState(false)

  const canSave = Boolean(workspace.selectedPath && workspace.form && workspace.isDirty)
  const metadataWindowPayload = useMemo(
    () => ({
      activeMetaTab: workspace.activeMetaTab,
      currentItem: workspace.currentItem,
      form: workspace.form,
      selectedName: workspace.selectedName,
      selectedPath: workspace.selectedPath,
    }),
    [workspace.activeMetaTab, workspace.currentItem, workspace.form, workspace.selectedName, workspace.selectedPath],
  )

  useEffect(() => {
    if (!metadataWindowActive || !workspace.selectedPath) {
      return
    }
    void syncMetadataInspectorWindow(metadataWindowPayload).catch(() => {
      setMetadataWindowActive(false)
    })
  }, [metadataWindowActive, metadataWindowPayload, workspace.selectedPath])

  useEffect(() => {
    if (!metadataWindowActive) {
      return
    }

    let unlisten: (() => void) | undefined
    let disposed = false

    const run = async (): Promise<void> => {
      unlisten = await listen<unknown>(TauriEvent.WINDOW_DESTROYED, (event: TauriEventPayload<unknown>) => {
        const payload = event.payload
        const label =
          typeof payload === 'string'
            ? payload
            : payload && typeof payload === 'object' && 'label' in payload
              ? String((payload as { label: unknown }).label)
              : ''
        if (label === METADATA_INSPECTOR_WINDOW_LABEL) {
          setMetadataWindowActive(false)
        }
      })
      if (disposed && unlisten) {
        unlisten()
      }
    }

    void run().catch(() => {
      // The browser fallback does not expose Tauri window lifecycle events.
    })

    return () => {
      disposed = true
      if (unlisten) {
        unlisten()
      }
    }
  }, [metadataWindowActive])

  const onToggleDetails = async (): Promise<void> => {
    if (metadataWindowActive) {
      try {
        const closed = await closeMetadataInspectorWindow()
        if (closed) {
          setMetadataWindowActive(false)
          setDetailsOpen(false)
          return
        }
      } catch {
        setMetadataWindowActive(false)
      }
    }
    if (detailsOpen) {
      setDetailsOpen(false)
      return
    }
    await onOpenDetails()
  }

  const onOpenDetails = async (): Promise<void> => {
    if (!workspace.selectedPath) {
      return
    }
    try {
      const result = await openMetadataInspectorWindow(metadataWindowPayload, detailsPlacement)
      if (result === 'window') {
        setMetadataWindowActive(true)
        setDetailsOpen(false)
        return
      }
    } catch {
      setMetadataWindowActive(false)
    }
    setDetailsOpen(true)
  }

  return (
    <div className={workbenchOpen ? 'workspace workspace-workbench-open' : 'workspace workspace-single'}>
      <header className="single-topbar">
        <div className="single-brand">
          <span className="app-icon" aria-hidden="true">
            D
          </span>
          <div>
            <p className="brand-name">D2I Lite Next</p>
            <h1>{workspace.selectedName || '打开一张图片开始'}</h1>
          </div>
          <span className={workspace.isDirty ? 'chip chip-warn' : 'chip chip-ok'}>
            {workspace.isDirty ? '有未保存修改' : '已同步'}
          </span>
        </div>

        <div className="single-actions">
          <button type="button" className="primary" onClick={() => void workspace.onPickAndOpenImage()} disabled={workspace.busy}>
            打开图片
          </button>
          <button type="button" onClick={() => void workspace.onPickAndLoadFolder()} disabled={workspace.busy}>
            打开目录
          </button>
          <button type="button" onClick={() => void workspace.onReloadCurrent()} disabled={workspace.busy || !workspace.selectedPath}>
            重新读取
          </button>
          <span className="toolbar-divider" aria-hidden="true" />
          <button type="button" onClick={() => void workspace.onGotoPrev()} disabled={workspace.busy || !workspace.selectedPath}>
            上一张
          </button>
          <button type="button" onClick={() => void workspace.onGotoNext()} disabled={workspace.busy || !workspace.selectedPath}>
            下一张
          </button>
          <span className="toolbar-divider" aria-hidden="true" />
          <button type="button" onClick={() => void workspace.onOpenCurrentFile()} disabled={workspace.busy || !workspace.selectedPath}>
            系统打开
          </button>
          <button type="button" onClick={() => void workspace.onRevealCurrentFile()} disabled={workspace.busy || !workspace.selectedPath}>
            打开所在文件夹
          </button>
          <span className="toolbar-divider" aria-hidden="true" />
          <button type="button" onClick={() => void onToggleDetails()} disabled={!workspace.selectedPath}>
            {metadataWindowActive || detailsOpen ? '关闭 metadata' : '完整 metadata'}
          </button>
          <button type="button" onClick={() => setWorkbenchOpen((value) => !value)}>
            {workbenchOpen ? '退出工作台' : '工作台模式'}
          </button>
          <button type="button" onClick={() => void workspace.onOpenSettingsPanel()} disabled={workspace.settingsBusy}>
            设置
          </button>
          <button
            type="button"
            className="primary"
            onClick={() => void workspace.onSave()}
            disabled={workspace.busy || !canSave}
          >
            保存
          </button>
        </div>
      </header>

      <AppSettingsPanel
        busy={workspace.settingsBusy}
        open={workspace.settingsOpen}
        settings={workspace.appSettings}
        settingsPath={workspace.settingsPath}
        onClose={() => workspace.setSettingsOpen(false)}
        onPickFolderPath={workspace.onPickFolderPath}
        onReload={() => void workspace.onReloadAppSettings()}
        onSave={(settings) => void workspace.onSaveAppSettings(settings)}
      />

      <main className={workbenchOpen ? 'workbench-main' : 'single-main'}>
        {!workbenchOpen ? (
          <>
            <section className="single-preview">
              <PreviewPane
                busy={workspace.busy}
                itemCount={workspace.items.length}
                previewFailed={workspace.previewFailed}
                previewUrl={workspace.previewUrl}
                selectedName={workspace.selectedName}
                selectedPath={workspace.selectedPath}
                onGotoNext={workspace.onGotoNext}
                onGotoPrev={workspace.onGotoPrev}
                onOpenCurrentFile={workspace.onOpenCurrentFile}
                onPreviewError={() => workspace.setPreviewFailed(true)}
                onRevealCurrentFile={workspace.onRevealCurrentFile}
              />
            </section>

            <aside className="single-side">
              <CurrentImageToolsPane
                busy={workspace.busy}
                canSave={canSave}
                form={workspace.form}
                imageActionBusy={workspace.imageActionBusy}
                selectedName={workspace.selectedName}
                selectedPath={workspace.selectedPath}
                onAddNameBar={workspace.onAddNameBar}
                onAiAutofillCurrentMetadata={workspace.onAiAutofillCurrentMetadata}
                onGenerateBiography={workspace.onGenerateBiography}
                onPickFolderPath={workspace.onPickFolderPath}
                onRenameCurrentImage={workspace.onRenameCurrentImage}
                onSave={workspace.onSave}
              />
              <CurrentImageSummaryPane
                currentItem={workspace.currentItem}
                form={workspace.form}
                isDirty={workspace.isDirty}
                keywordCount={workspace.keywordCount}
                selectedName={workspace.selectedName}
                selectedPath={workspace.selectedPath}
              />
            </aside>

            <div className="single-details-toggle">
              <div className="metadata-disclosure-row">
                <div className="bottom-image-info" title={workspace.selectedPath || workspace.selectedName || ''}>
                  <span className="meta-label">当前文件</span>
                  <strong>{workspace.selectedName || '-'}</strong>
                  <span className="meta-separator" aria-hidden="true" />
                  <span className="meta-label">完整路径</span>
                  <strong className="path-value">{workspace.selectedPath || '-'}</strong>
                </div>
                <button
                  type="button"
                  className="metadata-disclosure"
                  onClick={() => void onToggleDetails()}
                  disabled={!workspace.selectedPath}
                >
                  {metadataWindowActive || detailsOpen ? '关闭完整 metadata 窗口' : '打开完整 metadata 窗口'}
                </button>
              </div>
            </div>

            {detailsOpen ? (
              <section className={`single-details single-details-${detailsPlacement}`}>
                <div className="drawer-head">
                  <div>
                    <p className="panel-title">完整 metadata</p>
                    <span className="tool-subtitle">{workspace.selectedName || '未选择图片'}</span>
                  </div>
                  <div className="drawer-actions">
                    <button
                      type="button"
                      className={detailsPlacement === 'bottom' ? 'active' : ''}
                      onClick={() => setDetailsPlacement('bottom')}
                    >
                      底部
                    </button>
                    <button
                      type="button"
                      className={detailsPlacement === 'side' ? 'active' : ''}
                      onClick={() => setDetailsPlacement('side')}
                    >
                      侧边
                    </button>
                    <button type="button" onClick={() => setDetailsOpen(false)}>
                      关闭
                    </button>
                  </div>
                </div>
                <div className="drawer-body">
                  <MetadataInspectorPane
                    activeTabPayload={workspace.activeTabPayload}
                    activeMetaTab={workspace.activeMetaTab}
                    currentItem={workspace.currentItem}
                    form={workspace.form}
                    selectedName={workspace.selectedName}
                    selectedPath={workspace.selectedPath}
                    onTabChange={workspace.setActiveMetaTab}
                  />
                </div>
              </section>
            ) : null}
          </>
        ) : (
          <section className="workbench-panel">
            <div className="workbench-column library-column">
              <div className="workbench-head">
                <p className="panel-title">工作台 / 资源库</p>
                <span className="chip">{workspace.filteredItems.length}/{workspace.items.length}</span>
              </div>
              <div className="toolbar compact-toolbar">
                <label className="input-stack input-stack-wide">
                  <span>目录路径</span>
                  <input
                    className="path-input"
                    value={workspace.folder}
                    onChange={(event) => workspace.setFolder(event.target.value)}
                    placeholder="输入目录路径"
                  />
                </label>
                <div className="action-group">
                  <button onClick={() => void workspace.onLoadFolder()} disabled={workspace.busy}>
                    加载
                  </button>
                  <button onClick={() => void workspace.onPickAndLoadFolder()} disabled={workspace.busy}>
                    选择目录
                  </button>
                </div>
              </div>
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
                onBuildRoleIndex={workspace.onBuildRoleIndex}
                onClearSelection={workspace.onClearSelection}
                onFilterTextChange={workspace.setFilterText}
                onOpenItem={(path) => void workspace.onOpenItem(path)}
                onOriginalRoleFilterChange={workspace.setRoleFilterOriginal}
                onSelectFiltered={workspace.onSelectFiltered}
                onToggleSelection={workspace.onToggleSelection}
                getFileName={getFileName}
                getPreviewUrl={workspace.getPreviewUrl}
              />
            </div>

            <div className="workbench-column tools-column">
              <section className="workbench-preview">
                <PreviewPane
                  busy={workspace.busy}
                  itemCount={workspace.items.length}
                  previewFailed={workspace.previewFailed}
                  previewUrl={workspace.previewUrl}
                  selectedName={workspace.selectedName}
                  selectedPath={workspace.selectedPath}
                  onGotoNext={workspace.onGotoNext}
                  onGotoPrev={workspace.onGotoPrev}
                  onOpenCurrentFile={workspace.onOpenCurrentFile}
                  onPreviewError={() => workspace.setPreviewFailed(true)}
                  onRevealCurrentFile={workspace.onRevealCurrentFile}
                />
              </section>
              <div className="workbench-head">
                <p className="panel-title">批量 / 工作台工具</p>
                <span className={workspace.batchProgress.active ? 'chip chip-warn' : 'chip'}>
                  {workspace.batchProgress.active ? '处理中' : '待命'}
                </span>
              </div>
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
            </div>

            <div className="workbench-column metadata-column">
              <MetadataEditorPane
                activeMetaTab={workspace.activeMetaTab}
                activeTabPayload={workspace.activeTabPayload}
                currentItem={workspace.currentItem}
                form={workspace.form}
                keywordCount={workspace.keywordCount}
                metadataAiBusy={workspace.metadataAiBusy}
                selectedName={workspace.selectedName}
                selectedPath={workspace.selectedPath}
                onAiAutofillCurrentMetadata={workspace.onAiAutofillCurrentMetadata}
                onFieldChange={workspace.onFieldChange}
                onGenerateBiography={workspace.onGenerateBiography}
                onRoleAliasAdd={workspace.onRoleAliasAdd}
                onRoleAliasRemove={workspace.onRoleAliasRemove}
                onRoleAliasChange={workspace.onRoleAliasChange}
                onTabChange={workspace.setActiveMetaTab}
              />
            </div>
          </section>
        )}
      </main>

      <footer className="statusbar">
        <span className="status-primary">状态：{workspace.status}</span>
        <span>metadata：{workspace.isDirty ? '未保存' : workspace.selectedPath ? '已同步' : '未选择'}</span>
        <span>输出目录：{workspace.folder || '与原文件相同文件夹'}</span>
        {workbenchOpen ? (
          <>
            <span>Bridge：{workspace.provider} / {workspace.bridgeVersion}</span>
            <span>条目：{workspace.items.length}</span>
            <span>勾选：{workspace.selectedPaths.length}</span>
            <span>索引：{workspace.indexedCount}/{workspace.items.length}</span>
          </>
        ) : null}
      </footer>
    </div>
  )
}
