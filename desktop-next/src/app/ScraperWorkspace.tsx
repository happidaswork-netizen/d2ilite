import { ScraperTaskDetailPane } from '../features/scraper/components/ScraperTaskDetailPane'
import { ScraperTaskListPane } from '../features/scraper/components/ScraperTaskListPane'

import { useScraperWorkspace } from './useScraperWorkspace'

export function ScraperWorkspace() {
  const workspace = useScraperWorkspace()

  return (
    <div className="workspace">
      <header className="topbar">
        <div className="titleblock">
          <p className="eyebrow">D2I Lite Next</p>
          <h1>公共抓取工作台</h1>
          <p className="subtitle">新版抓取台已经承接任务目录管理、监控和已有任务控制；新任务启动表单后续再迁。</p>
        </div>

        <div className="bridge-strip">
          <span className="chip chip-strong">Provider {workspace.provider}</span>
          <span className="chip">Bridge {workspace.bridgeVersion}</span>
          <span className="chip">{workspace.taskStatusText}</span>
        </div>

        <div className="toolbar">
          <label className="input-stack input-stack-wide">
            <span>抓取任务根目录</span>
            <input
              className="path-input"
              value={workspace.baseRoot}
              onChange={(event) => workspace.setBaseRoot(event.target.value)}
              placeholder="输入 public_archive 根目录"
            />
          </label>
          <div className="action-group">
            <button onClick={() => void workspace.onRefresh()} disabled={workspace.busy}>
              刷新任务
            </button>
          </div>
        </div>
      </header>

      <main className="scraper-main">
        <ScraperTaskListPane
          busy={workspace.busy}
          selectedRoot={workspace.selectedRoot}
          statusText={workspace.taskStatusText}
          taskCount={workspace.taskCount}
          tasks={workspace.tasks}
          onSelectTask={(root) => void workspace.onSelectTask(root)}
        />
        <ScraperTaskDetailPane
          actionBusy={workspace.actionBusy}
          controlOptions={workspace.controlOptions}
          detail={workspace.detail}
          onContinueTask={() => void workspace.onContinueTask()}
          onPauseTask={() => void workspace.onPauseTask()}
          onRetryTask={() => void workspace.onRetryTask()}
          onRewriteMetadataTask={() => void workspace.onRewriteMetadataTask()}
          onSetAutoFallback={workspace.setControlAutoFallback}
          onSetDisablePageImages={workspace.setControlDisablePageImages}
          onSetMode={workspace.setControlMode}
        />
      </main>

      <footer className="statusbar">
        <span>状态：{workspace.status}</span>
        <span>Bridge：{workspace.provider}</span>
        <span>版本：{workspace.bridgeVersion}</span>
        <span>任务数：{workspace.taskCount}</span>
        <span>当前任务：{workspace.detail?.task || '-'}</span>
      </footer>
    </div>
  )
}
