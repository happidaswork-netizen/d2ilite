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
          <p className="subtitle">新版抓取台当前先承接任务目录管理、进度监控、进度表和日志尾部，启动控制后续再迁。</p>
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
        <ScraperTaskDetailPane detail={workspace.detail} />
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
