import type { ScraperTaskSummary } from '../../../types'

type ScraperTaskListPaneProps = {
  busy: boolean
  selectedRoot: string
  statusText: string
  taskCount: number
  tasks: ScraperTaskSummary[]
  onSelectTask: (root: string) => void
}

export function ScraperTaskListPane(props: ScraperTaskListPaneProps) {
  const { busy, selectedRoot, statusText, taskCount, tasks, onSelectTask } = props

  return (
    <section className="scraper-pane shell">
      <div className="section-head">
        <div>
          <p className="section-eyebrow">Scraper</p>
          <h2>抓取任务</h2>
        </div>
        <div className="section-meta">{statusText}</div>
      </div>

      <div className="scraper-task-list">
        {tasks.length === 0 ? (
          <div className="empty-list">当前目录下没有检测到抓取任务。</div>
        ) : (
          tasks.map((task) => {
            const isActive = task.root === selectedRoot
            return (
              <button
                key={task.root}
                className={`scraper-task-card${isActive ? ' active' : ''}`}
                onClick={() => onSelectTask(task.root)}
                disabled={busy}
              >
                <div className="scraper-task-card-head">
                  <strong>{task.task}</strong>
                  <span className={`scraper-status-chip scraper-status-${task.status.includes('完成') ? 'done' : 'live'}`}>
                    {task.status}
                  </span>
                </div>
                <div className="scraper-task-card-meta">
                  <span>详情 {task.profiles}</span>
                  <span>图片 {task.images}</span>
                  <span>元数据 {task.metadata_ok}</span>
                  <span>失败 {task.failures}</span>
                </div>
                <span className="scraper-task-card-path">{task.root}</span>
                <span className="scraper-task-card-updated">更新于 {task.updated_at || '-'}</span>
              </button>
            )
          })
        )}
      </div>

      <div className="scraper-pane-foot">任务数 {taskCount}</div>
    </section>
  )
}
