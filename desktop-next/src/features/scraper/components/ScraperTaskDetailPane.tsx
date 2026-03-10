import type { ScraperTaskDetail, ScraperProgressRow } from '../../../types'

type ScraperTaskDetailPaneProps = {
  detail: ScraperTaskDetail | null
}

function renderProgressRows(rows: ScraperProgressRow[]) {
  if (rows.length === 0) {
    return <p className="batch-report-empty">当前分组没有条目。</p>
  }

  return (
    <div className="scraper-table-wrap">
      <table className="scraper-table">
        <thead>
          <tr>
            <th>#</th>
            <th>人物</th>
            <th>详情</th>
            <th>图片</th>
            <th>元数据</th>
            <th>原因</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.idx}-${row.detail_url || row.name}`}>
              <td>{row.idx}</td>
              <td>
                <strong>{row.name || '-'}</strong>
                {row.detail_url ? <span className="scraper-row-sub">{row.detail_url}</span> : null}
              </td>
              <td>{row.detail || '-'}</td>
              <td>{row.image || '-'}</td>
              <td>{row.meta || '-'}</td>
              <td>
                <span>{row.reason || '-'}</span>
                {row.image_path ? <span className="scraper-row-sub">{row.image_path}</span> : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function ScraperTaskDetailPane(props: ScraperTaskDetailPaneProps) {
  const { detail } = props

  return (
    <section className="scraper-pane shell">
      <div className="section-head">
        <div>
          <p className="section-eyebrow">Monitor</p>
          <h2>抓取监控</h2>
        </div>
        <div className="section-meta">{detail ? detail.status : '未选择任务'}</div>
      </div>

      {!detail ? (
        <div className="placeholder">先在左侧选择一个抓取任务，再查看进度表、日志尾部和当前状态。</div>
      ) : (
        <div className="scraper-detail-grid">
          <div className="scraper-summary-grid">
            <div className="meta-summary-card">
              <span className="meta-summary-label">任务</span>
              <strong>{detail.task}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">当前动作</span>
              <strong>{detail.latest_action || '无'}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">发现 / 总目标</span>
              <strong>
                {detail.discovered_rows} / {detail.total_target}
              </strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">下载 / 完成</span>
              <strong>
                {detail.downloaded_rows} / {detail.completed_rows}
              </strong>
            </div>
          </div>

          <div className="scraper-detail-card">
            <p className="scraper-detail-title">任务概览</p>
            <p className="scraper-detail-copy">{detail.progress_text}</p>
            <dl className="scraper-detail-list">
              <div>
                <dt>任务目录</dt>
                <dd>{detail.root}</dd>
              </div>
              <div>
                <dt>输出目录</dt>
                <dd>{detail.output_path}</dd>
              </div>
              <div>
                <dt>日志路径</dt>
                <dd>{detail.log_path}</dd>
              </div>
              <div>
                <dt>更新时间</dt>
                <dd>{detail.updated_at || '-'}</dd>
              </div>
            </dl>
          </div>

          <div className="scraper-summary-grid scraper-summary-grid-wide">
            <div className="meta-summary-card">
              <span className="meta-summary-label">列表</span>
              <strong>{detail.list_rows}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">详情</span>
              <strong>{detail.profile_rows}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">图片</span>
              <strong>{detail.image_rows}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">元数据</span>
              <strong>{detail.metadata_rows}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">待复核</span>
              <strong>{detail.review_rows}</strong>
            </div>
            <div className="meta-summary-card">
              <span className="meta-summary-label">失败项</span>
              <strong>{detail.failure_rows}</strong>
            </div>
          </div>

          <div className="scraper-columns">
            <div className="scraper-detail-card">
              <div className="scraper-block-head">
                <p className="scraper-detail-title">待处理 / 失败</p>
                <span>{detail.pending_rows.length}</span>
              </div>
              {renderProgressRows(detail.pending_rows)}
            </div>

            <div className="scraper-detail-card">
              <div className="scraper-block-head">
                <p className="scraper-detail-title">已完成</p>
                <span>{detail.done_rows.length}</span>
              </div>
              {renderProgressRows(detail.done_rows)}
            </div>
          </div>

          <div className="scraper-detail-card">
            <div className="scraper-block-head">
              <p className="scraper-detail-title">日志尾部</p>
              <span>最近输出</span>
            </div>
            <div className="json-panel">
              <pre>{detail.log_tail || '暂无日志'}</pre>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}
