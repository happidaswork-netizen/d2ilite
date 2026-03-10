import type { ScraperLaunchForm, ScraperLaunchTemplateOption } from '../../../types'

type ScraperLaunchPaneProps = {
  busy: boolean
  form: ScraperLaunchForm | null
  templates: ScraperLaunchTemplateOption[]
  onFieldChange: <K extends keyof ScraperLaunchForm>(field: K, value: ScraperLaunchForm[K]) => void
  onSelectTemplate: (templatePath: string) => void
  onFillDefaultOutputRoot: () => void
  onStartTask: () => void
}

export function ScraperLaunchPane(props: ScraperLaunchPaneProps) {
  const { busy, form, templates, onFieldChange, onSelectTemplate, onFillDefaultOutputRoot, onStartTask } = props

  return (
    <section className="scraper-pane shell scraper-launch-pane">
      <div className="section-head">
        <div>
          <p className="section-eyebrow">Launch</p>
          <h2>新任务启动</h2>
        </div>
        <div className="section-meta">{form?.selected_template_path ? '模板模式' : '手动链接模式'}</div>
      </div>

      {!form ? (
        <div className="placeholder">正在准备启动表单...</div>
      ) : (
        <div className="scraper-launch-grid">
          <div className="scraper-launch-row">
            <label className="input-stack input-stack-wide">
              <span>列表页链接</span>
              <input
                className="path-input"
                value={form.start_url}
                onChange={(event) => onFieldChange('start_url', event.target.value)}
                disabled={busy || form.url_locked}
                placeholder="输入 http/https 列表页链接"
              />
            </label>
            <label className="input-stack">
              <span>模板</span>
              <select
                value={form.selected_template_path}
                onChange={(event) => onSelectTemplate(event.target.value)}
                disabled={busy}
              >
                <option value="">自动生成模板（按当前链接）</option>
                {templates.map((template) => (
                  <option key={template.path} value={template.path}>
                    {template.label}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <p className="scraper-launch-hint">{form.template_hint}</p>

          <div className="scraper-launch-row">
            <label className="input-stack input-stack-wide">
              <span>输出目录</span>
              <input
                className="path-input"
                value={form.output_root}
                onChange={(event) => onFieldChange('output_root', event.target.value)}
                disabled={busy}
                placeholder="输入最终任务输出目录"
              />
            </label>
            <div className="action-group">
              <button onClick={onFillDefaultOutputRoot} disabled={busy || Boolean(form.selected_template_path)}>
                按链接填充默认目录
              </button>
            </div>
          </div>

          <div className="scraper-control-grid scraper-launch-tuning-grid">
            <label className="input-stack">
              <span>最小间隔(秒)</span>
              <input value={form.interval_min} onChange={(event) => onFieldChange('interval_min', event.target.value)} disabled={busy} />
            </label>
            <label className="input-stack">
              <span>最大间隔(秒)</span>
              <input value={form.interval_max} onChange={(event) => onFieldChange('interval_max', event.target.value)} disabled={busy} />
            </label>
            <label className="input-stack">
              <span>请求超时(秒)</span>
              <input
                value={form.timeout_seconds}
                onChange={(event) => onFieldChange('timeout_seconds', event.target.value)}
                disabled={busy}
              />
            </label>
            <label className="input-stack">
              <span>连续失败阈值</span>
              <input
                value={form.suspect_block_consecutive_failures}
                onChange={(event) => onFieldChange('suspect_block_consecutive_failures', event.target.value)}
                disabled={busy}
              />
            </label>
            <label className="input-stack">
              <span>图片下载模式</span>
              <select
                value={form.image_download_mode}
                onChange={(event) => onFieldChange('image_download_mode', event.target.value)}
                disabled={busy}
              >
                <option value="requests_jsl">requests_jsl</option>
                <option value="browser">browser</option>
              </select>
            </label>
          </div>

          <div className="scraper-launch-checks">
            <label className="check-card">
              <input type="checkbox" checked={form.jsl_enabled} onChange={(event) => onFieldChange('jsl_enabled', event.target.checked)} disabled={busy} />
              <span>启用 JSL 反爬挑战处理</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.auto_fallback_to_browser}
                onChange={(event) => onFieldChange('auto_fallback_to_browser', event.target.checked)}
                disabled={busy}
              />
              <span>请求模式失败时自动回退浏览器</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.disable_page_images_during_crawl}
                onChange={(event) => onFieldChange('disable_page_images_during_crawl', event.target.checked)}
                disabled={busy}
              />
              <span>浏览器抓取时禁用页面图片渲染</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.output_minimal}
                onChange={(event) => onFieldChange('output_minimal', event.target.checked)}
                disabled={busy}
              />
              <span>仅保留最终图片和抓取记录</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.direct_write_images}
                onChange={(event) => onFieldChange('direct_write_images', event.target.checked)}
                disabled={busy}
              />
              <span>图片直写，减少中间缓存</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.llm_enrich_enabled}
                onChange={(event) => onFieldChange('llm_enrich_enabled', event.target.checked)}
                disabled={busy}
              />
              <span>启用 LLM 语义增强</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.save_generated_template}
                onChange={(event) => onFieldChange('save_generated_template', event.target.checked)}
                disabled={busy || form.disable_template_persistence_controls}
              />
              <span>保存本次生成的模板</span>
            </label>
            <label className="check-card">
              <input
                type="checkbox"
                checked={form.cleanup_generated_template}
                onChange={(event) => onFieldChange('cleanup_generated_template', event.target.checked)}
                disabled={busy || form.disable_template_persistence_controls}
              />
              <span>任务完成后清理本次生成模板</span>
            </label>
          </div>

          <div className="scraper-launch-row scraper-launch-llm">
            <label className="input-stack">
              <span>LLM 模型</span>
              <input value={form.llm_model} onChange={(event) => onFieldChange('llm_model', event.target.value)} disabled={busy} />
            </label>
            <label className="input-stack input-stack-wide">
              <span>LLM API Base</span>
              <input value={form.llm_api_base} onChange={(event) => onFieldChange('llm_api_base', event.target.value)} disabled={busy} />
            </label>
            <label className="input-stack input-stack-wide">
              <span>LLM API Key</span>
              <input
                value={form.llm_api_key}
                onChange={(event) => onFieldChange('llm_api_key', event.target.value)}
                disabled={busy}
                type="password"
              />
            </label>
          </div>

          <div className="scraper-action-row">
            <button className="primary" onClick={onStartTask} disabled={busy}>
              开始抓取
            </button>
          </div>
        </div>
      )}
    </section>
  )
}
