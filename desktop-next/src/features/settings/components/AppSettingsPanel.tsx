import { useState } from 'react'

import type { AppSettings, NameBarSettings } from '../../../types'

type AppSettingsPanelProps = {
  busy: boolean
  open: boolean
  settings: AppSettings | null
  settingsPath: string
  onClose: () => void
  onPickFolderPath: (initialFolder?: string) => Promise<string>
  onReload: () => void
  onSave: (settings: AppSettings) => void
}

function cloneSettings(settings: AppSettings): AppSettings {
  return JSON.parse(JSON.stringify(settings)) as AppSettings
}

function numberValue(value: string, fallback: number): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function ensureNameBarSettings(settings: AppSettings): NameBarSettings {
  return settings.image_actions.name_bar
}

export function AppSettingsPanel({
  busy,
  open,
  settings,
  settingsPath,
  onClose,
  onPickFolderPath,
  onReload,
  onSave,
}: AppSettingsPanelProps) {
  if (!open) {
    return null
  }

  return (
    <div className="settings-overlay" role="dialog" aria-label="全局设置">
      <AppSettingsPanelBody
        key={`${settingsPath}:${settings?.updated_at || ''}`}
        busy={busy}
        settings={settings}
        settingsPath={settingsPath}
        onClose={onClose}
        onPickFolderPath={onPickFolderPath}
        onReload={onReload}
        onSave={onSave}
      />
    </div>
  )
}

function AppSettingsPanelBody({
  busy,
  settings,
  settingsPath,
  onClose,
  onPickFolderPath,
  onReload,
  onSave,
}: Omit<AppSettingsPanelProps, 'open'>) {
  const [draft, setDraft] = useState<AppSettings | null>(() => (settings ? cloneSettings(settings) : null))

  const llm = draft?.llm
  const nameBar = draft ? ensureNameBarSettings(draft) : null

  const patch = (recipe: (next: AppSettings) => void): void => {
    setDraft((prev) => {
      if (!prev) {
        return prev
      }
      const next = cloneSettings(prev)
      recipe(next)
      return next
    })
  }

  return (
    <div className="settings-panel">
      <div className="settings-panel-head">
        <div>
          <p className="section-eyebrow">Settings</p>
          <h2>全局设置</h2>
          <span className="tool-subtitle">{settingsPath || '默认设置'}</span>
        </div>
        <div className="settings-actions">
          <button type="button" onClick={onReload} disabled={busy}>
            重新读取
          </button>
          <button type="button" onClick={onClose} disabled={busy}>
            关闭
          </button>
        </div>
      </div>

      {!draft || !llm || !nameBar ? (
        <div className="placeholder">正在读取全局设置...</div>
      ) : (
        <>
          <div className="settings-grid">
            <section className="settings-block">
              <div className="tool-block-head">
                <strong>LLM</strong>
              </div>
              <div className="settings-field-grid">
                <label className="check-card">
                  <input
                    type="checkbox"
                    checked={llm.enabled_default}
                    onChange={(event) => patch((next) => { next.llm.enabled_default = event.target.checked })}
                    disabled={busy}
                  />
                  <span>默认启用语义补全</span>
                </label>
                <label className="input-stack">
                  <span>API Base</span>
                  <input
                    value={llm.api_base}
                    onChange={(event) => patch((next) => { next.llm.api_base = event.target.value })}
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>Model</span>
                  <input
                    value={llm.model}
                    onChange={(event) => patch((next) => { next.llm.model = event.target.value })}
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>API Key</span>
                  <input
                    type="password"
                    value={llm.api_key}
                    onChange={(event) => patch((next) => { next.llm.api_key = event.target.value })}
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>Timeout(s)</span>
                  <input
                    value={String(llm.timeout_seconds)}
                    onChange={(event) =>
                      patch((next) => { next.llm.timeout_seconds = numberValue(event.target.value, 45) })
                    }
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>Retries</span>
                  <input
                    value={String(llm.max_retries)}
                    onChange={(event) =>
                      patch((next) => { next.llm.max_retries = numberValue(event.target.value, 2) })
                    }
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>Temperature</span>
                  <input
                    value={String(llm.temperature)}
                    onChange={(event) =>
                      patch((next) => { next.llm.temperature = numberValue(event.target.value, 0.1) })
                    }
                    disabled={busy}
                  />
                </label>
              </div>
            </section>

            <section className="settings-block">
              <div className="tool-block-head">
                <strong>加名字默认值</strong>
              </div>
              <div className="settings-field-grid">
                <label className="input-stack">
                  <span>输出格式</span>
                  <select
                    value={nameBar.output_format}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).output_format = event.target.value })
                    }
                    disabled={busy}
                  >
                    <option value="original">同原图</option>
                    <option value="png">PNG</option>
                    <option value="jpg">JPG</option>
                    <option value="webp">WEBP</option>
                  </select>
                </label>
                <label className="input-stack">
                  <span>命名方式</span>
                  <select
                    value={nameBar.output_name_mode}
                    onChange={(event) =>
                      patch((next) => {
                        ensureNameBarSettings(next).output_name_mode =
                          event.target.value === 'label' ? 'label' : 'suffix'
                      })
                    }
                    disabled={busy}
                  >
                    <option value="suffix">原图名 + 后缀</option>
                    <option value="label">直接用名字</option>
                  </select>
                </label>
                <label className="input-stack settings-span-2">
                  <span>输出目录</span>
                  <div className="path-pick-row">
                    <input
                      value={nameBar.output_dir}
                      onChange={(event) =>
                        patch((next) => { ensureNameBarSettings(next).output_dir = event.target.value })
                      }
                      disabled={busy}
                      placeholder="留空则输出到原图所在文件夹"
                    />
                    <button
                      type="button"
                      onClick={() => {
                        void onPickFolderPath(nameBar.output_dir).then((picked) => {
                          if (picked) {
                            patch((next) => { ensureNameBarSettings(next).output_dir = picked })
                          }
                        })
                      }}
                      disabled={busy}
                    >
                      选择
                    </button>
                  </div>
                </label>
                <label className="input-stack">
                  <span>后缀</span>
                  <input
                    value={nameBar.suffix}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).suffix = event.target.value })
                    }
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>对齐</span>
                  <select
                    value={nameBar.align}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).align = event.target.value })
                    }
                    disabled={busy}
                  >
                    <option value="center">居中</option>
                    <option value="left">左对齐</option>
                    <option value="right">右对齐</option>
                  </select>
                </label>
                <label className="input-stack">
                  <span>高度模式</span>
                  <input
                    value={nameBar.bar_height_mode}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).bar_height_mode = event.target.value })
                    }
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>最小高度</span>
                  <input
                    value={String(nameBar.min_bar_height)}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).min_bar_height = numberValue(event.target.value, 48) })
                    }
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>高度比例</span>
                  <input
                    value={String(nameBar.bar_height_ratio)}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).bar_height_ratio = numberValue(event.target.value, 0.14) })
                    }
                    disabled={busy}
                  />
                </label>
                <label className="input-stack">
                  <span>JPG 质量</span>
                  <input
                    value={String(nameBar.jpg_quality)}
                    onChange={(event) =>
                      patch((next) => { ensureNameBarSettings(next).jpg_quality = numberValue(event.target.value, 100) })
                    }
                    disabled={busy}
                  />
                </label>
              </div>
            </section>
          </div>

          <div className="settings-footer">
            <button type="button" className="primary" onClick={() => onSave(draft)} disabled={busy}>
              保存全局设置
            </button>
          </div>
        </>
      )}
    </div>
  )
}
