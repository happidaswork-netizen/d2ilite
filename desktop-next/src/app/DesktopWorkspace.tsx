import { useState } from 'react'

import { MetadataWorkspace } from './MetadataWorkspace'
import { ScraperWorkspace } from './ScraperWorkspace'

type WorkspaceMode = 'metadata' | 'scraper'

export function DesktopWorkspace() {
  const [mode, setMode] = useState<WorkspaceMode>('metadata')

  return (
    <div className="app-shell">
      <div className="workspace-switch">
        <button
          className={`workspace-switch-btn${mode === 'metadata' ? ' active' : ''}`}
          onClick={() => setMode('metadata')}
        >
          图片元数据工作台
        </button>
        <button
          className={`workspace-switch-btn${mode === 'scraper' ? ' active' : ''}`}
          onClick={() => setMode('scraper')}
        >
          公共抓取工作台
        </button>
      </div>

      {mode === 'metadata' ? <MetadataWorkspace /> : <ScraperWorkspace />}
    </div>
  )
}
