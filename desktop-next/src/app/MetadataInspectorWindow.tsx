import { listen } from '@tauri-apps/api/event'
import { useEffect, useMemo, useState } from 'react'

import { getTabPayload, type MetadataTabKey } from '../domain/metadata'
import { MetadataInspectorPane } from '../features/metadata/components/MetadataInspectorPane'
import {
  METADATA_INSPECTOR_UPDATE_EVENT,
  readStoredMetadataInspectorPayload,
  type MetadataInspectorWindowPayload,
} from './metadataInspectorWindowBridge'

export function MetadataInspectorWindow() {
  const [payload, setPayload] = useState<MetadataInspectorWindowPayload | null>(() =>
    readStoredMetadataInspectorPayload(),
  )
  const [activeMetaTab, setActiveMetaTab] = useState<MetadataTabKey>(
    () => readStoredMetadataInspectorPayload()?.activeMetaTab || 'profile',
  )

  useEffect(() => {
    let unlisten: (() => void) | undefined
    let disposed = false

    const run = async (): Promise<void> => {
      if (disposed) {
        return
      }
      unlisten = await listen<MetadataInspectorWindowPayload>(METADATA_INSPECTOR_UPDATE_EVENT, (event) => {
        setPayload(event.payload)
        setActiveMetaTab(event.payload.activeMetaTab || 'profile')
      })
    }

    void run().catch(() => {
      // This window is also renderable in a normal browser for layout checks.
    })

    return () => {
      disposed = true
      if (unlisten) {
        unlisten()
      }
    }
  }, [])

  const activeTabPayload = useMemo(
    () => getTabPayload(payload?.currentItem || null, activeMetaTab),
    [activeMetaTab, payload?.currentItem],
  )

  return (
    <div className="metadata-window-shell">
      <header className="metadata-window-head">
        <div>
          <p className="panel-title">完整 metadata</p>
          <span className="tool-subtitle">{payload?.selectedName || '未选择图片'}</span>
        </div>
      </header>
      <main className="metadata-window-main">
        <MetadataInspectorPane
          activeTabPayload={activeTabPayload}
          activeMetaTab={activeMetaTab}
          currentItem={payload?.currentItem || null}
          form={payload?.form || null}
          selectedName={payload?.selectedName || ''}
          selectedPath={payload?.selectedPath || ''}
          onTabChange={setActiveMetaTab}
        />
      </main>
    </div>
  )
}
