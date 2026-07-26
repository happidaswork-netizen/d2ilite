import { isTauri } from '@tauri-apps/api/core'
import { LogicalPosition, LogicalSize } from '@tauri-apps/api/dpi'
import { emitTo } from '@tauri-apps/api/event'
import { currentMonitor, getCurrentWindow } from '@tauri-apps/api/window'
import { WebviewWindow } from '@tauri-apps/api/webviewWindow'

import type { FormState, MetadataTabKey } from '../domain/metadata'
import type { MetadataItem } from '../types'

export const METADATA_INSPECTOR_WINDOW_LABEL = 'metadata-inspector'
export const METADATA_INSPECTOR_UPDATE_EVENT = 'metadata-inspector:update'
export const METADATA_INSPECTOR_STORAGE_KEY = 'd2i-lite.metadata-inspector.payload'

export type MetadataInspectorPlacement = 'bottom' | 'side'

export type MetadataInspectorWindowPayload = {
  activeMetaTab: MetadataTabKey
  currentItem: MetadataItem | null
  form: FormState | null
  selectedName: string
  selectedPath: string
}

type WindowGeometry = {
  width: number
  height: number
  x: number
  y: number
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

function persistPayload(payload: MetadataInspectorWindowPayload): void {
  try {
    window.localStorage.setItem(METADATA_INSPECTOR_STORAGE_KEY, JSON.stringify(payload))
  } catch {
    // Metadata can be large. The live event still carries the current payload.
  }
}

function getMetadataInspectorUrl(): string {
  const url = new URL(window.location.href)
  url.searchParams.set('view', 'metadata-inspector')
  url.hash = ''
  return url.toString()
}

export function readStoredMetadataInspectorPayload(): MetadataInspectorWindowPayload | null {
  try {
    const raw = window.localStorage.getItem(METADATA_INSPECTOR_STORAGE_KEY)
    if (!raw) {
      return null
    }
    return JSON.parse(raw) as MetadataInspectorWindowPayload
  } catch {
    return null
  }
}

async function getWindowGeometry(placement: MetadataInspectorPlacement): Promise<WindowGeometry> {
  const mainWindow = getCurrentWindow()
  const scaleFactor = await mainWindow.scaleFactor()
  const mainPosition = (await mainWindow.outerPosition()).toLogical(scaleFactor)
  const mainSize = (await mainWindow.outerSize()).toLogical(scaleFactor)
  const monitor = await currentMonitor()

  const workArea = monitor
    ? {
        x: monitor.workArea.position.x / scaleFactor,
        y: monitor.workArea.position.y / scaleFactor,
        width: monitor.workArea.size.width / scaleFactor,
        height: monitor.workArea.size.height / scaleFactor,
      }
    : {
        x: 0,
        y: 0,
        width: window.screen.availWidth || 1600,
        height: window.screen.availHeight || 900,
      }

  const gap = 8
  const margin = 12

  if (placement === 'bottom') {
    const width = clamp(Math.max(860, mainSize.width), 560, workArea.width - margin * 2)
    const height = clamp(440, 360, Math.max(360, workArea.height - margin * 2))
    let x = clamp(mainPosition.x, workArea.x + margin, workArea.x + workArea.width - width - margin)
    let y = mainPosition.y + mainSize.height + gap

    if (y + height > workArea.y + workArea.height - margin) {
      y = mainPosition.y - height - gap
    }
    y = clamp(y, workArea.y + margin, workArea.y + workArea.height - height - margin)
    x = clamp(x, workArea.x + margin, workArea.x + workArea.width - width - margin)

    return { width, height, x, y }
  }

  const width = clamp(600, 520, Math.max(520, workArea.width - margin * 2))
  const height = clamp(Math.max(640, mainSize.height), 520, workArea.height - margin * 2)
  let x = mainPosition.x + mainSize.width + gap
  if (x + width > workArea.x + workArea.width - margin) {
    x = mainPosition.x - width - gap
  }
  x = clamp(x, workArea.x + margin, workArea.x + workArea.width - width - margin)
  const y = clamp(mainPosition.y, workArea.y + margin, workArea.y + workArea.height - height - margin)

  return { width, height, x, y }
}

async function emitPayload(payload: MetadataInspectorWindowPayload): Promise<void> {
  await emitTo(METADATA_INSPECTOR_WINDOW_LABEL, METADATA_INSPECTOR_UPDATE_EVENT, payload)
}

export async function syncMetadataInspectorWindow(payload: MetadataInspectorWindowPayload): Promise<void> {
  if (!isTauri()) {
    return
  }
  const existingWindow = await WebviewWindow.getByLabel(METADATA_INSPECTOR_WINDOW_LABEL)
  if (!existingWindow) {
    return
  }
  persistPayload(payload)
  await emitPayload(payload)
}

export async function closeMetadataInspectorWindow(): Promise<boolean> {
  if (!isTauri()) {
    return false
  }
  const existingWindow = await WebviewWindow.getByLabel(METADATA_INSPECTOR_WINDOW_LABEL)
  if (!existingWindow) {
    return false
  }
  await existingWindow.close()
  return true
}

export async function openMetadataInspectorWindow(
  payload: MetadataInspectorWindowPayload,
  placement: MetadataInspectorPlacement,
): Promise<'window' | 'fallback'> {
  persistPayload(payload)
  if (!isTauri()) {
    return 'fallback'
  }

  const geometry = await getWindowGeometry(placement)
  const existingWindow = await WebviewWindow.getByLabel(METADATA_INSPECTOR_WINDOW_LABEL)
  if (existingWindow) {
    await existingWindow.setSize(new LogicalSize(geometry.width, geometry.height))
    await existingWindow.setPosition(new LogicalPosition(geometry.x, geometry.y))
    await existingWindow.show()
    await existingWindow.setFocus()
    await emitPayload(payload)
    return 'window'
  }

  const inspectorWindow = new WebviewWindow(METADATA_INSPECTOR_WINDOW_LABEL, {
    title: '完整 metadata - D2I Lite Next',
    url: getMetadataInspectorUrl(),
    width: geometry.width,
    height: geometry.height,
    x: geometry.x,
    y: geometry.y,
    minWidth: 520,
    minHeight: 520,
    resizable: true,
    focus: true,
    visible: true,
    decorations: true,
    parent: 'main',
    preventOverflow: true,
  })

  await new Promise<void>((resolve, reject) => {
    let settled = false
    const settle = (handler: () => void): void => {
      if (settled) {
        return
      }
      settled = true
      handler()
    }

    void inspectorWindow.once('tauri://created', () => {
      settle(resolve)
    })
    void inspectorWindow.once('tauri://error', (event) => {
      settle(() => reject(new Error(String(event.payload || 'metadata window creation failed'))))
    })
    window.setTimeout(() => {
      settle(resolve)
    }, 500)
  })

  await emitPayload(payload)
  return 'window'
}
