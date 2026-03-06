import type { BridgeHealth, DesktopBridge, MetadataItem, SavePayload } from '../types'

type TauriInvoke = (command: string, args?: Record<string, unknown>) => Promise<unknown>
type TauriConvertFileSrc = (filePath: string, protocol?: string) => string

type BridgeEnvelope<T> = {
  ok: boolean
  error?: string
  detail?: string
} & T

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function splitFileName(path: string): string {
  const normalized = String(path || '').replace(/\//g, '\\')
  const idx = normalized.lastIndexOf('\\')
  return idx >= 0 ? normalized.slice(idx + 1) : normalized
}

function nowId(): string {
  return Math.random().toString(16).slice(2) + '-' + Date.now().toString(16)
}

function buildMockRecord(path: string): MetadataItem {
  const filename = splitFileName(path)
  const person = filename.replace(/\.(jpg|jpeg|png|webp|bmp|tif|tiff)$/i, '').replace(/[_-]\d+$/, '')
  return {
    filepath: path,
    filename,
    title: `${person} - 姓名：${person}`,
    description: `${person}，公开资料来源可查。该说明区域用于展示可读版人物小传，不重复结构化字段。`,
    keywords: [person, '警察', '民警'],
    source: 'https://www.mps.gov.cn/',
    image_url: 'https://www.mps.gov.cn/example.jpg',
    city: '',
    person,
    gender: '',
    position: '',
    police_id: '',
    titi_asset_id: nowId(),
    titi_world_id: 'default',
  }
}

function getTauriInvoke(): TauriInvoke | null {
  const w = window as unknown as {
    __TAURI__?: {
      core?: {
        invoke?: TauriInvoke
        convertFileSrc?: TauriConvertFileSrc
      }
      invoke?: TauriInvoke
      convertFileSrc?: TauriConvertFileSrc
    }
  }
  const coreInvoke = w.__TAURI__?.core?.invoke
  if (typeof coreInvoke === 'function') {
    return coreInvoke
  }
  const legacyInvoke = w.__TAURI__?.invoke
  if (typeof legacyInvoke === 'function') {
    return legacyInvoke
  }
  return null
}

function getTauriConvertFileSrc(): TauriConvertFileSrc | null {
  const w = window as unknown as {
    __TAURI__?: {
      core?: { convertFileSrc?: TauriConvertFileSrc }
      convertFileSrc?: TauriConvertFileSrc
    }
  }
  const coreConvert = w.__TAURI__?.core?.convertFileSrc
  if (typeof coreConvert === 'function') {
    return coreConvert
  }
  const legacyConvert = w.__TAURI__?.convertFileSrc
  if (typeof legacyConvert === 'function') {
    return legacyConvert
  }
  return null
}

async function invokeJson<T>(invoke: TauriInvoke, command: string, args?: Record<string, unknown>): Promise<T> {
  const result = (await invoke(command, args)) as BridgeEnvelope<T>
  if (!result || result.ok !== true) {
    const error = result?.error || 'bridge invoke failed'
    const detail = result?.detail ? ` (${result.detail})` : ''
    throw new Error(`${error}${detail}`)
  }
  return result as unknown as T
}

async function fetchJson<T>(input: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(input, init)
  const data = (await resp.json()) as BridgeEnvelope<T>
  if (!resp.ok || !data || data.ok !== true) {
    const error = data?.error || `http ${resp.status}`
    const detail = data?.detail ? ` (${data.detail})` : ''
    throw new Error(`${error}${detail}`)
  }
  return data as unknown as T
}

function createMockBridge(): DesktopBridge {
  const store = new Map<string, MetadataItem>()

  return {
    provider: 'mock',
    async ping(): Promise<BridgeHealth> {
      return { provider: 'mock', version: 'ui-phase1' }
    },
    async listImages(folder: string, limit = 0): Promise<string[]> {
      const root = String(folder || '').trim()
      if (!root) return []
      if (store.size === 0) {
        const seed = [`${root}\\龚云龙.jpg`, `${root}\\艾冬.jpg`, `${root}\\王涛_3.jpg`]
        for (const p of seed) {
          store.set(p, buildMockRecord(p))
        }
      }
      let items = Array.from(store.keys()).filter((x) => x.startsWith(root))
      if (items.length === 0) {
        const fallback = `${root}\\样例人物.jpg`
        store.set(fallback, buildMockRecord(fallback))
        items = [fallback]
      }
      if (limit > 0) {
        items = items.slice(0, limit)
      }
      return clone(items)
    },
    async readMetadata(path: string): Promise<MetadataItem> {
      const key = String(path || '').trim()
      if (!key) throw new Error('path is required')
      if (!store.has(key)) {
        store.set(key, buildMockRecord(key))
      }
      return clone(store.get(key) as MetadataItem)
    },
    async saveMetadata(path: string, payload: SavePayload): Promise<void> {
      const key = String(path || '').trim()
      if (!key) throw new Error('path is required')
      const current = store.get(key) || buildMockRecord(key)
      const next: MetadataItem = {
        ...current,
        ...payload,
        keywords: Array.isArray(payload.keywords) ? [...payload.keywords] : [],
      }
      store.set(key, next)
    },
    getPreviewUrl(): string {
      return ''
    },
  }
}

function createTauriBridge(invoke: TauriInvoke): DesktopBridge {
  const convertFileSrc = getTauriConvertFileSrc()
  return {
    provider: 'tauri',
    async ping(): Promise<BridgeHealth> {
      return invokeJson<BridgeHealth>(invoke, 'bridge_ping')
    },
    async listImages(folder: string, limit = 0): Promise<string[]> {
      const data = await invokeJson<{ items: string[] }>(invoke, 'bridge_list_images', { folder, limit })
      return Array.isArray(data.items) ? data.items : []
    },
    async readMetadata(path: string): Promise<MetadataItem> {
      const data = await invokeJson<{ item: MetadataItem }>(invoke, 'bridge_read_metadata', { path })
      return data.item
    },
    async saveMetadata(path: string, payload: SavePayload): Promise<void> {
      await invokeJson<{ saved: boolean }>(invoke, 'bridge_save_metadata', { path, payload })
    },
    getPreviewUrl(path: string): string {
      if (typeof convertFileSrc === 'function') {
        return convertFileSrc(String(path || ''))
      }
      return ''
    },
  }
}

function createHttpBridge(): DesktopBridge {
  return {
    provider: 'vite-python-cli',
    async ping(): Promise<BridgeHealth> {
      return fetchJson<BridgeHealth>('/api/bridge/ping')
    },
    async listImages(folder: string, limit = 0): Promise<string[]> {
      const params = new URLSearchParams({
        folder: String(folder || ''),
        limit: String(limit || 0),
      })
      const data = await fetchJson<{ items: string[] }>(`/api/bridge/list?${params.toString()}`)
      return Array.isArray(data.items) ? data.items : []
    },
    async readMetadata(path: string): Promise<MetadataItem> {
      const params = new URLSearchParams({ path: String(path || '') })
      const data = await fetchJson<{ item: MetadataItem }>(`/api/bridge/read?${params.toString()}`)
      return data.item
    },
    async saveMetadata(path: string, payload: SavePayload): Promise<void> {
      await fetchJson<{ saved: boolean }>('/api/bridge/save', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          path,
          payload,
        }),
      })
    },
    getPreviewUrl(path: string): string {
      const params = new URLSearchParams({ path: String(path || '') })
      return `/api/bridge/preview?${params.toString()}`
    },
  }
}

export function createDesktopBridge(): DesktopBridge {
  const invoke = getTauriInvoke()
  if (invoke) {
    return createTauriBridge(invoke)
  }
  if (import.meta.env.DEV) {
    return createHttpBridge()
  }
  return createMockBridge()
}
