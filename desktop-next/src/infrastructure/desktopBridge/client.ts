import { convertFileSrc, invoke, isTauri } from '@tauri-apps/api/core'

import type {
  AppSettings,
  AppSettingsResult,
  BiographyResult,
  BridgeHealth,
  DesktopBridge,
  MetadataAutofillOptions,
  MetadataAutofillResult,
  MetadataItem,
  NameBarOptions,
  NameBarResult,
  PathInfo,
  RenameImageResult,
  SavePayload,
  ScraperActionName,
  ScraperActionResult,
  ScraperControlOptions,
  ScraperLaunchForm,
  ScraperLaunchState,
  ScraperReviewClearResult,
  ScraperStartResult,
  ScraperTaskDetail,
  ScraperTaskSummary,
  ScraperWorkspaceSnapshot,
} from '../../types.ts'

export type TauriInvoke = (command: string, args?: Record<string, unknown>) => Promise<unknown>
export type TauriConvertFileSrc = (filePath: string, protocol?: string) => string

export type BridgeEnvelope<T> = {
  ok: boolean
  error?: string
  detail?: string
} & T

export type DesktopSmokeRequest = {
  smoke_id: string
  folder: string
  marker: string
  filename?: string
  scraper_base_root?: string
  scraper_task_root?: string
}

const MOCK_SCRAPER_CONTROL_DEFAULTS: ScraperControlOptions = {
  mode: 'requests_jsl',
  auto_fallback: true,
  disable_page_images: true,
}

export function resolveDesktopBridgeProvider(hasTauriInvoke: boolean, isDev: boolean): DesktopBridge['provider'] {
  if (hasTauriInvoke) {
    return 'tauri'
  }
  if (isDev) {
    return 'vite-native'
  }
  return 'mock'
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function splitFileName(path: string): string {
  const normalized = String(path || '').replace(/\//g, '\\')
  const idx = normalized.lastIndexOf('\\')
  return idx >= 0 ? normalized.slice(idx + 1) : normalized
}

function normalizeWindowsPathText(value: string): string {
  let text = String(value || '').trim().replace(/^"+|"+$/g, '')
  if (!text) {
    return ''
  }
  text = text.replace(/\//g, '\\')
  while (text.startsWith('\\\\\\') && !text.startsWith('\\\\?\\')) {
    text = text.replace(/^\\\\\\/, '\\\\')
  }
  const lower = text.toLowerCase()
  if (lower.startsWith('\\\\?\\unc\\')) {
    return `\\\\${text.slice(8)}`
  }
  if (lower.startsWith('\\?\\unc\\')) {
    return `\\\\${text.slice(7)}`
  }
  if (lower.startsWith('?\\unc\\')) {
    return `\\\\${text.slice(6)}`
  }
  if (lower.startsWith('\\\\?\\')) {
    return text.slice(4)
  }
  if (lower.startsWith('\\?\\')) {
    return text.slice(3)
  }
  if (lower.startsWith('?\\')) {
    return text.slice(2)
  }
  return text
}

const MOCK_APP_SETTINGS: AppSettings = {
  version: 1,
  updated_at: '',
  llm: {
    enabled_default: false,
    api_base: '',
    api_key: '',
    model: '',
    timeout_seconds: 45,
    max_retries: 2,
    temperature: 0.1,
  },
  image_actions: {
    name_bar: {
      output_format: 'original',
      jpg_quality: 100,
      output_name_mode: 'suffix',
      output_dir: '',
      suffix: '_named',
      bar_height_mode: 'auto',
      bar_height_ratio: 0.14,
      min_bar_height: 48,
      align: 'center',
      bar_color: 'white',
      text_color: 'black',
      webp_lossless: true,
    },
  },
}

function joinSiblingPath(sourcePath: string, nextFileName: string): string {
  const normalized = String(sourcePath || '').replace(/\//g, '\\')
  const idx = normalized.lastIndexOf('\\')
  if (idx < 0) return nextFileName
  return `${normalized.slice(0, idx + 1)}${nextFileName}`
}

function extensionOf(path: string): string {
  const name = splitFileName(path)
  const idx = name.lastIndexOf('.')
  return idx >= 0 ? name.slice(idx) : ''
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

function buildMockScraperTask(root: string, status: string, updatedAt: string, profiles: number, failures: number): ScraperTaskSummary {
  const manualPaused = status.includes('暂停')
  const sessionRunning = !manualPaused && !status.includes('完成')
  return {
    root,
    task: splitFileName(root),
    status,
    profiles,
    images: Math.max(0, profiles - 1),
    metadata_ok: Math.max(0, profiles - failures),
    pending: Math.max(0, failures),
    review: failures > 0 ? 1 : 0,
    failures,
    updated_at: updatedAt,
    pid: sessionRunning ? 4242 : 0,
    session_running: sessionRunning,
    manual_paused: manualPaused,
    runtime_state: manualPaused ? '已暂停(手动)' : status.includes('完成') ? '已完成' : '运行中',
  }
}

function buildMockScraperDetail(task: ScraperTaskSummary): ScraperTaskDetail {
  const pendingRows = task.failures
    ? [
        {
          idx: '1',
          name: '示例人物',
          detail: '√',
          image: '×',
          meta: '×',
          reason: '图片下载失败：HTTP 错误',
          detail_url: 'https://example.com/detail/1',
          image_path: '',
        },
      ]
    : []
  const doneRows = [
    {
      idx: '2',
      name: '已完成人物',
      detail: '√',
      image: '√',
      meta: '√',
      reason: '',
      detail_url: 'https://example.com/detail/2',
      image_path: `${task.root}\\named_images\\done.jpg`,
    },
  ]
  return {
    root: task.root,
    task: task.task,
    status: task.status,
    updated_at: task.updated_at,
    output_path: `${task.root}\\named_images`,
    log_path: `${task.root}\\reports\\gui_public_scraper.log`,
    progress_text: `抓取中 下载:${task.images}/${task.profiles}(80.0%) 发现:${task.profiles}/${task.profiles}(100.0%) 完成:${doneRows.length} 列表:${task.profiles} 详情:${task.profiles} 图片:${task.images} 元数据:${task.metadata_ok}`,
    latest_action: task.failures ? '下载图片' : '写入元数据',
    total_target: task.profiles,
    discovered_rows: task.profiles,
    downloaded_rows: task.images,
    completed_rows: doneRows.length,
    list_rows: task.profiles,
    profile_rows: task.profiles,
    image_rows: task.images,
    metadata_rows: task.metadata_ok,
    review_rows: task.review,
    failure_rows: task.failures,
    review_queue: task.review
      ? [
          {
            idx: '1',
            detail_url: 'https://example.com/detail/review',
            name: '待复核人物',
            reason: 'audit_missing_metadata_fields:gender,birth_date',
            missing_fields: ['gender', 'birth_date'],
            scraped_at: task.updated_at,
            image_path: `${task.root}\\named_images\\review.jpg`,
            detail: '√',
            image: '√',
            meta: '×',
          },
        ]
      : [],
    pending_rows: pendingRows,
    done_rows: doneRows,
    log_tail: `[${task.updated_at}] ${task.task}\n已载入示例抓取日志。\n`,
    pid: task.pid || 0,
    session_running: Boolean(task.session_running),
    manual_paused: Boolean(task.manual_paused),
    runtime_state: String(task.runtime_state || task.status || ''),
    can_pause: Boolean(task.session_running) && !task.manual_paused,
    can_continue: Boolean(task.manual_paused) || !task.session_running,
    can_retry: !task.session_running,
    can_rewrite_metadata: !task.session_running,
  }
}

function buildMockScraperWorkspace(baseRoot: string, selectedRoot?: string): ScraperWorkspaceSnapshot {
  const root = String(baseRoot || '').trim() || 'C:\\mock\\public_archive'
  const tasks = [
    buildMockScraperTask(`${root}\\tiantonglaw_team`, '未完成', '2026-03-10 12:30:00', 16, 2),
    buildMockScraperTask(`${root}\\wenming_candidates`, '已完成', '2026-03-09 18:12:00', 12, 0),
  ]
  const activeRoot = selectedRoot && tasks.some((task) => task.root === selectedRoot) ? selectedRoot : tasks[0]?.root || ''
  const selectedTask = tasks.find((task) => task.root === activeRoot) || null
  return {
    base_root: root,
    task_count: tasks.length,
    status_text: `任务数: ${tasks.length}`,
    tasks,
    selected_root: activeRoot,
    selected_task: selectedTask,
    detail: selectedTask ? buildMockScraperDetail(selectedTask) : null,
    control_defaults: clone(MOCK_SCRAPER_CONTROL_DEFAULTS),
  }
}

function buildMockScraperLaunchState(sourceHint = ''): ScraperLaunchState {
  return {
    start_url: sourceHint || 'https://example.com/list',
    output_root: 'C:\\mock\\public_archive\\example',
    interval_min: '5',
    interval_max: '8',
    timeout_seconds: '30',
    suspect_block_consecutive_failures: '3',
    jsl_enabled: true,
    image_download_mode: 'requests_jsl',
    auto_fallback_to_browser: true,
    disable_page_images_during_crawl: true,
    output_minimal: true,
    direct_write_images: true,
    llm_enrich_enabled: false,
    llm_model: 'qwen2.5:7b-instruct',
    llm_api_base: 'http://127.0.0.1:11434/v1',
    llm_api_key: '',
    template_hint: '未选择模板时，需手动输入链接。',
    template_start_url: '',
    save_generated_template: true,
    cleanup_generated_template: true,
    disable_template_persistence_controls: false,
    url_locked: false,
    selected_template_path: '',
    templates: [],
  }
}

function getTauriInvoke(): TauriInvoke | null {
  if (isTauri()) {
    return (command: string, args?: Record<string, unknown>) => invoke(command, args)
  }
  return null
}

function getTauriConvertFileSrc(): TauriConvertFileSrc | null {
  if (isTauri()) {
    return convertFileSrc
  }
  return null
}

export async function fetchJson<T>(input: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(input, init)
  const data = (await resp.json()) as BridgeEnvelope<T>
  if (!resp.ok || !data || data.ok !== true) {
    const error = data?.error || `http ${resp.status}`
    const detail = data?.detail ? ` (${data.detail})` : ''
    throw new Error(`${error}${detail}`)
  }
  return data as unknown as T
}

export async function invokeJson<T>(invokeImpl: TauriInvoke, command: string, args?: Record<string, unknown>): Promise<T> {
  const result = (await invokeImpl(command, args)) as BridgeEnvelope<T>
  if (!result || result.ok !== true) {
    const error = result?.error || 'bridge invoke failed'
    const detail = result?.detail ? ` (${result.detail})` : ''
    throw new Error(`${error}${detail}`)
  }
  return result as unknown as T
}

export async function reportDesktopFrontendStatus(payload: Record<string, unknown>): Promise<void> {
  if (!import.meta.env.DEV) {
    return
  }
  await fetchJson<{ reported: boolean }>('/api/bridge/frontend-status', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload ?? {}),
  })
}

export async function fetchDesktopSmokeRequest(): Promise<DesktopSmokeRequest | null> {
  if (!import.meta.env.DEV) {
    return null
  }
  const resp = await fetch('/api/bridge/smoke-request', {
    method: 'GET',
    cache: 'no-store',
  })
  if (resp.status === 404) {
    return null
  }
  const data = (await resp.json()) as BridgeEnvelope<{ request: DesktopSmokeRequest }>
  if (!resp.ok || !data || data.ok !== true || !data.request) {
    const error = data?.error || `http ${resp.status}`
    const detail = data?.detail ? ` (${data.detail})` : ''
    throw new Error(`${error}${detail}`)
  }
  return data.request
}

export async function reportDesktopSmokeResult(payload: Record<string, unknown>): Promise<void> {
  if (!import.meta.env.DEV) {
    return
  }
  await fetchJson<{ reported: boolean }>('/api/bridge/smoke-report', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload ?? {}),
  })
}

function createMockBridge(): DesktopBridge {
  const store = new Map<string, MetadataItem>()

  return {
    provider: 'mock',
    async ping(): Promise<BridgeHealth> {
      return { provider: 'mock', version: 'ui-phase1' }
    },
    async pickImage(initialFolder?: string): Promise<string> {
      const root = String(initialFolder || '').trim() || 'C:\\mock\\public_archive'
      return `${root}\\样例人物.jpg`
    },
    async pickFolder(initialFolder?: string): Promise<string> {
      return String(initialFolder || '').trim() || 'C:\\mock\\public_archive'
    },
    async getLaunchPath(): Promise<PathInfo> {
      return {
        path: '',
        exists: false,
        is_file: false,
        is_dir: false,
      }
    },
    async getPathInfo(path: string): Promise<PathInfo> {
      const text = normalizeWindowsPathText(path)
      return {
        path: text,
        exists: Boolean(text),
        is_file: /\.[a-z0-9]+$/i.test(text),
        is_dir: Boolean(text) && !/\.[a-z0-9]+$/i.test(text),
      }
    },
    async listImages(folder: string, limit = 0): Promise<string[]> {
      const root = String(folder || '').trim()
      if (!root) return []
      if (store.size === 0) {
        const seed = [`${root}\\龚云龙.jpg`, `${root}\\艾冬.jpg`, `${root}\\王涛_3.jpg`]
        for (const path of seed) {
          store.set(path, buildMockRecord(path))
        }
      }
      let items = Array.from(store.keys()).filter((path) => path.startsWith(root))
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
    async addNameBar(path: string, options: NameBarOptions): Promise<NameBarResult> {
      const name = String(options?.name || '').trim()
      if (!path) throw new Error('path is required')
      if (!name) throw new Error('name is required')
      return {
        message: `mock name bar: ${name}`,
        output_path: joinSiblingPath(path, `${name}.png`),
      }
    },
    async renameImage(path: string, newName: string): Promise<RenameImageResult> {
      const name = String(newName || '').trim()
      if (!path) throw new Error('path is required')
      if (!name) throw new Error('new name is required')
      const oldRecord = store.get(path) || buildMockRecord(path)
      const newPath = joinSiblingPath(path, `${name}${extensionOf(path)}`)
      store.delete(path)
      store.set(newPath, {
        ...oldRecord,
        filepath: newPath,
        filename: splitFileName(newPath),
      })
      return { old_path: path, new_path: newPath, filename: splitFileName(newPath) }
    },
    async autofillMetadata(path: string, options: MetadataAutofillOptions): Promise<MetadataAutofillResult> {
      if (!path) throw new Error('path is required')
      const inputMode = options?.input_mode || 'filename_metadata'
      return {
        input_mode: inputMode,
        result: {
          title: inputMode === 'filename' ? splitFileName(path).replace(/\.[^.]+$/, '') : '',
          keywords: ['样例'],
          extra_fields: {
            ai_mock: true,
          },
        },
      }
    },
    async generateBiography(path: string): Promise<BiographyResult> {
      if (!path) throw new Error('path is required')
      return {
        result: {
          biography_short: '样例小传',
          description: '样例小传',
        },
      }
    },
    async readAppSettings(): Promise<AppSettingsResult> {
      return { settings: clone(MOCK_APP_SETTINGS), path: 'mock-settings.json' }
    },
    async saveAppSettings(settings: AppSettings): Promise<AppSettingsResult> {
      return { settings: clone(settings), path: 'mock-settings.json' }
    },
    async openPath(path: string): Promise<void> {
      if (!path) throw new Error('path is required')
    },
    async revealPath(path: string): Promise<void> {
      if (!path) throw new Error('path is required')
    },
    async getDefaultScraperBaseRoot(): Promise<string> {
      return 'C:\\mock\\public_archive'
    },
    async readScraperWorkspace(
      baseRoot: string,
      options?: {
        selectedRoot?: string
      },
    ): Promise<ScraperWorkspaceSnapshot> {
      return buildMockScraperWorkspace(baseRoot, options?.selectedRoot)
    },
    async readScraperLaunchState(sourceHint?: string): Promise<ScraperLaunchState> {
      return buildMockScraperLaunchState(String(sourceHint || ''))
    },
    async startScraperTask(values: ScraperLaunchForm): Promise<ScraperStartResult> {
      const outputRoot = String(values.output_root || 'C:\\mock\\public_archive\\example')
      return {
        message: 'mock start',
        created_root: outputRoot,
        config_path: `${outputRoot}\\state\\runtime_config.json`,
        workspace: buildMockScraperWorkspace('C:\\mock\\public_archive', outputRoot),
      }
    },
    async runScraperAction(action: ScraperActionName, outputRoot: string): Promise<ScraperActionResult> {
      return {
        action,
        message: `mock action: ${action}`,
        workspace: buildMockScraperWorkspace('C:\\mock\\public_archive', outputRoot),
      }
    },
    async clearScraperReviewItem(outputRoot: string): Promise<ScraperReviewClearResult> {
      return {
        message: 'mock review cleared',
        removed: 1,
        workspace: buildMockScraperWorkspace('C:\\mock\\public_archive', outputRoot),
      }
    },
    getPreviewUrl(): string {
      return ''
    },
  }
}

function createTauriBridge(invokeImpl: TauriInvoke): DesktopBridge {
  const convertFileSrcImpl = getTauriConvertFileSrc()
  return {
    provider: 'tauri',
    async ping(): Promise<BridgeHealth> {
      return invokeJson<BridgeHealth>(invokeImpl, 'bridge_ping')
    },
    async pickImage(initialFolder?: string): Promise<string> {
      const data = await invokeJson<{ path: string; canceled?: boolean }>(invokeImpl, 'bridge_pick_image', {
        initialFolder: initialFolder || '',
      })
      return String(data.path || '')
    },
    async pickFolder(initialFolder?: string): Promise<string> {
      const data = await invokeJson<{ path: string; canceled?: boolean }>(invokeImpl, 'bridge_pick_folder', {
        initialFolder: initialFolder || '',
      })
      return String(data.path || '')
    },
    async getLaunchPath(): Promise<PathInfo> {
      return invokeJson<PathInfo>(invokeImpl, 'bridge_launch_path')
    },
    async getPathInfo(path: string): Promise<PathInfo> {
      return invokeJson<PathInfo>(invokeImpl, 'bridge_path_info', { path })
    },
    async listImages(folder: string, limit = 0): Promise<string[]> {
      const data = await invokeJson<{ items: string[] }>(invokeImpl, 'bridge_list_images', { folder, limit })
      return Array.isArray(data.items) ? data.items : []
    },
    async readMetadata(path: string): Promise<MetadataItem> {
      const data = await invokeJson<{ item: MetadataItem }>(invokeImpl, 'bridge_read_metadata', { path })
      return data.item
    },
    async saveMetadata(path: string, payload: SavePayload): Promise<void> {
      await invokeJson<{ saved: boolean }>(invokeImpl, 'bridge_save_metadata', { path, payload })
    },
    async addNameBar(path: string, options: NameBarOptions): Promise<NameBarResult> {
      return invokeJson<NameBarResult>(invokeImpl, 'bridge_add_name_bar', { path, options })
    },
    async renameImage(path: string, newName: string): Promise<RenameImageResult> {
      return invokeJson<RenameImageResult>(invokeImpl, 'bridge_rename_image', { path, newName })
    },
    async autofillMetadata(path: string, options: MetadataAutofillOptions): Promise<MetadataAutofillResult> {
      return invokeJson<MetadataAutofillResult>(invokeImpl, 'bridge_autofill_metadata', { path, options })
    },
    async generateBiography(path: string, options: { form?: Record<string, unknown> }): Promise<BiographyResult> {
      return invokeJson<BiographyResult>(invokeImpl, 'bridge_generate_biography', { path, options })
    },
    async readAppSettings(): Promise<AppSettingsResult> {
      return invokeJson<AppSettingsResult>(invokeImpl, 'bridge_read_app_settings')
    },
    async saveAppSettings(settings: AppSettings): Promise<AppSettingsResult> {
      return invokeJson<AppSettingsResult>(invokeImpl, 'bridge_save_app_settings', { settings })
    },
    async openPath(path: string): Promise<void> {
      await invokeJson<{ opened: boolean }>(invokeImpl, 'bridge_open_path', { path })
    },
    async revealPath(path: string): Promise<void> {
      await invokeJson<{ revealed: boolean }>(invokeImpl, 'bridge_reveal_path', { path })
    },
    async getDefaultScraperBaseRoot(): Promise<string> {
      const data = await invokeJson<{ base_root: string }>(invokeImpl, 'bridge_get_default_scraper_base_root')
      return String(data.base_root || '')
    },
    async readScraperWorkspace(
      baseRoot: string,
      options?: {
        selectedRoot?: string
        progressLimit?: number
        logLines?: number
      },
    ): Promise<ScraperWorkspaceSnapshot> {
      return invokeJson<ScraperWorkspaceSnapshot>(invokeImpl, 'bridge_read_scraper_workspace', {
        baseRoot,
        selectedRoot: options?.selectedRoot || '',
        progressLimit: options?.progressLimit || 300,
        logLines: options?.logLines || 80,
      })
    },
    async readScraperLaunchState(sourceHint?: string, templatePath?: string): Promise<ScraperLaunchState> {
      return invokeJson<ScraperLaunchState>(invokeImpl, 'bridge_read_scraper_launch_state', {
        sourceHint: sourceHint || '',
        templatePath: templatePath || '',
      })
    },
    async startScraperTask(values: ScraperLaunchForm, options?: { baseRoot?: string }): Promise<ScraperStartResult> {
      return invokeJson<ScraperStartResult>(invokeImpl, 'bridge_start_scraper_task', {
        values,
        baseRoot: options?.baseRoot || '',
      })
    },
    async runScraperAction(
      action: ScraperActionName,
      outputRoot: string,
      options?: {
        baseRoot?: string
        control?: Partial<ScraperControlOptions>
      },
    ): Promise<ScraperActionResult> {
      return invokeJson<ScraperActionResult>(invokeImpl, 'bridge_run_scraper_action', {
        action,
        outputRoot,
        baseRoot: options?.baseRoot || '',
        control: options?.control || {},
      })
    },
    async clearScraperReviewItem(
      outputRoot: string,
      detailUrl: string,
      options?: { baseRoot?: string },
    ): Promise<ScraperReviewClearResult> {
      return invokeJson<ScraperReviewClearResult>(invokeImpl, 'bridge_clear_scraper_review_item', {
        outputRoot,
        detailUrl,
        baseRoot: options?.baseRoot || '',
      })
    },
    getPreviewUrl(path: string): string {
      if (typeof convertFileSrcImpl === 'function') {
        return convertFileSrcImpl(normalizeWindowsPathText(path))
      }
      return ''
    },
  }
}

function createHttpBridge(): DesktopBridge {
  return {
    provider: 'vite-native',
    async ping(): Promise<BridgeHealth> {
      return fetchJson<BridgeHealth>('/api/bridge/ping')
    },
    async pickImage(initialFolder?: string): Promise<string> {
      return String(initialFolder || '').trim()
    },
    async pickFolder(initialFolder?: string): Promise<string> {
      return String(initialFolder || '').trim()
    },
    async getLaunchPath(): Promise<PathInfo> {
      return {
        path: '',
        exists: false,
        is_file: false,
        is_dir: false,
      }
    },
    async getPathInfo(path: string): Promise<PathInfo> {
      const params = new URLSearchParams({ path: normalizeWindowsPathText(path) })
      return fetchJson<PathInfo>(`/api/bridge/path-info?${params.toString()}`)
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
    async addNameBar(path: string, options: NameBarOptions): Promise<NameBarResult> {
      return fetchJson<NameBarResult>('/api/bridge/name-bar', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ path, options }),
      })
    },
    async renameImage(path: string, newName: string): Promise<RenameImageResult> {
      return fetchJson<RenameImageResult>('/api/bridge/rename', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ path, newName }),
      })
    },
    async autofillMetadata(path: string, options: MetadataAutofillOptions): Promise<MetadataAutofillResult> {
      return fetchJson<MetadataAutofillResult>('/api/bridge/autofill-metadata', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ path, options }),
      })
    },
    async generateBiography(path: string, options: { form?: Record<string, unknown> }): Promise<BiographyResult> {
      return fetchJson<BiographyResult>('/api/bridge/generate-biography', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ path, options }),
      })
    },
    async readAppSettings(): Promise<AppSettingsResult> {
      return fetchJson<AppSettingsResult>('/api/bridge/settings')
    },
    async saveAppSettings(settings: AppSettings): Promise<AppSettingsResult> {
      return fetchJson<AppSettingsResult>('/api/bridge/settings', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ settings }),
      })
    },
    async openPath(path: string): Promise<void> {
      await fetchJson<{ opened: boolean }>('/api/bridge/open-path', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ path }),
      })
    },
    async revealPath(path: string): Promise<void> {
      await fetchJson<{ revealed: boolean }>('/api/bridge/reveal-path', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ path }),
      })
    },
    async getDefaultScraperBaseRoot(): Promise<string> {
      const data = await fetchJson<{ base_root: string }>('/api/bridge/scraper/default-root')
      return String(data.base_root || '')
    },
    async readScraperWorkspace(
      baseRoot: string,
      options?: {
        selectedRoot?: string
        progressLimit?: number
        logLines?: number
      },
    ): Promise<ScraperWorkspaceSnapshot> {
      const params = new URLSearchParams({
        baseRoot: String(baseRoot || ''),
        selectedRoot: String(options?.selectedRoot || ''),
        progressLimit: String(options?.progressLimit || 300),
        logLines: String(options?.logLines || 80),
      })
      return fetchJson<ScraperWorkspaceSnapshot>(`/api/bridge/scraper/workspace?${params.toString()}`)
    },
    async readScraperLaunchState(sourceHint?: string, templatePath?: string): Promise<ScraperLaunchState> {
      const params = new URLSearchParams({
        sourceHint: String(sourceHint || ''),
        templatePath: String(templatePath || ''),
      })
      return fetchJson<ScraperLaunchState>(`/api/bridge/scraper/launch-state?${params.toString()}`)
    },
    async startScraperTask(values: ScraperLaunchForm, options?: { baseRoot?: string }): Promise<ScraperStartResult> {
      return fetchJson<ScraperStartResult>('/api/bridge/scraper/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          values,
          baseRoot: options?.baseRoot || '',
        }),
      })
    },
    async runScraperAction(
      action: ScraperActionName,
      outputRoot: string,
      options?: {
        baseRoot?: string
        control?: Partial<ScraperControlOptions>
      },
    ): Promise<ScraperActionResult> {
      return fetchJson<ScraperActionResult>('/api/bridge/scraper/action', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          action,
          outputRoot,
          baseRoot: options?.baseRoot || '',
          control: options?.control || {},
        }),
      })
    },
    async clearScraperReviewItem(
      outputRoot: string,
      detailUrl: string,
      options?: { baseRoot?: string },
    ): Promise<ScraperReviewClearResult> {
      return fetchJson<ScraperReviewClearResult>('/api/bridge/scraper/review-clear', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          outputRoot,
          detailUrl,
          baseRoot: options?.baseRoot || '',
        }),
      })
    },
    getPreviewUrl(path: string): string {
      const params = new URLSearchParams({ path: normalizeWindowsPathText(path) })
      return `/api/bridge/preview?${params.toString()}`
    },
  }
}

export function createDesktopBridge(): DesktopBridge {
  const invokeImpl = getTauriInvoke()
  const provider = resolveDesktopBridgeProvider(Boolean(invokeImpl), Boolean(import.meta.env.DEV))
  if (provider === 'tauri' && invokeImpl) {
    return createTauriBridge(invokeImpl)
  }
  if (provider === 'vite-native') {
    return createHttpBridge()
  }
  return createMockBridge()
}
