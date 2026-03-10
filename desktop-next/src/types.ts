export type BridgeProvider = 'mock' | 'tauri' | 'vite-python-cli'

export interface BridgeHealth {
  provider: string
  version: string
}

export interface ScraperTaskSummary {
  root: string
  task: string
  status: string
  profiles: number
  images: number
  metadata_ok: number
  pending: number
  review: number
  failures: number
  updated_at: string
  pid?: number
  session_running?: boolean
  manual_paused?: boolean
  runtime_state?: string
}

export interface ScraperProgressRow {
  idx: string
  name: string
  detail: string
  image: string
  meta: string
  reason: string
  detail_url: string
  image_path: string
}

export interface ScraperTaskDetail {
  root: string
  task: string
  status: string
  updated_at: string
  output_path: string
  log_path: string
  progress_text: string
  latest_action: string
  total_target: number
  discovered_rows: number
  downloaded_rows: number
  completed_rows: number
  list_rows: number
  profile_rows: number
  image_rows: number
  metadata_rows: number
  review_rows: number
  failure_rows: number
  pending_rows: ScraperProgressRow[]
  done_rows: ScraperProgressRow[]
  log_tail: string
  pid: number
  session_running: boolean
  manual_paused: boolean
  runtime_state: string
  can_pause: boolean
  can_continue: boolean
  can_retry: boolean
  can_rewrite_metadata: boolean
}

export type ScraperActionName = 'pause' | 'continue' | 'retry' | 'rewrite'

export interface ScraperControlOptions {
  mode: string
  auto_fallback: boolean
  disable_page_images: boolean
}

export interface ScraperActionResult {
  action: ScraperActionName
  message: string
  workspace: ScraperWorkspaceSnapshot
}

export interface ScraperWorkspaceSnapshot {
  base_root: string
  task_count: number
  status_text: string
  tasks: ScraperTaskSummary[]
  selected_root: string
  selected_task: ScraperTaskSummary | null
  detail: ScraperTaskDetail | null
  control_defaults: ScraperControlOptions
}

export interface RoleAliasPayload {
  name: string
  note?: string
  enabled?: boolean
}

export interface MetadataItem {
  filepath: string
  filename: string
  title: string
  description: string
  keywords: string[]
  source: string
  image_url: string
  city: string
  person: string
  gender: string
  position: string
  police_id?: string
  titi_asset_id: string
  titi_world_id: string
  filesize?: number
  modified_time?: string
  titi_json?: Record<string, unknown> | null
  other_xmp?: Record<string, unknown>
  other_exif?: Record<string, unknown>
  other_iptc?: Record<string, unknown>
  status?: string
  matched_row?: Record<string, unknown> | null
}

export interface SavePayload {
  title: string
  person: string
  gender: string
  position: string
  city: string
  source: string
  image_url: string
  keywords: string[]
  titi_asset_id: string
  titi_world_id: string
  description: string
  d2i_profile?: Record<string, unknown>
  role_aliases?: RoleAliasPayload[]
}

export interface DesktopBridge {
  provider: BridgeProvider
  ping(): Promise<BridgeHealth>
  listImages(folder: string, limit?: number): Promise<string[]>
  readMetadata(path: string): Promise<MetadataItem>
  saveMetadata(path: string, payload: SavePayload): Promise<void>
  getDefaultScraperBaseRoot(): Promise<string>
  readScraperWorkspace(
    baseRoot: string,
    options?: {
      selectedRoot?: string
      progressLimit?: number
      logLines?: number
    },
  ): Promise<ScraperWorkspaceSnapshot>
  runScraperAction(
    action: ScraperActionName,
    outputRoot: string,
    options?: {
      baseRoot?: string
      control?: Partial<ScraperControlOptions>
    },
  ): Promise<ScraperActionResult>
  getPreviewUrl(path: string): string
}
