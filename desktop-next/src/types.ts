export type BridgeProvider = 'mock' | 'tauri' | 'vite-native'

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

export interface ScraperReviewRow {
  idx: string
  detail_url: string
  name: string
  reason: string
  missing_fields: string[]
  scraped_at: string
  image_path: string
  detail: string
  image: string
  meta: string
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
  review_queue: ScraperReviewRow[]
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

export interface ScraperLaunchTemplateOption {
  label: string
  path: string
  status: string
  updated_at: string
}

export interface ScraperLaunchForm {
  start_url: string
  output_root: string
  interval_min: string
  interval_max: string
  timeout_seconds: string
  suspect_block_consecutive_failures: string
  jsl_enabled: boolean
  image_download_mode: string
  auto_fallback_to_browser: boolean
  disable_page_images_during_crawl: boolean
  output_minimal: boolean
  direct_write_images: boolean
  llm_enrich_enabled: boolean
  llm_model: string
  llm_api_base: string
  llm_api_key: string
  template_hint: string
  template_start_url: string
  save_generated_template: boolean
  cleanup_generated_template: boolean
  disable_template_persistence_controls: boolean
  url_locked: boolean
  selected_template_path: string
}

export interface ScraperLaunchState extends ScraperLaunchForm {
  templates: ScraperLaunchTemplateOption[]
}

export interface ScraperActionResult {
  action: ScraperActionName
  message: string
  workspace: ScraperWorkspaceSnapshot
}

export interface ScraperReviewClearResult {
  message: string
  removed: number
  workspace: ScraperWorkspaceSnapshot
}

export interface ScraperStartResult {
  message: string
  created_root: string
  config_path: string
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
  metadata_read_error?: string
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

export interface NameBarOptions {
  name: string
  output_dir?: string
  output_format?: string
  output_name?: 'suffix' | 'label'
  open_after_generate?: boolean
  reveal_after_generate?: boolean
}

export interface LlmSettings {
  enabled_default: boolean
  api_base: string
  api_key: string
  model: string
  timeout_seconds: number
  max_retries: number
  temperature: number
}

export interface NameBarSettings {
  output_format: string
  jpg_quality: number
  output_name_mode: 'suffix' | 'label'
  output_dir: string
  suffix: string
  bar_height_mode: string
  bar_height_ratio: number
  min_bar_height: number
  align: string
  bar_color: string
  text_color: string
  webp_lossless: boolean
}

export interface AppSettings {
  version: number
  updated_at: string
  llm: LlmSettings
  image_actions: {
    name_bar: NameBarSettings
    [key: string]: unknown
  }
  [key: string]: unknown
}

export interface AppSettingsResult {
  settings: AppSettings
  path: string
}

export interface NameBarResult {
  message: string
  output_path: string
  reveal_path?: string
}

export interface RenameImageResult {
  old_path: string
  new_path: string
  filename: string
}

export type MetadataAutofillInputMode = 'filename' | 'metadata' | 'filename_metadata'

export interface MetadataAutofillOptions {
  input_mode: MetadataAutofillInputMode
  form?: Record<string, unknown>
}

export interface MetadataAutofillResult {
  result: Record<string, unknown>
  input_mode: MetadataAutofillInputMode
}

export interface BiographyResult {
  result: {
    biography_short?: string
    description?: string
  }
}

export interface PathInfo {
  path: string
  exists: boolean
  is_file: boolean
  is_dir: boolean
}

export interface DesktopBridge {
  provider: BridgeProvider
  ping(): Promise<BridgeHealth>
  pickImage(initialFolder?: string): Promise<string>
  pickFolder(initialFolder?: string): Promise<string>
  getLaunchPath(): Promise<PathInfo>
  getPathInfo(path: string): Promise<PathInfo>
  listImages(folder: string, limit?: number): Promise<string[]>
  readMetadata(path: string): Promise<MetadataItem>
  saveMetadata(path: string, payload: SavePayload): Promise<void>
  addNameBar(path: string, options: NameBarOptions): Promise<NameBarResult>
  renameImage(path: string, newName: string): Promise<RenameImageResult>
  autofillMetadata(path: string, options: MetadataAutofillOptions): Promise<MetadataAutofillResult>
  generateBiography(path: string, options: { form?: Record<string, unknown> }): Promise<BiographyResult>
  readAppSettings(): Promise<AppSettingsResult>
  saveAppSettings(settings: AppSettings): Promise<AppSettingsResult>
  openPath(path: string): Promise<void>
  revealPath(path: string): Promise<void>
  getDefaultScraperBaseRoot(): Promise<string>
  readScraperWorkspace(
    baseRoot: string,
    options?: {
      selectedRoot?: string
      progressLimit?: number
      logLines?: number
    },
  ): Promise<ScraperWorkspaceSnapshot>
  readScraperLaunchState(sourceHint?: string, templatePath?: string): Promise<ScraperLaunchState>
  startScraperTask(values: ScraperLaunchForm, options?: { baseRoot?: string }): Promise<ScraperStartResult>
  runScraperAction(
    action: ScraperActionName,
    outputRoot: string,
    options?: {
      baseRoot?: string
      control?: Partial<ScraperControlOptions>
    },
  ): Promise<ScraperActionResult>
  clearScraperReviewItem(outputRoot: string, detailUrl: string, options?: { baseRoot?: string }): Promise<ScraperReviewClearResult>
  getPreviewUrl(path: string): string
}
