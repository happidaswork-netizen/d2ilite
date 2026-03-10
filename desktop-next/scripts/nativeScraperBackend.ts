import { spawn } from 'node:child_process'
import { existsSync, openSync } from 'node:fs'
import { mkdir, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

type JsonRecord = Record<string, unknown>

type RegistryEntry = {
  pid: number
  config_path: string
  log_path: string
  named_dir: string
  runtime_state: string
  manual_paused: boolean
  active_template_path: string
  last_exit_code: number | null
  updated_at_ts: number
  started_at: number
}

type RegistryPayload = {
  active_root: string
  tasks: Record<string, RegistryEntry>
  updated_at: string
}

type ContinueOptions = {
  mode: string
  auto_fallback: boolean
  disable_page_images: boolean
}

type StartExistingTaskOptions = {
  skip_crawl?: boolean
  skip_images?: boolean
  skip_metadata?: boolean
  runtime_state?: string
  mode_override?: string
  auto_fallback_override?: boolean
  disable_page_images_override?: boolean
}

type ScraperLaunchTemplateOption = {
  label: string
  path: string
  status: string
  updated_at: string
}

type ScraperLaunchValues = {
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

const scriptsRoot = path.dirname(fileURLToPath(import.meta.url))
const desktopRoot = path.resolve(scriptsRoot, '..')
const projectRoot = path.resolve(desktopRoot, '..')
const tempRoot = path.join(projectRoot, '.tmp', 'desktop-next')
const registryPath = path.join(tempRoot, 'scraper-runtime-registry.json')
const runningLikeStates = new Set(['运行中', '继续运行中', '失败重试中', '元数据重写中'])
const VALID_IMAGE_DOWNLOAD_MODES = new Set(['requests_jsl', 'browser'])
const DEFAULT_TEMPLATE_HINT = '未选择模板时，需手动输入链接。'
const okTokens = new Set(['√', '✓'])
const controlDefaults: ContinueOptions = {
  mode: 'requests_jsl',
  auto_fallback: true,
  disable_page_images: true,
}
const skipDirNames = new Set(['raw', 'downloads', 'reports', 'state', '__pycache__'])

export const NATIVE_SCRAPER_PROVIDER = 'native-scraper-runtime'
export const NATIVE_SCRAPER_VERSION = 'scraper-native-v1'

function asRecord(value: unknown): JsonRecord | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }
  return value as JsonRecord
}

function normalizePublicTaskRoot(value: unknown): string {
  const raw = String(value || '').trim()
  return raw ? path.resolve(raw) : ''
}

function ensureNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeRegistryEntry(entry: unknown): RegistryEntry {
  const data = asRecord(entry) || {}
  const lastExitCodeRaw = data.last_exit_code
  const lastExitCode =
    lastExitCodeRaw == null || String(lastExitCodeRaw).trim() === '' ? null : Math.trunc(ensureNumber(lastExitCodeRaw, 0))

  return {
    pid: Math.max(0, Math.trunc(ensureNumber(data.pid, 0))),
    config_path: String(data.config_path || '').trim(),
    log_path: String(data.log_path || '').trim(),
    named_dir: String(data.named_dir || '').trim(),
    runtime_state: String(data.runtime_state || '').trim(),
    manual_paused: Boolean(data.manual_paused),
    active_template_path: String(data.active_template_path || '').trim(),
    last_exit_code: lastExitCode,
    updated_at_ts: ensureNumber(data.updated_at_ts, 0),
    started_at: ensureNumber(data.started_at, 0),
  }
}

function registryDefaultPayload(): RegistryPayload {
  return {
    active_root: '',
    tasks: {},
    updated_at: '',
  }
}

async function loadRuntimeRegistry(): Promise<RegistryPayload> {
  if (!existsSync(registryPath)) {
    return registryDefaultPayload()
  }
  try {
    const raw = JSON.parse(await readFile(registryPath, 'utf-8')) as unknown
    const data = asRecord(raw) || {}
    const tasksRaw = asRecord(data.tasks) || {}
    const tasks: Record<string, RegistryEntry> = {}
    for (const [root, entry] of Object.entries(tasksRaw)) {
      const normalized = normalizePublicTaskRoot(root)
      if (!normalized) {
        continue
      }
      tasks[normalized] = normalizeRegistryEntry(entry)
    }
    return {
      active_root: normalizePublicTaskRoot(data.active_root),
      tasks,
      updated_at: String(data.updated_at || '').trim(),
    }
  } catch {
    return registryDefaultPayload()
  }
}

async function saveRuntimeRegistry(registry: RegistryPayload): Promise<void> {
  await mkdir(tempRoot, { recursive: true })
  const tasks: Record<string, RegistryEntry> = {}
  for (const [root, entry] of Object.entries(registry.tasks || {})) {
    const normalized = normalizePublicTaskRoot(root)
    if (!normalized) {
      continue
    }
    tasks[normalized] = normalizeRegistryEntry(entry)
  }
  const payload: RegistryPayload = {
    active_root: normalizePublicTaskRoot(registry.active_root),
    tasks,
    updated_at: new Date().toISOString(),
  }
  await writeFile(registryPath, JSON.stringify(payload, null, 2), 'utf-8')
}

function isPidRunning(pid: number): boolean {
  const target = Math.max(0, Math.trunc(pid || 0))
  if (!target) {
    return false
  }
  try {
    process.kill(target, 0)
    return true
  } catch (error) {
    const code = (error as NodeJS.ErrnoException)?.code || ''
    return code === 'EPERM'
  }
}

function entryRunning(entry: RegistryEntry | null | undefined): boolean {
  return Boolean(entry && isPidRunning(entry.pid))
}

function countJsonlRowsFromText(text: string): number {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean).length
}

async function readJsonFile(filePath: string): Promise<JsonRecord> {
  const target = String(filePath || '').trim()
  if (!target || !existsSync(target)) {
    return {}
  }
  try {
    const payload = JSON.parse(await readFile(target, 'utf-8')) as unknown
    return asRecord(payload) || {}
  } catch {
    return {}
  }
}

async function readJsonlRows(filePath: string, maxRows = 0): Promise<JsonRecord[]> {
  const target = String(filePath || '').trim()
  if (!target || !existsSync(target)) {
    return []
  }
  try {
    const body = await readFile(target, 'utf-8')
    const rows: JsonRecord[] = []
    for (const line of body.split(/\r?\n/)) {
      const trimmed = line.trim()
      if (!trimmed) {
        continue
      }
      try {
        const payload = JSON.parse(trimmed) as unknown
        const record = asRecord(payload)
        if (record) {
          rows.push(record)
          if (maxRows > 0 && rows.length >= maxRows) {
            break
          }
        }
      } catch {
        continue
      }
    }
    return rows
  } catch {
    return []
  }
}

async function countJsonlRows(filePath: string): Promise<number> {
  const target = String(filePath || '').trim()
  if (!target || !existsSync(target)) {
    return 0
  }
  try {
    return countJsonlRowsFromText(await readFile(target, 'utf-8'))
  } catch {
    return 0
  }
}

async function countLatestMetadataStatus(root: string): Promise<{ ok: number; failed: number }> {
  const latest = new Map<string, string>()
  const rows = await readJsonlRows(path.join(root, 'raw', 'metadata_write_results.jsonl'))
  for (const row of rows) {
    const detailUrl = String(row.detail_url || '').trim()
    if (!detailUrl) {
      continue
    }
    latest.set(detailUrl, String(row.status || '').trim().toLowerCase())
  }
  let ok = 0
  let failed = 0
  for (const status of latest.values()) {
    if (status === 'ok') {
      ok += 1
    } else if (status) {
      failed += 1
    }
  }
  return { ok, failed }
}

async function readScraperBackoffState(root: string): Promise<{ blocked_until: string; blocked_reason: string }> {
  const payload = await readJsonFile(path.join(root, 'state', 'backoff_state.json'))
  return {
    blocked_until: String(payload.blocked_until || '').trim(),
    blocked_reason: String(payload.blocked_reason || '').trim(),
  }
}

function publicScraperPauseFlagPath(root: string): string {
  return path.join(root, 'state', 'manual_pause.flag')
}

async function setPublicScraperManualPauseFlag(root: string, paused: boolean): Promise<boolean> {
  const flagPath = publicScraperPauseFlagPath(root)
  try {
    if (paused) {
      await mkdir(path.dirname(flagPath), { recursive: true })
      await writeFile(
        flagPath,
        JSON.stringify(
          {
            paused: true,
            updated_at: new Date().toISOString(),
          },
          null,
          2,
        ),
        'utf-8',
      )
    } else if (existsSync(flagPath)) {
      await rm(flagPath, { force: true })
    }
    return true
  } catch {
    return false
  }
}

async function discoverPublicTaskRoots(baseRoot: string): Promise<string[]> {
  const base = normalizePublicTaskRoot(baseRoot)
  if (!base || !existsSync(base)) {
    return []
  }

  const roots = new Set<string>()
  const stack = [base]
  while (stack.length > 0) {
    const current = stack.pop()
    if (!current) {
      continue
    }
    const runtimeConfigPath = path.join(current, 'state', 'runtime_config.json')
    if (existsSync(runtimeConfigPath)) {
      roots.add(path.resolve(current))
      continue
    }
    let entries
    try {
      entries = await readdir(current, { withFileTypes: true })
    } catch {
      continue
    }
    for (const entry of entries) {
      if (!entry.isDirectory() || skipDirNames.has(entry.name)) {
        continue
      }
      stack.push(path.join(current, entry.name))
    }
  }
  return Array.from(roots).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }))
}

function normalizeExistingPath(value: unknown): string {
  const target = String(value || '').trim()
  if (!target) {
    return ''
  }
  const resolved = path.resolve(target)
  return existsSync(resolved) ? resolved : ''
}

function jsonClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function normalizeHttpUrl(value: unknown): string {
  const raw = String(value || '').trim()
  if (!raw) {
    return ''
  }
  try {
    const parsed = new URL(raw)
    if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
      return parsed.toString()
    }
  } catch {
    return raw
  }
  return raw
}

function defaultPublicScraperTemplate(): JsonRecord {
  return {
    site_name: 'generic_profiles',
    start_urls: ['https://example.org/list'],
    allowed_domains: ['example.org'],
    user_agent: 'D2ILiteArchiveBot/1.0 (+local archival use)',
    default_headers: {
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    },
    image_headers: {},
    output_root: 'data/public_archive/generic_profiles',
    selectors: {
      list_item:
        'article a[href], .list a[href], .news a[href], .item a[href], li a[href], dl dd a[href], a[href]',
      name: ['::text', 'img::attr(alt)', 'img::attr(title)'],
      detail_link: '::attr(href)',
      list_fields: {},
      next_page: [
        'a.next::attr(href)',
        "a[rel='next']::attr(href)",
        "xpath://a[contains(@class,'next')]/@href",
        "xpath://a[contains(normalize-space(),'下一页')]/@href",
        "xpath://a[contains(normalize-space(),'下页')]/@href",
      ],
      detail_name: [
        'h1::text',
        'h2::text',
        '.title::text',
        '.name::text',
        "meta[property='og:title']::attr(content)",
        'title::text',
      ],
      detail_image: [
        "meta[property='og:image']::attr(content)",
        '.article img::attr(src)',
        '.content img::attr(src)',
        '.detail img::attr(src)',
        '.main img::attr(src)',
        'img::attr(src)',
      ],
      detail_gender: ['.gender::text', "xpath:string(//*[contains(normalize-space(),'性别')][1])"],
      detail_summary: [
        '.article p::text',
        '.content p::text',
        '.detail p::text',
        '.main p::text',
        'article p::text',
        'p::text',
      ],
      detail_full_text: [],
      detail_fields: {},
      detail_field_labels: {},
    },
    rules: {
      obey_robots_txt: false,
      snapshot_html: true,
      extract_images: true,
      write_metadata: true,
      retry_failed_first: true,
      metadata_write_retries: 3,
      metadata_write_retry_delay_seconds: 1.2,
      metadata_write_retry_backoff_factor: 1.5,
      named_images_dir: '',
      image_referer_from_detail_url: true,
      required_fields: ['name', 'detail_url', 'image_url'],
      default_gender: '',
      gender_map: { 男: 'male', 女: 'female' },
      field_map: {},
      detail_field_labels: {},
      auto_unit_subdir: false,
      unit_name: '',
      output_subdir_pattern: '{unit}{year_suffix}',
      year_hint: '',
      jsl_clearance_enabled: true,
      jsl_max_retries: 3,
      image_download_mode: 'requests_jsl',
      auto_fallback_to_browser: true,
      disable_page_images_during_crawl: true,
      browser_engine: 'edge',
      llm_enrich_enabled: false,
      llm_api_base: 'http://127.0.0.1:11434/v1',
      llm_api_key: '',
      llm_model: 'qwen2.5:7b-instruct',
      llm_timeout_seconds: 45,
      llm_max_retries: 2,
      llm_temperature: 0.1,
      llm_only_when_missing_fields: true,
      llm_generate_biography: true,
      llm_append_biography_to_description: true,
      llm_cache_enabled: true,
      llm_max_input_chars: 6000,
      output_mode: 'images_only_with_record',
      keep_record_file: true,
    },
    crawl: {
      concurrent_requests: 1,
      download_delay: 5,
      autothrottle_start_delay: 5,
      autothrottle_max_delay: 8,
      retry_times: 3,
      timeout_seconds: 30,
      blocked_statuses: [403, 429],
      blocked_backoff_hours: 6,
      suspect_block_consecutive_failures: 3,
      interval_min_seconds: 5,
      interval_max_seconds: 8,
      image_interval_min_seconds: 5,
      image_interval_max_seconds: 8,
    },
  }
}

function guessPublicSiteName(startUrl: unknown): string {
  try {
    const parsed = new URL(String(startUrl || '').trim())
    const host = (parsed.hostname || 'site').trim().toLowerCase()
    const firstPath = parsed.pathname.replace(/^\/+|\/+$/g, '').split('/', 1)[0]?.trim().toLowerCase() || 'index'
    const seed = `${host}_${firstPath}`
    const normalized = seed.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '')
    if (normalized) {
      return normalized
    }
  } catch {
    // fall through
  }
  const fallback = new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)
  return `site_${fallback}`
}

function sanitizePublicSubdirName(value: unknown): string {
  let text = String(value || '').trim()
  if (!text) {
    return ''
  }
  const invalidChars = new Set(['<', '>', ':', '"', '/', '\\', '|', '?', '*'])
  text = Array.from(text)
    .map((char) => {
      const code = char.charCodeAt(0)
      if (code < 32 || invalidChars.has(char)) {
        return '_'
      }
      return char
    })
    .join('')
  text = text.replace(/\s+/g, ' ').trim().replace(/[. ]+$/g, '')
  text = text.replace(/_+/g, '_').replace(/^_+|_+$/g, '')
  return text
}

function extractPublicYearToken(...texts: unknown[]): string {
  for (const raw of texts) {
    const match = String(raw || '').match(/(?<!\d)(19|20)\d{2}(?!\d)/)
    if (match) {
      return match[0]
    }
  }
  return ''
}

function guessPublicUnitName(startUrl: unknown, payload: JsonRecord, templatePath = ''): string {
  const rules = asRecord(payload.rules) || {}
  for (const key of ['unit_name', 'organization_name', 'org_name', 'unit']) {
    const candidate = String(rules[key] || '').trim()
    if (candidate) {
      return candidate
    }
  }

  let host = ''
  try {
    host = (new URL(String(startUrl || '').trim()).hostname || '').trim().toLowerCase()
  } catch {
    host = ''
  }
  if (host.includes('tiantonglaw.com')) {
    return '天同律师事务所'
  }
  if (host.endsWith('mps.gov.cn') || host.includes('mps.gov.cn')) {
    return '公安部'
  }

  const siteName = String(payload.site_name || '').trim()
  if (siteName) {
    const cleaned = siteName.replace(/[_-]+/g, ' ').trim()
    if (cleaned) {
      return cleaned
    }
  }

  const templateName = path.parse(String(templatePath || '').trim()).name
  if (templateName) {
    return templateName
  }

  if (host) {
    const parts = host.split('.').filter(Boolean)
    if (parts.length >= 2) {
      return parts[parts.length - 2]
    }
    return host
  }
  return '单位'
}

function defaultPublicScraperOutputRoot(startUrl: unknown): string {
  return path.resolve(projectRoot, 'data', 'public_archive', guessPublicSiteName(startUrl))
}

function normalizePublicScraperTemplateOutputRoot(pathText: unknown): string {
  const raw = String(pathText || '').trim()
  if (!raw) {
    return ''
  }
  return path.resolve(projectRoot, raw)
}

function resolvePublicTaskOutputRoot(
  baseOutputRoot: unknown,
  startUrl: unknown,
  payload: JsonRecord,
  options?: { templatePath?: string; fallbackOutputRoot?: string },
): { outputRoot: string; payload: JsonRecord } {
  const data = jsonClone(payload || {})
  const baseRoot = path.resolve(
    String(baseOutputRoot || '').trim() || String(options?.fallbackOutputRoot || '').trim() || projectRoot,
  )
  const rules = asRecord(data.rules) || {}
  data.rules = rules

  const autoUnitSubdir = Boolean(rules.auto_unit_subdir)
  if (!autoUnitSubdir) {
    delete rules.output_root_parent
    delete rules.resolved_output_subdir
    delete rules.resolved_unit_name
    delete rules.resolved_year
    return { outputRoot: baseRoot, payload: data }
  }

  const templatePath = String(options?.templatePath || '').trim()
  const unitName = sanitizePublicSubdirName(guessPublicUnitName(startUrl, data, templatePath))
  const siteName = sanitizePublicSubdirName(String(data.site_name || ''))
  let host = ''
  try {
    host = sanitizePublicSubdirName((new URL(String(startUrl || '').trim()).hostname || '').trim().toLowerCase())
  } catch {
    host = ''
  }
  const year = sanitizePublicSubdirName(
    String(rules.year_hint || extractPublicYearToken(startUrl, data.site_name || '', path.basename(templatePath))),
  )
  const yearSuffix = year ? `_${year}` : ''

  let pattern = String(rules.output_subdir_pattern || '').trim()
  if (!pattern) {
    pattern = '{unit}{year_suffix}'
  }
  const formatCtx: Record<string, string> = {
    unit: unitName,
    year,
    year_suffix: yearSuffix,
    site_name: siteName,
    host,
  }
  const subdirRaw = pattern.replace(/\{([^}]+)\}/g, (_match, key: string) => formatCtx[key] || '')
  let subdirName = sanitizePublicSubdirName(subdirRaw)
  if (!subdirName) {
    subdirName = sanitizePublicSubdirName(unitName || siteName || host)
  }
  if (!subdirName) {
    delete rules.output_root_parent
    delete rules.resolved_output_subdir
    delete rules.resolved_unit_name
    delete rules.resolved_year
    return { outputRoot: baseRoot, payload: data }
  }

  const resolvedRoot = path.resolve(baseRoot, subdirName)
  rules.output_root_parent = baseRoot
  rules.resolved_output_subdir = subdirName
  rules.resolved_unit_name = unitName || siteName || host
  if (year) {
    rules.resolved_year = year
  } else {
    delete rules.resolved_year
  }
  return { outputRoot: resolvedRoot, payload: data }
}

function publicScraperTemplatesDir(): string {
  const dir = path.join(projectRoot, 'scraper', 'templates')
  return dir
}

function publicScraperTemplateStatePath(): string {
  return path.join(projectRoot, 'scraper', 'state', 'template_run_state.json')
}

async function loadPublicScraperTemplateStates(): Promise<Record<string, { status: string; updated_at: string }>> {
  const payload = await readJsonFile(publicScraperTemplateStatePath())
  const templates = asRecord(payload.templates) || {}
  const states: Record<string, { status: string; updated_at: string }> = {}
  for (const [rawKey, rawValue] of Object.entries(templates)) {
    const absKey = path.resolve(String(rawKey || '').trim())
    if (!absKey) {
      continue
    }
    if (rawValue && typeof rawValue === 'object' && !Array.isArray(rawValue)) {
      const entry = asRecord(rawValue) || {}
      const status = String(entry.status || '').trim().toLowerCase()
      const updatedAt = String(entry.updated_at || '').trim()
      if (status) {
        states[absKey] = { status, updated_at: updatedAt }
      }
      continue
    }
    const status = String(rawValue || '').trim().toLowerCase()
    if (status) {
      states[absKey] = { status, updated_at: '' }
    }
  }
  return states
}

async function savePublicScraperTemplateStates(states: Record<string, { status: string; updated_at: string }>): Promise<void> {
  const normalized: Record<string, { status: string; updated_at: string }> = {}
  for (const [rawKey, rawValue] of Object.entries(states || {})) {
    const absKey = path.resolve(String(rawKey || '').trim())
    const status = String(rawValue?.status || '').trim().toLowerCase()
    const updatedAt = String(rawValue?.updated_at || '').trim()
    if (!absKey || !status) {
      continue
    }
    normalized[absKey] = {
      status,
      updated_at: updatedAt || new Date().toISOString().slice(0, 19),
    }
  }
  const statePath = publicScraperTemplateStatePath()
  await mkdir(path.dirname(statePath), { recursive: true })
  await writeFile(
    statePath,
    JSON.stringify(
      {
        updated_at: new Date().toISOString().slice(0, 19),
        templates: normalized,
      },
      null,
      2,
    ),
    'utf-8',
  )
}

async function setPublicScraperTemplateState(templatePath: unknown, status: unknown): Promise<void> {
  const absPath = path.resolve(String(templatePath || '').trim())
  const statusText = String(status || '').trim().toLowerCase()
  if (!absPath || !statusText) {
    return
  }
  const states = await loadPublicScraperTemplateStates()
  states[absPath] = { status: statusText, updated_at: new Date().toISOString().slice(0, 19) }
  await savePublicScraperTemplateStates(states)
}

async function listPublicScraperTemplates(): Promise<ScraperLaunchTemplateOption[]> {
  const unfinished: ScraperLaunchTemplateOption[] = []
  const done: ScraperLaunchTemplateOption[] = []
  const templatesDir = publicScraperTemplatesDir()
  const templateStates = await loadPublicScraperTemplateStates()
  const seen = new Set<string>()
  for (const folder of [templatesDir, path.join(projectRoot, 'scraper')]) {
    if (!existsSync(folder)) {
      continue
    }
    const entries = await readdir(folder, { withFileTypes: true }).catch(() => [])
    const names = entries
      .filter((entry) => entry.isFile())
      .map((entry) => entry.name)
      .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }))
    for (const name of names) {
      if (!name.toLowerCase().endsWith('.json')) {
        continue
      }
      const full = path.resolve(folder, name)
      if (seen.has(full) || name.toLowerCase() === 'template_run_state.json') {
        continue
      }
      if (folder !== templatesDir && !name.toLowerCase().includes('config.')) {
        continue
      }
      seen.add(full)
      const rawStatus = String(templateStates[full]?.status || '').trim().toLowerCase()
      const isDone = new Set(['done', 'completed', 'finished', 'success']).has(rawStatus)
      const item: ScraperLaunchTemplateOption = {
        label: `${isDone ? '已完成' : '未完成'} | ${path.relative(projectRoot, full)}`,
        path: full,
        status: rawStatus || (isDone ? 'done' : 'pending'),
        updated_at: String(templateStates[full]?.updated_at || '').trim(),
      }
      if (isDone) {
        done.push(item)
      } else {
        unfinished.push(item)
      }
    }
  }
  return [...unfinished, ...done]
}

function mergeStatusReason(entry: JsonRecord, message: string): void {
  const next = String(message || '').trim()
  if (!next) {
    return
  }
  const current = String(entry.reason || '').trim()
  if (!current) {
    entry.reason = next
    return
  }
  if (!current.includes(next)) {
    entry.reason = `${current} | ${next}`
  }
}

function humanizeScraperReason(text: string): string {
  const raw = String(text || '').trim()
  if (!raw) {
    return ''
  }
  const lower = raw.toLowerCase()
  if (lower.startsWith('audit_missing_metadata_fields')) return '元数据待补充：关键字段缺失'
  if (lower === 'metadata_missing_local_image_path') return '元数据未写入：本地图片缺失'
  if (lower === 'image_download_http_error') return '图片下载失败：HTTP 错误'
  if (lower === 'image_download_not_image') return '图片下载失败：返回内容不是图片'
  if (lower === 'image_download_request_failed') return '图片下载失败：请求异常'
  if (lower === 'image_download_browser_failed') return '图片下载失败：浏览器模式异常'
  if (lower === 'missing_detail_url_from_list') return '列表项缺少详情链接'
  if (lower.includes('missing_required_fields')) return '详情页关键字段缺失'
  if (lower.startsWith('metadata_write_failed')) return '元数据写入失败'
  return raw
}

function isScraperRowCompleted(row: JsonRecord): boolean {
  return okTokens.has(String(row.detail || '').trim()) && okTokens.has(String(row.image || '').trim()) && okTokens.has(String(row.meta || '').trim())
}

function isScraperRowImageDownloaded(row: JsonRecord): boolean {
  return okTokens.has(String(row.image || '').trim())
}

function splitScraperProgressRows(rows: JsonRecord[]): { pending: JsonRecord[]; done: JsonRecord[] } {
  const pending: JsonRecord[] = []
  const done: JsonRecord[] = []
  for (const row of rows) {
    if (isScraperRowCompleted(row)) {
      done.push(row)
    } else {
      pending.push(row)
    }
  }
  return { pending, done }
}

function summarizeScraperProgressRows(rows: JsonRecord[]): { discovered_rows: number; downloaded_rows: number; completed_rows: number } {
  return {
    discovered_rows: rows.length,
    downloaded_rows: rows.filter((row) => isScraperRowImageDownloaded(row)).length,
    completed_rows: rows.filter((row) => isScraperRowCompleted(row)).length,
  }
}

function buildPublicScraperProgressText(counts: {
  discovered_rows: number
  downloaded_rows: number
  completed_rows: number
  total_target: number
  list_rows: number
  profile_rows: number
  image_rows: number
  metadata_rows: number
}): string {
  const discoveredRows = Math.max(0, Math.trunc(counts.discovered_rows || 0))
  const downloadedRows = Math.max(0, Math.trunc(counts.downloaded_rows || 0))
  const completedRows = Math.max(0, Math.trunc(counts.completed_rows || 0))
  const totalTarget = Math.max(0, Math.trunc(counts.total_target || 0))
  const listRows = Math.max(0, Math.trunc(counts.list_rows || 0))
  const profileRows = Math.max(0, Math.trunc(counts.profile_rows || 0))
  const imageRows = Math.max(0, Math.trunc(counts.image_rows || 0))
  const metadataRows = Math.max(0, Math.trunc(counts.metadata_rows || 0))
  const discoveredPct = totalTarget > 0 ? (discoveredRows / totalTarget) * 100 : 0
  const downloadTarget = Math.max(discoveredRows, 0)
  const downloadPct = downloadTarget > 0 ? Math.min(100, (downloadedRows / downloadTarget) * 100) : 0
  return `抓取中 下载:${downloadedRows}/${downloadTarget}(${downloadPct.toFixed(1)}%) 发现:${discoveredRows}/${totalTarget}(${discoveredPct.toFixed(1)}%) 完成:${completedRows} 列表:${listRows} 详情:${profileRows} 图片:${imageRows} 元数据:${metadataRows}`
}

function normalizeContinueOptions(options?: JsonRecord | null): ContinueOptions {
  const data = options || {}
  return {
    mode: String(data.mode || '').trim(),
    auto_fallback: data.auto_fallback == null ? true : Boolean(data.auto_fallback),
    disable_page_images: data.disable_page_images == null ? true : Boolean(data.disable_page_images),
  }
}

function buildContinueStartExistingTaskArgs(options?: JsonRecord | null): StartExistingTaskOptions {
  const next = normalizeContinueOptions(options)
  return {
    skip_crawl: false,
    skip_images: false,
    skip_metadata: false,
    runtime_state: '继续运行中',
    mode_override: next.mode,
    auto_fallback_override: next.auto_fallback,
    disable_page_images_override: next.disable_page_images,
  }
}

function buildRetryStartExistingTaskArgs(needCrawl: boolean, options?: JsonRecord | null): StartExistingTaskOptions {
  const next = normalizeContinueOptions(options)
  return {
    skip_crawl: !needCrawl,
    skip_images: false,
    skip_metadata: false,
    runtime_state: needCrawl ? '继续运行中' : '失败重试中',
    mode_override: next.mode,
    auto_fallback_override: next.auto_fallback,
    disable_page_images_override: next.disable_page_images,
  }
}

function buildRewriteMetadataStartExistingTaskArgs(): StartExistingTaskOptions {
  return {
    skip_crawl: true,
    skip_images: true,
    skip_metadata: false,
    runtime_state: '元数据重写中',
  }
}

function resolveActiveTemplatePath(rules: JsonRecord): string {
  const active = String(rules.template_source_path || rules.generated_template_path || '').trim()
  return active ? path.resolve(active) : ''
}

function resolveNamedImagesDir(outputRoot: string, rules: JsonRecord): string {
  const namedRaw = String(rules.named_images_dir || '').trim()
  return namedRaw ? path.resolve(outputRoot, namedRaw) : outputRoot
}

function normalizeApiBase(value: unknown): string {
  const raw = String(value || '').trim()
  return raw ? raw.replace(/\/+$/, '') : ''
}

async function loadGlobalLlmSettings(): Promise<JsonRecord> {
  const settingsPath = path.join(os.homedir(), '.d2ilite', 'settings.json')
  const payload = await readJsonFile(settingsPath)
  return asRecord(payload.llm) || {}
}

function buildPublicScraperSetupInitialState(sourceHint: unknown, globalLlmSettings?: JsonRecord): ScraperLaunchValues {
  const defaults = defaultPublicScraperTemplate()
  const crawlDefaults = asRecord(defaults.crawl) || {}
  const rulesDefaults = asRecord(defaults.rules) || {}
  const globalLlm = globalLlmSettings || {}
  const initialUrl = String(sourceHint || '').trim() || 'https://'
  const initialOutput = defaultPublicScraperOutputRoot(initialUrl)

  let intervalMin = Number(
    crawlDefaults.interval_min_seconds ??
      crawlDefaults.image_interval_min_seconds ??
      crawlDefaults.download_delay ??
      5,
  )
  let intervalMax = Number(
    crawlDefaults.interval_max_seconds ??
      crawlDefaults.image_interval_max_seconds ??
      Math.max(intervalMin || 5, 8),
  )
  if (!Number.isFinite(intervalMin)) intervalMin = 5
  if (!Number.isFinite(intervalMax)) intervalMax = Math.max(intervalMin, 8)
  if (intervalMax < intervalMin) intervalMax = intervalMin

  let suspectFailures = Number(crawlDefaults.suspect_block_consecutive_failures ?? 3)
  if (!Number.isFinite(suspectFailures)) suspectFailures = 3
  if (suspectFailures < 2) suspectFailures = 2

  let imageMode = String(rulesDefaults.image_download_mode || 'requests_jsl').trim().toLowerCase()
  if (!VALID_IMAGE_DOWNLOAD_MODES.has(imageMode)) {
    imageMode = 'requests_jsl'
  }

  const globalLlmEnabled = Boolean(globalLlm.enabled_default)
  const globalLlmModel = String(globalLlm.model || '').trim()
  const globalLlmApiBase = String(globalLlm.api_base || '').trim()
  const globalLlmApiKey = String(globalLlm.api_key || '').trim()

  return {
    start_url: initialUrl,
    output_root: initialOutput,
    interval_min: String(intervalMin),
    interval_max: String(intervalMax),
    timeout_seconds: String(crawlDefaults.timeout_seconds ?? 30),
    suspect_block_consecutive_failures: String(suspectFailures),
    jsl_enabled: Boolean(rulesDefaults.jsl_clearance_enabled ?? true),
    image_download_mode: imageMode,
    auto_fallback_to_browser: Boolean(rulesDefaults.auto_fallback_to_browser ?? true),
    disable_page_images_during_crawl: Boolean(rulesDefaults.disable_page_images_during_crawl ?? true),
    output_minimal: ['images_only', 'images_only_with_record'].includes(
      String(rulesDefaults.output_mode || 'images_only_with_record').trim().toLowerCase(),
    ),
    direct_write_images: Boolean(rulesDefaults.direct_write_images ?? true),
    llm_enrich_enabled:
      globalLlmApiBase || globalLlmModel
        ? globalLlmEnabled
        : Boolean(rulesDefaults.llm_enrich_enabled ?? false),
    llm_model: globalLlmModel || String(rulesDefaults.llm_model || 'qwen2.5:7b-instruct'),
    llm_api_base: globalLlmApiBase || String(rulesDefaults.llm_api_base || 'http://127.0.0.1:11434/v1'),
    llm_api_key: globalLlmApiKey || String(rulesDefaults.llm_api_key || ''),
    template_hint: DEFAULT_TEMPLATE_HINT,
    template_start_url: '',
    save_generated_template: true,
    cleanup_generated_template: true,
    disable_template_persistence_controls: false,
    url_locked: false,
    selected_template_path: '',
  }
}

async function buildPublicScraperTemplateSelectionState(
  templatePath: string,
  globalLlmSettings?: JsonRecord,
): Promise<Partial<ScraperLaunchValues>> {
  const selectedPath = String(templatePath || '').trim()
  if (!selectedPath) {
    return {
      template_hint: DEFAULT_TEMPLATE_HINT,
      template_start_url: '',
      url_locked: false,
      disable_template_persistence_controls: false,
    }
  }

  let payload: JsonRecord = {}
  try {
    const loaded = JSON.parse(await readFile(selectedPath, 'utf-8')) as unknown
    payload = asRecord(loaded) || {}
    if (!Object.keys(payload).length) {
      throw new Error('模板不是 JSON 对象')
    }
  } catch (error) {
    return {
      template_hint: `模板读取失败：${error instanceof Error ? error.message : String(error)}`,
      template_start_url: '',
      url_locked: false,
      disable_template_persistence_controls: false,
    }
  }

  let startUrl = ''
  const startUrls = payload.start_urls
  if (Array.isArray(startUrls) && startUrls.length > 0) {
    startUrl = normalizeHttpUrl(startUrls[0])
  } else if (typeof startUrls === 'string') {
    startUrl = normalizeHttpUrl(startUrls)
  }
  if (!/^https?:\/\//i.test(startUrl)) {
    return {
      template_hint: '模板缺少有效 start_urls，无法直接启动。',
      template_start_url: '',
      url_locked: false,
      disable_template_persistence_controls: false,
    }
  }

  const defaults = defaultPublicScraperTemplate()
  const crawlDefaults = asRecord(defaults.crawl) || {}
  const crawlData = asRecord(payload.crawl) || {}
  const rulesData = asRecord(payload.rules) || {}
  const globalLlm = globalLlmSettings || {}
  const globalLlmEnabled = Boolean(globalLlm.enabled_default)
  const globalLlmModel = String(globalLlm.model || '').trim()
  const globalLlmApiBase = String(globalLlm.api_base || '').trim()
  const globalLlmApiKey = String(globalLlm.api_key || '').trim()
  let imageMode = String(rulesData.image_download_mode || 'requests_jsl').trim().toLowerCase()
  if (!VALID_IMAGE_DOWNLOAD_MODES.has(imageMode)) {
    imageMode = 'requests_jsl'
  }

  const outputCfg = normalizePublicScraperTemplateOutputRoot(payload.output_root || '')
  return {
    template_hint: '已使用模板内置链接，可直接开始任务。',
    template_start_url: startUrl,
    start_url: startUrl,
    url_locked: true,
    interval_min: String(crawlData.interval_min_seconds ?? crawlData.image_interval_min_seconds ?? crawlData.download_delay ?? ''),
    interval_max: String(crawlData.interval_max_seconds ?? crawlData.image_interval_max_seconds ?? crawlData.interval_min_seconds ?? ''),
    timeout_seconds: String(crawlData.timeout_seconds ?? ''),
    suspect_block_consecutive_failures: String(
      crawlData.suspect_block_consecutive_failures ?? crawlDefaults.suspect_block_consecutive_failures ?? 3,
    ),
    jsl_enabled: Boolean(rulesData.jsl_clearance_enabled ?? true),
    image_download_mode: imageMode,
    auto_fallback_to_browser: Boolean(rulesData.auto_fallback_to_browser ?? true),
    disable_page_images_during_crawl: Boolean(rulesData.disable_page_images_during_crawl ?? true),
    llm_enrich_enabled:
      globalLlmApiBase || globalLlmModel
        ? globalLlmEnabled
        : Boolean(rulesData.llm_enrich_enabled ?? false),
    llm_model: globalLlmModel || String(rulesData.llm_model || 'qwen2.5:7b-instruct'),
    llm_api_base: globalLlmApiBase || String(rulesData.llm_api_base || 'http://127.0.0.1:11434/v1'),
    llm_api_key: globalLlmApiKey || String(rulesData.llm_api_key || ''),
    output_minimal: ['images_only', 'images_only_with_record'].includes(
      String(rulesData.output_mode || 'images_only_with_record').trim().toLowerCase(),
    ),
    direct_write_images: Boolean(rulesData.direct_write_images ?? true),
    output_root: outputCfg || defaultPublicScraperOutputRoot(startUrl),
    save_generated_template: false,
    cleanup_generated_template: false,
    disable_template_persistence_controls: true,
  }
}

function buildPublicScraperSetupSubmitPlan(
  rawValues: JsonRecord,
  options: { selectedTemplatePath?: string; templateStartUrl?: string },
): { ok: true; result: JsonRecord } | { ok: false; title: string; message: string } {
  const selectedPath = String(options.selectedTemplatePath || '').trim()
  let startUrl = ''
  if (selectedPath) {
    startUrl = String(options.templateStartUrl || '').trim() || normalizeHttpUrl(rawValues.start_url || '')
    if (!/^https?:\/\//i.test(startUrl)) {
      return { ok: false, title: '模板错误', message: '所选模板缺少有效 start_urls，无法直接启动。' }
    }
  } else {
    startUrl = normalizeHttpUrl(rawValues.start_url || '')
    if (!/^https?:\/\//i.test(startUrl)) {
      return { ok: false, title: '链接无效', message: '请输入有效的 http/https 链接。' }
    }
  }

  const outputRootRaw = String(rawValues.output_root || '').trim()
  const outputRoot = path.resolve(outputRootRaw || defaultPublicScraperOutputRoot(startUrl))
  const intervalMin = Number(String(rawValues.interval_min || '').trim())
  let intervalMax = Number(String(rawValues.interval_max || '').trim())
  const timeoutSeconds = Number(String(rawValues.timeout_seconds || '').trim())
  const suspectFailures = Number(String(rawValues.suspect_block_consecutive_failures || '').trim())
  if (!Number.isFinite(intervalMin) || !Number.isFinite(intervalMax) || !Number.isFinite(timeoutSeconds) || !Number.isFinite(suspectFailures)) {
    return { ok: false, title: '参数错误', message: '间隔、超时、连续失败阈值必须是数字。' }
  }
  if (intervalMin < 0.1) {
    return { ok: false, title: '参数错误', message: '统一间隔最小值必须 >= 0.1 秒。' }
  }
  if (intervalMax < intervalMin) {
    intervalMax = intervalMin
  }
  if (timeoutSeconds < 5) {
    return { ok: false, title: '参数错误', message: '请求超时必须 >= 5 秒。' }
  }
  if (suspectFailures < 2) {
    return { ok: false, title: '参数错误', message: '连续失败阈值必须 >= 2。' }
  }

  let mode = String(rawValues.image_download_mode || 'requests_jsl').trim().toLowerCase()
  if (!VALID_IMAGE_DOWNLOAD_MODES.has(mode)) {
    mode = 'requests_jsl'
  }
  return {
    ok: true,
    result: {
      start_url: startUrl,
      output_root: outputRoot,
      interval_min: Math.round(intervalMin * 1000) / 1000,
      interval_max: Math.round(intervalMax * 1000) / 1000,
      timeout_seconds: Math.trunc(timeoutSeconds),
      suspect_block_consecutive_failures: Math.trunc(suspectFailures),
      jsl_enabled: Boolean(rawValues.jsl_enabled),
      image_download_mode: mode,
      auto_fallback_to_browser: rawValues.auto_fallback_to_browser == null ? true : Boolean(rawValues.auto_fallback_to_browser),
      disable_page_images_during_crawl:
        rawValues.disable_page_images_during_crawl == null ? true : Boolean(rawValues.disable_page_images_during_crawl),
      llm_enrich_enabled: Boolean(rawValues.llm_enrich_enabled),
      llm_model: String(rawValues.llm_model || '').trim(),
      llm_api_base: String(rawValues.llm_api_base || '').trim(),
      llm_api_key: String(rawValues.llm_api_key || '').trim(),
      output_minimal: rawValues.output_minimal == null ? true : Boolean(rawValues.output_minimal),
      direct_write_images: rawValues.direct_write_images == null ? true : Boolean(rawValues.direct_write_images),
      template_path: selectedPath,
      save_generated_template: Boolean(rawValues.save_generated_template) && !selectedPath,
      cleanup_generated_template: Boolean(rawValues.cleanup_generated_template) && !selectedPath,
    },
  }
}

function applyPublicScraperSetupToRuntimeConfig(
  runtimeConfig: JsonRecord,
  setup: JsonRecord,
  globalLlmSettings?: JsonRecord,
): { runtime_config: JsonRecord; llm_model: string; llm_api_base: string; llm_api_key: string } {
  const config = jsonClone(runtimeConfig || {})
  const setupData = setup || {}
  const globalLlm = globalLlmSettings || {}
  const crawl = asRecord(config.crawl) || {}
  const rules = asRecord(config.rules) || {}

  const intervalMin = Number(setupData.interval_min || 0)
  const intervalMax = Number(setupData.interval_max || intervalMin)
  crawl.interval_min_seconds = intervalMin
  crawl.interval_max_seconds = intervalMax
  crawl.download_delay = intervalMin
  crawl.autothrottle_start_delay = intervalMin
  crawl.autothrottle_max_delay = intervalMax
  crawl.image_interval_min_seconds = intervalMin
  crawl.image_interval_max_seconds = intervalMax
  crawl.timeout_seconds = Math.trunc(Number(setupData.timeout_seconds || crawl.timeout_seconds || 30))
  crawl.suspect_block_consecutive_failures = Math.max(
    2,
    Math.trunc(Number(setupData.suspect_block_consecutive_failures || crawl.suspect_block_consecutive_failures || 3)),
  )

  rules.jsl_clearance_enabled = Boolean(setupData.jsl_enabled)
  let mode = String(setupData.image_download_mode || 'requests_jsl').trim().toLowerCase()
  if (!VALID_IMAGE_DOWNLOAD_MODES.has(mode)) {
    mode = 'requests_jsl'
  }
  rules.image_download_mode = mode
  rules.auto_fallback_to_browser = Boolean(setupData.auto_fallback_to_browser ?? true)
  rules.disable_page_images_during_crawl = Boolean(setupData.disable_page_images_during_crawl ?? true)
  rules.direct_write_images = Boolean(setupData.direct_write_images ?? true)
  rules.llm_enrich_enabled = Boolean(setupData.llm_enrich_enabled ?? false)

  const globalLlmModel = String(globalLlm.model || '').trim()
  const globalLlmApiBase = String(globalLlm.api_base || '').trim()
  const globalLlmApiKey = String(globalLlm.api_key || '').trim()
  const llmModel = String(setupData.llm_model || '').trim() || globalLlmModel
  const llmApiBase = normalizeApiBase(String(setupData.llm_api_base || '').trim() || globalLlmApiBase)
  const llmApiKey = String(setupData.llm_api_key || '').trim() || globalLlmApiKey
  if (llmModel) {
    rules.llm_model = llmModel
  }
  if (llmApiBase) {
    rules.llm_api_base = llmApiBase
  }
  delete rules.llm_api_key
  if (rules.image_download_mode === 'browser') {
    rules.browser_engine = String(rules.browser_engine || 'edge').trim().toLowerCase() || 'edge'
  }
  if (setupData.output_minimal ?? true) {
    rules.output_mode = 'images_only_with_record'
    rules.keep_record_file = true
  } else {
    rules.output_mode = 'full'
    rules.keep_record_file = true
  }

  config.crawl = crawl
  config.rules = rules
  return {
    runtime_config: config,
    llm_model: llmModel,
    llm_api_base: llmApiBase,
    llm_api_key: llmApiKey,
  }
}

async function saveGeneratedPublicScraperTemplate(startUrl: string, runtimeConfig: JsonRecord): Promise<string> {
  const payload = jsonClone(runtimeConfig || {})
  const siteName = guessPublicSiteName(startUrl)
  payload.site_name = siteName
  payload.output_root = `data/public_archive/${siteName}`
  const rules = asRecord(payload.rules) || {}
  delete rules.cleanup_paths
  delete rules.template_source_path
  delete rules.generated_template_path
  delete rules.output_root_parent
  delete rules.resolved_output_subdir
  delete rules.resolved_unit_name
  delete rules.resolved_year
  payload.rules = rules

  const templatesDir = publicScraperTemplatesDir()
  await mkdir(templatesDir, { recursive: true })
  const basePath = path.join(templatesDir, `${siteName}.json`)
  let target = basePath
  if (existsSync(target)) {
    const stamp = new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)
    target = path.join(templatesDir, `${siteName}_${stamp}.json`)
  }
  await writeFile(target, JSON.stringify(payload, null, 2), 'utf-8')
  return target
}

function registerGeneratedTemplate(runtimeConfig: JsonRecord, generatedTemplatePath: string, cleanupAfterRun: boolean): JsonRecord {
  const config = jsonClone(runtimeConfig || {})
  const target = String(generatedTemplatePath || '').trim()
  if (!target) {
    return config
  }
  const rules = asRecord(config.rules) || {}
  rules.generated_template_path = target
  if (cleanupAfterRun) {
    const cleanupPaths = Array.isArray(rules.cleanup_paths) ? [...rules.cleanup_paths] : []
    if (!cleanupPaths.includes(target)) {
      cleanupPaths.push(target)
    }
    rules.cleanup_paths = cleanupPaths
  }
  config.rules = rules
  return config
}

function applyLlmEnv(baseEnv: NodeJS.ProcessEnv, options: { api_base?: unknown; api_key?: unknown; model?: unknown }): NodeJS.ProcessEnv {
  const env = { ...baseEnv }
  const apiBase = normalizeApiBase(options.api_base)
  const apiKey = String(options.api_key || '').trim()
  const model = String(options.model || '').trim()
  if (apiBase) env.D2I_LLM_API_BASE = apiBase
  if (apiKey) env.D2I_LLM_API_KEY = apiKey
  if (model) env.D2I_LLM_MODEL = model
  env.PYTHONUTF8 = '1'
  env.PYTHONIOENCODING = 'utf-8'
  return env
}

async function buildPublicScraperRuntimeConfig(
  startUrl: string,
  outputRoot: string,
  templatePath = '',
): Promise<{ config_path: string; runtime_config: JsonRecord }> {
  const templateCandidates = [
    templatePath,
    path.join(projectRoot, 'scraper', 'config.template.generic.json'),
    path.join(projectRoot, 'scraper', 'config.example.json'),
  ]
  let payload: JsonRecord = {}
  let loadedTemplatePath = ''
  for (const candidate of templateCandidates) {
    if (!candidate || !existsSync(candidate)) {
      continue
    }
    try {
      const loaded = JSON.parse(await readFile(candidate, 'utf-8')) as unknown
      const data = asRecord(loaded) || {}
      if (Object.keys(data).length > 0) {
        payload = data
        loadedTemplatePath = path.resolve(candidate)
        break
      }
    } catch {
      continue
    }
  }
  if (!Object.keys(payload).length) {
    payload = defaultPublicScraperTemplate()
  }
  payload = jsonClone(payload)

  let parsed: URL
  try {
    parsed = new URL(startUrl)
  } catch {
    throw new Error('无效链接：无法解析域名')
  }
  const host = (parsed.hostname || '').trim().toLowerCase()
  if (!host) {
    throw new Error('无效链接：无法解析域名')
  }

  const customTemplatePath = templatePath ? path.resolve(templatePath) : ''
  const usingCustomTemplate = Boolean(customTemplatePath && loadedTemplatePath && customTemplatePath === loadedTemplatePath)
  const templateStartUrls: string[] = []
  const templateStartUrlsRaw = payload.start_urls
  if (Array.isArray(templateStartUrlsRaw)) {
    for (const item of templateStartUrlsRaw) {
      const value = String(item || '').trim()
      if (value) {
        templateStartUrls.push(value)
      }
    }
  } else if (typeof templateStartUrlsRaw === 'string' && templateStartUrlsRaw.trim()) {
    templateStartUrls.push(templateStartUrlsRaw.trim())
  }

  const runtimeStartUrls: string[] = []
  if (usingCustomTemplate && templateStartUrls.length > 0) {
    for (const item of templateStartUrls) {
      if (!runtimeStartUrls.includes(item)) {
        runtimeStartUrls.push(item)
      }
    }
    if (!runtimeStartUrls.includes(startUrl)) {
      runtimeStartUrls.unshift(startUrl)
    }
  } else {
    runtimeStartUrls.push(startUrl)
  }

  const allowedDomains: string[] = []
  for (const seedUrl of runtimeStartUrls) {
    try {
      const seedHost = (new URL(seedUrl).hostname || '').trim().toLowerCase()
      if (!seedHost) {
        continue
      }
      for (const domain of [seedHost, seedHost.startsWith('www.') ? seedHost.slice(4) : `www.${seedHost}`]) {
        const normalized = String(domain || '').trim().toLowerCase()
        if (normalized && !allowedDomains.includes(normalized)) {
          allowedDomains.push(normalized)
        }
      }
    } catch {
      continue
    }
  }

  if (usingCustomTemplate && Array.isArray(payload.allowed_domains)) {
    for (const item of payload.allowed_domains) {
      const domain = String(item || '').trim().toLowerCase()
      if (domain && !allowedDomains.includes(domain)) {
        allowedDomains.push(domain)
      }
    }
  }

  let siteName = usingCustomTemplate ? String(payload.site_name || '').trim() : ''
  if (!siteName) {
    siteName = guessPublicSiteName(startUrl)
  }
  const referer = `${parsed.protocol}//${parsed.host}/`
  const baseOutputRoot = path.resolve(String(outputRoot || '').trim() || defaultPublicScraperOutputRoot(startUrl))
  payload.site_name = siteName
  payload.start_urls = runtimeStartUrls
  payload.allowed_domains = allowedDomains
  payload.output_root = baseOutputRoot

  const defaultHeaders = asRecord(payload.default_headers) || {}
  if (!String(defaultHeaders.Referer || '').trim()) {
    defaultHeaders.Referer = referer
  }
  payload.default_headers = defaultHeaders

  const imageHeaders = asRecord(payload.image_headers) || {}
  if (!String(imageHeaders.Referer || '').trim()) {
    imageHeaders.Referer = referer
  }
  payload.image_headers = imageHeaders

  const defaults = defaultPublicScraperTemplate()
  const selectors = asRecord(payload.selectors) || {}
  for (const [key, value] of Object.entries(asRecord(defaults.selectors) || {})) {
    if (!(key in selectors)) {
      selectors[key] = value
    }
  }
  payload.selectors = selectors

  const rules = asRecord(payload.rules) || {}
  for (const [key, value] of Object.entries(asRecord(defaults.rules) || {})) {
    if (!(key in rules)) {
      rules[key] = value
    }
  }
  rules.named_images_dir = ''
  rules.final_output_root = ''
  rules.record_root = ''
  rules.default_gender = ''
  rules.template_source_path = templatePath ? path.resolve(templatePath) : ''
  payload.rules = rules

  const resolved = resolvePublicTaskOutputRoot(baseOutputRoot, startUrl, payload, {
    templatePath,
    fallbackOutputRoot: defaultPublicScraperOutputRoot(startUrl),
  })
  payload = resolved.payload
  payload.output_root = resolved.outputRoot

  const crawl = asRecord(payload.crawl) || {}
  for (const [key, value] of Object.entries(asRecord(defaults.crawl) || {})) {
    if (!(key in crawl)) {
      crawl[key] = value
    }
  }
  payload.crawl = crawl

  const runtimeConfigPath = path.join(String(payload.output_root || resolved.outputRoot), 'state', 'runtime_config.json')
  await mkdir(path.dirname(runtimeConfigPath), { recursive: true })
  await writeFile(runtimeConfigPath, JSON.stringify(payload, null, 2), 'utf-8')
  return { config_path: runtimeConfigPath, runtime_config: payload }
}

function buildStartedTaskMessage(pid: number, namedDir: string, logPath: string, runtimeRules: JsonRecord): string {
  const usedTemplatePath = String(runtimeRules.template_source_path || '').trim()
  const generatedTemplatePath = String(runtimeRules.generated_template_path || '').trim()
  const imageMode = String(runtimeRules.image_download_mode || 'requests_jsl').trim().toLowerCase()
  const imageModeText = imageMode === 'browser' ? '浏览器模式(慢稳)' : '请求模式(快)'
  const resolvedSubdir = String(runtimeRules.resolved_output_subdir || '').trim()
  const folderMsg = resolvedSubdir ? `\n任务子目录：${resolvedSubdir}\n` : ''
  let templateMsg = ''
  if (usedTemplatePath) {
    templateMsg = `\n模板：\n${usedTemplatePath}`
  } else if (generatedTemplatePath) {
    templateMsg = `\n模板（本次生成）：\n${generatedTemplatePath}`
  }
  return (
    '抓取任务已在后台启动。\n' +
    `任务进程 PID: ${pid}\n\n` +
    `图片下载方式：${imageModeText}\n\n` +
    `${folderMsg}` +
    `最终图片会输出到：\n${namedDir}\n\n` +
    `运行日志：\n${logPath}${templateMsg}`
  )
}

function prepareExistingTaskRuntimeConfig(runtimeConfig: JsonRecord, outputRoot: string, options: StartExistingTaskOptions): JsonRecord {
  const config = JSON.parse(JSON.stringify(runtimeConfig || {})) as JsonRecord
  config.output_root = outputRoot

  const rules = asRecord(config.rules) || {}
  rules.named_images_dir = ''
  rules.final_output_root = ''
  rules.record_root = ''
  if (rules.retry_failed_first == null) rules.retry_failed_first = true
  if (rules.metadata_write_retries == null) rules.metadata_write_retries = 3
  if (rules.metadata_write_retry_delay_seconds == null) rules.metadata_write_retry_delay_seconds = 1.2
  if (rules.metadata_write_retry_backoff_factor == null) rules.metadata_write_retry_backoff_factor = 1.5

  const mode = String(options.mode_override || '').trim().toLowerCase()
  if (mode === 'requests_jsl' || mode === 'browser') {
    rules.image_download_mode = mode
    if (mode === 'browser') {
      rules.download_images_during_crawl = true
    }
  }
  if (typeof options.auto_fallback_override === 'boolean') {
    rules.auto_fallback_to_browser = options.auto_fallback_override
  }
  if (typeof options.disable_page_images_override === 'boolean') {
    rules.disable_page_images_during_crawl = options.disable_page_images_override
  }
  delete rules.llm_api_key
  config.rules = rules
  return config
}

function resolvePythonExecutable(): string {
  const candidates = [
    path.join(projectRoot, '.venv', 'Scripts', 'python.exe'),
    path.join(projectRoot, '.venv', 'bin', 'python'),
  ]
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate
    }
  }
  return 'python'
}

function resolveScraperRunnerScript(): string {
  const override = String(process.env.D2I_DESKTOP_SCRAPER_RUNNER || '').trim()
  return override ? path.resolve(override) : path.join(projectRoot, 'scraper', 'run_public_scraper.py')
}

function buildPublicScraperCommand(
  pythonExecutable: string,
  scriptPath: string,
  configPath: string,
  outputRoot: string,
  options: StartExistingTaskOptions,
): string[] {
  const args = [pythonExecutable, '-X', 'utf8', scriptPath, '--config', configPath, '--output-root', outputRoot]
  if (options.skip_crawl) args.push('--skip-crawl')
  if (options.skip_images) args.push('--skip-images')
  if (options.skip_metadata) args.push('--skip-metadata')
  return args
}

function buildPublicScraperLogBanner(runLabel: string): string {
  return `\n\n=== D2I Public Scraper ${String(runLabel || '').trim() || 'Run'} ${new Date().toISOString().replace('T', ' ').slice(0, 19)} ===\n`
}

function buildExistingTaskStartedMessage(pid: number, outputRoot: string, namedDir: string, logPath: string, skipCrawl: boolean): string {
  const modeHint = skipCrawl ? '（仅重试失败阶段）' : ''
  return `已按已有配置继续抓取任务。\n\n${modeHint}\n任务进程 PID: ${pid}\n\n任务目录：\n${outputRoot}\n\n最终图片目录：\n${namedDir}\n\n运行日志：\n${logPath}`
}

async function startPreparedTask(
  root: string,
  configPath: string,
  runtimeConfig: JsonRecord,
  options: StartExistingTaskOptions,
  extras?: {
    runLabel?: string
    messageBuilder?: (entry: RegistryEntry, namedDir: string, logPath: string, rules: JsonRecord) => string
  },
): Promise<{ entry: RegistryEntry; message: string }> {
  const scriptPath = resolveScraperRunnerScript()
  if (!existsSync(scriptPath)) throw new Error(`未找到抓取脚本: ${scriptPath}`)

  const prepared = jsonClone(runtimeConfig || {})
  await mkdir(path.dirname(configPath), { recursive: true })
  await writeFile(configPath, JSON.stringify(prepared, null, 2), 'utf-8')

  const rules = asRecord(prepared.rules) || {}
  const namedDir = resolveNamedImagesDir(root, rules)
  const logPath = path.join(root, 'reports', 'gui_public_scraper.log')
  await mkdir(path.dirname(logPath), { recursive: true })
  await setPublicScraperManualPauseFlag(root, false)
  await writeFile(logPath, buildPublicScraperLogBanner(extras?.runLabel || (options.skip_crawl ? 'Retry' : 'Continue')), {
    encoding: 'utf-8',
    flag: 'a',
  })

  const globalLlm = await loadGlobalLlmSettings()
  const env = applyLlmEnv(process.env, {
    api_base: rules.llm_api_base || globalLlm.api_base,
    api_key: rules.llm_api_key || globalLlm.api_key,
    model: rules.llm_model || globalLlm.model,
  })
  const command = buildPublicScraperCommand(resolvePythonExecutable(), scriptPath, configPath, root, options)
  const logFd = openSync(logPath, 'a')
  const child = spawn(command[0], command.slice(1), {
    cwd: path.dirname(scriptPath) || projectRoot,
    detached: true,
    windowsHide: true,
    stdio: ['ignore', logFd, logFd],
    env,
  })
  child.unref()

  const entry: RegistryEntry = {
    pid: child.pid ?? 0,
    config_path: configPath,
    log_path: logPath,
    named_dir: namedDir,
    runtime_state: String(options.runtime_state || '继续运行中').trim() || '继续运行中',
    manual_paused: false,
    active_template_path: resolveActiveTemplatePath(rules),
    last_exit_code: null,
    updated_at_ts: Date.now() / 1000,
    started_at: Date.now() / 1000,
  }
  if (entry.active_template_path) {
    await setPublicScraperTemplateState(entry.active_template_path, 'pending').catch(() => undefined)
  }
  const message =
    extras?.messageBuilder?.(entry, namedDir, logPath, rules) ||
    buildExistingTaskStartedMessage(entry.pid, root, namedDir, logPath, Boolean(options.skip_crawl))
  return { entry, message }
}

async function readTextTail(filePath: string, maxLines: number): Promise<string> {
  const target = String(filePath || '').trim()
  if (!target || !existsSync(target)) {
    return ''
  }
  try {
    const body = await readFile(target, 'utf-8')
    return body
      .split(/\r?\n/)
      .slice(-Math.max(1, maxLines))
      .join('\n')
      .trim()
  } catch {
    return ''
  }
}

async function detectLatestActionFromLog(root: string, logLines: number): Promise<string> {
  const text = await readTextTail(path.join(root, 'reports', 'gui_public_scraper.log'), Math.max(60, logLines * 2))
  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index]
    if (line.includes('正在下载') && line.includes('图片')) return '正在下载图片'
    if (line.includes('正在写入') && line.includes('元数据')) return '正在写入元数据'
    if (line.includes('正在抓取') || line.includes('详情页=')) return '正在抓取详情页'
  }
  return ''
}

async function estimateScraperTotalTarget(root: string): Promise<number> {
  const crawlReport = await readJsonFile(path.join(root, 'reports', 'crawl_report.json'))
  const candidates: number[] = []
  const metrics = asRecord(crawlReport.metrics_this_run)
  const totals = asRecord(crawlReport.totals_on_disk)
  if (metrics) candidates.push(Math.max(0, Math.trunc(ensureNumber(metrics.detail_requests_enqueued, 0))))
  if (totals) candidates.push(Math.max(0, Math.trunc(ensureNumber(totals.profiles, 0))))
  const details = new Set<string>()
  for (const row of await readJsonlRows(path.join(root, 'raw', 'list_records.jsonl'))) {
    const detailUrl = String(row.detail_url || '').trim()
    if (detailUrl) details.add(detailUrl)
  }
  candidates.push(details.size)
  candidates.push(await countJsonlRows(path.join(root, 'raw', 'profiles.jsonl')))
  candidates.push(await countJsonlRows(path.join(root, 'downloads', 'image_downloads.jsonl')))
  return Math.max(...candidates, 0)
}

async function collectScraperProgressRows(root: string, maxRows = 3000): Promise<JsonRecord[]> {
  const rows: JsonRecord[] = []
  const detailIndex = new Map<string, number>()
  const detailSeen = new Set<string>()

  const appendRow = (name: string, detailUrl: string): number => {
    const idx = rows.length + 1
    const row: JsonRecord = {
      idx: String(idx),
      name: name || `未命名_${idx}`,
      detail_url: detailUrl,
      detail: '…',
      image: '…',
      meta: '…',
      reason: '',
      image_path: '',
      _has_image_url: false,
      _image_url: '',
    }
    rows.push(row)
    if (detailUrl) detailIndex.set(detailUrl, rows.length - 1)
    return rows.length - 1
  }

  for (const item of await readJsonlRows(path.join(root, 'raw', 'list_records.jsonl'), maxRows * 2)) {
    const detailUrl = String(item.detail_url || '').trim()
    if (detailUrl && detailSeen.has(detailUrl)) continue
    if (detailUrl) detailSeen.add(detailUrl)
    const position = appendRow(String(item.name || '').trim(), detailUrl)
    if (!detailUrl) {
      rows[position].detail = '×'
      rows[position].image = '-'
      rows[position].meta = '-'
      mergeStatusReason(rows[position], '列表缺少详情链接')
    }
    if (rows.length >= maxRows) break
  }

  for (const item of await readJsonlRows(path.join(root, 'raw', 'profiles.jsonl'), maxRows * 2)) {
    const detailUrl = String(item.detail_url || '').trim()
    if (!detailUrl) continue
    const rowPos = detailIndex.has(detailUrl) ? detailIndex.get(detailUrl)! : appendRow(String(item.name || '').trim(), detailUrl)
    const row = rows[rowPos]
    if (!String(row.name || '').trim() && String(item.name || '').trim()) row.name = String(item.name || '').trim()
    row.detail = '√'
    const imageUrl = String(item.image_url || '').trim()
    row._has_image_url = Boolean(imageUrl)
    if (imageUrl) {
      row._image_url = imageUrl
    } else if (String(row.image || '').trim() !== '√') {
      row.image = '×'
      mergeStatusReason(row, '详情缺少图片链接')
    }
  }

  for (const item of await readJsonlRows(path.join(root, 'downloads', 'image_downloads.jsonl'), maxRows * 3)) {
    const detailUrl = String(item.detail_url || '').trim()
    if (!detailUrl) continue
    const rowPos = detailIndex.has(detailUrl) ? detailIndex.get(detailUrl)! : appendRow(String(item.name || '').trim(), detailUrl)
    rows[rowPos].image = '√'
    const candidate = normalizeExistingPath(item.named_path) || normalizeExistingPath(item.saved_path)
    if (candidate) rows[rowPos].image_path = candidate
  }

  const metadataSummary = new Map<string, { ok: boolean; failed: boolean; error: string; output_path: string }>()
  for (const item of await readJsonlRows(path.join(root, 'raw', 'metadata_write_results.jsonl'), maxRows * 3)) {
    const detailUrl = String(item.detail_url || '').trim()
    if (!detailUrl) continue
    const state = metadataSummary.get(detailUrl) || { ok: false, failed: false, error: '', output_path: '' }
    const status = String(item.status || '').trim().toLowerCase()
    if (status === 'ok') {
      state.ok = true
      const outputPath = normalizeExistingPath(item.output_path)
      if (outputPath) state.output_path = outputPath
    } else if (status) {
      state.failed = true
      state.error = String(item.error || '').trim() || `元数据失败(${status})`
    }
    metadataSummary.set(detailUrl, state)
  }

  for (const [detailUrl, state] of metadataSummary.entries()) {
    const rowPos = detailIndex.has(detailUrl) ? detailIndex.get(detailUrl)! : appendRow('', detailUrl)
    const row = rows[rowPos]
    if (state.ok) {
      row.meta = '√'
      if (state.output_path) {
        row.image_path = state.output_path
        row.image = '√'
      }
    } else if (state.failed) {
      row.meta = '×'
      mergeStatusReason(row, state.error || '元数据写入失败')
    }
  }

  for (const item of await readJsonlRows(path.join(root, 'raw', 'review_queue.jsonl'), maxRows * 3)) {
    let detailUrl = String(item.detail_url || '').trim()
    if (!detailUrl) {
      const record = asRecord(item.record)
      detailUrl = String(record?.detail_url || '').trim()
    }
    if (!detailUrl) continue
    const rowPos = detailIndex.has(detailUrl) ? detailIndex.get(detailUrl)! : appendRow('', detailUrl)
    const row = rows[rowPos]
    const reason = String(item.reason || '').trim()
    const lower = reason.toLowerCase()
    if (lower.startsWith('image_') && String(row.image || '').trim() !== '√') row.image = '×'
    if (lower.startsWith('metadata_') && String(row.meta || '').trim() !== '√') row.meta = '×'
    if (lower.includes('missing_required_fields') && String(row.detail || '').trim() !== '√') row.detail = '×'
    mergeStatusReason(row, humanizeScraperReason(reason))
  }

  for (const item of await readJsonlRows(path.join(root, 'raw', 'failures.jsonl'), maxRows * 3)) {
    const url = String(item.url || '').trim()
    if (!url || !detailIndex.has(url)) continue
    const row = rows[detailIndex.get(url)!]
    const context = asRecord(item.context)
    if (String(context?.phase || '').trim().toLowerCase() === 'detail') row.detail = '×'
    mergeStatusReason(row, humanizeScraperReason(String(item.reason || '').trim()))
  }

  const output: JsonRecord[] = []
  for (const row of rows.slice(0, maxRows)) {
    const next = { ...row }
    if (String(next.detail || '').trim() === '√' && Boolean(next._has_image_url) && String(next.image || '').trim() === '…') {
      next.image = '⌛'
    }
    if (String(next.image || '').trim() === '√' && String(next.meta || '').trim() === '…') {
      next.meta = '⌛'
    }
    next.reason = humanizeScraperReason(String(next.reason || '').trim())
    delete next._has_image_url
    delete next._image_url
    output.push(next)
  }
  return output
}

async function derivePublicTaskStatus(root: string, entry: RegistryEntry | null, currentActiveRoot: string): Promise<string> {
  const listRows = await countJsonlRows(path.join(root, 'raw', 'list_records.jsonl'))
  const profileRows = await countJsonlRows(path.join(root, 'raw', 'profiles.jsonl'))
  const imageRows = await countJsonlRows(path.join(root, 'downloads', 'image_downloads.jsonl'))
  const reviewRows = await countJsonlRows(path.join(root, 'raw', 'review_queue.jsonl'))
  const failureRows = await countJsonlRows(path.join(root, 'raw', 'failures.jsonl'))
  const metadata = await countLatestMetadataStatus(root)
  const pendingRows = Math.max(0, profileRows - metadata.ok)
  const backoffState = await readScraperBackoffState(root)
  const pauseFlagExists = existsSync(publicScraperPauseFlagPath(root))
  const activityTotal = listRows + profileRows + imageRows + reviewRows + failureRows + metadata.ok + metadata.failed

  if (entryRunning(entry)) {
    if (entry?.manual_paused && currentActiveRoot === root) return '手动暂停(当前)'
    return entry?.manual_paused ? '手动暂停' : '运行中'
  }
  if (pauseFlagExists) return '手动暂停'
  if (backoffState.blocked_until) return '风控暂停'
  if (profileRows > 0 && pendingRows === 0 && metadata.failed === 0) return '已完成'
  if (activityTotal > 0) return '未完成'
  return String(entry?.runtime_state || '').trim() || '初始化'
}

async function summarizePublicTask(root: string, entry: RegistryEntry | null, currentActiveRoot: string): Promise<JsonRecord> {
  const profileRows = await countJsonlRows(path.join(root, 'raw', 'profiles.jsonl'))
  const imageRows = await countJsonlRows(path.join(root, 'downloads', 'image_downloads.jsonl'))
  const reviewRows = await countJsonlRows(path.join(root, 'raw', 'review_queue.jsonl'))
  const failureRows = await countJsonlRows(path.join(root, 'raw', 'failures.jsonl'))
  const metadata = await countLatestMetadataStatus(root)
  const pendingRows = Math.max(0, profileRows - metadata.ok)
  const status = await derivePublicTaskStatus(root, entry, currentActiveRoot)
  const candidates = [
    path.join(root, 'state', 'runtime_config.json'),
    path.join(root, 'crawl_record.json'),
    path.join(root, 'reports', 'reconcile_report.json'),
    path.join(root, 'reports', 'gui_public_scraper.log'),
  ]
  let updatedAt = '-'
  let maxMtime = 0
  for (const candidate of candidates) {
    if (!existsSync(candidate)) continue
    try {
      const next = await stat(candidate)
      if (next.mtimeMs > maxMtime) maxMtime = next.mtimeMs
    } catch {
      continue
    }
  }
  if (maxMtime > 0) updatedAt = new Date(maxMtime).toISOString().replace('T', ' ').slice(0, 19)
  return {
    root,
    task: path.basename(root) || root,
    status,
    profiles: profileRows,
    images: imageRows,
    metadata_ok: metadata.ok,
    pending: pendingRows,
    review: reviewRows,
    failures: failureRows,
    updated_at: updatedAt,
  }
}

async function refreshRuntimeRegistry(registry: RegistryPayload): Promise<RegistryPayload> {
  const next: RegistryPayload = {
    active_root: normalizePublicTaskRoot(registry.active_root),
    tasks: {},
    updated_at: registry.updated_at,
  }
  for (const [root, entry] of Object.entries(registry.tasks || {})) {
    const normalizedRoot = normalizePublicTaskRoot(root)
    if (!normalizedRoot) continue
    const normalizedEntry = normalizeRegistryEntry(entry)
    if (entryRunning(normalizedEntry)) {
      normalizedEntry.runtime_state = normalizedEntry.manual_paused ? '已暂停(手动)' : runningLikeStates.has(normalizedEntry.runtime_state) ? normalizedEntry.runtime_state : '运行中'
      next.tasks[normalizedRoot] = normalizedEntry
      continue
    }

    normalizedEntry.pid = 0
    if (normalizedEntry.manual_paused) {
      normalizedEntry.runtime_state = '已暂停(手动)'
      next.tasks[normalizedRoot] = normalizedEntry
      continue
    }
    if (runningLikeStates.has(normalizedEntry.runtime_state)) {
      const summary = await summarizePublicTask(normalizedRoot, normalizedEntry, next.active_root)
      const backoffState = await readScraperBackoffState(normalizedRoot)
      if (backoffState.blocked_until) {
        normalizedEntry.runtime_state = '已暂停(风控等待)'
        normalizedEntry.last_exit_code = 2
        if (normalizedEntry.active_template_path) {
          await setPublicScraperTemplateState(normalizedEntry.active_template_path, 'pending').catch(() => undefined)
        }
      } else if (String(summary.status || '').trim() === '已完成' || normalizedEntry.last_exit_code === 0) {
        normalizedEntry.runtime_state = '已完成'
        normalizedEntry.last_exit_code = 0
        if (normalizedEntry.active_template_path) {
          await setPublicScraperTemplateState(normalizedEntry.active_template_path, 'done').catch(() => undefined)
        }
      } else if (typeof normalizedEntry.last_exit_code === 'number' && normalizedEntry.last_exit_code > 0) {
        normalizedEntry.runtime_state = `异常结束(${normalizedEntry.last_exit_code})`
        if (normalizedEntry.active_template_path) {
          await setPublicScraperTemplateState(normalizedEntry.active_template_path, 'pending').catch(() => undefined)
        }
      } else {
        normalizedEntry.runtime_state = '已停止(待继续)'
        if (normalizedEntry.active_template_path) {
          await setPublicScraperTemplateState(normalizedEntry.active_template_path, 'pending').catch(() => undefined)
        }
      }
    }
    next.tasks[normalizedRoot] = normalizedEntry
  }
  return next
}

function serializeTaskSummary(row: JsonRecord, entry: RegistryEntry | null): JsonRecord {
  const sessionRunning = entryRunning(entry)
  return {
    root: String(row.root || '').trim(),
    task: String(row.task || '').trim(),
    status: String(row.status || '').trim(),
    profiles: Math.max(0, Math.trunc(ensureNumber(row.profiles, 0))),
    images: Math.max(0, Math.trunc(ensureNumber(row.images, 0))),
    metadata_ok: Math.max(0, Math.trunc(ensureNumber(row.metadata_ok, 0))),
    pending: Math.max(0, Math.trunc(ensureNumber(row.pending, 0))),
    review: Math.max(0, Math.trunc(ensureNumber(row.review, 0))),
    failures: Math.max(0, Math.trunc(ensureNumber(row.failures, 0))),
    updated_at: String(row.updated_at || '').trim(),
    pid: sessionRunning ? entry?.pid || 0 : 0,
    session_running: sessionRunning,
    manual_paused: Boolean(entry?.manual_paused),
    runtime_state: String(entry?.runtime_state || '').trim(),
  }
}

async function buildScraperWorkspacePayload(
  baseRoot: string,
  options?: { selectedRoot?: string; progressLimit?: number; logLines?: number },
): Promise<JsonRecord> {
  const registry = await refreshRuntimeRegistry(await loadRuntimeRegistry())
  await saveRuntimeRegistry(registry)
  const base = normalizePublicTaskRoot(baseRoot) || path.join(projectRoot, 'data', 'public_archive')
  const roots = await discoverPublicTaskRoots(base)
  const rows = await Promise.all(
    roots.map(async (root) => serializeTaskSummary(await summarizePublicTask(root, registry.tasks[root] || null, registry.active_root), registry.tasks[root] || null)),
  )
  rows.sort((left, right) => {
    const leftStatus = String(left.status || '')
    const rightStatus = String(right.status || '')
    const leftRunning = leftStatus.startsWith('运行中') ? 0 : 1
    const rightRunning = rightStatus.startsWith('运行中') ? 0 : 1
    if (leftRunning !== rightRunning) return leftRunning - rightRunning
    const leftPaused = leftStatus.includes('暂停') ? 0 : 1
    const rightPaused = rightStatus.includes('暂停') ? 0 : 1
    if (leftPaused !== rightPaused) return leftPaused - rightPaused
    return String(right.updated_at || '').localeCompare(String(left.updated_at || ''))
  })

  const requestedRoot = normalizePublicTaskRoot(options?.selectedRoot || registry.active_root)
  const selectedRoot = requestedRoot && rows.some((row) => row.root === requestedRoot) ? requestedRoot : String(rows[0]?.root || '').trim()
  const selectedTask = (rows.find((row) => row.root === selectedRoot) || null) as JsonRecord | null
  const selectedEntry = selectedRoot ? registry.tasks[selectedRoot] || null : null
  let detail: JsonRecord | null = null

  if (selectedRoot) {
    const progressRows = await collectScraperProgressRows(selectedRoot, Math.max(20, Math.trunc(options?.progressLimit || 300)))
    const split = splitScraperProgressRows(progressRows)
    const counts = summarizeScraperProgressRows(progressRows)
    const listRows = await countJsonlRows(path.join(selectedRoot, 'raw', 'list_records.jsonl'))
    const profileRows = await countJsonlRows(path.join(selectedRoot, 'raw', 'profiles.jsonl'))
    const imageRows = await countJsonlRows(path.join(selectedRoot, 'downloads', 'image_downloads.jsonl'))
    const metadataRows = await countJsonlRows(path.join(selectedRoot, 'raw', 'metadata_write_results.jsonl'))
    const totalTarget = Math.max(counts.discovered_rows, await estimateScraperTotalTarget(selectedRoot))
    const logPath = path.join(selectedRoot, 'reports', 'gui_public_scraper.log')
    const logTail = await readTextTail(logPath, Math.max(20, Math.trunc(options?.logLines || 80)))
    const latestAction = await detectLatestActionFromLog(selectedRoot, Math.max(20, Math.trunc(options?.logLines || 80)))
    const runtimeConfig = await readJsonFile(path.join(selectedRoot, 'state', 'runtime_config.json'))
    const runtimeRules = asRecord(runtimeConfig.rules) || {}
    const namedDir = resolveNamedImagesDir(selectedRoot, runtimeRules)
    const runtime = normalizeRegistryEntry(selectedEntry || {})
    const sessionRunning = entryRunning(runtime)

    detail = {
      root: selectedRoot,
      task: String(selectedTask?.task || path.basename(selectedRoot)).trim(),
      status: String(selectedTask?.status || '').trim(),
      updated_at: String(selectedTask?.updated_at || '').trim(),
      output_path: namedDir || selectedRoot,
      log_path: logPath,
      progress_text: buildPublicScraperProgressText({ discovered_rows: counts.discovered_rows, downloaded_rows: counts.downloaded_rows, completed_rows: counts.completed_rows, total_target: totalTarget, list_rows: listRows, profile_rows: profileRows, image_rows: imageRows, metadata_rows: metadataRows }),
      latest_action: latestAction,
      total_target: totalTarget,
      discovered_rows: counts.discovered_rows,
      downloaded_rows: counts.downloaded_rows,
      completed_rows: counts.completed_rows,
      list_rows: listRows,
      profile_rows: profileRows,
      image_rows: imageRows,
      metadata_rows: metadataRows,
      review_rows: Math.max(0, Math.trunc(ensureNumber(selectedTask?.review, 0))),
      failure_rows: Math.max(0, Math.trunc(ensureNumber(selectedTask?.failures, 0))),
      pending_rows: split.pending,
      done_rows: split.done,
      log_tail: logTail,
      pid: sessionRunning ? runtime.pid : 0,
      session_running: sessionRunning,
      manual_paused: runtime.manual_paused,
      runtime_state: runtime.runtime_state,
      can_pause: sessionRunning && !runtime.manual_paused,
      can_continue: runtime.manual_paused || (!sessionRunning && String(selectedTask?.status || '').trim() !== '已完成'),
      can_retry: !sessionRunning && Math.max(0, Math.trunc(ensureNumber(selectedTask?.failures, 0))) > 0,
      can_rewrite_metadata: !sessionRunning && (profileRows > 0 || metadataRows > 0),
    }
  }

  return { ok: true, base_root: base, task_count: rows.length, status_text: `任务数: ${rows.length}`, tasks: rows, selected_root: selectedRoot, selected_task: selectedTask, detail, control_defaults: { ...controlDefaults } }
}

function retryRequiresCrawlPhase(rows: JsonRecord[]): boolean {
  for (const row of rows) {
    if (isScraperRowCompleted(row)) continue
    if (!okTokens.has(String(row.detail || '').trim())) return true
  }
  return false
}

async function startExistingTask(root: string, options: StartExistingTaskOptions): Promise<{ entry: RegistryEntry; message: string }> {
  const configPath = path.join(root, 'state', 'runtime_config.json')
  if (!existsSync(configPath)) throw new Error(`未找到运行配置文件: ${configPath}`)

  const runtimeConfig = await readJsonFile(configPath)
  const prepared = prepareExistingTaskRuntimeConfig(runtimeConfig, root, options)
  return startPreparedTask(root, configPath, prepared, options, {
    runLabel: options.skip_crawl ? 'Retry' : 'Continue',
    messageBuilder: (entry, namedDir, logPath) =>
      buildExistingTaskStartedMessage(entry.pid, root, namedDir, logPath, Boolean(options.skip_crawl)),
  })
}

async function pauseTask(registry: RegistryPayload, root: string): Promise<string> {
  const entry = registry.tasks[root]
  if (!entry || !entryRunning(entry)) throw new Error('当前没有运行中的抓取任务。')
  if (entry.manual_paused) throw new Error('当前任务已处于手动暂停状态。')
  const ok = await setPublicScraperManualPauseFlag(root, true)
  if (!ok) throw new Error('无法写入暂停标记文件，请检查目录写权限。')
  entry.manual_paused = true
  entry.runtime_state = '已暂停(手动)'
  entry.updated_at_ts = Date.now() / 1000
  registry.active_root = root
  return '抓取任务已手动暂停，可点击继续运行'
}

async function continueTask(registry: RegistryPayload, root: string, options?: JsonRecord | null): Promise<string> {
  const entry = registry.tasks[root]
  if (entry && entryRunning(entry)) {
    if (entry.manual_paused) {
      const ok = await setPublicScraperManualPauseFlag(root, false)
      if (!ok) throw new Error('无法移除暂停标记文件，请检查目录写权限。')
      entry.manual_paused = false
      entry.runtime_state = '运行中'
      entry.updated_at_ts = Date.now() / 1000
      registry.active_root = root
      return '抓取任务已继续运行'
    }
    throw new Error('该任务已在运行中。')
  }
  const result = await startExistingTask(root, buildContinueStartExistingTaskArgs(options || controlDefaults))
  registry.tasks[root] = normalizeRegistryEntry(result.entry)
  registry.active_root = root
  return result.message || '已按已有配置继续抓取任务。'
}

async function retryTask(registry: RegistryPayload, root: string, options?: JsonRecord | null): Promise<string> {
  const entry = registry.tasks[root]
  if (entry && entryRunning(entry)) throw new Error('当前任务正在运行，请先暂停后再重试失败项。')
  const needCrawl = retryRequiresCrawlPhase(await collectScraperProgressRows(root, 3000))
  const result = await startExistingTask(root, buildRetryStartExistingTaskArgs(needCrawl, options || controlDefaults))
  registry.tasks[root] = normalizeRegistryEntry(result.entry)
  registry.active_root = root
  return result.message || '重试任务已启动'
}

async function rewriteTask(registry: RegistryPayload, root: string): Promise<string> {
  const entry = registry.tasks[root]
  if (entry && entryRunning(entry)) throw new Error('当前任务正在运行，请先暂停后再重写元数据。')
  const result = await startExistingTask(root, buildRewriteMetadataStartExistingTaskArgs())
  registry.tasks[root] = normalizeRegistryEntry(result.entry)
  registry.active_root = root
  return result.message || '元数据重写任务已启动'
}

function resolveWorkspaceBaseRootForTask(baseRoot: string, outputRoot: string): string {
  const normalizedBase = normalizePublicTaskRoot(baseRoot)
  const normalizedOutput = normalizePublicTaskRoot(outputRoot)
  if (!normalizedOutput) {
    return normalizedBase || path.join(projectRoot, 'data', 'public_archive')
  }
  if (normalizedBase && normalizedOutput.toLowerCase().startsWith(`${normalizedBase.toLowerCase()}${path.sep.toLowerCase()}`)) {
    return normalizedBase
  }
  if (normalizedBase && normalizedBase.toLowerCase() === normalizedOutput.toLowerCase()) {
    return normalizedBase
  }
  return path.dirname(normalizedOutput)
}

export async function readNativeScraperLaunchState(sourceHint = '', templatePath = ''): Promise<JsonRecord> {
  const globalLlm = await loadGlobalLlmSettings()
  const initial = buildPublicScraperSetupInitialState(sourceHint, globalLlm)
  const selectedTemplatePath = normalizeExistingPath(templatePath)
  const patch = await buildPublicScraperTemplateSelectionState(selectedTemplatePath, globalLlm)
  const form = {
    ...initial,
    ...patch,
    selected_template_path: selectedTemplatePath,
    template_hint: String((patch.template_hint ?? initial.template_hint) || ''),
    template_start_url: String((patch.template_start_url ?? initial.template_start_url) || ''),
    url_locked: Boolean(patch.url_locked ?? initial.url_locked),
    disable_template_persistence_controls: Boolean(
      patch.disable_template_persistence_controls ?? initial.disable_template_persistence_controls,
    ),
  }
  return {
    ok: true,
    ...form,
    templates: await listPublicScraperTemplates(),
  }
}

export async function startNativeScraperTask(
  values: JsonRecord,
  options?: { baseRoot?: string },
): Promise<JsonRecord> {
  const rawValues = jsonClone(values || {})
  const selectedTemplatePath = normalizeExistingPath(rawValues.selected_template_path || '')
  const templateStartUrl = String(rawValues.template_start_url || '').trim()
  const submitPlan = buildPublicScraperSetupSubmitPlan(rawValues, {
    selectedTemplatePath,
    templateStartUrl,
  })
  if (!submitPlan.ok) {
    throw new Error(String(submitPlan.message || '抓取启动参数错误'))
  }
  const setup = jsonClone(submitPlan.result)
  const startUrl = String(setup.start_url || '').trim()
  const configBuilt = await buildPublicScraperRuntimeConfig(
    startUrl,
    String(setup.output_root || '').trim(),
    String(setup.template_path || '').trim(),
  )
  let runtimeConfig = jsonClone(configBuilt.runtime_config)
  const outputRoot = normalizePublicTaskRoot(runtimeConfig.output_root || setup.output_root)
  if (!outputRoot) {
    throw new Error('无法解析任务输出目录。')
  }

  const registry = await refreshRuntimeRegistry(await loadRuntimeRegistry())
  const existingEntry = registry.tasks[outputRoot] ? normalizeRegistryEntry(registry.tasks[outputRoot]) : null
  if (existingEntry && entryRunning(existingEntry)) {
    registry.active_root = outputRoot
    await saveRuntimeRegistry(registry)
    const workspaceBaseRoot = resolveWorkspaceBaseRootForTask(options?.baseRoot || '', outputRoot)
    return {
      ok: true,
      message: `该任务已在运行中：\n${outputRoot}`,
      created_root: outputRoot,
      config_path: configBuilt.config_path,
      workspace: await buildScraperWorkspacePayload(workspaceBaseRoot, { selectedRoot: outputRoot }),
    }
  }

  const globalLlm = await loadGlobalLlmSettings()
  const applyPlan = applyPublicScraperSetupToRuntimeConfig(runtimeConfig, setup, globalLlm)
  runtimeConfig = jsonClone(applyPlan.runtime_config)
  if (!selectedTemplatePath && Boolean(setup.save_generated_template)) {
    try {
      const generatedTemplatePath = await saveGeneratedPublicScraperTemplate(startUrl, runtimeConfig)
      runtimeConfig = registerGeneratedTemplate(
        runtimeConfig,
        generatedTemplatePath,
        Boolean(setup.cleanup_generated_template),
      )
    } catch {
      // ignore template persistence failures
    }
  }

  const configPath = path.join(outputRoot, 'state', 'runtime_config.json')
  const result = await startPreparedTask(
    outputRoot,
    configPath,
    runtimeConfig,
    {
      skip_crawl: false,
      skip_images: false,
      skip_metadata: false,
      runtime_state: '运行中',
    },
    {
      runLabel: 'Run',
      messageBuilder: (entry, namedDir, logPath, rules) => buildStartedTaskMessage(entry.pid, namedDir, logPath, rules),
    },
  )

  registry.tasks[outputRoot] = normalizeRegistryEntry(result.entry)
  registry.active_root = outputRoot
  await saveRuntimeRegistry(registry)

  const workspaceBaseRoot = resolveWorkspaceBaseRootForTask(options?.baseRoot || '', outputRoot)
  return {
    ok: true,
    message: result.message,
    created_root: outputRoot,
    config_path: configPath,
    workspace: await buildScraperWorkspacePayload(workspaceBaseRoot, { selectedRoot: outputRoot }),
  }
}

export async function readNativeScraperWorkspace(baseRoot: string, options?: { selectedRoot?: string; progressLimit?: number; logLines?: number }): Promise<JsonRecord> {
  return buildScraperWorkspacePayload(baseRoot, options)
}

export async function getDefaultScraperBaseRoot(): Promise<JsonRecord> {
  return { ok: true, base_root: path.join(projectRoot, 'data', 'public_archive') }
}

export async function runNativeScraperAction(
  action: string,
  outputRoot: string,
  options?: { baseRoot?: string; control?: JsonRecord | null },
): Promise<JsonRecord> {
  const root = normalizePublicTaskRoot(outputRoot)
  if (!root) throw new Error('请先选择一个抓取任务。')
  const registry = await refreshRuntimeRegistry(await loadRuntimeRegistry())
  registry.tasks[root] = normalizeRegistryEntry(registry.tasks[root] || {})
  registry.active_root = root

  const actionText = String(action || '').trim().toLowerCase()
  let message = ''
  if (actionText === 'pause') {
    message = await pauseTask(registry, root)
  } else if (actionText === 'continue') {
    message = await continueTask(registry, root, options?.control || controlDefaults)
  } else if (actionText === 'retry') {
    message = await retryTask(registry, root, options?.control || controlDefaults)
  } else if (actionText === 'rewrite') {
    message = await rewriteTask(registry, root)
  } else {
    throw new Error(`不支持的抓取动作: ${actionText}`)
  }

  await saveRuntimeRegistry(registry)
  return {
    ok: true,
    action: actionText,
    message,
    workspace: await buildScraperWorkspacePayload(options?.baseRoot || path.join(projectRoot, 'data', 'public_archive'), { selectedRoot: root }),
  }
}

async function readOptionsFile(filePath: string): Promise<JsonRecord> {
  const payload = await readJsonFile(filePath)
  return payload
}

export async function executeScraperCli(args: string[]): Promise<JsonRecord> {
  const [command, ...rest] = args
  if (command === 'default-root') {
    return getDefaultScraperBaseRoot()
  }
  if (command === 'launch-state') {
    const sourceHint = rest.includes('--source-hint') ? rest[rest.indexOf('--source-hint') + 1] || '' : ''
    const templatePath = rest.includes('--template-path') ? rest[rest.indexOf('--template-path') + 1] || '' : ''
    return readNativeScraperLaunchState(sourceHint, templatePath)
  }
  if (command === 'workspace') {
    const baseRoot = rest[rest.indexOf('--base-root') + 1] || ''
    const selectedRoot = rest.includes('--selected-root') ? rest[rest.indexOf('--selected-root') + 1] || '' : ''
    const progressLimit = rest.includes('--progress-limit') ? ensureNumber(rest[rest.indexOf('--progress-limit') + 1], 300) : 300
    const logLines = rest.includes('--log-lines') ? ensureNumber(rest[rest.indexOf('--log-lines') + 1], 80) : 80
    return readNativeScraperWorkspace(baseRoot, { selectedRoot, progressLimit, logLines })
  }
  if (command === 'start') {
    const baseRoot = rest.includes('--base-root') ? rest[rest.indexOf('--base-root') + 1] || '' : ''
    const valuesFile = rest.includes('--values-file') ? rest[rest.indexOf('--values-file') + 1] || '' : ''
    const values = valuesFile ? await readOptionsFile(valuesFile) : {}
    return startNativeScraperTask(values, { baseRoot })
  }
  if (command === 'action') {
    const action = rest[rest.indexOf('--action') + 1] || ''
    const outputRoot = rest[rest.indexOf('--output-root') + 1] || ''
    const baseRoot = rest.includes('--base-root') ? rest[rest.indexOf('--base-root') + 1] || '' : ''
    const optionsFile = rest.includes('--options-file') ? rest[rest.indexOf('--options-file') + 1] || '' : ''
    const control = optionsFile ? await readOptionsFile(optionsFile) : {}
    return runNativeScraperAction(action, outputRoot, { baseRoot, control })
  }
  throw new Error(`unsupported command: ${String(command || '').trim() || '<empty>'}`)
}

async function main(): Promise<number> {
  try {
    const payload = await executeScraperCli(process.argv.slice(2))
    process.stdout.write(`${JSON.stringify({ ok: true, ...payload })}\n`)
    return 0
  } catch (error) {
    process.stdout.write(
      `${JSON.stringify({ ok: false, error: 'native scraper backend failed', detail: error instanceof Error ? error.message : String(error) })}\n`,
    )
    return 1
  }
}

const isDirectRun = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url : false
if (isDirectRun) {
  void main().then((code) => {
    process.exitCode = code
  })
}
