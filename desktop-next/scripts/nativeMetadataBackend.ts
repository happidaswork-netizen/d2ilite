import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, rm, stat, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

type JsonRecord = Record<string, unknown>
type SavePayload = {
  title?: unknown
  person?: unknown
  gender?: unknown
  position?: unknown
  city?: unknown
  source?: unknown
  image_url?: unknown
  keywords?: unknown
  titi_asset_id?: unknown
  titi_world_id?: unknown
  description?: unknown
  d2i_profile?: unknown
  role_aliases?: unknown
  police_id?: unknown
}

const scriptsRoot = path.dirname(fileURLToPath(import.meta.url))
const desktopRoot = path.resolve(scriptsRoot, '..')
const projectRoot = path.resolve(desktopRoot, '..')
const exiftoolConfigPath = path.join(desktopRoot, 'config', 'exiftool-titi.config')
const tempRoot = path.join(projectRoot, '.tmp', 'desktop-next')

export const NATIVE_METADATA_PROVIDER = 'native-exiftool'
export const NATIVE_METADATA_VERSION = 'metadata-native-v1'

function asRecord(value: unknown): JsonRecord | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }
  return value as JsonRecord
}

function normalizeText(value: unknown): string {
  if (Array.isArray(value)) {
    return normalizeText(value[0])
  }
  if (value == null) {
    return ''
  }
  if (typeof value === 'object') {
    const record = asRecord(value)
    if (record) {
      const first = Object.values(record)[0]
      return typeof first === 'string' ? first.trim() : String(first ?? '').trim()
    }
  }
  return String(value).replace(/\0/g, '').trim()
}

function normalizeList(value: unknown): string[] {
  const rawItems = Array.isArray(value) ? value : value == null ? [] : [value]
  const seen = new Set<string>()
  const out: string[] = []
  for (const entry of rawItems) {
    const text = normalizeText(entry)
    if (!text) {
      continue
    }
    const normalized = text.toLowerCase()
    if (seen.has(normalized)) {
      continue
    }
    seen.add(normalized)
    out.push(text)
  }
  return out
}

function parseJsonCandidate(value: unknown): JsonRecord | null {
  const text = normalizeText(value)
  if (!text || !text.startsWith('{') || !text.endsWith('}')) {
    return null
  }
  try {
    const parsed = JSON.parse(text) as unknown
    return asRecord(parsed)
  } catch {
    return null
  }
}

function normalizeGenderValue(value: unknown): string {
  const raw = normalizeText(value)
  if (!raw) {
    return ''
  }
  const lowered = raw.toLowerCase()
  const mapping: Record<string, string> = {
    男: '男',
    male: '男',
    m: '男',
    man: '男',
    男性: '男',
    女: '女',
    female: '女',
    f: '女',
    woman: '女',
    女性: '女',
  }
  return mapping[lowered] || mapping[raw] || raw
}

function normalizePoliceIdValue(value: unknown): string {
  const raw = normalizeText(value)
  if (!raw) {
    return ''
  }
  const lowered = raw.toLowerCase()
  const unknownTokens = new Set(['unknown', 'unkonw', 'n/a', 'na', 'none', 'null', '未知', '未详', '不详', '待补充', '-'])
  return unknownTokens.has(lowered) || unknownTokens.has(raw) ? '' : raw
}

function extractPoliceIdFromProfile(profile: unknown): string {
  const data = asRecord(profile)
  if (!data) {
    return ''
  }
  const candidateKeys = [
    'police_id',
    'police_no',
    'police_number',
    'badge_no',
    'badge_id',
    'badge_number',
    'officer_id',
    '警号',
  ]
  for (const key of candidateKeys) {
    const value = normalizePoliceIdValue(data[key])
    if (value) {
      return value
    }
  }
  const extra = asRecord(data.extra_fields)
  if (!extra) {
    return ''
  }
  for (const key of candidateKeys) {
    const value = normalizePoliceIdValue(extra[key])
    if (value) {
      return value
    }
  }
  return ''
}

function extractImageUrlFromTitiJson(titiJson: unknown): string {
  const data = asRecord(titiJson)
  if (!data) {
    return ''
  }
  const direct = normalizeText(data.source_image || data.image_url)
  if (direct) {
    return direct
  }
  const sourceImages = Array.isArray(data.source_images) ? data.source_images : []
  for (const item of sourceImages) {
    const value = normalizeText(item)
    if (value) {
      return value
    }
  }
  const sourceInputs = Array.isArray(data.source_inputs) ? data.source_inputs : []
  for (const item of sourceInputs) {
    const record = asRecord(item)
    if (!record) {
      continue
    }
    for (const key of ['source_image', 'url', 'image_url', 'filename', 'path']) {
      const value = normalizeText(record[key])
      if (value) {
        return value
      }
    }
  }
  const profile = asRecord(data.d2i_profile)
  return normalizeText(profile?.image_url || profile?.url)
}

function buildUuidLikeId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  const stamp = Date.now().toString(16)
  const rand = Math.random().toString(16).slice(2, 14)
  return `${stamp}-${rand}`
}

function toIsoString(value: Date): string {
  return value.toISOString()
}

function calculateStatus(item: {
  title: string
  description: string
  keywords: string[]
  source: string
  city: string
  person: string
  image_url: string
  titi_json: JsonRecord | null
  titi_asset_id: string
}): 'none' | 'partial' | 'complete' {
  const hasReadable = Boolean(
    item.title ||
      item.description ||
      item.keywords.length > 0 ||
      item.source ||
      item.city ||
      item.person ||
      item.image_url,
  )
  const hasTiti = Boolean(item.titi_json || item.titi_asset_id)
  if (!hasReadable && !hasTiti) {
    return 'none'
  }
  if (item.title && item.description && item.keywords.length > 0 && hasTiti) {
    return 'complete'
  }
  return 'partial'
}

function filterTagGroup(tags: JsonRecord, prefixes: string[]): JsonRecord {
  const output: JsonRecord = {}
  for (const [key, value] of Object.entries(tags)) {
    if (prefixes.some((prefix) => key.startsWith(prefix))) {
      output[key] = value
    }
  }
  return output
}

function parseTitiJson(tags: JsonRecord): JsonRecord | null {
  const candidates = [
    tags['XMP-titi:Meta'],
    tags['EXIF:UserComment'],
    tags['PNG:Titi'],
    tags['PNG:Comment'],
  ]
  for (const candidate of candidates) {
    const parsed = parseJsonCandidate(candidate)
    if (parsed && (parsed.titi_asset_id || parsed.schema === 'titi-meta')) {
      return parsed
    }
  }
  return null
}

function normalizeKeywordsInput(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return []
  }
  return normalizeList(value)
}

function buildMergedTitiJson(rawPayload: SavePayload, existing: JsonRecord | null): JsonRecord {
  const now = toIsoString(new Date())
  const base: JsonRecord = existing ? JSON.parse(JSON.stringify(existing)) : {}

  if (!normalizeText(base.schema)) {
    base.schema = 'titi-meta'
  }
  if (typeof base.schema_version !== 'number') {
    base.schema_version = 1
  }
  if (!normalizeText(base.app) || normalizeText(base.app) === 'D2I') {
    base.app = 'PWI'
  }
  if (!normalizeText(base.component)) {
    base.component = 'forge'
  }

  const assetId =
    normalizeText(rawPayload.titi_asset_id) || normalizeText(base.titi_asset_id) || buildUuidLikeId()
  base.titi_asset_id = assetId
  base.titi_world_id = normalizeText(rawPayload.titi_world_id) || normalizeText(base.titi_world_id) || 'default'

  const imageUrl = normalizeText(rawPayload.image_url)
  if (imageUrl) {
    base.source_image = imageUrl
  }

  const profileBase = asRecord(base.d2i_profile)
  const profile: JsonRecord = profileBase ? { ...profileBase } : {}
  const profilePayload = asRecord(rawPayload.d2i_profile)
  if (profilePayload) {
    for (const [key, value] of Object.entries(profilePayload)) {
      const normalized =
        typeof value === 'string'
          ? value.trim()
          : Array.isArray(value)
            ? value
            : value && typeof value === 'object'
              ? value
              : value
      if (normalized == null || normalized === '' || (Array.isArray(normalized) && normalized.length === 0)) {
        delete profile[key]
      } else if (typeof normalized === 'object' && !Array.isArray(normalized) && Object.keys(normalized as object).length === 0) {
        delete profile[key]
      } else {
        profile[key] = normalized
      }
    }
  }

  const person = normalizeText(rawPayload.person)
  const title = normalizeText(rawPayload.title)
  const description = normalizeText(rawPayload.description)
  const keywords = normalizeKeywordsInput(rawPayload.keywords)
  const source = normalizeText(rawPayload.source)
  const city = normalizeText(rawPayload.city)
  const gender = normalizeGenderValue(rawPayload.gender ?? profilePayload?.gender)
  let policeId = normalizePoliceIdValue(rawPayload.police_id)
  if (!policeId && profilePayload) {
    policeId = extractPoliceIdFromProfile(profilePayload)
  }

  if (!profilePayload && person) {
    profile.name = person
  }
  if (description) {
    profile.description = description
  }
  if (keywords.length > 0) {
    profile.keywords = keywords
  }
  if (source) {
    profile.source = source
  }
  if (imageUrl) {
    profile.image_url = imageUrl
  }
  if (city) {
    profile.city = city
  }
  if (gender) {
    profile.gender = gender
  } else if ('gender' in profile) {
    const existingGender = normalizeGenderValue(profile.gender)
    if (existingGender) {
      profile.gender = existingGender
    } else {
      delete profile.gender
    }
  }
  if (policeId) {
    profile.police_id = policeId
  } else if ('police_id' in profile) {
    const existingPoliceId = normalizePoliceIdValue(profile.police_id)
    if (existingPoliceId) {
      profile.police_id = existingPoliceId
    } else {
      delete profile.police_id
    }
  }
  if (!profilePayload && !person && title && !normalizeText(profile.name)) {
    profile.name = title.includes(' - ') ? title.split(' - ', 1)[0].trim() : title
  }

  if (Object.keys(profile).length > 0) {
    profile.extracted_at = now
    base.d2i_profile = profile
  }

  if (Array.isArray(rawPayload.role_aliases)) {
    if (rawPayload.role_aliases.length > 0) {
      base.role_aliases = rawPayload.role_aliases
    } else {
      delete base.role_aliases
    }
  }

  return base
}

function resolveExiftoolExecutable(): string {
  const candidates = [
    path.join(desktopRoot, 'node_modules', 'exiftool-vendored.exe', 'bin', 'exiftool.exe'),
    path.join(desktopRoot, 'node_modules', 'exiftool-vendored.pl', 'bin', 'exiftool'),
  ]
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate
    }
  }
  return 'exiftool'
}

function runExiftool(args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      resolveExiftoolExecutable(),
      ['-config', exiftoolConfigPath, '-charset', 'ExifTool=UTF8', '-charset', 'filename=UTF8', ...args],
      {
      cwd: projectRoot,
      windowsHide: true,
      },
    )
    const stdout: Buffer[] = []
    const stderr: Buffer[] = []
    child.stdout.on('data', (chunk) => stdout.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    child.stderr.on('data', (chunk) => stderr.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    child.on('error', reject)
    child.on('close', (code) => {
      const out = Buffer.concat(stdout).toString('utf-8').trim()
      const err = Buffer.concat(stderr).toString('utf-8').trim()
      if (code && code !== 0) {
        reject(new Error(err || out || `exiftool failed with code ${String(code)}`))
        return
      }
      resolve(out || err)
    })
  })
}

async function runExiftoolWithArgsFile(args: string[]): Promise<string> {
  await mkdir(tempRoot, { recursive: true })
  const argsFile = path.join(tempRoot, `exiftool-write-${Date.now()}-${Math.random().toString(16).slice(2)}.args`)
  await writeFile(argsFile, `${args.join('\n')}\n`, 'utf-8')
  try {
    return await runExiftool(['-@', argsFile])
  } finally {
    await rm(argsFile, { force: true })
  }
}

async function readTagMap(filePath: string): Promise<JsonRecord> {
  const payload = await runExiftool(['-json', '-G1', '-a', '-struct', filePath])
  const parsed = JSON.parse(payload) as unknown
  if (!Array.isArray(parsed) || !parsed[0] || typeof parsed[0] !== 'object') {
    throw new Error(`invalid exiftool payload: ${payload}`)
  }
  return parsed[0] as JsonRecord
}

function buildMetadataItem(filePath: string, tags: JsonRecord, fileStat: { size: number; mtime: Date }): JsonRecord {
  const titiJson = parseTitiJson(tags)
  const adaptiveProfile = asRecord(titiJson?.d2i_profile)
  const keywords = normalizeList(tags['XMP-dc:Subject'])
  const personList = normalizeList(tags['XMP-iptcExt:PersonInImage'])
  const person = personList[0] || normalizeText(adaptiveProfile?.name)
  const title = normalizeText(tags['XMP-dc:Title'])
  const description = normalizeText(tags['XMP-dc:Description']) || normalizeText(adaptiveProfile?.description)
  const source = normalizeText(tags['XMP-dc:Source']) || normalizeText(adaptiveProfile?.source)
  const imageUrl = normalizeText(tags['XMP-titi:SourceImage']) || extractImageUrlFromTitiJson(titiJson)
  const city = normalizeText(tags['XMP-photoshop:City']) || normalizeText(adaptiveProfile?.city)
  const gender = normalizeGenderValue(adaptiveProfile?.gender)
  const position = normalizeText(tags['XMP-photoshop:AuthorsPosition'])
  const policeId = extractPoliceIdFromProfile(adaptiveProfile)
  const titiAssetId = normalizeText(titiJson?.titi_asset_id)
  const titiWorldId = normalizeText(titiJson?.titi_world_id)

  return {
    filepath: filePath,
    filename: path.basename(filePath),
    title,
    description,
    keywords,
    source,
    image_url: imageUrl,
    city,
    person,
    gender,
    position,
    police_id: policeId,
    titi_asset_id: titiAssetId,
    titi_world_id: titiWorldId,
    filesize: fileStat.size,
    modified_time: toIsoString(fileStat.mtime),
    titi_json: titiJson,
    other_xmp: filterTagGroup(tags, ['XMP-']),
    other_exif: filterTagGroup(tags, ['EXIF:', 'JFIF:']),
    other_iptc: filterTagGroup(tags, ['IPTC:']),
    status: calculateStatus({
      title,
      description,
      keywords,
      source,
      city,
      person,
      image_url: imageUrl,
      titi_json: titiJson,
      titi_asset_id: titiAssetId,
    }),
    matched_row: null,
  }
}

export function buildNativeMetadataPing(): JsonRecord {
  return {
    provider: NATIVE_METADATA_PROVIDER,
    version: NATIVE_METADATA_VERSION,
  }
}

export async function readNativeMetadata(filePath: string): Promise<JsonRecord> {
  const targetPath = path.resolve(String(filePath || '').trim())
  if (!targetPath) {
    throw new Error('path is required')
  }
  const fileStat = await stat(targetPath)
  const tags = await readTagMap(targetPath)
  return {
    item: buildMetadataItem(targetPath, tags, { size: fileStat.size, mtime: fileStat.mtime }),
  }
}

export async function saveNativeMetadata(filePath: string, rawPayload: SavePayload): Promise<JsonRecord> {
  const targetPath = path.resolve(String(filePath || '').trim())
  if (!targetPath) {
    throw new Error('path is required')
  }

  const tags = await readTagMap(targetPath)
  const existingTitiJson = parseTitiJson(tags)
  const mergedTitiJson = buildMergedTitiJson(rawPayload, existingTitiJson)
  const mergedJsonText = JSON.stringify(mergedTitiJson)

  const args: string[] = ['-overwrite_original']
  const title = normalizeText(rawPayload.title)
  const description = normalizeText(rawPayload.description)
  const keywords = normalizeKeywordsInput(rawPayload.keywords)
  const source = normalizeText(rawPayload.source)
  const imageUrl = normalizeText(rawPayload.image_url)
  const city = normalizeText(rawPayload.city)
  const person = normalizeText(rawPayload.person)
  const position = normalizeText(rawPayload.position)
  const userCommentExisting = normalizeText(tags['EXIF:UserComment']).toLowerCase()
  const canUpdateUserComment =
    !userCommentExisting ||
    userCommentExisting.includes('titi_asset_id') ||
    (userCommentExisting.includes('schema') && userCommentExisting.includes('titi-meta'))

  if (title) {
    args.push(`-XMP-dc:Title=${title}`)
  }
  if (description) {
    args.push(`-XMP-dc:Description=${description}`)
    args.push(`-EXIF:XPComment=${description}`)
  }
  if (keywords.length > 0) {
    args.push(`-XMP-dc:Subject=${keywords[0]}`)
    for (const keyword of keywords.slice(1)) {
      args.push(`-XMP-dc:Subject+=${keyword}`)
    }
  }
  if (source) {
    args.push(`-XMP-dc:Source=${source}`)
  }
  if (imageUrl) {
    args.push(`-XMP-titi:SourceImage=${imageUrl}`)
  }
  if (city) {
    args.push(`-XMP-photoshop:City=${city}`)
  }
  if (person) {
    args.push(`-XMP-iptcExt:PersonInImage=${person}`)
  }
  if (position) {
    args.push(`-XMP-photoshop:AuthorsPosition=${position}`)
  }

  args.push(`-XMP-titi:Meta=${mergedJsonText}`)
  if (canUpdateUserComment) {
    args.push(`-EXIF:UserComment=${mergedJsonText}`)
  }
  args.push(targetPath)

  await runExiftoolWithArgsFile(args)
  return {
    saved: true,
    path: targetPath,
  }
}
