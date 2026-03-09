import type { MetadataItem, RoleAliasPayload, SavePayload } from '../../types'

export type RoleAliasFormItem = {
  id: string
  name: string
  note: string
  enabled: boolean
}

export type RoleMetadataSummary = {
  originalRoleName: string
  roleAliasNames: string[]
}

export type BatchRoleScope = 'selected' | 'filtered'
export type BatchOriginalRoleMode = 'ignore' | 'set' | 'clear'
export type BatchAliasMode = 'ignore' | 'append' | 'replace' | 'clear'

export type BatchRoleOperation = {
  originalRoleMode: BatchOriginalRoleMode
  originalRoleName: string
  aliasMode: BatchAliasMode
  aliasText: string
}

export type FormState = {
  title: string
  person: string
  gender: string
  position: string
  city: string
  source: string
  image_url: string
  keywords_text: string
  titi_asset_id: string
  titi_world_id: string
  description: string
  original_role_name: string
  role_aliases: RoleAliasFormItem[]
}

export type MetadataTabKey = 'profile' | 'titi' | 'xmp' | 'exif' | 'iptc' | 'match'

export const METADATA_TABS: Array<{ key: MetadataTabKey; label: string }> = [
  { key: 'profile', label: 'Profile' },
  { key: 'titi', label: 'TITI' },
  { key: 'xmp', label: 'XMP' },
  { key: 'exif', label: 'EXIF' },
  { key: 'iptc', label: 'IPTC' },
  { key: 'match', label: 'Match' },
]

function createRoleAliasId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `role-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

export function createRoleAliasFormItem(partial?: Partial<RoleAliasFormItem>): RoleAliasFormItem {
  return {
    id: partial?.id || createRoleAliasId(),
    name: String(partial?.name || '').trim(),
    note: String(partial?.note || '').trim(),
    enabled: partial?.enabled !== false,
  }
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }
  return value as Record<string, unknown>
}

export function getTitiJson(item: MetadataItem | null): Record<string, unknown> {
  return asRecord(item?.titi_json) || {}
}

export function getAdaptiveProfile(item: MetadataItem | null): Record<string, unknown> {
  return { ...(asRecord(getTitiJson(item).d2i_profile) || {}) }
}

export function normalizeRoleAliases(raw: unknown): RoleAliasFormItem[] {
  if (!Array.isArray(raw)) {
    return []
  }
  return dedupeRoleAliases(
    raw.map((entry) => {
      if (typeof entry === 'string') {
        return createRoleAliasFormItem({
          name: entry.trim(),
        })
      }
      const record = asRecord(entry) || {}
      return createRoleAliasFormItem({
        name: String(record.name || record.label || '').trim(),
        note: String(record.note || record.remark || record.description || '').trim(),
        enabled: record.enabled !== false && record.disabled !== true,
      })
    }),
  )
}

export function buildRoleAliasesFromText(raw: string): RoleAliasFormItem[] {
  const seen = new Set<string>()
  const out: RoleAliasFormItem[] = []
  for (const token of String(raw || '').split(/[;,，、\n]+/)) {
    const name = token.trim()
    if (!name) continue
    const normalized = name.toLowerCase()
    if (seen.has(normalized)) continue
    seen.add(normalized)
    out.push(createRoleAliasFormItem({ name }))
  }
  return out
}

export function dedupeRoleAliases(items: RoleAliasFormItem[]): RoleAliasFormItem[] {
  const seen = new Set<string>()
  const out: RoleAliasFormItem[] = []
  for (const item of items) {
    const name = String(item.name || '').trim()
    if (!name) continue
    const normalized = name.toLowerCase()
    if (seen.has(normalized)) continue
    seen.add(normalized)
    out.push(createRoleAliasFormItem(item))
  }
  return out
}

export function extractRoleMetadataSummary(item: MetadataItem | null): RoleMetadataSummary {
  const adaptiveProfile = getAdaptiveProfile(item)
  const titiJson = getTitiJson(item)
  const roleAliases = normalizeRoleAliases(titiJson.role_aliases)
  return {
    originalRoleName: String(adaptiveProfile.name || '').trim(),
    roleAliasNames: roleAliases.map((entry) => entry.name).filter(Boolean),
  }
}

export function extractRoleMetadataSummaryFromForm(form: FormState | null): RoleMetadataSummary {
  return {
    originalRoleName: String(form?.original_role_name || '').trim(),
    roleAliasNames: (form?.role_aliases || []).map((entry) => String(entry.name || '').trim()).filter(Boolean),
  }
}

export function applyBatchRoleOperation(form: FormState, operation: BatchRoleOperation): FormState {
  let nextOriginalRoleName = form.original_role_name
  let nextRoleAliases = [...form.role_aliases]

  if (operation.originalRoleMode === 'set') {
    nextOriginalRoleName = String(operation.originalRoleName || '').trim()
  } else if (operation.originalRoleMode === 'clear') {
    nextOriginalRoleName = ''
  }

  if (operation.aliasMode === 'clear') {
    nextRoleAliases = []
  } else if (operation.aliasMode === 'replace') {
    nextRoleAliases = buildRoleAliasesFromText(operation.aliasText)
  } else if (operation.aliasMode === 'append') {
    nextRoleAliases = dedupeRoleAliases([
      ...nextRoleAliases,
      ...buildRoleAliasesFromText(operation.aliasText),
    ])
  }

  return {
    ...form,
    original_role_name: nextOriginalRoleName,
    role_aliases: nextRoleAliases,
  }
}

export function toForm(item: MetadataItem): FormState {
  const adaptiveProfile = getAdaptiveProfile(item)
  const titiJson = getTitiJson(item)
  return {
    title: String(item.title || ''),
    person: String(item.person || ''),
    gender: String(item.gender || ''),
    position: String(item.position || ''),
    city: String(item.city || ''),
    source: String(item.source || ''),
    image_url: String(item.image_url || ''),
    keywords_text: Array.isArray(item.keywords) ? item.keywords.join(', ') : '',
    titi_asset_id: String(item.titi_asset_id || ''),
    titi_world_id: String(item.titi_world_id || ''),
    description: String(item.description || ''),
    original_role_name: String(adaptiveProfile.name || ''),
    role_aliases: normalizeRoleAliases(titiJson.role_aliases),
  }
}

export function parseKeywords(raw: string): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const token of String(raw || '').split(/[;,，、\n]+/)) {
    const value = token.trim()
    if (!value) continue
    const normalized = value.toLowerCase()
    if (seen.has(normalized)) continue
    seen.add(normalized)
    out.push(value)
  }
  return out
}

export function buildRoleAliasesPayload(roleAliases: RoleAliasFormItem[]): RoleAliasPayload[] {
  return dedupeRoleAliases(roleAliases)
    .map((entry) => ({
      name: String(entry.name || '').trim(),
      note: String(entry.note || '').trim(),
      enabled: Boolean(entry.enabled),
    }))
    .filter((entry) => entry.name)
}

export function toPayload(form: FormState, item: MetadataItem | null): SavePayload {
  const adaptiveProfile = getAdaptiveProfile(item)
  const originalRoleName = String(form.original_role_name || '').trim()
  if (originalRoleName) {
    adaptiveProfile.name = originalRoleName
  } else {
    adaptiveProfile.name = ''
  }

  return {
    title: form.title.trim(),
    person: form.person.trim(),
    gender: form.gender.trim(),
    position: form.position.trim(),
    city: form.city.trim(),
    source: form.source.trim(),
    image_url: form.image_url.trim(),
    keywords: parseKeywords(form.keywords_text),
    titi_asset_id: form.titi_asset_id.trim(),
    titi_world_id: form.titi_world_id.trim(),
    description: form.description.trim(),
    d2i_profile: adaptiveProfile,
    role_aliases: buildRoleAliasesPayload(form.role_aliases),
  }
}

export function sameForm(left: FormState | null, right: FormState | null): boolean {
  return JSON.stringify(left || {}) === JSON.stringify(right || {})
}

export function hasData(value: unknown): boolean {
  if (Array.isArray(value)) {
    return value.length > 0
  }
  if (value && typeof value === 'object') {
    return Object.keys(value as Record<string, unknown>).length > 0
  }
  return Boolean(value)
}

export function stringifyJson(value: unknown): string {
  return JSON.stringify(value, null, 2)
}

export function formatFileSize(value: number | undefined): string {
  if (typeof value !== 'number' || Number.isNaN(value) || value <= 0) {
    return '-'
  }
  const units = ['B', 'KB', 'MB', 'GB']
  let size = value
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size >= 10 || unitIndex === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unitIndex]}`
}

export function formatTimestamp(value: string | undefined): string {
  const target = String(value || '').trim()
  if (!target) {
    return '-'
  }
  return target.replace('T', ' ').replace('Z', '')
}

export function getTabPayload(item: MetadataItem | null, tab: MetadataTabKey): unknown {
  if (!item) {
    return null
  }
  switch (tab) {
    case 'titi':
      return item.titi_json
    case 'xmp':
      return item.other_xmp
    case 'exif':
      return item.other_exif
    case 'iptc':
      return item.other_iptc
    case 'match':
      return item.matched_row
    default:
      return null
  }
}
