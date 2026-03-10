import type { MetadataItem, RoleAliasPayload } from '../../types.ts'

import type {
  BatchMatchMode,
  BatchRoleOperation,
  FormState,
  RoleAliasFormItem,
  RoleMetadataSummary,
} from './types.ts'

function createRoleAliasId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `role-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }
  return value as Record<string, unknown>
}

export function createRoleAliasFormItem(partial?: Partial<RoleAliasFormItem>): RoleAliasFormItem {
  return {
    id: partial?.id || createRoleAliasId(),
    name: String(partial?.name || '').trim(),
    note: String(partial?.note || '').trim(),
    enabled: partial?.enabled !== false,
  }
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
        return createRoleAliasFormItem({ name: entry.trim() })
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

export function shouldApplyBatchRoleOperation(form: FormState, matchMode: BatchMatchMode): boolean {
  const hasOriginalRole = Boolean(String(form.original_role_name || '').trim())
  const hasRoleAliases = form.role_aliases.some((entry) => String(entry.name || '').trim())

  switch (matchMode) {
    case 'missing_original':
      return !hasOriginalRole
    case 'missing_alias':
      return !hasRoleAliases
    case 'missing_any':
      return !hasOriginalRole || !hasRoleAliases
    default:
      return true
  }
}

export function hasBatchRoleChange(left: FormState, right: FormState): boolean {
  if (String(left.original_role_name || '').trim() !== String(right.original_role_name || '').trim()) {
    return true
  }
  const leftAliases = left.role_aliases.map((entry) => `${String(entry.name || '').trim()}|${String(entry.note || '').trim()}|${entry.enabled ? '1' : '0'}`)
  const rightAliases = right.role_aliases.map((entry) => `${String(entry.name || '').trim()}|${String(entry.note || '').trim()}|${entry.enabled ? '1' : '0'}`)
  return JSON.stringify(leftAliases) !== JSON.stringify(rightAliases)
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
    nextRoleAliases = dedupeRoleAliases([...nextRoleAliases, ...buildRoleAliasesFromText(operation.aliasText)])
  }

  return {
    ...form,
    original_role_name: nextOriginalRoleName,
    role_aliases: nextRoleAliases,
  }
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
