import type { MetadataItem, SavePayload } from '../../types.ts'

import {
  buildRoleAliasesPayload,
  getAdaptiveProfile,
  getTitiJson,
  normalizeRoleAliases,
} from './roles.ts'
import type { FormState, MetadataTabKey } from './types.ts'

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
