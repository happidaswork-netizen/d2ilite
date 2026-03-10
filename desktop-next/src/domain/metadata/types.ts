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
export type BatchMatchMode = 'all' | 'missing_original' | 'missing_alias' | 'missing_any'

export type BatchRoleOperation = {
  originalRoleMode: BatchOriginalRoleMode
  originalRoleName: string
  aliasMode: BatchAliasMode
  aliasText: string
}

export type BatchExecutionFailure = {
  path: string
  error: string
}

export type BatchExecutionProgress = {
  active: boolean
  total: number
  processed: number
  changed: number
  skipped: number
  failed: number
}

export type BatchExecutionReport = {
  completedAt: string
  scope: BatchRoleScope
  matchMode: BatchMatchMode
  total: number
  changed: number
  skipped: number
  failed: number
  failures: BatchExecutionFailure[]
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
