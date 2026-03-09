export type BridgeProvider = 'mock' | 'tauri' | 'vite-python-cli'

export interface BridgeHealth {
  provider: string
  version: string
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
  getPreviewUrl(path: string): string
}
