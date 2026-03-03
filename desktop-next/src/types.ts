export type BridgeProvider = 'mock' | 'tauri'

export interface BridgeHealth {
  provider: string
  version: string
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
}

export interface DesktopBridge {
  provider: BridgeProvider
  ping(): Promise<BridgeHealth>
  listImages(folder: string, limit?: number): Promise<string[]>
  readMetadata(path: string): Promise<MetadataItem>
  saveMetadata(path: string, payload: SavePayload): Promise<void>
}

