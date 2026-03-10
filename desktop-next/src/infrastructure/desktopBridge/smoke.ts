import type { BridgeHealth, DesktopBridge, SavePayload } from '../../types.ts'
import { getFileName } from '../../shared/path.ts'

import { fetchDesktopSmokeRequest, reportDesktopSmokeResult } from './client.ts'

async function checkPreviewLoad(src: string): Promise<boolean> {
  const target = String(src || '').trim()
  if (!target) {
    return false
  }
  return new Promise<boolean>((resolve) => {
    const img = new Image()
    img.onload = () => resolve(true)
    img.onerror = () => resolve(false)
    img.src = target
  })
}

export async function runDesktopSmokeRoundtrip(
  bridge: DesktopBridge,
  provider: DesktopBridge['provider'],
  health: BridgeHealth,
): Promise<void> {
  if (provider !== 'tauri') {
    return
  }

  const request = await fetchDesktopSmokeRequest()
  if (!request) {
    return
  }

  const runtimeWindow = window as unknown as {
    __D2I_TAURI_SMOKE_ID__?: string
  }
  if (runtimeWindow.__D2I_TAURI_SMOKE_ID__ === request.smoke_id) {
    return
  }
  runtimeWindow.__D2I_TAURI_SMOKE_ID__ = request.smoke_id

  const smokeBase = {
    smoke_id: request.smoke_id,
    provider,
    ping_ok: true,
    bridge_provider: health.provider,
    bridge_version: health.version,
  }

  try {
    const items = await bridge.listImages(request.folder, 20)
    const targetPath =
      (request.filename && items.find((itemPath) => getFileName(itemPath) === request.filename)) || items[0] || ''
    if (!targetPath) {
      throw new Error('smoke list returned no items')
    }

    const before = await bridge.readMetadata(targetPath)
    const previewUrl = bridge.getPreviewUrl(targetPath)
    const previewOk = await checkPreviewLoad(previewUrl)
    const nextTitle = String(request.marker || '').trim() || `smoke-${Date.now()}`
    const payload: SavePayload = {
      title: nextTitle,
      person: String(before.person || ''),
      gender: String(before.gender || ''),
      position: String(before.position || ''),
      city: String(before.city || ''),
      source: String(before.source || ''),
      image_url: String(before.image_url || ''),
      keywords: Array.isArray(before.keywords) ? before.keywords : [],
      titi_asset_id: String(before.titi_asset_id || ''),
      titi_world_id: String(before.titi_world_id || ''),
      description: String(before.description || ''),
    }
    await bridge.saveMetadata(targetPath, payload)
    const after = await bridge.readMetadata(targetPath)
    const saveOk = String(after.title || '') === nextTitle

    await reportDesktopSmokeResult({
      ...smokeBase,
      ok: true,
      list_ok: items.length > 0,
      read_ok: true,
      save_ok: saveOk,
      preview_ok: previewOk,
      item_path: targetPath,
      filename: getFileName(targetPath),
      title_before: String(before.title || ''),
      title_after: String(after.title || ''),
    })
  } catch (error) {
    await reportDesktopSmokeResult({
      ...smokeBase,
      ok: false,
      error: String(error),
    })
  }
}
