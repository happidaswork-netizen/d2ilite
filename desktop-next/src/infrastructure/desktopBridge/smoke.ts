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

async function waitForScraperPause(
  bridge: DesktopBridge,
  baseRoot: string,
  taskRoot: string,
  timeoutMs = 15000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const workspace = await bridge.readScraperWorkspace(baseRoot, {
      selectedRoot: taskRoot,
      progressLimit: 80,
      logLines: 40,
    })
    const detail = workspace.detail
    if (detail && !detail.session_running) {
      return true
    }
    await new Promise((resolve) => window.setTimeout(resolve, 400))
  }
  return false
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
    let scraperOk = true
    let scraperControlOk = true
    let scraperLaunchOk = true
    if (request.scraper_base_root && request.scraper_task_root) {
      const workspace = await bridge.readScraperWorkspace(request.scraper_base_root, {
        selectedRoot: request.scraper_task_root,
        progressLimit: 80,
        logLines: 40,
      })
      scraperOk =
        String(workspace.selected_root || '') === String(request.scraper_task_root || '') &&
        Number(workspace.task_count || 0) >= 1 &&
        Boolean(workspace.detail)
      if (scraperOk) {
        const continued = await bridge.runScraperAction('continue', request.scraper_task_root, {
          baseRoot: request.scraper_base_root,
          control: {
            mode: 'browser',
            auto_fallback: false,
            disable_page_images: false,
          },
        })
        const continueDetail = continued.workspace.detail
        scraperControlOk = Boolean(continueDetail?.session_running) && Number(continueDetail?.pid || 0) > 0
        if (scraperControlOk) {
          await bridge.runScraperAction('pause', request.scraper_task_root, {
            baseRoot: request.scraper_base_root,
            control: {},
          })
          scraperControlOk = await waitForScraperPause(bridge, request.scraper_base_root, request.scraper_task_root)
        }
      } else {
        scraperControlOk = false
      }

      const launchState = await bridge.readScraperLaunchState('https://example.com/list', '')
      const launchRoot = `${request.scraper_base_root}\\tauri_launch_task`
      const started = await bridge.startScraperTask(
        {
          ...launchState,
          start_url: 'https://example.com/list',
          output_root: launchRoot,
          interval_min: '1.5',
          interval_max: '2.5',
          timeout_seconds: '20',
          suspect_block_consecutive_failures: '3',
          image_download_mode: 'browser',
          auto_fallback_to_browser: false,
          disable_page_images_during_crawl: false,
          save_generated_template: false,
          cleanup_generated_template: false,
          selected_template_path: '',
          template_start_url: '',
        },
        { baseRoot: request.scraper_base_root },
      )
      const launchDetail = started.workspace.detail
      scraperLaunchOk = Boolean(launchDetail?.session_running) && Number(launchDetail?.pid || 0) > 0
      if (scraperLaunchOk) {
        await bridge.runScraperAction('pause', launchRoot, {
          baseRoot: request.scraper_base_root,
          control: {},
        })
        scraperLaunchOk = await waitForScraperPause(bridge, request.scraper_base_root, launchRoot)
      }
    }

    await reportDesktopSmokeResult({
      ...smokeBase,
      ok: true,
      list_ok: items.length > 0,
      read_ok: true,
      save_ok: saveOk,
      preview_ok: previewOk,
      scraper_ok: scraperOk,
      scraper_control_ok: scraperControlOk,
      scraper_launch_ok: scraperLaunchOk,
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
