import { createReadStream, existsSync } from 'node:fs'
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import react from '@vitejs/plugin-react'
import { defineConfig, type Plugin } from 'vite'

import {
  buildNativeMetadataPing,
  readNativeMetadata,
  saveNativeMetadata,
} from './scripts/nativeMetadataBackend.ts'
import {
  getDefaultScraperBaseRoot,
  readNativeScraperLaunchState,
  readNativeScraperWorkspace,
  startNativeScraperTask,
  runNativeScraperAction,
} from './scripts/nativeScraperBackend.ts'

type BridgePayload = Record<string, unknown>

const desktopRoot = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.resolve(desktopRoot, '..')
const tempRoot = path.join(projectRoot, '.tmp', 'desktop-next')
const frontendStatusPath = path.join(tempRoot, 'frontend-status.json')
const smokeRequestPath = path.join(tempRoot, 'smoke-request.json')
const smokeReportPath = path.join(tempRoot, 'smoke-report.json')
const imageExts = new Set(['.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tif', '.tiff'])

function jsonResponse(res: import('node:http').ServerResponse, statusCode: number, payload: BridgePayload): void {
  res.statusCode = statusCode
  res.setHeader('Content-Type', 'application/json; charset=utf-8')
  res.setHeader('Cache-Control', 'no-store')
  res.end(JSON.stringify(payload))
}

function parseBody(req: import('node:http').IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    req.on('end', () => {
      if (chunks.length === 0) {
        resolve({})
        return
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf-8')))
      } catch (error) {
        reject(error)
      }
    })
    req.on('error', reject)
  })
}

function guessContentType(targetPath: string): string {
  const ext = path.extname(targetPath).toLowerCase()
  if (ext === '.png') return 'image/png'
  if (ext === '.webp') return 'image/webp'
  if (ext === '.bmp') return 'image/bmp'
  if (ext === '.gif') return 'image/gif'
  if (ext === '.svg') return 'image/svg+xml'
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg'
  if (ext === '.tif' || ext === '.tiff') return 'image/tiff'
  return 'application/octet-stream'
}

async function listImagesInFolder(folder: string, limit = 0): Promise<string[]> {
  const targetFolder = path.resolve(String(folder || '').trim())
  if (!targetFolder) {
    throw new Error('folder is required')
  }
  if (!existsSync(targetFolder)) {
    throw new Error(`folder not found (${targetFolder})`)
  }

  const entries = await readdir(targetFolder, { withFileTypes: true })
  const items = entries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter((name) => imageExts.has(path.extname(name).toLowerCase()))
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }))
    .map((name) => path.join(targetFolder, name))

  if (limit > 0) {
    return items.slice(0, limit)
  }
  return items
}

function desktopBridgeDevPlugin(): Plugin {
  return {
    name: 'desktop-bridge-dev-plugin',
    configureServer(server) {
      server.middlewares.use('/api/bridge', async (req, res, next) => {
        try {
          const base = `http://${req.headers.host || '127.0.0.1'}`
          const url = new URL(req.url || '/', base)
          const routePath = url.pathname

          if (req.method === 'GET' && routePath === '/ping') {
            jsonResponse(res, 200, { ok: true, ...buildNativeMetadataPing() })
            return
          }

          if (req.method === 'GET' && routePath === '/list') {
            const folder = url.searchParams.get('folder') || ''
            const limit = Math.max(0, Number(url.searchParams.get('limit') || '0') || 0)
            const items = await listImagesInFolder(folder, limit)
            jsonResponse(res, 200, { ok: true, folder: path.resolve(folder), count: items.length, items })
            return
          }

          if (req.method === 'GET' && routePath === '/scraper/default-root') {
            jsonResponse(res, 200, await getDefaultScraperBaseRoot())
            return
          }

          if (req.method === 'GET' && routePath === '/scraper/launch-state') {
            const sourceHint = url.searchParams.get('sourceHint') || ''
            const templatePath = url.searchParams.get('templatePath') || ''
            jsonResponse(res, 200, await readNativeScraperLaunchState(sourceHint, templatePath))
            return
          }

          if (req.method === 'GET' && routePath === '/scraper/workspace') {
            const baseRoot = url.searchParams.get('baseRoot') || ''
            const selectedRoot = url.searchParams.get('selectedRoot') || ''
            const progressLimit = Math.max(20, Number(url.searchParams.get('progressLimit') || '300') || 300)
            const logLines = Math.max(20, Number(url.searchParams.get('logLines') || '80') || 80)
            jsonResponse(res, 200, await readNativeScraperWorkspace(baseRoot, { selectedRoot, progressLimit, logLines }))
            return
          }

          if (req.method === 'POST' && routePath === '/scraper/start') {
            const body = (await parseBody(req)) as {
              values?: unknown
              baseRoot?: string
            }
            const response = await startNativeScraperTask((body?.values as BridgePayload) ?? {}, {
              baseRoot: String(body?.baseRoot || '').trim(),
            })
            jsonResponse(res, 200, response)
            return
          }

          if (req.method === 'POST' && routePath === '/scraper/action') {
            const body = (await parseBody(req)) as {
              action?: string
              outputRoot?: string
              baseRoot?: string
              control?: unknown
            }
            const action = String(body?.action || '').trim()
            const outputRoot = String(body?.outputRoot || '').trim()
            const baseRoot = String(body?.baseRoot || '').trim()
            const response = await runNativeScraperAction(action, outputRoot, {
              baseRoot,
              control: (body?.control as BridgePayload) ?? {},
            })
            jsonResponse(res, 200, response)
            return
          }

          if (req.method === 'GET' && routePath === '/read') {
            const filePath = url.searchParams.get('path') || ''
            jsonResponse(res, 200, { ok: true, ...(await readNativeMetadata(filePath)) })
            return
          }

          if (req.method === 'POST' && routePath === '/save') {
            const body = (await parseBody(req)) as {
              path?: string
              payload?: unknown
            }
            const targetPath = String(body?.path || '').trim()
            const payload = body?.payload ?? {}
            jsonResponse(res, 200, { ok: true, ...(await saveNativeMetadata(targetPath, payload as BridgePayload)) })
            return
          }

          if (req.method === 'POST' && routePath === '/frontend-status') {
            const body = (await parseBody(req)) as BridgePayload
            await mkdir(tempRoot, { recursive: true })
            await writeFile(frontendStatusPath, JSON.stringify(body ?? {}, null, 2), 'utf-8')
            jsonResponse(res, 200, { ok: true, reported: true, path: frontendStatusPath })
            return
          }

          if (req.method === 'GET' && routePath === '/smoke-request') {
            if (!existsSync(smokeRequestPath)) {
              jsonResponse(res, 404, { ok: false, error: 'smoke request not found' })
              return
            }
            const body = JSON.parse(await readFile(smokeRequestPath, 'utf-8')) as BridgePayload
            jsonResponse(res, 200, { ok: true, request: body })
            return
          }

          if (req.method === 'POST' && routePath === '/smoke-report') {
            const body = (await parseBody(req)) as BridgePayload
            await mkdir(tempRoot, { recursive: true })
            await writeFile(smokeReportPath, JSON.stringify(body ?? {}, null, 2), 'utf-8')
            jsonResponse(res, 200, { ok: true, reported: true, path: smokeReportPath })
            return
          }

          if (req.method === 'GET' && routePath === '/preview') {
            const rawPath = String(url.searchParams.get('path') || '').trim()
            if (!rawPath) {
              jsonResponse(res, 400, { ok: false, error: 'path is required' })
              return
            }
            const filePath = path.resolve(rawPath)
            if (!existsSync(filePath)) {
              jsonResponse(res, 404, { ok: false, error: 'file not found', detail: filePath })
              return
            }
            res.statusCode = 200
            res.setHeader('Content-Type', guessContentType(filePath))
            res.setHeader('Cache-Control', 'no-store')
            createReadStream(filePath).pipe(res)
            return
          }

          next()
        } catch (error) {
          jsonResponse(res, 500, {
            ok: false,
            error: 'desktop bridge dev server error',
            detail: error instanceof Error ? error.message : String(error),
          })
        }
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), desktopBridgeDevPlugin()],
  server: {
    host: '127.0.0.1',
    port: 1420,
  },
})
