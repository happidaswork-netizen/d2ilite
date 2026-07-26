import { createReadStream, existsSync, statSync } from 'node:fs'
import { mkdir, readFile, readdir, rename, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { spawn } from 'node:child_process'
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
  clearNativeScraperReviewItem,
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
  const targetFolder = resolveInputPath(String(folder || '').trim())
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

function sanitizeFileNameStem(value: string, fallback = 'image'): string {
  let cleaned = Array.from(String(value || ''), (char) => {
    const invalid = '<>:"/\\|?*'.includes(char) || char.charCodeAt(0) < 32
    return invalid ? '_' : char
  }).join('')
  cleaned = cleaned.replace(/\s+/g, ' ').trim()
  cleaned = cleaned.replace(/[. ]+$/g, '')
  if (!cleaned) cleaned = fallback
  return cleaned
}

function normalizeWindowsPathText(value: string): string {
  let text = String(value || '').trim().replace(/^"+|"+$/g, '').replace(/\//g, '\\')
  if (process.platform === 'win32') {
    while (text.startsWith('\\\\\\') && !text.startsWith('\\\\?\\')) {
      text = text.replace(/^\\\\\\/, '\\\\')
    }
    const lower = text.toLowerCase()
    if (lower.startsWith('\\\\?\\unc\\')) {
      text = `\\\\${text.slice(8)}`
    } else if (lower.startsWith('\\?\\unc\\')) {
      text = `\\\\${text.slice(7)}`
    } else if (lower.startsWith('?\\unc\\')) {
      text = `\\\\${text.slice(6)}`
    } else if (lower.startsWith('\\\\?\\')) {
      text = text.slice(4)
    } else if (lower.startsWith('\\?\\')) {
      text = text.slice(3)
    } else if (lower.startsWith('?\\')) {
      text = text.slice(2)
    }
  }
  return text
}

function resolveInputPath(value: string): string {
  const normalized = normalizeWindowsPathText(value)
  if (!normalized) {
    return ''
  }
  if (process.platform === 'win32' && normalized.startsWith('\\\\')) {
    return normalized
  }
  return path.resolve(normalized)
}

async function uniqueSiblingPath(sourcePath: string, newName: string): Promise<string> {
  const ext = path.extname(sourcePath)
  const dir = path.dirname(sourcePath)
  const base = sanitizeFileNameStem(newName, path.basename(sourcePath, ext))
  let candidate = path.join(dir, `${base}${ext}`)
  if (path.resolve(candidate).toLowerCase() === path.resolve(sourcePath).toLowerCase()) {
    return sourcePath
  }
  if (!existsSync(candidate)) return candidate
  for (let index = 2; index < 10000; index += 1) {
    candidate = path.join(dir, `${base}_${index}${ext}`)
    if (!existsSync(candidate)) return candidate
  }
  throw new Error('unable to create unique filename')
}

function resolvePythonExecutable(): string {
  const venvPython = path.join(projectRoot, '.venv', 'Scripts', 'python.exe')
  if (existsSync(venvPython)) return venvPython
  return process.platform === 'win32' ? 'python' : 'python3'
}

function runImageActionCli(args: string[]): Promise<Record<string, unknown>> {
  return runPythonCli(path.join(projectRoot, 'scripts', 'image_action_cli.py'), args)
}

function runMetadataAiCli(args: string[]): Promise<Record<string, unknown>> {
  return runPythonCli(path.join(projectRoot, 'scripts', 'metadata_ai_cli.py'), args)
}

function runSettingsCli(args: string[]): Promise<Record<string, unknown>> {
  return runPythonCli(path.join(projectRoot, 'scripts', 'settings_cli.py'), args)
}

function runPythonCli(scriptPath: string, args: string[]): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const child = spawn(resolvePythonExecutable(), [scriptPath, ...args], {
      cwd: projectRoot,
      windowsHide: true,
    })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => {
      stdout += Buffer.isBuffer(chunk) ? chunk.toString('utf-8') : String(chunk)
    })
    child.stderr.on('data', (chunk) => {
      stderr += Buffer.isBuffer(chunk) ? chunk.toString('utf-8') : String(chunk)
    })
    child.on('error', reject)
    child.on('close', (code) => {
      const line = stdout.trim().split(/\r?\n/).filter(Boolean).pop() || ''
      let parsed: Record<string, unknown> = {}
      try {
        parsed = JSON.parse(line) as Record<string, unknown>
      } catch {
        reject(new Error(stderr.trim() || stdout.trim() || `image action failed: ${code}`))
        return
      }
      if (code !== 0 || parsed.ok !== true) {
        reject(new Error(String(parsed.detail || parsed.error || stderr.trim() || `image action failed: ${code}`)))
        return
      }
      resolve(parsed)
    })
  })
}

function openWithSystem(targetPath: string, reveal = false): Promise<void> {
  return new Promise((resolve, reject) => {
    const resolved = resolveInputPath(targetPath)
    if (!existsSync(resolved)) {
      reject(new Error(`path not found (${resolved})`))
      return
    }

    let command = ''
    let args: string[] = []
    if (process.platform === 'win32') {
      command = 'explorer.exe'
      args = reveal ? ['/select,', resolved] : [resolved]
    } else if (process.platform === 'darwin') {
      command = 'open'
      args = reveal ? ['-R', resolved] : [resolved]
    } else {
      command = 'xdg-open'
      args = [reveal ? path.dirname(resolved) : resolved]
    }

    const child = spawn(command, args, {
      detached: true,
      stdio: 'ignore',
      windowsHide: false,
    })
    child.on('error', reject)
    child.unref()
    resolve()
  })
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

          if (req.method === 'POST' && routePath === '/scraper/review-clear') {
            const body = (await parseBody(req)) as {
              outputRoot?: string
              detailUrl?: string
              baseRoot?: string
            }
            const response = await clearNativeScraperReviewItem(
              String(body?.outputRoot || '').trim(),
              String(body?.detailUrl || '').trim(),
              {
                baseRoot: String(body?.baseRoot || '').trim(),
              },
            )
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

          if (req.method === 'POST' && routePath === '/name-bar') {
            const body = (await parseBody(req)) as {
              path?: string
              options?: Record<string, unknown>
            }
            const filePath = String(body?.path || '').trim()
            const options = (body?.options ?? {}) as Record<string, unknown>
            const args = ['name-bar', '--image', filePath, '--name', String(options.name || '')]
            if (options.output_dir) args.push('--output-dir', String(options.output_dir))
            if (options.output_format) args.push('--format', String(options.output_format))
            if (options.output_name) args.push('--output-name', String(options.output_name))
            const result = await runImageActionCli(args)
            jsonResponse(res, 200, {
              ok: true,
              message: String(result.message || ''),
              output_path: String(result.output_path || ''),
              reveal_path: String(result.reveal_path || result.output_path || ''),
            })
            return
          }

          if (req.method === 'POST' && routePath === '/rename') {
            const body = (await parseBody(req)) as {
              path?: string
              newName?: string
            }
            const filePath = resolveInputPath(String(body?.path || '').trim())
            if (!existsSync(filePath)) {
              throw new Error(`file not found (${filePath})`)
            }
            const nextPath = await uniqueSiblingPath(filePath, String(body?.newName || '').trim())
            await rename(filePath, nextPath)
            jsonResponse(res, 200, {
              ok: true,
              old_path: filePath,
              new_path: nextPath,
              filename: path.basename(nextPath),
            })
            return
          }

          if (req.method === 'POST' && routePath === '/autofill-metadata') {
            const body = (await parseBody(req)) as {
              path?: string
              options?: Record<string, unknown>
            }
            const filePath = String(body?.path || '').trim()
            const options = (body?.options ?? {}) as Record<string, unknown>
            const inputMode = String(options.input_mode || 'filename_metadata')
            const args = ['autofill', '--image', filePath, '--input-mode', inputMode]
            if (options.form && typeof options.form === 'object') {
              args.push('--form-json', JSON.stringify(options.form))
            }
            const result = await runMetadataAiCli(args)
            jsonResponse(res, 200, {
              ok: true,
              result: (result.result ?? {}) as BridgePayload,
              input_mode: String(result.input_mode || inputMode),
            })
            return
          }

          if (req.method === 'POST' && routePath === '/generate-biography') {
            const body = (await parseBody(req)) as {
              path?: string
              options?: Record<string, unknown>
            }
            const filePath = String(body?.path || '').trim()
            const options = (body?.options ?? {}) as Record<string, unknown>
            const args = ['biography', '--image', filePath]
            if (options.form && typeof options.form === 'object') {
              args.push('--form-json', JSON.stringify(options.form))
            }
            const result = await runMetadataAiCli(args)
            jsonResponse(res, 200, {
              ok: true,
              result: (result.result ?? {}) as BridgePayload,
            })
            return
          }

          if (routePath === '/settings') {
            if (req.method === 'GET') {
              const result = await runSettingsCli(['get'])
              jsonResponse(res, 200, {
                ok: true,
                settings: (result.settings ?? {}) as BridgePayload,
                path: String(result.path || ''),
              })
              return
            }
            if (req.method === 'POST') {
              const body = (await parseBody(req)) as {
                settings?: Record<string, unknown>
              }
              const result = await runSettingsCli(['save', '--payload-json', JSON.stringify(body?.settings ?? {})])
              jsonResponse(res, 200, {
                ok: true,
                settings: (result.settings ?? {}) as BridgePayload,
                path: String(result.path || ''),
              })
              return
            }
          }

          if (req.method === 'POST' && routePath === '/open-path') {
            const body = (await parseBody(req)) as {
              path?: string
            }
            await openWithSystem(String(body?.path || '').trim(), false)
            jsonResponse(res, 200, { ok: true, opened: true })
            return
          }

          if (req.method === 'POST' && routePath === '/reveal-path') {
            const body = (await parseBody(req)) as {
              path?: string
            }
            await openWithSystem(String(body?.path || '').trim(), true)
            jsonResponse(res, 200, { ok: true, revealed: true })
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

          if (req.method === 'GET' && routePath === '/path-info') {
            const rawPath = String(url.searchParams.get('path') || '').trim()
            const filePath = resolveInputPath(rawPath)
            const exists = Boolean(filePath) && existsSync(filePath)
            const stats = exists ? statSync(filePath) : null
            jsonResponse(res, 200, {
              ok: true,
              path: filePath,
              exists,
              is_file: Boolean(stats?.isFile()),
              is_dir: Boolean(stats?.isDirectory()),
            })
            return
          }

          if (req.method === 'GET' && routePath === '/preview') {
            const rawPath = String(url.searchParams.get('path') || '').trim()
            if (!rawPath) {
              jsonResponse(res, 400, { ok: false, error: 'path is required' })
              return
            }
            const filePath = resolveInputPath(rawPath)
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
