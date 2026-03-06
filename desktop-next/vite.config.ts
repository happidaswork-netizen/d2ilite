import { spawn } from 'node:child_process'
import { createReadStream, existsSync } from 'node:fs'
import { mkdir, rm, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import react from '@vitejs/plugin-react'
import { defineConfig, type Plugin } from 'vite'

type BridgePayload = Record<string, unknown>

const desktopRoot = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.resolve(desktopRoot, '..')
const bridgeScriptPath = path.join(projectRoot, 'scripts', 'desktop_bridge_cli.py')
const tempRoot = path.join(projectRoot, '.tmp', 'desktop-next')

function resolvePythonExecutable(): string {
  const candidates = [
    path.join(projectRoot, '.venv', 'Scripts', 'python.exe'),
    path.join(projectRoot, '.venv', 'bin', 'python'),
  ]
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate
    }
  }
  return 'python'
}

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

async function withPayloadFile(payload: unknown, run: (filePath: string) => Promise<BridgePayload>): Promise<BridgePayload> {
  await mkdir(tempRoot, { recursive: true })
  const payloadFile = path.join(tempRoot, `payload-${Date.now()}-${Math.random().toString(16).slice(2)}.json`)
  await writeFile(payloadFile, JSON.stringify(payload ?? {}, null, 2), 'utf-8')
  try {
    return await run(payloadFile)
  } finally {
    await rm(payloadFile, { force: true })
  }
}

function runBridgeCli(args: string[]): Promise<BridgePayload> {
  return new Promise((resolve, reject) => {
    const pythonExec = resolvePythonExecutable()
    const child = spawn(pythonExec, [bridgeScriptPath, ...args], {
      cwd: projectRoot,
      env: {
        ...process.env,
        PYTHONUTF8: '1',
      },
      windowsHide: true,
    })

    const stdout: Buffer[] = []
    const stderr: Buffer[] = []
    child.stdout.on('data', (chunk) => stdout.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    child.stderr.on('data', (chunk) => stderr.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    child.on('error', reject)
    child.on('close', (code) => {
      const outText = Buffer.concat(stdout).toString('utf-8').trim()
      const errText = Buffer.concat(stderr).toString('utf-8').trim()
      if (!outText) {
        reject(new Error(errText || `desktop bridge returned empty stdout (code=${String(code)})`))
        return
      }
      try {
        const payload = JSON.parse(outText) as BridgePayload
        if (code && code !== 0 && !payload.ok) {
          const detail = typeof payload.detail === 'string' ? ` (${payload.detail})` : ''
          reject(new Error(`${String(payload.error || 'bridge command failed')}${detail}`))
          return
        }
        resolve(payload)
      } catch (error) {
        reject(new Error(`invalid bridge json: ${outText}\n${errText || String(error)}`))
      }
    })
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
            jsonResponse(res, 200, await runBridgeCli(['ping']))
            return
          }

          if (req.method === 'GET' && routePath === '/list') {
            const folder = url.searchParams.get('folder') || ''
            const limit = url.searchParams.get('limit') || '0'
            jsonResponse(res, 200, await runBridgeCli(['list', '--folder', folder, '--limit', limit]))
            return
          }

          if (req.method === 'GET' && routePath === '/read') {
            const filePath = url.searchParams.get('path') || ''
            jsonResponse(res, 200, await runBridgeCli(['read', '--path', filePath]))
            return
          }

          if (req.method === 'POST' && routePath === '/save') {
            const body = (await parseBody(req)) as {
              path?: string
              payload?: unknown
            }
            const targetPath = String(body?.path || '').trim()
            const payload = body?.payload ?? {}
            const response = await withPayloadFile(payload, (payloadFile) =>
              runBridgeCli(['save', '--path', targetPath, '--payload-file', payloadFile]),
            )
            jsonResponse(res, 200, response)
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
