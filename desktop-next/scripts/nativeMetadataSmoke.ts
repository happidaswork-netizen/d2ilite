import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { saveNativeMetadata, readNativeMetadata, buildNativeMetadataPing, NATIVE_METADATA_PROVIDER } from './nativeMetadataBackend.ts'

async function main(): Promise<void> {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'd2i-native-metadata-'))
  const scriptsRoot = path.dirname(fileURLToPath(import.meta.url))
  const repoRoot = path.resolve(scriptsRoot, '..', '..')
  const imagePath = path.join(tempRoot, 'sample.jpg')

  const pythonExec = process.platform === 'win32'
    ? path.join(repoRoot, '.venv', 'Scripts', 'python.exe')
    : path.join(repoRoot, '.venv', 'bin', 'python')
  const runner = await import('node:child_process')

  await new Promise<void>((resolve, reject) => {
    const child = runner.spawn(
      pythonExec,
      [
        '-c',
        [
          'from PIL import Image',
          `Image.new("RGB",(48,48),(90,120,200)).save(r"${imagePath.replace(/\\/g, '\\\\')}")`,
        ].join(';'),
      ],
      { cwd: repoRoot, windowsHide: true },
    )
    child.on('error', reject)
    child.on('close', (code) => {
      if (code && code !== 0) {
        reject(new Error(`sample image creation failed: ${code}`))
        return
      }
      resolve()
    })
  })

  const ping = buildNativeMetadataPing()
  assert.equal(ping.provider, NATIVE_METADATA_PROVIDER)

  const before = await readNativeMetadata(imagePath)
  assert.equal(String((before.item as Record<string, unknown>).filename || ''), 'sample.jpg')

  const marker = `native-metadata-${Date.now()}`
  const roleAliases = [{ name: '角色A', note: '测试', enabled: true }]
  await saveNativeMetadata(imagePath, {
    title: marker,
    person: '测试人物',
    gender: '男',
    position: '民警',
    city: '测试市',
    source: 'https://example.com/source',
    image_url: 'https://example.com/image.jpg',
    keywords: ['角色A', '测试人物'],
    titi_asset_id: '',
    titi_world_id: '',
    description: 'native metadata smoke',
    d2i_profile: {
      name: '原角色名',
      police_id: 'A-001',
    },
    role_aliases: roleAliases,
  })

  const after = await readNativeMetadata(imagePath)
  const item = after.item as Record<string, unknown>
  const titiJson = (item.titi_json || {}) as Record<string, unknown>
  const profile = ((titiJson.d2i_profile || {}) as Record<string, unknown>)
  const aliases = Array.isArray(titiJson.role_aliases) ? titiJson.role_aliases : []

  assert.equal(String(item.title || ''), marker)
  assert.equal(String(item.person || ''), '测试人物')
  assert.equal(String(item.position || ''), '民警')
  assert.equal(String(profile.name || ''), '原角色名')
  assert.equal(String(profile.police_id || ''), 'A-001')
  assert.equal(aliases.length, 1)
  assert.equal(String((aliases[0] as Record<string, unknown>).name || ''), '角色A')

  await fs.rm(tempRoot, { recursive: true, force: true })
  console.log('[OK] native metadata smoke passed')
}

void main().catch((error) => {
  console.error(error)
  process.exit(1)
})
