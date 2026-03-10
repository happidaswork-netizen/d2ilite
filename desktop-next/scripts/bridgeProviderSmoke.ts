import assert from 'node:assert/strict'

import { resolveDesktopBridgeProvider } from '../src/infrastructure/desktopBridge/index.ts'

type SmokeCase = {
  name: string
  hasTauriInvoke: boolean
  isDev: boolean
  expected: string
}

const cases: SmokeCase[] = [
  {
    name: 'tauri runtime wins over dev flag',
    hasTauriInvoke: true,
    isDev: true,
    expected: 'tauri',
  },
  {
    name: 'vite python cli is used in dev when tauri is absent',
    hasTauriInvoke: false,
    isDev: true,
    expected: 'vite-python-cli',
  },
  {
    name: 'mock is used outside dev without tauri runtime',
    hasTauriInvoke: false,
    isDev: false,
    expected: 'mock',
  },
]

for (const item of cases) {
  const actual = resolveDesktopBridgeProvider(item.hasTauriInvoke, item.isDev)
  assert.equal(actual, item.expected, item.name)
}

console.log(`[OK] desktop bridge provider smoke passed: ${cases.length} cases`)
