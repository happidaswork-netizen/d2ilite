import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import {
  readNativeScraperLaunchState,
  readNativeScraperWorkspace,
  runNativeScraperAction,
  startNativeScraperTask,
} from './nativeScraperBackend.ts'

type JsonRecord = Record<string, unknown>

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..')
const registryPath = path.join(projectRoot, '.tmp', 'desktop-next', 'scraper-runtime-registry.json')

function safeAssert(condition: unknown, message: string): void {
  if (!condition) {
    throw new Error(message)
  }
}

async function writeJson(filePath: string, payload: unknown): Promise<void> {
  await writeFile(filePath, JSON.stringify(payload, null, 2), 'utf-8')
}

async function writeJsonl(filePath: string, rows: JsonRecord[]): Promise<void> {
  await writeFile(filePath, rows.map((row) => JSON.stringify(row)).join('\n') + '\n', 'utf-8')
}

async function writeFakeRunner(filePath: string): Promise<void> {
  await writeFile(
    filePath,
    `#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import time
from pathlib import Path

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--skip-crawl", action="store_true")
    parser.add_argument("--skip-images", action="store_true")
    parser.add_argument("--skip-metadata", action="store_true")
    args = parser.parse_args()
    root = Path(args.output_root).resolve()
    pause_flag = root / "state" / "manual_pause.flag"
    for index in range(80):
        print(f"[fake-runner] tick={index}", flush=True)
        if pause_flag.exists():
            print("[fake-runner] manual pause detected", flush=True)
            return 0
        time.sleep(0.2)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
`,
    'utf-8',
  )
}

async function prepareSampleTask(baseRoot: string): Promise<string> {
  const taskRoot = path.join(baseRoot, 'sample_scraper_task')
  const imagePath = path.join(taskRoot, 'downloads', 'named_images', 'done.jpg')
  await mkdir(path.dirname(imagePath), { recursive: true })
  await mkdir(path.join(taskRoot, 'state'), { recursive: true })
  await mkdir(path.join(taskRoot, 'raw'), { recursive: true })
  await mkdir(path.join(taskRoot, 'reports'), { recursive: true })
  await writeFile(imagePath, 'fake-image', 'utf-8')

  await writeJson(path.join(taskRoot, 'state', 'runtime_config.json'), { rules: {} })
  await writeJsonl(path.join(taskRoot, 'raw', 'list_records.jsonl'), [
    { name: '已完成人物', detail_url: 'https://example.com/detail/done' },
    { name: '缺失详情人物', detail_url: '' },
  ])
  await writeJsonl(path.join(taskRoot, 'raw', 'profiles.jsonl'), [
    { name: '已完成人物', detail_url: 'https://example.com/detail/done', image_url: 'https://example.com/image/done.jpg' },
  ])
  await writeJsonl(path.join(taskRoot, 'downloads', 'image_downloads.jsonl'), [
    { name: '已完成人物', detail_url: 'https://example.com/detail/done', named_path: imagePath },
  ])
  await writeJsonl(path.join(taskRoot, 'raw', 'metadata_write_results.jsonl'), [
    { detail_url: 'https://example.com/detail/done', status: 'ok', output_path: imagePath },
  ])
  await writeJsonl(path.join(taskRoot, 'raw', 'review_queue.jsonl'), [{ detail_url: 'https://example.com/detail/review' }])
  await writeJsonl(path.join(taskRoot, 'raw', 'failures.jsonl'), [{ url: 'https://example.com/detail/failure', reason: 'image_download_http_error' }])
  await writeFile(path.join(taskRoot, 'reports', 'gui_public_scraper.log'), '[2026-03-10 12:00:00] 开始抓取\n', 'utf-8')
  return taskRoot
}

async function readRuntimeConfig(taskRoot: string): Promise<JsonRecord> {
  return JSON.parse(await readFile(path.join(taskRoot, 'state', 'runtime_config.json'), 'utf-8')) as JsonRecord
}

async function waitForWorkspace(
  baseRoot: string,
  taskRoot: string,
  predicate: (detail: JsonRecord) => boolean,
  timeoutMs = 15000,
): Promise<JsonRecord> {
  const deadline = Date.now() + timeoutMs
  let last: JsonRecord = {}
  while (Date.now() < deadline) {
    last = await readNativeScraperWorkspace(baseRoot, { selectedRoot: taskRoot, progressLimit: 50, logLines: 20 })
    const detail = (last.detail || {}) as JsonRecord
    if (predicate(detail)) {
      return last
    }
    await new Promise((resolve) => setTimeout(resolve, 400))
  }
  throw new Error(`workspace wait timeout: ${JSON.stringify(last)}`)
}

async function main(): Promise<void> {
  const backup = await readFile(registryPath).catch(() => null)
  await rm(registryPath, { force: true }).catch(() => undefined)

  const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'd2i-native-scraper-'))
  const baseRoot = path.join(tempRoot, 'public_archive')
  const taskRoot = await prepareSampleTask(baseRoot)
  const runnerPath = path.join(tempRoot, 'fake_scraper_runner.py')
  await writeFakeRunner(runnerPath)
  process.env.D2I_DESKTOP_SCRAPER_RUNNER = runnerPath
  process.env.PYTHONUTF8 = '1'

  try {
    const launchState = await readNativeScraperLaunchState('https://example.com/list', '')
    safeAssert(typeof launchState.start_url === 'string', `unexpected launch state: ${JSON.stringify(launchState)}`)

    const newTaskRoot = path.join(baseRoot, 'new_launch_task')
    const started = await startNativeScraperTask(
      {
        ...launchState,
        start_url: 'https://example.com/list',
        output_root: newTaskRoot,
        interval_min: '1.5',
        interval_max: '2.5',
        timeout_seconds: '20',
        suspect_block_consecutive_failures: '3',
        image_download_mode: 'browser',
        auto_fallback_to_browser: false,
        disable_page_images_during_crawl: false,
        output_minimal: true,
        direct_write_images: true,
        llm_enrich_enabled: false,
        save_generated_template: false,
        cleanup_generated_template: false,
        selected_template_path: '',
        template_start_url: '',
      } as JsonRecord,
      { baseRoot },
    )
    const newDetail = ((started.workspace as JsonRecord).detail || {}) as JsonRecord
    safeAssert(String(started.created_root || '') === newTaskRoot, `unexpected created root: ${JSON.stringify(started)}`)
    safeAssert(Boolean(newDetail.session_running) && Number(newDetail.pid || 0) > 0, `new task did not start: ${JSON.stringify(started)}`)
    const newConfig = await readRuntimeConfig(newTaskRoot)
    const newRules = (newConfig.rules || {}) as JsonRecord
    safeAssert(String(newRules.image_download_mode || '') === 'browser', 'new task missing image mode')
    safeAssert(newRules.auto_fallback_to_browser === false, 'new task missing auto_fallback override')
    await runNativeScraperAction('pause', newTaskRoot, { baseRoot, control: {} })
    await waitForWorkspace(baseRoot, newTaskRoot, (detail) => !detail.session_running)

    const initial = await readNativeScraperWorkspace(baseRoot, { selectedRoot: taskRoot, progressLimit: 50, logLines: 20 })
    safeAssert(Number(initial.task_count || 0) >= 2, `unexpected initial workspace: ${JSON.stringify(initial)}`)

    const continueOptions = { mode: 'browser', auto_fallback: false, disable_page_images: false }
    const continued = await runNativeScraperAction('continue', taskRoot, { baseRoot, control: continueOptions })
    const continueDetail = ((continued.workspace as JsonRecord).detail || {}) as JsonRecord
    safeAssert(continueDetail.session_running && Number(continueDetail.pid || 0) > 0, `continue did not start task: ${JSON.stringify(continued)}`)
    const continueRules = (await readRuntimeConfig(taskRoot)).rules as JsonRecord
    safeAssert(String(continueRules.image_download_mode || '') === 'browser', 'continue options missing image mode')
    safeAssert(continueRules.auto_fallback_to_browser === false, 'continue options missing auto_fallback')

    const paused = await runNativeScraperAction('pause', taskRoot, { baseRoot, control: {} })
    const pauseDetail = ((paused.workspace as JsonRecord).detail || {}) as JsonRecord
    safeAssert(pauseDetail.manual_paused, `pause did not mark task paused: ${JSON.stringify(paused)}`)
    await waitForWorkspace(baseRoot, taskRoot, (detail) => !detail.session_running)

    const resumed = await runNativeScraperAction('continue', taskRoot, { baseRoot, control: continueOptions })
    const resumeDetail = ((resumed.workspace as JsonRecord).detail || {}) as JsonRecord
    safeAssert(resumeDetail.session_running && !resumeDetail.manual_paused, `continue did not resume task: ${JSON.stringify(resumed)}`)

    await runNativeScraperAction('pause', taskRoot, { baseRoot, control: {} })
    await waitForWorkspace(baseRoot, taskRoot, (detail) => !detail.session_running)

    const retryOptions = { mode: 'requests_jsl', auto_fallback: true, disable_page_images: true }
    const retried = await runNativeScraperAction('retry', taskRoot, { baseRoot, control: retryOptions })
    const retryDetail = ((retried.workspace as JsonRecord).detail || {}) as JsonRecord
    safeAssert(retryDetail.session_running, `retry did not start task: ${JSON.stringify(retried)}`)
    const retryRules = (await readRuntimeConfig(taskRoot)).rules as JsonRecord
    safeAssert(String(retryRules.image_download_mode || '') === 'requests_jsl', 'retry options missing image mode')
    safeAssert(retryRules.disable_page_images_during_crawl === true, 'retry options missing disable_page_images')

    await runNativeScraperAction('pause', taskRoot, { baseRoot, control: {} })
    await waitForWorkspace(baseRoot, taskRoot, (detail) => !detail.session_running)

    const rewritten = await runNativeScraperAction('rewrite', taskRoot, { baseRoot, control: {} })
    const rewriteDetail = ((rewritten.workspace as JsonRecord).detail || {}) as JsonRecord
    safeAssert(rewriteDetail.session_running && String(rewriteDetail.runtime_state || '') === '元数据重写中', `rewrite did not start task: ${JSON.stringify(rewritten)}`)

    await runNativeScraperAction('pause', taskRoot, { baseRoot, control: {} })
    await waitForWorkspace(baseRoot, taskRoot, (detail) => !detail.session_running)
    process.stdout.write('native scraper smoke passed\n')
  } finally {
    await rm(tempRoot, { recursive: true, force: true }).catch(() => undefined)
    if (backup) {
      await mkdir(path.dirname(registryPath), { recursive: true })
      await writeFile(registryPath, backup)
    } else {
      await rm(registryPath, { force: true }).catch(() => undefined)
    }
  }
}

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
  process.exitCode = 1
})
