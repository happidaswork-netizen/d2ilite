import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import type { ScraperWorkspaceSnapshot } from '../types'
import { createDesktopBridge } from '../infrastructure/desktopBridge'

const DEFAULT_PROGRESS_LIMIT = 320
const DEFAULT_LOG_LINES = 90
const AUTO_REFRESH_INTERVAL_MS = 15000

type LoadOptions = {
  baseRoot?: string
  selectedRoot?: string
  silent?: boolean
}

export function useScraperWorkspace() {
  const bridge = useMemo(() => createDesktopBridge(), [])
  const baseRootRef = useRef('')
  const selectedRootRef = useRef('')

  const [baseRoot, setBaseRoot] = useState('')
  const [selectedRoot, setSelectedRoot] = useState('')
  const [snapshot, setSnapshot] = useState<ScraperWorkspaceSnapshot | null>(null)
  const [status, setStatus] = useState('就绪')
  const [busy, setBusy] = useState(false)
  const [bridgeVersion, setBridgeVersion] = useState('未探测')

  useEffect(() => {
    baseRootRef.current = baseRoot
  }, [baseRoot])

  useEffect(() => {
    selectedRootRef.current = selectedRoot
  }, [selectedRoot])

  const loadWorkspace = useCallback(
    async (options?: LoadOptions): Promise<void> => {
      const targetBaseRoot = String(options?.baseRoot ?? baseRootRef.current).trim()
      const targetSelectedRoot = String(options?.selectedRoot ?? selectedRootRef.current).trim()
      const silent = Boolean(options?.silent)
      if (!targetBaseRoot) {
        setStatus('请输入抓取任务根目录')
        return
      }

      if (!silent) {
        setBusy(true)
        setStatus('正在加载抓取工作台...')
      }

      try {
        const next = await bridge.readScraperWorkspace(targetBaseRoot, {
          selectedRoot: targetSelectedRoot,
          progressLimit: DEFAULT_PROGRESS_LIMIT,
          logLines: DEFAULT_LOG_LINES,
        })
        setSnapshot(next)
        setBaseRoot(next.base_root)
        setSelectedRoot(next.selected_root)
        if (!silent) {
          if (next.selected_task) {
            setStatus(`已载入 ${next.task_count} 个任务，当前：${next.selected_task.task}`)
          } else {
            setStatus(next.task_count > 0 ? `已载入 ${next.task_count} 个任务` : '当前目录没有抓取任务')
          }
        }
      } catch (error) {
        setStatus(`加载失败：${String(error)}`)
      } finally {
        if (!silent) {
          setBusy(false)
        }
      }
    },
    [bridge],
  )

  useEffect(() => {
    let disposed = false

    const run = async (): Promise<void> => {
      try {
        const health = await bridge.ping()
        if (disposed) {
          return
        }
        setBridgeVersion(`${health.provider} / ${health.version}`)
        const defaultRoot = await bridge.getDefaultScraperBaseRoot()
        if (disposed) {
          return
        }
        setBaseRoot(defaultRoot)
        await loadWorkspace({ baseRoot: defaultRoot, selectedRoot: '' })
      } catch (error) {
        if (!disposed) {
          setBridgeVersion('不可用')
          setStatus(`Bridge 检测失败：${String(error)}`)
        }
      }
    }

    void run()
    return () => {
      disposed = true
    }
  }, [bridge, loadWorkspace])

  useEffect(() => {
    if (!baseRoot) {
      return undefined
    }

    const timer = window.setInterval(() => {
      void loadWorkspace({
        baseRoot: baseRootRef.current,
        selectedRoot: selectedRootRef.current,
        silent: true,
      })
    }, AUTO_REFRESH_INTERVAL_MS)

    return () => {
      window.clearInterval(timer)
    }
  }, [baseRoot, loadWorkspace])

  const onRefresh = async (): Promise<void> => {
    await loadWorkspace({ baseRoot, selectedRoot })
  }

  const onSelectTask = async (root: string): Promise<void> => {
    const nextRoot = String(root || '').trim()
    if (!nextRoot || nextRoot === selectedRoot) {
      return
    }
    await loadWorkspace({ baseRoot, selectedRoot: nextRoot })
  }

  return {
    baseRoot,
    bridgeVersion,
    busy,
    detail: snapshot?.detail ?? null,
    provider: bridge.provider,
    selectedRoot,
    status,
    taskCount: snapshot?.task_count ?? 0,
    taskStatusText: snapshot?.status_text ?? '任务数: 0',
    tasks: snapshot?.tasks ?? [],
    setBaseRoot,
    onRefresh,
    onSelectTask,
  }
}
