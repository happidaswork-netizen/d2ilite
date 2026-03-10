import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import type {
  ScraperActionName,
  ScraperControlOptions,
  ScraperLaunchForm,
  ScraperLaunchState,
  ScraperLaunchTemplateOption,
  ScraperWorkspaceSnapshot,
} from '../types'
import { createDesktopBridge } from '../infrastructure/desktopBridge'

const DEFAULT_PROGRESS_LIMIT = 320
const DEFAULT_LOG_LINES = 90
const AUTO_REFRESH_INTERVAL_MS = 15000
const DEFAULT_CONTROL_OPTIONS: ScraperControlOptions = {
  mode: 'requests_jsl',
  auto_fallback: true,
  disable_page_images: true,
}

type LoadOptions = {
  baseRoot?: string
  selectedRoot?: string
  silent?: boolean
}

const ACTION_LABELS: Record<ScraperActionName, string> = {
  pause: '暂停任务',
  continue: '继续任务',
  retry: '重试失败项',
  rewrite: '重写元数据',
}

export function useScraperWorkspace() {
  const bridge = useMemo(() => createDesktopBridge(), [])
  const baseRootRef = useRef('')
  const selectedRootRef = useRef('')
  const controlOptionsRef = useRef<ScraperControlOptions>(DEFAULT_CONTROL_OPTIONS)
  const launchFormRef = useRef<ScraperLaunchForm | null>(null)
  const defaultsAppliedRef = useRef(false)

  const [baseRoot, setBaseRoot] = useState('')
  const [selectedRoot, setSelectedRoot] = useState('')
  const [snapshot, setSnapshot] = useState<ScraperWorkspaceSnapshot | null>(null)
  const [status, setStatus] = useState('就绪')
  const [busy, setBusy] = useState(false)
  const [actionBusy, setActionBusy] = useState(false)
  const [launchBusy, setLaunchBusy] = useState(false)
  const [bridgeVersion, setBridgeVersion] = useState('未探测')
  const [controlOptions, setControlOptions] = useState<ScraperControlOptions>(DEFAULT_CONTROL_OPTIONS)
  const [launchForm, setLaunchForm] = useState<ScraperLaunchForm | null>(null)
  const [launchTemplates, setLaunchTemplates] = useState<ScraperLaunchTemplateOption[]>([])

  useEffect(() => {
    baseRootRef.current = baseRoot
  }, [baseRoot])

  useEffect(() => {
    selectedRootRef.current = selectedRoot
  }, [selectedRoot])

  useEffect(() => {
    controlOptionsRef.current = controlOptions
  }, [controlOptions])

  useEffect(() => {
    launchFormRef.current = launchForm
  }, [launchForm])

  const applySnapshot = useCallback((next: ScraperWorkspaceSnapshot) => {
    setSnapshot(next)
    setBaseRoot(next.base_root)
    setSelectedRoot(next.selected_root)
    if (!defaultsAppliedRef.current && next.control_defaults) {
      defaultsAppliedRef.current = true
      setControlOptions({
        mode: String(next.control_defaults.mode || DEFAULT_CONTROL_OPTIONS.mode),
        auto_fallback: Boolean(next.control_defaults.auto_fallback),
        disable_page_images: Boolean(next.control_defaults.disable_page_images),
      })
    }
  }, [])

  const applyLaunchState = useCallback((next: ScraperLaunchState) => {
    const { templates, ...form } = next
    setLaunchForm(form)
    setLaunchTemplates(Array.isArray(templates) ? templates : [])
  }, [])

  const loadLaunchState = useCallback(
    async (sourceHint?: string, templatePath?: string, silent = false): Promise<void> => {
      if (!silent) {
        setLaunchBusy(true)
      }
      try {
        const next = await bridge.readScraperLaunchState(sourceHint || '', templatePath || '')
        applyLaunchState(next)
      } catch (error) {
        setStatus(`启动表单载入失败：${String(error)}`)
      } finally {
        if (!silent) {
          setLaunchBusy(false)
        }
      }
    },
    [applyLaunchState, bridge],
  )

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
        applySnapshot(next)
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
    [applySnapshot, bridge],
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
        await Promise.all([
          loadWorkspace({ baseRoot: defaultRoot, selectedRoot: '' }),
          loadLaunchState('', '', true),
        ])
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
  }, [bridge, loadLaunchState, loadWorkspace])

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
    await Promise.all([
      loadWorkspace({ baseRoot, selectedRoot }),
      loadLaunchState(launchFormRef.current?.start_url || '', launchFormRef.current?.selected_template_path || '', true),
    ])
  }

  const onSelectTask = async (root: string): Promise<void> => {
    const nextRoot = String(root || '').trim()
    if (!nextRoot || nextRoot === selectedRoot) {
      return
    }
    await loadWorkspace({ baseRoot, selectedRoot: nextRoot })
  }

  const setControlMode = useCallback((mode: string) => {
    setControlOptions((prev) => ({ ...prev, mode: String(mode || '').trim() || DEFAULT_CONTROL_OPTIONS.mode }))
  }, [])

  const setControlAutoFallback = useCallback((value: boolean) => {
    setControlOptions((prev) => ({ ...prev, auto_fallback: Boolean(value) }))
  }, [])

  const setControlDisablePageImages = useCallback((value: boolean) => {
    setControlOptions((prev) => ({ ...prev, disable_page_images: Boolean(value) }))
  }, [])

  const onRunAction = useCallback(
    async (action: ScraperActionName): Promise<void> => {
      const root = String(selectedRootRef.current || '').trim()
      const currentBaseRoot = String(baseRootRef.current || '').trim()
      if (!root) {
        setStatus('请先选择一个抓取任务')
        return
      }

      setActionBusy(true)
      setStatus(`正在执行：${ACTION_LABELS[action]}`)
      try {
        const result = await bridge.runScraperAction(action, root, {
          baseRoot: currentBaseRoot,
          control: action === 'rewrite' ? {} : controlOptionsRef.current,
        })
        applySnapshot(result.workspace)
        setStatus(String(result.message || `${ACTION_LABELS[action]}完成`))
      } catch (error) {
        setStatus(`${ACTION_LABELS[action]}失败：${String(error)}`)
      } finally {
        setActionBusy(false)
      }
    },
    [applySnapshot, bridge],
  )

  const onLaunchFieldChange = useCallback(
    <K extends keyof ScraperLaunchForm>(field: K, value: ScraperLaunchForm[K]) => {
      setLaunchForm((prev) => (prev ? { ...prev, [field]: value } : prev))
    },
    [],
  )

  const onSelectLaunchTemplate = useCallback(
    async (templatePath: string): Promise<void> => {
      const sourceHint = launchFormRef.current?.start_url || ''
      await loadLaunchState(sourceHint, templatePath, false)
    },
    [loadLaunchState],
  )

  const onFillDefaultOutputRoot = useCallback(async (): Promise<void> => {
    const current = launchFormRef.current
    if (!current) {
      return
    }
    try {
      const suggested = await bridge.readScraperLaunchState(current.start_url || '', '')
      setLaunchTemplates(Array.isArray(suggested.templates) ? suggested.templates : [])
      setLaunchForm((prev) => (prev ? { ...prev, output_root: suggested.output_root } : prev))
    } catch (error) {
      setStatus(`默认输出目录生成失败：${String(error)}`)
    }
  }, [bridge])

  const onStartTask = useCallback(async (): Promise<void> => {
    const current = launchFormRef.current
    if (!current) {
      setStatus('启动表单尚未准备完成')
      return
    }
    setLaunchBusy(true)
    setStatus('正在启动抓取任务...')
    try {
      const result = await bridge.startScraperTask(current, {
        baseRoot: baseRootRef.current,
      })
      applySnapshot(result.workspace)
      setStatus(String(result.message || '抓取任务已启动'))
      await loadLaunchState(current.start_url || '', current.selected_template_path || '', true)
    } catch (error) {
      setStatus(`启动失败：${String(error)}`)
    } finally {
      setLaunchBusy(false)
    }
  }, [applySnapshot, bridge, loadLaunchState])

  return {
    actionBusy,
    baseRoot,
    bridgeVersion,
    busy: busy || actionBusy || launchBusy,
    controlOptions,
    detail: snapshot?.detail ?? null,
    launchBusy,
    launchForm,
    launchTemplates,
    provider: bridge.provider,
    selectedRoot,
    status,
    taskCount: snapshot?.task_count ?? 0,
    taskStatusText: snapshot?.status_text ?? '任务数: 0',
    tasks: snapshot?.tasks ?? [],
    setBaseRoot,
    setControlAutoFallback,
    setControlDisablePageImages,
    setControlMode,
    onContinueTask: () => onRunAction('continue'),
    onFillDefaultOutputRoot,
    onLaunchFieldChange,
    onPauseTask: () => onRunAction('pause'),
    onRefresh,
    onRetryTask: () => onRunAction('retry'),
    onRewriteMetadataTask: () => onRunAction('rewrite'),
    onSelectLaunchTemplate,
    onSelectTask,
    onStartTask,
  }
}
