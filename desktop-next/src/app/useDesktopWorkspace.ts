import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import type { AppSettings, MetadataAutofillInputMode, MetadataItem, NameBarOptions } from '../types'
import {
  applyBatchRoleOperation,
  createRoleAliasFormItem,
  extractRoleMetadataSummary,
  extractRoleMetadataSummaryFromForm,
  getTabPayload,
  hasBatchRoleChange,
  parseKeywords,
  sameForm,
  shouldApplyBatchRoleOperation,
  toForm,
  toPayload,
  type BatchAliasMode,
  type BatchExecutionFailure,
  type BatchExecutionProgress,
  type BatchExecutionReport,
  type BatchMatchMode,
  type BatchOriginalRoleMode,
  type BatchRoleOperation,
  type BatchRoleScope,
  type FormState,
  type MetadataTabKey,
  type RoleMetadataSummary,
} from '../domain/metadata'
import { createMetadataIndexCache } from '../infrastructure/cache/metadataIndexCache'
import {
  createDesktopBridge,
  reportDesktopFrontendStatus,
} from '../infrastructure/desktopBridge'
import { loadPreferredFolder, persistPreferredFolder } from '../infrastructure/runtime/folderPreference'
import { getFileName } from '../shared/path'

const EMPTY_BATCH_PROGRESS: BatchExecutionProgress = {
  active: false,
  total: 0,
  processed: 0,
  changed: 0,
  skipped: 0,
  failed: 0,
}

function folderOfPath(value: string): string {
  return String(value || '').replace(/[\\/][^\\/]*$/, '')
}

function samePath(left: string, right: string): boolean {
  return normalizeWindowsPath(left).toLowerCase() === normalizeWindowsPath(right).toLowerCase()
}

function normalizeWindowsPath(value: string): string {
  let text = String(value || '').trim().replace(/^"+|"+$/g, '')
  if (!text) {
    return ''
  }
  text = text.replace(/\//g, '\\')
  while (text.startsWith('\\\\\\') && !text.startsWith('\\\\?\\')) {
    text = text.replace(/^\\\\\\/, '\\\\')
  }
  const lower = text.toLowerCase()
  if (lower.startsWith('\\\\?\\unc\\')) {
    return `\\\\${text.slice(8)}`
  }
  if (lower.startsWith('\\?\\unc\\')) {
    return `\\\\${text.slice(7)}`
  }
  if (lower.startsWith('?\\unc\\')) {
    return `\\\\${text.slice(6)}`
  }
  if (lower.startsWith('\\\\?\\')) {
    return text.slice(4)
  }
  if (lower.startsWith('\\?\\')) {
    return text.slice(3)
  }
  if (lower.startsWith('?\\')) {
    return text.slice(2)
  }
  return text
}

type UseDesktopWorkspaceOptions = {
  initialPath?: string
}

function sortImagePaths(paths: string[]): string[] {
  return [...paths].sort((left, right) =>
    getFileName(left).localeCompare(getFileName(right), undefined, { sensitivity: 'base' }),
  )
}

function uniqueNormalizedPaths(paths: string[]): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const path of paths) {
    const normalized = normalizeWindowsPath(path)
    if (!normalized) {
      continue
    }
    const key = normalized.toLowerCase()
    if (seen.has(key)) {
      continue
    }
    seen.add(key)
    out.push(normalized)
  }
  return out
}

function metadataReadErrorOf(item: MetadataItem): string {
  return String(
    item.metadata_read_error ||
      (item.other_exif && typeof item.other_exif.metadata_read_error === 'string'
        ? item.other_exif.metadata_read_error
        : '') ||
      '',
  ).trim()
}

function metadataReadStatusText(item: MetadataItem, successText: string): string {
  const metadataReadError = metadataReadErrorOf(item)
  return metadataReadError ? `已打开图片，但元数据读取失败：${metadataReadError}` : successText
}

export function useDesktopWorkspace(options?: UseDesktopWorkspaceOptions) {
  const bridge = useMemo(() => createDesktopBridge(), [])
  const metadataIndexCacheRef = useRef(createMetadataIndexCache())
  const currentItemRef = useRef<MetadataItem | null>(null)
  const selectedPathRef = useRef<string>('')
  const externalPathRef = useRef<string>('')

  const [folder, setFolder] = useState<string>(loadPreferredFolder)
  const [items, setItems] = useState<string[]>([])
  const [currentItem, setCurrentItem] = useState<MetadataItem | null>(null)
  const [selectedPath, setSelectedPath] = useState<string>('')
  const [selectedPaths, setSelectedPaths] = useState<string[]>([])
  const [loadedForm, setLoadedForm] = useState<FormState | null>(null)
  const [form, setForm] = useState<FormState | null>(null)
  const [status, setStatus] = useState<string>('就绪')
  const [busy, setBusy] = useState<boolean>(false)
  const [indexBusy, setIndexBusy] = useState<boolean>(false)
  const [bridgeVersion, setBridgeVersion] = useState<string>('未探测')
  const [filterText, setFilterText] = useState<string>('')
  const [roleFilterOriginal, setRoleFilterOriginal] = useState<string>('')
  const [roleFilterAlias, setRoleFilterAlias] = useState<string>('')
  const [previewFailed, setPreviewFailed] = useState<boolean>(false)
  const [activeMetaTab, setActiveMetaTab] = useState<MetadataTabKey>('profile')
  const [roleSummaryByPath, setRoleSummaryByPath] = useState<Record<string, RoleMetadataSummary>>({})
  const [batchScope, setBatchScope] = useState<BatchRoleScope>('selected')
  const [batchOriginalRoleMode, setBatchOriginalRoleMode] = useState<BatchOriginalRoleMode>('ignore')
  const [batchOriginalRoleName, setBatchOriginalRoleName] = useState<string>('')
  const [batchAliasMode, setBatchAliasMode] = useState<BatchAliasMode>('ignore')
  const [batchAliasText, setBatchAliasText] = useState<string>('')
  const [batchMatchMode, setBatchMatchMode] = useState<BatchMatchMode>('all')
  const [batchProgress, setBatchProgress] = useState<BatchExecutionProgress>(EMPTY_BATCH_PROGRESS)
  const [lastBatchReport, setLastBatchReport] = useState<BatchExecutionReport | null>(null)
  const [imageActionBusy, setImageActionBusy] = useState<boolean>(false)
  const [metadataAiBusy, setMetadataAiBusy] = useState<boolean>(false)
  const [settingsBusy, setSettingsBusy] = useState<boolean>(false)
  const [settingsOpen, setSettingsOpen] = useState<boolean>(false)
  const [appSettings, setAppSettings] = useState<AppSettings | null>(null)
  const [settingsPath, setSettingsPath] = useState<string>('')

  const provider = bridge.provider
  const selectedName = selectedPath ? getFileName(selectedPath) : ''
  const previewUrl = selectedPath ? bridge.getPreviewUrl(selectedPath) : ''
  const isDirty = useMemo(() => !sameForm(form, loadedForm), [form, loadedForm])
  const keywordCount = useMemo(() => parseKeywords(form?.keywords_text || '').length, [form?.keywords_text])
  const activeTabPayload = useMemo(
    () => getTabPayload(currentItem, activeMetaTab),
    [activeMetaTab, currentItem],
  )
  const indexedCount = useMemo(() => Object.keys(roleSummaryByPath).length, [roleSummaryByPath])
  const hasBatchOperation = batchOriginalRoleMode !== 'ignore' || batchAliasMode !== 'ignore'
  const filteredItems = useMemo(() => {
    const keyword = filterText.trim().toLowerCase()
    const originalRoleKeyword = roleFilterOriginal.trim().toLowerCase()
    const aliasRoleKeyword = roleFilterAlias.trim().toLowerCase()

    return items.filter((itemPath) => {
      const fileName = getFileName(itemPath).toLowerCase()
      const fullPath = itemPath.toLowerCase()
      if (keyword && !fileName.includes(keyword) && !fullPath.includes(keyword)) {
        return false
      }

      const summary = roleSummaryByPath[itemPath]
      if (originalRoleKeyword) {
        const originalRoleName = String(summary?.originalRoleName || '').toLowerCase()
        if (!originalRoleName.includes(originalRoleKeyword)) {
          return false
        }
      }

      if (aliasRoleKeyword) {
        const matched = (summary?.roleAliasNames || []).some((name) =>
          String(name || '').toLowerCase().includes(aliasRoleKeyword),
        )
        if (!matched) {
          return false
        }
      }

      return true
    })
  }, [filterText, items, roleFilterAlias, roleFilterOriginal, roleSummaryByPath])

  useEffect(() => {
    currentItemRef.current = currentItem
  }, [currentItem])

  useEffect(() => {
    selectedPathRef.current = selectedPath
  }, [selectedPath])

  useEffect(() => {
    setPreviewFailed(false)
  }, [selectedPath])

  useEffect(() => {
    persistPreferredFolder(folder)
  }, [folder])

  useEffect(() => {
    setSelectedPaths((prev) => prev.filter((path) => items.includes(path)))
  }, [items])

  useEffect(() => {
    let disposed = false
    const run = async (): Promise<void> => {
      try {
        const health = await bridge.ping()
        if (disposed) return
        setBridgeVersion(`${health.provider} / ${health.version}`)
        setStatus(`Bridge 已连接：${health.provider}`)
        try {
          await reportDesktopFrontendStatus({
            provider,
            ping_ok: true,
            bridge_provider: health.provider,
            bridge_version: health.version,
          })
        } catch {
          // ignore frontend status report failures during startup
        }
      } catch (error) {
        if (disposed) return
        setBridgeVersion('不可用')
        setStatus(`Bridge 检测失败：${String(error)}`)
        try {
          await reportDesktopFrontendStatus({
            provider,
            ping_ok: false,
            error: String(error),
          })
        } catch {
          // ignore frontend status report failures during startup
        }
      }
    }
    void run()
    return () => {
      disposed = true
    }
  }, [bridge, provider])

  useEffect(() => {
    const cache = metadataIndexCacheRef.current
    const currentPath = selectedPathRef.current

    if (currentPath && currentItemRef.current && items.includes(currentPath)) {
      cache.rememberItem(currentItemRef.current)
    }

    setRoleSummaryByPath(cache.getSummarySnapshot(items))
    setIndexBusy(false)
  }, [items])

  const rememberMetadataItem = useCallback((item: MetadataItem): void => {
    const summary = metadataIndexCacheRef.current.rememberItem(item)
    if (item.filepath) {
      setRoleSummaryByPath((prev) => ({ ...prev, [item.filepath]: summary }))
    }
  }, [])

  useEffect(() => {
    if (!selectedPath || items.length <= 1) {
      return undefined
    }

    const currentIndex = items.findIndex((path) => samePath(path, selectedPath))
    if (currentIndex < 0) {
      return undefined
    }

    const neighborPaths = [items[currentIndex - 1], items[currentIndex + 1]].filter(
      (path): path is string => Boolean(path),
    )
    if (neighborPaths.length === 0) {
      return undefined
    }

    let disposed = false
    const previewImages: HTMLImageElement[] = []
    for (const rawPath of neighborPaths) {
      const neighborPath = normalizeWindowsPath(rawPath)
      if (!neighborPath) {
        continue
      }

      const previewSrc = bridge.getPreviewUrl(neighborPath)
      if (previewSrc) {
        const image = new Image()
        image.src = previewSrc
        previewImages.push(image)
      }

      void metadataIndexCacheRef.current
        .readMetadata(neighborPath, (targetPath) => bridge.readMetadata(targetPath))
        .then((item) => {
          if (!disposed) {
            rememberMetadataItem(item)
          }
        })
        .catch(() => {
          // Adjacent image prefetch is an opportunistic speed-up.
        })
    }

    return () => {
      disposed = true
      previewImages.length = 0
    }
  }, [bridge, items, rememberMetadataItem, selectedPath])

  const loadMetadata = async (
    path: string,
    statusText: string,
    options?: {
      forceRefresh?: boolean
    },
  ): Promise<void> => {
    const normalizedPath = normalizeWindowsPath(path)
    setBusy(true)
    setStatus(statusText)
    try {
      if (options?.forceRefresh) {
        metadataIndexCacheRef.current.forgetItem(normalizedPath)
      }
      const data = await metadataIndexCacheRef.current.readMetadata(normalizedPath, (targetPath) => bridge.readMetadata(targetPath))
      const nextForm = toForm(data)
      rememberMetadataItem(data)
      setCurrentItem(data)
      setSelectedPath(normalizedPath)
      setLoadedForm(nextForm)
      setForm(nextForm)
      setStatus(metadataReadStatusText(data, `已读取：${getFileName(normalizedPath)}`))
    } catch (error) {
      setStatus(`读取失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }

  const openImagePath = useCallback(
    async (
      path: string,
      options?: {
        statusText?: string
        successText?: string
        setFolderFromPath?: boolean
        loadSiblingList?: boolean
      },
    ): Promise<void> => {
      const normalizedPath = normalizeWindowsPath(path)
      if (!normalizedPath) {
        setStatus('请输入图片路径')
        return
      }
      const targetFolder = folderOfPath(normalizedPath)
      if (options?.setFolderFromPath !== false) {
        if (targetFolder) {
          setFolder(targetFolder)
        }
      }
      setBusy(true)
      setStatus(options?.statusText || '正在打开图片...')
      try {
        let nextItems: string[] | null = null
        if (options?.loadSiblingList !== false && targetFolder) {
          try {
            const siblings = await bridge.listImages(targetFolder, 500)
            const normalizedSiblings = uniqueNormalizedPaths(siblings)
            nextItems = normalizedSiblings.some((item) => samePath(item, normalizedPath))
              ? normalizedSiblings
              : sortImagePaths([...normalizedSiblings, normalizedPath])
          } catch {
            nextItems = null
          }
        }
        setItems((prev) => {
          if (nextItems) {
            return nextItems
          }
          return prev.some((item) => samePath(item, normalizedPath)) ? prev : [normalizedPath]
        })
        setSelectedPaths([])
        setRoleSummaryByPath({})
        metadataIndexCacheRef.current.forgetItem(normalizedPath)
        const data = await bridge.readMetadata(normalizedPath)
        const nextForm = toForm(data)
        rememberMetadataItem(data)
        setSelectedPath(normalizedPath)
        setCurrentItem(data)
        setLoadedForm(nextForm)
        setForm(nextForm)
        setStatus(metadataReadStatusText(data, options?.successText || `已打开：${getFileName(normalizedPath)}`))
      } catch (error) {
        setStatus(`打开图片失败：${String(error)}；路径：${normalizedPath}`)
      } finally {
        setBusy(false)
      }
    },
    [bridge, rememberMetadataItem],
  )

  const confirmDiscard = useCallback((): boolean => {
    if (!isDirty) {
      return true
    }
    return window.confirm('当前有未保存修改，确定放弃并切换条目吗？')
  }, [isDirty])

  const loadFolderPath = useCallback(async (targetFolder: string, options?: { skipConfirm?: boolean }): Promise<void> => {
    const normalizedFolder = normalizeWindowsPath(targetFolder)
    if (!normalizedFolder) {
      setStatus('请输入目录路径')
      return
    }
    if (!options?.skipConfirm && !confirmDiscard()) {
      return
    }

    setBusy(true)
    setStatus('正在加载目录...')
    try {
      setFolder(normalizedFolder)
      const list = uniqueNormalizedPaths(await bridge.listImages(normalizedFolder, 500))
      setItems(list)
      setSelectedPaths([])
      setRoleSummaryByPath({})
      setIndexBusy(false)
      if (list.length === 0) {
        setSelectedPath('')
        setCurrentItem(null)
        setLoadedForm(null)
        setForm(null)
        setRoleSummaryByPath({})
        setStatus('目录中没有图片')
        return
      }

      const preferred = selectedPath && list.some((path) => samePath(path, selectedPath)) ? selectedPath : list[0]
      const data = await metadataIndexCacheRef.current.readMetadata(preferred, (path) => bridge.readMetadata(path))
      const nextForm = toForm(data)
      rememberMetadataItem(data)
      setSelectedPath(preferred)
      setCurrentItem(data)
      setLoadedForm(nextForm)
      setForm(nextForm)
      setStatus(metadataReadStatusText(data, `已加载 ${list.length} 项`))
    } catch (error) {
      setStatus(`加载失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }, [bridge, confirmDiscard, rememberMetadataItem, selectedPath])

  const onLoadFolder = async (): Promise<void> => {
    await loadFolderPath(folder)
  }

  const onPickAndOpenImage = async (): Promise<void> => {
    if (!confirmDiscard()) {
      return
    }
    setBusy(true)
    setStatus('请选择图片...')
    try {
      const picked = await bridge.pickImage(folder)
      if (!picked) {
        setStatus('已取消选择图片')
        return
      }
      setBusy(false)
      await openImagePath(picked)
    } catch (error) {
      setStatus(`打开图片失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }

  const onPickAndLoadFolder = async (): Promise<void> => {
    if (!confirmDiscard()) {
      return
    }
    setBusy(true)
    setStatus('请选择图片目录...')
    try {
      const picked = await bridge.pickFolder(folder)
      if (!picked) {
        setStatus('已取消选择目录')
        return
      }
      setBusy(false)
      await loadFolderPath(normalizeWindowsPath(picked), { skipConfirm: true })
    } catch (error) {
      setStatus(`选择目录失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }

  const onPickFolderPath = async (initialFolder?: string): Promise<string> => {
    setStatus('请选择目录...')
    const picked = await bridge.pickFolder(initialFolder || folder)
    const normalizedPicked = normalizeWindowsPath(picked)
    if (!normalizedPicked) {
      setStatus('已取消选择目录')
      return ''
    }
    setStatus(`已选择目录：${normalizedPicked}`)
    return normalizedPicked
  }

  const onBuildRoleIndex = async (): Promise<void> => {
    if (items.length === 0) {
      setStatus('请先加载目录')
      return
    }

    const cache = metadataIndexCacheRef.current
    if (selectedPathRef.current && currentItemRef.current && items.includes(selectedPathRef.current)) {
      cache.rememberItem(currentItemRef.current)
    }

    setIndexBusy(true)
    setStatus('正在建立角色索引...')
    try {
      await cache.hydrateRoleSummaries(items, (path) => bridge.readMetadata(path), {
        flushEvery: 8,
        onUpdate: ({ indexedCount, totalCount }) => {
          setRoleSummaryByPath(cache.getSummarySnapshot(items))
          setStatus(`正在建立角色索引：${indexedCount}/${totalCount}`)
        },
      })
      setRoleSummaryByPath(cache.getSummarySnapshot(items))
      setStatus(`角色索引完成：${cache.countIndexed(items)}/${items.length}`)
    } catch (error) {
      setStatus(`建立角色索引失败：${String(error)}`)
    } finally {
      setIndexBusy(false)
    }
  }

  const openExternalPath = useCallback(async (path: string): Promise<void> => {
    const targetPath = normalizeWindowsPath(path)
    if (!targetPath) {
      return
    }
    if (!confirmDiscard()) {
      return
    }

    setBusy(true)
    setStatus('正在打开外部路径...')
    try {
      const info = await bridge.getPathInfo(targetPath)
      const normalizedInfoPath = normalizeWindowsPath(info.path || targetPath)
      setBusy(false)
      if (info.is_dir) {
        await loadFolderPath(normalizedInfoPath, { skipConfirm: true })
        return
      }
      if (info.is_file) {
        await openImagePath(normalizedInfoPath, {
          statusText: '正在打开外部图片...',
          successText: `已打开：${getFileName(normalizedInfoPath)}`,
        })
        return
      }
      setStatus(`外部路径不存在或不可读取：${normalizedInfoPath}`)
    } catch (error) {
      setStatus(`打开路径失败：${String(error)}；路径：${targetPath}`)
    } finally {
      setBusy(false)
    }
  }, [bridge, confirmDiscard, loadFolderPath, openImagePath])

  useEffect(() => {
    const targetPath = normalizeWindowsPath(options?.initialPath || '')
    if (!targetPath || targetPath === externalPathRef.current) {
      return
    }
    externalPathRef.current = targetPath
    void openExternalPath(targetPath)
  }, [openExternalPath, options?.initialPath])

  useEffect(() => {
    let disposed = false
    const run = async (): Promise<void> => {
      try {
        const info = await bridge.getLaunchPath()
        if (disposed) return
        const targetPath = normalizeWindowsPath(info.path || '')
        if (!targetPath || targetPath === externalPathRef.current) {
          return
        }
        externalPathRef.current = targetPath
        void openExternalPath(targetPath)
      } catch {
        // Launch path is optional; ignore unsupported runtimes.
      }
    }
    void run()
    return () => {
      disposed = true
    }
  }, [bridge, openExternalPath])

  const replaceImagePath = (oldPath: string, newPath: string): void => {
    const cache = metadataIndexCacheRef.current
    cache.forgetItem(oldPath)
    setItems((prev) => prev.map((path) => (path === oldPath ? newPath : path)))
    setSelectedPaths((prev) => prev.map((path) => (path === oldPath ? newPath : path)))
    setRoleSummaryByPath((prev) => {
      const next = { ...prev }
      if (oldPath in next) {
        next[newPath] = next[oldPath]
        delete next[oldPath]
      }
      return next
    })
  }

  const insertGeneratedImage = (sourcePath: string, outputPath: string): void => {
    const target = String(outputPath || '').trim()
    if (!target) {
      return
    }
    const sourceFolder = folderOfPath(sourcePath).toLowerCase()
    const outputFolder = folderOfPath(target).toLowerCase()
    if (!sourceFolder || sourceFolder !== outputFolder) {
      return
    }
    metadataIndexCacheRef.current.forgetItem(target)
    setItems((prev) => (prev.includes(target) ? prev : sortImagePaths([...prev, target])))
  }

  const onAddNameBar = async (options: NameBarOptions): Promise<void> => {
    const target = selectedPathRef.current
    const label = String(options?.name || '').trim()
    if (!target || !label) {
      setStatus('请先选择图片并输入名字')
      return
    }
    setImageActionBusy(true)
    setStatus('正在加名字...')
    try {
      const actionOptions: NameBarOptions = {
        name: label,
        output_name: options?.output_name === 'label' ? 'label' : 'suffix',
      }
      const outputDir = String(options?.output_dir || '').trim()
      const outputFormat = String(options?.output_format || '').trim()
      if (outputDir) {
        actionOptions.output_dir = outputDir
      }
      if (outputFormat) {
        actionOptions.output_format = outputFormat
      }

      const result = await bridge.addNameBar(target, actionOptions)
      const outputPath = String(result.output_path || '').trim()
      setStatus(`已生成：${outputPath || result.message}`)
      insertGeneratedImage(target, outputPath)
      if (outputPath && options?.open_after_generate) {
        if (!isDirty || window.confirm('当前有未保存元数据修改，打开生成图会放弃这些修改。确定继续吗？')) {
          await openImagePath(outputPath, {
            statusText: '已生成，正在打开新图...',
            successText: `已打开生成图：${getFileName(outputPath)}`,
            loadSiblingList: true,
          })
        }
      }
      if (outputPath && options?.reveal_after_generate) {
        await bridge.revealPath(outputPath)
        setStatus(`已生成并定位：${getFileName(outputPath)}`)
      }
    } catch (error) {
      setStatus(`加名字失败：${String(error)}`)
    } finally {
      setImageActionBusy(false)
    }
  }

  const onRenameCurrentImage = async (name: string): Promise<void> => {
    const target = selectedPathRef.current
    const nextName = String(name || '').trim()
    if (!target || !nextName) {
      setStatus('请先选择图片并输入新文件名')
      return
    }
    if (isDirty && !window.confirm('当前有未保存元数据修改，重命名会重新读取文件并放弃这些修改。确定继续吗？')) {
      return
    }
    setImageActionBusy(true)
    setStatus('正在重命名当前图片...')
    try {
      const result = await bridge.renameImage(target, nextName)
      replaceImagePath(result.old_path || target, result.new_path)
      await loadMetadata(result.new_path, '已重命名，正在读取元数据...', { forceRefresh: true })
      setStatus(`已重命名为：${result.filename}`)
    } catch (error) {
      setStatus(`重命名失败：${String(error)}`)
    } finally {
      setImageActionBusy(false)
    }
  }

  const applyAutofillResult = (payload: Record<string, unknown>): string[] => {
    const applied: string[] = []
    setForm((prev) => {
      if (!prev) {
        return prev
      }
      const next: FormState = { ...prev }
      const setIfEmpty = (key: keyof FormState, value: unknown): void => {
        if (key === 'role_aliases') {
          return
        }
        if (String(next[key] || '').trim()) {
          return
        }
        const text = String(value || '').trim()
        if (!text) {
          return
        }
        next[key] = text as never
        applied.push(String(key))
      }

      setIfEmpty('title', payload.title)
      setIfEmpty('person', payload.person)
      setIfEmpty('gender', payload.gender)
      setIfEmpty('position', payload.position)
      setIfEmpty('city', payload.city)
      setIfEmpty('source', payload.source)
      setIfEmpty('image_url', payload.image_url)
      setIfEmpty('description', payload.description)

      if (!String(next.keywords_text || '').trim() && Array.isArray(payload.keywords)) {
        const keywords = payload.keywords.map((item) => String(item || '').trim()).filter(Boolean)
        if (keywords.length > 0) {
          next.keywords_text = keywords.join(', ')
          applied.push('keywords')
        }
      }

      if (!String(next.original_role_name || '').trim()) {
        const extraFields = payload.extra_fields
        const extraName =
          extraFields && typeof extraFields === 'object'
            ? String((extraFields as Record<string, unknown>).name || '').trim()
            : ''
        const roleName = extraName || String(payload.person || '').trim()
        if (roleName) {
          next.original_role_name = roleName
          applied.push('original_role_name')
        }
      }

      return next
    })
    return applied
  }

  const onAiAutofillCurrentMetadata = async (inputMode: MetadataAutofillInputMode): Promise<void> => {
    const target = selectedPathRef.current
    if (!target || !form) {
      setStatus('请先选择图片')
      return
    }
    setMetadataAiBusy(true)
    setStatus('AI 自动补全中...')
    try {
      const result = await bridge.autofillMetadata(target, {
        input_mode: inputMode,
        form: {
          title: form.title,
          person: form.person,
          gender: form.gender,
          position: form.position,
          city: form.city,
          source: form.source,
          image_url: form.image_url,
          keywords_text: form.keywords_text,
          description: form.description,
        },
      })
      const applied = applyAutofillResult(result.result || {})
      setStatus(applied.length > 0 ? `AI 自动补全完成：${applied.join(', ')}` : 'AI 自动补全完成（无可应用字段）')
    } catch (error) {
      setStatus(`AI 自动补全失败：${String(error)}`)
    } finally {
      setMetadataAiBusy(false)
    }
  }

  const buildCurrentFormPayload = (): Record<string, unknown> => {
    const current = form
    if (!current) {
      return {}
    }
    return {
      title: current.title,
      person: current.person,
      gender: current.gender,
      position: current.position,
      city: current.city,
      source: current.source,
      image_url: current.image_url,
      keywords_text: current.keywords_text,
      description: current.description,
      original_role_name: current.original_role_name,
    }
  }

  const onGenerateBiography = async (): Promise<void> => {
    const target = selectedPathRef.current
    if (!target || !form) {
      setStatus('请先选择图片')
      return
    }
    setMetadataAiBusy(true)
    setStatus('AI 小传生成中...')
    try {
      const result = await bridge.generateBiography(target, {
        form: buildCurrentFormPayload(),
      })
      const biography = String(result.result.biography_short || result.result.description || '').trim()
      if (!biography) {
        setStatus('AI 小传生成完成（无可应用内容）')
        return
      }
      setForm((prev) => {
        if (!prev) {
          return prev
        }
        const currentDescription = String(prev.description || '').trim()
        const nextDescription = currentDescription ? `${currentDescription}\n\n小传：${biography}` : biography
        return {
          ...prev,
          description: nextDescription,
        }
      })
      setStatus('AI 小传已写入描述')
    } catch (error) {
      setStatus(`AI 小传失败：${String(error)}`)
    } finally {
      setMetadataAiBusy(false)
    }
  }

  const onReloadAppSettings = async (): Promise<void> => {
    setSettingsBusy(true)
    setStatus('正在读取全局设置...')
    try {
      const result = await bridge.readAppSettings()
      setAppSettings(result.settings)
      setSettingsPath(result.path || '')
      setStatus('全局设置已读取')
    } catch (error) {
      setStatus(`读取全局设置失败：${String(error)}`)
    } finally {
      setSettingsBusy(false)
    }
  }

  const onOpenSettingsPanel = async (): Promise<void> => {
    setSettingsOpen(true)
    if (!appSettings) {
      await onReloadAppSettings()
    }
  }

  const onSaveAppSettings = async (settings: AppSettings): Promise<void> => {
    setSettingsBusy(true)
    setStatus('正在保存全局设置...')
    try {
      const result = await bridge.saveAppSettings(settings)
      setAppSettings(result.settings)
      setSettingsPath(result.path || '')
      setStatus('全局设置已保存')
    } catch (error) {
      setStatus(`保存全局设置失败：${String(error)}`)
    } finally {
      setSettingsBusy(false)
    }
  }

  const onOpenCurrentFile = async (): Promise<void> => {
    const target = selectedPathRef.current
    if (!target) {
      setStatus('请先选择图片')
      return
    }
    try {
      await bridge.openPath(target)
      setStatus(`已打开：${getFileName(target)}`)
    } catch (error) {
      setStatus(`打开文件失败：${String(error)}`)
    }
  }

  const onRevealCurrentFile = async (): Promise<void> => {
    const target = selectedPathRef.current
    if (!target) {
      setStatus('请先选择图片')
      return
    }
    try {
      await bridge.revealPath(target)
      setStatus(`已定位：${getFileName(target)}`)
    } catch (error) {
      setStatus(`定位文件失败：${String(error)}`)
    }
  }

  const onGotoOffset = async (offset: number): Promise<void> => {
    if (!selectedPath) {
      return
    }
    if (!confirmDiscard()) {
      return
    }
    let navigableItems = items
    if (navigableItems.length <= 1) {
      const targetFolder = folderOfPath(selectedPath)
      if (targetFolder) {
        try {
          const rescanned = uniqueNormalizedPaths(await bridge.listImages(targetFolder, 500))
          navigableItems = rescanned.some((path) => samePath(path, selectedPath))
            ? rescanned
            : sortImagePaths([...rescanned, selectedPath])
          setItems(navigableItems)
          setFolder(targetFolder)
          setStatus(`已刷新目录：${navigableItems.length} 项`)
        } catch (error) {
          setStatus(`刷新当前目录失败：${String(error)}`)
          return
        }
      }
    }
    const currentIndex = navigableItems.findIndex((path) => samePath(path, selectedPath))
    if (currentIndex < 0) {
      setStatus('当前图片不在目录列表中')
      return
    }
    const nextIndex = currentIndex + offset
    if (nextIndex < 0 || nextIndex >= navigableItems.length) {
      setStatus(offset < 0 ? '已经是第一张' : '已经是最后一张')
      return
    }
    await loadMetadata(navigableItems[nextIndex], '读取元数据...')
  }

  const onGotoPrev = async (): Promise<void> => {
    await onGotoOffset(-1)
  }

  const onGotoNext = async (): Promise<void> => {
    await onGotoOffset(1)
  }

  const onOpenItem = async (path: string): Promise<void> => {
    if (path === selectedPath) {
      return
    }
    if (!confirmDiscard()) {
      return
    }
    await loadMetadata(path, '读取元数据...')
  }

  const onReloadCurrent = async (): Promise<void> => {
    if (!selectedPath) {
      return
    }
    if (!confirmDiscard()) {
      return
    }
    await loadMetadata(selectedPath, '正在重新读取元数据...', { forceRefresh: true })
  }

  const onSave = async (): Promise<void> => {
    if (!selectedPath || !form || !isDirty) {
      return
    }
    setBusy(true)
    setStatus('正在保存元数据...')
    try {
      await bridge.saveMetadata(selectedPath, toPayload(form, currentItem))
      metadataIndexCacheRef.current.forgetItem(selectedPath)
      const refreshed = await bridge.readMetadata(selectedPath)
      const nextForm = toForm(refreshed)
      rememberMetadataItem(refreshed)
      setCurrentItem(refreshed)
      setLoadedForm(nextForm)
      setForm(nextForm)
      setStatus('保存成功')
    } catch (error) {
      setStatus(`保存失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }

  const onApplyBatchRoleChanges = async (): Promise<void> => {
    const targetPaths = batchScope === 'selected' ? selectedPaths : filteredItems
    if (targetPaths.length === 0) {
      setStatus('没有可批量处理的条目')
      return
    }
    if (!hasBatchOperation) {
      setStatus('请选择至少一个批量角色操作')
      return
    }
    if (
      isDirty &&
      selectedPath &&
      targetPaths.includes(selectedPath) &&
      !window.confirm('当前图片有未保存修改，批量操作会把这些修改一并写入当前图片，继续吗？')
    ) {
      return
    }

    const operation: BatchRoleOperation = {
      originalRoleMode: batchOriginalRoleMode,
      originalRoleName: batchOriginalRoleName,
      aliasMode: batchAliasMode,
      aliasText: batchAliasText,
    }

    const cache = metadataIndexCacheRef.current
    const failures: BatchExecutionFailure[] = []
    const summaryUpdates: Record<string, RoleMetadataSummary> = {}
    let changed = 0
    let skipped = 0
    let failed = 0
    let processed = 0
    let selectedChanged = false

    setBusy(true)
    setBatchProgress({ active: true, total: targetPaths.length, processed: 0, changed: 0, skipped: 0, failed: 0 })

    try {
      for (const path of targetPaths) {
        setStatus(`正在批量应用角色编辑（${processed + 1}/${targetPaths.length}）...`)
        try {
          let baseItem: MetadataItem
          let baseForm: FormState
          if (path === selectedPath && currentItem && form) {
            baseItem = currentItem
            baseForm = form
          } else {
            baseItem = await cache.readMetadata(path, (targetPath) => bridge.readMetadata(targetPath))
            baseForm = toForm(baseItem)
          }

          if (!shouldApplyBatchRoleOperation(baseForm, batchMatchMode)) {
            skipped += 1
          } else {
            const nextForm = applyBatchRoleOperation(baseForm, operation)
            if (!hasBatchRoleChange(baseForm, nextForm)) {
              skipped += 1
            } else {
              await bridge.saveMetadata(path, toPayload(nextForm, baseItem))
              cache.forgetItem(path)
              const summary = extractRoleMetadataSummaryFromForm(nextForm)
              cache.rememberSummary(path, summary)
              summaryUpdates[path] = summary
              changed += 1
              if (path === selectedPath) {
                selectedChanged = true
              }
            }
          }
        } catch (error) {
          failed += 1
          failures.push({ path, error: String(error) })
          cache.forgetItem(path)
        } finally {
          processed += 1
          setBatchProgress({
            active: true,
            total: targetPaths.length,
            processed,
            changed,
            skipped,
            failed,
          })
        }
      }

      if (selectedPath && selectedChanged) {
        const refreshed = await bridge.readMetadata(selectedPath)
        const nextForm = toForm(refreshed)
        rememberMetadataItem(refreshed)
        setCurrentItem(refreshed)
        setLoadedForm(nextForm)
        setForm(nextForm)
        summaryUpdates[selectedPath] = extractRoleMetadataSummary(refreshed)
      }

      setRoleSummaryByPath(cache.getSummarySnapshot(items))

      const report: BatchExecutionReport = {
        completedAt: new Date().toISOString(),
        scope: batchScope,
        matchMode: batchMatchMode,
        total: targetPaths.length,
        changed,
        skipped,
        failed,
        failures,
      }
      setLastBatchReport(report)
      setStatus(`批量角色编辑完成：改写 ${changed}，跳过 ${skipped}，失败 ${failed}`)
    } finally {
      setBatchProgress({
        active: false,
        total: targetPaths.length,
        processed,
        changed,
        skipped,
        failed,
      })
      setBusy(false)
    }
  }

  const updateField = (key: keyof FormState, value: string): void => {
    setForm((prev) => (prev ? { ...prev, [key]: value } : prev))
  }

  const addRoleAlias = (): void => {
    setForm((prev) => (prev ? { ...prev, role_aliases: [...prev.role_aliases, createRoleAliasFormItem()] } : prev))
  }

  const removeRoleAlias = (id: string): void => {
    setForm((prev) =>
      prev ? { ...prev, role_aliases: prev.role_aliases.filter((entry) => entry.id !== id) } : prev,
    )
  }

  const updateRoleAlias = (id: string, key: 'name' | 'note' | 'enabled', value: string | boolean): void => {
    setForm((prev) =>
      prev
        ? {
            ...prev,
            role_aliases: prev.role_aliases.map((entry) =>
              entry.id === id ? { ...entry, [key]: value } : entry,
            ),
          }
        : prev,
    )
  }

  const toggleSelection = (path: string): void => {
    setSelectedPaths((prev) => (prev.includes(path) ? prev.filter((entry) => entry !== path) : [...prev, path]))
  }

  const selectFiltered = (): void => {
    setSelectedPaths(filteredItems)
  }

  const clearSelection = (): void => {
    setSelectedPaths([])
  }

  return {
    activeMetaTab,
    activeTabPayload,
    appSettings,
    batchAliasMode,
    batchAliasText,
    batchMatchMode,
    batchOriginalRoleMode,
    batchOriginalRoleName,
    batchProgress,
    batchScope,
    bridgeVersion,
    busy: busy || imageActionBusy || metadataAiBusy || settingsBusy,
    currentItem,
    filterText,
    filteredItems,
    folder,
    form,
    getPreviewUrl: bridge.getPreviewUrl,
    hasBatchOperation,
    imageActionBusy,
    indexedCount,
    indexBusy,
    isDirty,
    items,
    keywordCount,
    lastBatchReport,
    metadataAiBusy,
    previewFailed,
    previewUrl,
    provider,
    roleFilterAlias,
    roleFilterOriginal,
    roleSummaryByPath,
    selectedName,
    selectedPath,
    selectedPaths,
    settingsBusy,
    settingsOpen,
    settingsPath,
    status,
    setActiveMetaTab,
    setBatchAliasMode,
    setBatchAliasText,
    setBatchMatchMode,
    setBatchOriginalRoleMode,
    setBatchOriginalRoleName,
    setBatchScope,
    setFilterText,
    setFolder,
    setPreviewFailed,
    setRoleFilterAlias,
    setRoleFilterOriginal,
    setSettingsOpen,
    onApplyBatchRoleChanges,
    onAddNameBar,
    onAiAutofillCurrentMetadata,
    onFieldChange: updateField,
    onGenerateBiography,
    onGotoNext,
    onGotoPrev,
    onPickAndOpenImage,
    onPickAndLoadFolder,
    onPickFolderPath,
    onLoadFolder,
    onBuildRoleIndex,
    onOpenItem,
    onOpenCurrentFile,
    onReloadCurrent,
    onReloadAppSettings,
    onOpenSettingsPanel,
    onRevealCurrentFile,
    onRoleAliasAdd: addRoleAlias,
    onRoleAliasChange: updateRoleAlias,
    onRoleAliasRemove: removeRoleAlias,
    onSave,
    onSaveAppSettings,
    onRenameCurrentImage,
    onSelectFiltered: selectFiltered,
    onClearSelection: clearSelection,
    onToggleSelection: toggleSelection,
  }
}
