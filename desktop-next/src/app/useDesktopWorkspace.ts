import { useEffect, useMemo, useRef, useState } from 'react'

import type { MetadataItem } from '../types'
import {
  applyBatchRoleOperation,
  createRoleAliasFormItem,
  extractRoleMetadataSummary,
  extractRoleMetadataSummaryFromForm,
  getTabPayload,
  parseKeywords,
  sameForm,
  toForm,
  toPayload,
  type BatchAliasMode,
  type BatchOriginalRoleMode,
  type BatchRoleOperation,
  type BatchRoleScope,
  type FormState,
  type MetadataTabKey,
  type RoleMetadataSummary,
} from '../domain/metadata'
import {
  createDesktopBridge,
  reportDesktopFrontendStatus,
  runDesktopSmokeRoundtrip,
} from '../infrastructure/desktopBridge'
import { loadPreferredFolder, persistPreferredFolder } from '../infrastructure/runtime/folderPreference'
import { getFileName } from '../shared/path'

export function useDesktopWorkspace() {
  const bridge = useMemo(() => createDesktopBridge(), [])
  const startupSmokeRef = useRef<boolean>(false)
  const currentItemRef = useRef<MetadataItem | null>(null)
  const selectedPathRef = useRef<string>('')

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
        if (!startupSmokeRef.current) {
          startupSmokeRef.current = true
          try {
            await runDesktopSmokeRoundtrip(bridge, provider, health)
          } catch {
            // ignore smoke bootstrap failures during startup
          }
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
    let disposed = false
    const currentPath = selectedPathRef.current
    const seed: Record<string, RoleMetadataSummary> = {}

    if (currentPath && currentItemRef.current && items.includes(currentPath)) {
      seed[currentPath] = extractRoleMetadataSummary(currentItemRef.current)
    }

    setRoleSummaryByPath(seed)

    if (items.length === 0) {
      setIndexBusy(false)
      return () => {
        disposed = true
      }
    }

    const run = async (): Promise<void> => {
      setIndexBusy(true)
      const nextSummaryMap = { ...seed }
      let pendingFlush = 0

      for (const path of items) {
        if (disposed) {
          return
        }
        if (nextSummaryMap[path]) {
          continue
        }

        try {
          const item =
            path === selectedPathRef.current && currentItemRef.current
              ? currentItemRef.current
              : await bridge.readMetadata(path)
          nextSummaryMap[path] = extractRoleMetadataSummary(item)
        } catch {
          // ignore per-item index failures and continue scanning
        }

        pendingFlush += 1
        if (pendingFlush >= 8) {
          pendingFlush = 0
          setRoleSummaryByPath({ ...nextSummaryMap })
        }
      }

      if (!disposed) {
        setRoleSummaryByPath({ ...nextSummaryMap })
        setIndexBusy(false)
      }
    }

    void run().catch(() => {
      if (!disposed) {
        setIndexBusy(false)
      }
    })

    return () => {
      disposed = true
    }
  }, [bridge, items])

  const updateRoleSummary = (path: string, summary: RoleMetadataSummary): void => {
    setRoleSummaryByPath((prev) => ({ ...prev, [path]: summary }))
  }

  const loadMetadata = async (path: string, statusText: string): Promise<void> => {
    setBusy(true)
    setStatus(statusText)
    try {
      const data = await bridge.readMetadata(path)
      const nextForm = toForm(data)
      setCurrentItem(data)
      setSelectedPath(path)
      setLoadedForm(nextForm)
      setForm(nextForm)
      updateRoleSummary(path, extractRoleMetadataSummary(data))
      setStatus(`已读取：${getFileName(path)}`)
    } catch (error) {
      setStatus(`读取失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }

  const confirmDiscard = (): boolean => {
    if (!isDirty) {
      return true
    }
    return window.confirm('当前有未保存修改，确定放弃并切换条目吗？')
  }

  const onLoadFolder = async (): Promise<void> => {
    const targetFolder = folder.trim()
    if (!targetFolder) {
      setStatus('请输入目录路径')
      return
    }
    if (!confirmDiscard()) {
      return
    }

    setBusy(true)
    setStatus('正在加载目录...')
    try {
      const list = await bridge.listImages(targetFolder, 500)
      setItems(list)
      setSelectedPaths([])
      if (list.length === 0) {
        setSelectedPath('')
        setCurrentItem(null)
        setLoadedForm(null)
        setForm(null)
        setRoleSummaryByPath({})
        setStatus('目录中没有图片')
        return
      }

      const preferred = selectedPath && list.includes(selectedPath) ? selectedPath : list[0]
      const data = await bridge.readMetadata(preferred)
      const nextForm = toForm(data)
      setSelectedPath(preferred)
      setCurrentItem(data)
      setLoadedForm(nextForm)
      setForm(nextForm)
      updateRoleSummary(preferred, extractRoleMetadataSummary(data))
      setStatus(`已加载 ${list.length} 项`)
    } catch (error) {
      setStatus(`加载失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
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
    await loadMetadata(selectedPath, '正在重新读取元数据...')
  }

  const onSave = async (): Promise<void> => {
    if (!selectedPath || !form || !isDirty) {
      return
    }
    setBusy(true)
    setStatus('正在保存元数据...')
    try {
      await bridge.saveMetadata(selectedPath, toPayload(form, currentItem))
      const refreshed = await bridge.readMetadata(selectedPath)
      const nextForm = toForm(refreshed)
      setCurrentItem(refreshed)
      setLoadedForm(nextForm)
      setForm(nextForm)
      updateRoleSummary(selectedPath, extractRoleMetadataSummary(refreshed))
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

    setBusy(true)
    try {
      const summaryUpdates: Record<string, RoleMetadataSummary> = {}

      for (let index = 0; index < targetPaths.length; index += 1) {
        const path = targetPaths[index]
        setStatus(`正在批量应用角色编辑（${index + 1}/${targetPaths.length}）...`)

        let baseItem: MetadataItem
        let baseForm: FormState
        if (path === selectedPath && currentItem && form) {
          baseItem = currentItem
          baseForm = form
        } else {
          baseItem = await bridge.readMetadata(path)
          baseForm = toForm(baseItem)
        }

        const nextForm = applyBatchRoleOperation(baseForm, operation)
        await bridge.saveMetadata(path, toPayload(nextForm, baseItem))
        summaryUpdates[path] = extractRoleMetadataSummaryFromForm(nextForm)
      }

      if (selectedPath && targetPaths.includes(selectedPath)) {
        const refreshed = await bridge.readMetadata(selectedPath)
        const nextForm = toForm(refreshed)
        setCurrentItem(refreshed)
        setLoadedForm(nextForm)
        setForm(nextForm)
        summaryUpdates[selectedPath] = extractRoleMetadataSummary(refreshed)
      }

      setRoleSummaryByPath((prev) => ({ ...prev, ...summaryUpdates }))
      setStatus(`批量角色编辑已应用到 ${targetPaths.length} 项`)
    } catch (error) {
      setStatus(`批量角色编辑失败：${String(error)}`)
    } finally {
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
    batchAliasMode,
    batchAliasText,
    batchOriginalRoleMode,
    batchOriginalRoleName,
    batchScope,
    bridgeVersion,
    busy,
    currentItem,
    filterText,
    filteredItems,
    folder,
    form,
    hasBatchOperation,
    indexedCount,
    indexBusy,
    isDirty,
    items,
    keywordCount,
    previewFailed,
    previewUrl,
    provider,
    roleFilterAlias,
    roleFilterOriginal,
    roleSummaryByPath,
    selectedName,
    selectedPath,
    selectedPaths,
    status,
    setActiveMetaTab,
    setBatchAliasMode,
    setBatchAliasText,
    setBatchOriginalRoleMode,
    setBatchOriginalRoleName,
    setBatchScope,
    setFilterText,
    setFolder,
    setPreviewFailed,
    setRoleFilterAlias,
    setRoleFilterOriginal,
    onApplyBatchRoleChanges,
    onFieldChange: updateField,
    onLoadFolder,
    onOpenItem,
    onReloadCurrent,
    onRoleAliasAdd: addRoleAlias,
    onRoleAliasChange: updateRoleAlias,
    onRoleAliasRemove: removeRoleAlias,
    onSave,
    onSelectFiltered: selectFiltered,
    onClearSelection: clearSelection,
    onToggleSelection: toggleSelection,
  }
}
