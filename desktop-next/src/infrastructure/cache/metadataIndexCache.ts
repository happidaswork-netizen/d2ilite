import { extractRoleMetadataSummary, type RoleMetadataSummary } from '../../domain/metadata'
import type { MetadataItem } from '../../types'

export type MetadataReader = (path: string) => Promise<MetadataItem>

export type MetadataIndexUpdate = {
  indexedCount: number
  totalCount: number
}

export function createMetadataIndexCache() {
  const itemCache = new Map<string, MetadataItem>()
  const summaryCache = new Map<string, RoleMetadataSummary>()

  const countIndexed = (paths: string[]): number => {
    let count = 0
    for (const path of paths) {
      if (summaryCache.has(path)) {
        count += 1
      }
    }
    return count
  }

  const rememberItem = (item: MetadataItem): RoleMetadataSummary => {
    const path = String(item.filepath || '').trim()
    const summary = extractRoleMetadataSummary(item)
    if (!path) {
      return summary
    }
    itemCache.set(path, item)
    summaryCache.set(path, summary)
    return summary
  }

  const rememberSummary = (path: string, summary: RoleMetadataSummary): void => {
    const target = String(path || '').trim()
    if (!target) {
      return
    }
    summaryCache.set(target, summary)
  }

  const forgetItem = (path: string): void => {
    const target = String(path || '').trim()
    if (!target) {
      return
    }
    itemCache.delete(target)
  }

  const forgetItems = (paths: string[]): void => {
    for (const path of paths) {
      forgetItem(path)
    }
  }

  const getItem = (path: string): MetadataItem | null => {
    return itemCache.get(String(path || '').trim()) || null
  }

  const getSummary = (path: string): RoleMetadataSummary | null => {
    return summaryCache.get(String(path || '').trim()) || null
  }

  const getSummarySnapshot = (paths: string[]): Record<string, RoleMetadataSummary> => {
    const snapshot: Record<string, RoleMetadataSummary> = {}
    for (const path of paths) {
      const summary = getSummary(path)
      if (summary) {
        snapshot[path] = summary
      }
    }
    return snapshot
  }

  const readMetadata = async (path: string, reader: MetadataReader): Promise<MetadataItem> => {
    const cached = getItem(path)
    if (cached) {
      return cached
    }
    const item = await reader(path)
    rememberItem(item)
    return item
  }

  const hydrateRoleSummaries = async (
    paths: string[],
    reader: MetadataReader,
    options?: {
      flushEvery?: number
      onUpdate?: (update: MetadataIndexUpdate) => void
    },
  ): Promise<Record<string, RoleMetadataSummary>> => {
    const flushEvery = Math.max(1, options?.flushEvery || 8)
    options?.onUpdate?.({ indexedCount: countIndexed(paths), totalCount: paths.length })

    let pendingFlush = 0
    for (const path of paths) {
      if (summaryCache.has(path)) {
        continue
      }
      try {
        const item = await readMetadata(path, reader)
        rememberItem(item)
      } catch {
        // ignore individual index failures and continue
      }
      pendingFlush += 1
      if (pendingFlush >= flushEvery) {
        pendingFlush = 0
        options?.onUpdate?.({ indexedCount: countIndexed(paths), totalCount: paths.length })
      }
    }

    options?.onUpdate?.({ indexedCount: countIndexed(paths), totalCount: paths.length })
    return getSummarySnapshot(paths)
  }

  return {
    forgetItem,
    forgetItems,
    getItem,
    getSummary,
    getSummarySnapshot,
    hydrateRoleSummaries,
    readMetadata,
    rememberItem,
    rememberSummary,
  }
}
