export function hasData(value: unknown): boolean {
  if (Array.isArray(value)) {
    return value.length > 0
  }
  if (value && typeof value === 'object') {
    return Object.keys(value as Record<string, unknown>).length > 0
  }
  return Boolean(value)
}

export function stringifyJson(value: unknown): string {
  return JSON.stringify(value, null, 2)
}

export function formatFileSize(value: number | undefined): string {
  if (typeof value !== 'number' || Number.isNaN(value) || value <= 0) {
    return '-'
  }
  const units = ['B', 'KB', 'MB', 'GB']
  let size = value
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size >= 10 || unitIndex === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unitIndex]}`
}

export function formatTimestamp(value: string | undefined): string {
  const target = String(value || '').trim()
  if (!target) {
    return '-'
  }
  return target.replace('T', ' ').replace('Z', '')
}
