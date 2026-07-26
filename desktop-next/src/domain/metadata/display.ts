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

function formatDateParts(date: Date): string {
  const pad = (value: number): string => String(value).padStart(2, '0')
  return [
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`,
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`,
  ].join(' ')
}

function normalizeExifTimestamp(value: string): string | null {
  const match = value.match(
    /^(\d{4}):(\d{2}):(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\s*([+-]\d{2}:?\d{2}|Z))?$/i,
  )
  if (!match) {
    return null
  }
  const [, year, month, day, hour, minute, second, offset] = match
  if (!offset) {
    return `${year}-${month}-${day} ${hour}:${minute}:${second}`
  }
  const normalizedOffset =
    offset.toUpperCase() === 'Z' || offset.includes(':')
      ? offset.toUpperCase()
      : `${offset.slice(0, 3)}:${offset.slice(3)}`
  return `${year}-${month}-${day}T${hour}:${minute}:${second}${normalizedOffset}`
}

export function formatTimestamp(value: string | undefined): string {
  const target = String(value || '').trim()
  if (!target) {
    return '-'
  }
  const normalizedExif = normalizeExifTimestamp(target)
  const normalized = normalizedExif || target
  const hasExplicitTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(normalized)
  if (hasExplicitTimezone) {
    const parsed = new Date(normalized)
    if (!Number.isNaN(parsed.getTime())) {
      return formatDateParts(parsed)
    }
  }
  return normalized.replace('T', ' ').replace(/Z$/i, '')
}
