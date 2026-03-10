export function getFileName(path: string): string {
  return String(path || '').split(/[\\/]/).pop() || ''
}
