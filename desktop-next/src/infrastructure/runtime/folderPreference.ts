const DEFAULT_FOLDER = 'Z:\\生成图片\\角色肖像\\警察\\原图\\公安部英烈_2021'
const FOLDER_STORAGE_KEY = 'd2i-lite-next-folder'

export function loadPreferredFolder(): string {
  try {
    const cached = window.localStorage.getItem(FOLDER_STORAGE_KEY)
    if (cached && cached.trim()) {
      return cached.trim()
    }
  } catch {
    // ignore localStorage errors
  }
  return DEFAULT_FOLDER
}

export function persistPreferredFolder(folder: string): void {
  try {
    window.localStorage.setItem(FOLDER_STORAGE_KEY, folder)
  } catch {
    // ignore localStorage errors
  }
}
