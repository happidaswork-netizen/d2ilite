import './App.css'
import { useEffect, useMemo, useState } from 'react'

import { createDesktopBridge } from './bridge/desktopBridge'
import type { MetadataItem, SavePayload } from './types'

type FormState = {
  title: string
  person: string
  gender: string
  position: string
  city: string
  source: string
  image_url: string
  keywords_text: string
  titi_asset_id: string
  titi_world_id: string
  description: string
}

const DEFAULT_FOLDER = 'Z:\\生成图片\\角色肖像\\警察\\原图\\公安部英烈_2021'
const FOLDER_STORAGE_KEY = 'd2i-lite-next-folder'

function toForm(item: MetadataItem): FormState {
  return {
    title: String(item.title || ''),
    person: String(item.person || ''),
    gender: String(item.gender || ''),
    position: String(item.position || ''),
    city: String(item.city || ''),
    source: String(item.source || ''),
    image_url: String(item.image_url || ''),
    keywords_text: Array.isArray(item.keywords) ? item.keywords.join(', ') : '',
    titi_asset_id: String(item.titi_asset_id || ''),
    titi_world_id: String(item.titi_world_id || ''),
    description: String(item.description || ''),
  }
}

function parseKeywords(raw: string): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const token of String(raw || '').split(/[;,，、\n]+/)) {
    const value = token.trim()
    if (!value) continue
    const normalized = value.toLowerCase()
    if (seen.has(normalized)) continue
    seen.add(normalized)
    out.push(value)
  }
  return out
}

function toPayload(form: FormState): SavePayload {
  return {
    title: form.title.trim(),
    person: form.person.trim(),
    gender: form.gender.trim(),
    position: form.position.trim(),
    city: form.city.trim(),
    source: form.source.trim(),
    image_url: form.image_url.trim(),
    keywords: parseKeywords(form.keywords_text),
    titi_asset_id: form.titi_asset_id.trim(),
    titi_world_id: form.titi_world_id.trim(),
    description: form.description.trim(),
  }
}

function getInitialFolder(): string {
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

function getFileName(path: string): string {
  return String(path || '').split(/[\\/]/).pop() || ''
}

function sameForm(left: FormState | null, right: FormState | null): boolean {
  return JSON.stringify(left || {}) === JSON.stringify(right || {})
}

function App() {
  const bridge = useMemo(() => createDesktopBridge(), [])
  const [folder, setFolder] = useState<string>(getInitialFolder)
  const [items, setItems] = useState<string[]>([])
  const [selectedPath, setSelectedPath] = useState<string>('')
  const [loadedForm, setLoadedForm] = useState<FormState | null>(null)
  const [form, setForm] = useState<FormState | null>(null)
  const [status, setStatus] = useState<string>('就绪')
  const [busy, setBusy] = useState<boolean>(false)
  const [bridgeVersion, setBridgeVersion] = useState<string>('未探测')
  const [filterText, setFilterText] = useState<string>('')
  const [previewFailed, setPreviewFailed] = useState<boolean>(false)

  const provider = bridge.provider
  const selectedName = selectedPath ? getFileName(selectedPath) : ''
  const previewUrl = selectedPath ? bridge.getPreviewUrl(selectedPath) : ''
  const isDirty = useMemo(() => !sameForm(form, loadedForm), [form, loadedForm])
  const filteredItems = useMemo(() => {
    const keyword = filterText.trim().toLowerCase()
    if (!keyword) {
      return items
    }
    return items.filter((itemPath) => itemPath.toLowerCase().includes(keyword))
  }, [filterText, items])

  useEffect(() => {
    setPreviewFailed(false)
  }, [selectedPath])

  useEffect(() => {
    try {
      window.localStorage.setItem(FOLDER_STORAGE_KEY, folder)
    } catch {
      // ignore localStorage errors
    }
  }, [folder])

  useEffect(() => {
    let disposed = false
    const run = async (): Promise<void> => {
      try {
        const health = await bridge.ping()
        if (disposed) return
        setBridgeVersion(`${health.provider} / ${health.version}`)
        setStatus(`Bridge 已连接：${health.provider}`)
      } catch (error) {
        if (disposed) return
        setBridgeVersion('不可用')
        setStatus(`Bridge 检测失败：${String(error)}`)
      }
    }
    void run()
    return () => {
      disposed = true
    }
  }, [bridge])

  const loadMetadata = async (path: string, statusText: string): Promise<void> => {
    setBusy(true)
    setStatus(statusText)
    try {
      const data = await bridge.readMetadata(path)
      const nextForm = toForm(data)
      setSelectedPath(path)
      setLoadedForm(nextForm)
      setForm(nextForm)
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
      if (list.length === 0) {
        setSelectedPath('')
        setLoadedForm(null)
        setForm(null)
        setStatus('目录中没有图片')
        return
      }

      const preferred = selectedPath && list.includes(selectedPath) ? selectedPath : list[0]
      const data = await bridge.readMetadata(preferred)
      const nextForm = toForm(data)
      setSelectedPath(preferred)
      setLoadedForm(nextForm)
      setForm(nextForm)
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
    if (!selectedPath || !form || !isDirty) return
    setBusy(true)
    setStatus('正在保存元数据...')
    try {
      await bridge.saveMetadata(selectedPath, toPayload(form))
      const refreshed = await bridge.readMetadata(selectedPath)
      const nextForm = toForm(refreshed)
      setLoadedForm(nextForm)
      setForm(nextForm)
      setStatus('保存成功')
    } catch (error) {
      setStatus(`保存失败：${String(error)}`)
    } finally {
      setBusy(false)
    }
  }

  const updateField = (key: keyof FormState, value: string): void => {
    setForm((prev) => (prev ? { ...prev, [key]: value } : prev))
  }

  return (
    <div className="workspace">
      <header className="topbar">
        <div className="titleblock">
          <p className="eyebrow">D2I Lite Next</p>
          <h1>本地看图与元数据工作台</h1>
          <p className="subtitle">Phase 1 最小闭环：目录浏览、图片预览、元数据读取与保存。</p>
        </div>

        <div className="bridge-strip">
          <span className="chip chip-strong">Provider {provider}</span>
          <span className="chip">Bridge {bridgeVersion}</span>
          <span className={isDirty ? 'chip chip-warn' : 'chip'}>
            {isDirty ? '有未保存修改' : '已同步'}
          </span>
        </div>

        <div className="toolbar">
          <label className="input-stack input-stack-wide">
            <span>图片目录</span>
            <input
              className="path-input"
              value={folder}
              onChange={(event) => setFolder(event.target.value)}
              placeholder="输入目录路径"
            />
          </label>
          <div className="action-group">
            <button onClick={() => void onLoadFolder()} disabled={busy}>
              加载目录
            </button>
            <button onClick={() => void onReloadCurrent()} disabled={busy || !selectedPath}>
              重新读取
            </button>
            <button className="primary" onClick={() => void onSave()} disabled={busy || !selectedPath || !form || !isDirty}>
              保存元数据
            </button>
          </div>
        </div>
      </header>

      <main className="main">
        <section className="preview shell">
          <div className="section-head">
            <div>
              <p className="section-eyebrow">Preview</p>
              <h2>{selectedName || '未选择图片'}</h2>
            </div>
            <span className="section-meta">{items.length} 项</span>
          </div>

          <div className="preview-canvas">
            {previewUrl && !previewFailed ? (
              <img
                key={previewUrl}
                className="preview-image"
                src={previewUrl}
                alt={selectedName || '预览图'}
                onError={() => setPreviewFailed(true)}
              />
            ) : (
              <div className="placeholder">
                {selectedName ? '当前环境无法渲染该预览，元数据编辑仍可继续。' : '图片预览'}
              </div>
            )}
          </div>

          <div className="preview-meta">
            <div className="meta-card">
              <span className="meta-label">当前文件</span>
              <strong>{selectedName || '-'}</strong>
            </div>
            <div className="meta-card">
              <span className="meta-label">完整路径</span>
              <strong className="path-value">{selectedPath || '-'}</strong>
            </div>
          </div>

          <div className="list-toolbar">
            <label className="input-stack">
              <span>筛选条目</span>
              <input
                value={filterText}
                onChange={(event) => setFilterText(event.target.value)}
                placeholder="按文件名或路径过滤"
              />
            </label>
            <div className="list-meta">
              <span>显示 {filteredItems.length}</span>
            </div>
          </div>

          <div className="list-wrap">
            {filteredItems.length === 0 ? (
              <div className="empty-list">{items.length === 0 ? '暂无条目' : '筛选后无结果'}</div>
            ) : (
              <ul className="image-list">
                {filteredItems.map((path) => (
                  <li key={path}>
                    <button
                      className={path === selectedPath ? 'item-btn active' : 'item-btn'}
                      onClick={() => void onOpenItem(path)}
                      disabled={busy}
                    >
                      <span className="item-name">{getFileName(path)}</span>
                      <span className="item-path">{path}</span>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>

        <section className="editor shell">
          <div className="section-head">
            <div>
              <p className="section-eyebrow">Metadata</p>
              <h2>结构化编辑</h2>
            </div>
            <span className="section-meta">{selectedPath ? '已绑定当前图片' : '等待选择条目'}</span>
          </div>

          <div className="editor-grid">
            <div className="field-grid">
              <label>标题</label>
              <input value={form?.title || ''} onChange={(event) => updateField('title', event.target.value)} />
              <label>人物</label>
              <input value={form?.person || ''} onChange={(event) => updateField('person', event.target.value)} />
              <label>性别</label>
              <input value={form?.gender || ''} onChange={(event) => updateField('gender', event.target.value)} />
              <label>职务</label>
              <input value={form?.position || ''} onChange={(event) => updateField('position', event.target.value)} />
              <label>城市</label>
              <input value={form?.city || ''} onChange={(event) => updateField('city', event.target.value)} />
              <label>来源</label>
              <input value={form?.source || ''} onChange={(event) => updateField('source', event.target.value)} />
              <label>原图链接</label>
              <input value={form?.image_url || ''} onChange={(event) => updateField('image_url', event.target.value)} />
              <label>关键词</label>
              <input value={form?.keywords_text || ''} onChange={(event) => updateField('keywords_text', event.target.value)} />
              <label>Asset ID</label>
              <input value={form?.titi_asset_id || ''} onChange={(event) => updateField('titi_asset_id', event.target.value)} />
              <label>World ID</label>
              <input value={form?.titi_world_id || ''} onChange={(event) => updateField('titi_world_id', event.target.value)} />
            </div>

            <div className="bio">
              <div className="bio-head">
                <p className="bio-title">人物小传</p>
                <span>{parseKeywords(form?.keywords_text || '').length} 个关键词</span>
              </div>
              <textarea
                value={form?.description || ''}
                onChange={(event) => updateField('description', event.target.value)}
                placeholder="这里显示或编辑描述/小传正文"
              />
            </div>
          </div>
        </section>
      </main>

      <footer className="statusbar">
        <span>状态：{status}</span>
        <span>Bridge：{provider}</span>
        <span>版本：{bridgeVersion}</span>
        <span>条目：{items.length}</span>
      </footer>
    </div>
  )
}

export default App
