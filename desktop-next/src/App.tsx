import './App.css'
import { useMemo, useState } from 'react'
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
    const s = token.trim()
    if (!s) continue
    const k = s.toLowerCase()
    if (seen.has(k)) continue
    seen.add(k)
    out.push(s)
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

function App() {
  const bridge = useMemo(() => createDesktopBridge(), [])
  const [folder, setFolder] = useState<string>(DEFAULT_FOLDER)
  const [items, setItems] = useState<string[]>([])
  const [selectedPath, setSelectedPath] = useState<string>('')
  const [form, setForm] = useState<FormState | null>(null)
  const [status, setStatus] = useState<string>('就绪')
  const [busy, setBusy] = useState<boolean>(false)
  const [provider] = useState<string>(bridge.provider)

  const selectedName = selectedPath ? selectedPath.split(/[\\/]/).pop() || '' : ''

  const onLoadFolder = async (): Promise<void> => {
    setBusy(true)
    setStatus('正在加载目录...')
    try {
      const list = await bridge.listImages(folder, 500)
      setItems(list)
      if (list.length > 0) {
        const first = list[0]
        setSelectedPath(first)
        const data = await bridge.readMetadata(first)
        setForm(toForm(data))
        setStatus(`已加载 ${list.length} 项`)
      } else {
        setSelectedPath('')
        setForm(null)
        setStatus('目录中没有图片')
      }
    } catch (e) {
      setStatus(`加载失败：${String(e)}`)
    } finally {
      setBusy(false)
    }
  }

  const onOpenItem = async (path: string): Promise<void> => {
    setBusy(true)
    setStatus('读取元数据...')
    try {
      const data = await bridge.readMetadata(path)
      setSelectedPath(path)
      setForm(toForm(data))
      setStatus('读取完成')
    } catch (e) {
      setStatus(`读取失败：${String(e)}`)
    } finally {
      setBusy(false)
    }
  }

  const onSave = async (): Promise<void> => {
    if (!selectedPath || !form) return
    setBusy(true)
    setStatus('正在保存元数据...')
    try {
      await bridge.saveMetadata(selectedPath, toPayload(form))
      setStatus('保存成功')
    } catch (e) {
      setStatus(`保存失败：${String(e)}`)
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
        </div>
        <div className="toolbar">
          <input
            className="path-input"
            value={folder}
            onChange={(e) => setFolder(e.target.value)}
            placeholder="输入目录路径"
          />
          <button onClick={onLoadFolder} disabled={busy}>
            加载目录
          </button>
          <button className="primary" onClick={onSave} disabled={busy || !form || !selectedPath}>
            保存元数据
          </button>
        </div>
      </header>

      <main className="main">
        <section className="preview">
          <div className="preview-head">预览区</div>
          <div className="preview-canvas">
            <div className="placeholder">{selectedName ? `当前：${selectedName}` : '图片预览'}</div>
          </div>
          <div className="preview-actions list-wrap">
            {items.length === 0 ? (
              <div className="empty-list">暂无条目</div>
            ) : (
              <ul className="image-list">
                {items.map((path) => (
                  <li key={path}>
                    <button
                      className={path === selectedPath ? 'item-btn active' : 'item-btn'}
                      onClick={() => void onOpenItem(path)}
                      disabled={busy}
                    >
                      {path.split(/[\\/]/).pop()}
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>

        <section className="editor">
          <nav className="tabbar">
            <button className="tab active">编辑</button>
            <button className="tab">结构化</button>
            <button className="tab">XMP</button>
            <button className="tab">EXIF</button>
            <button className="tab">IPTC</button>
            <button className="tab">全部</button>
          </nav>

          <div className="panel">
            <div className="field-grid">
              <label>标题</label>
              <input value={form?.title || ''} onChange={(e) => updateField('title', e.target.value)} />
              <label>人物</label>
              <input value={form?.person || ''} onChange={(e) => updateField('person', e.target.value)} />
              <label>性别</label>
              <input value={form?.gender || ''} onChange={(e) => updateField('gender', e.target.value)} />
              <label>职务</label>
              <input value={form?.position || ''} onChange={(e) => updateField('position', e.target.value)} />
              <label>城市</label>
              <input value={form?.city || ''} onChange={(e) => updateField('city', e.target.value)} />
              <label>来源</label>
              <input value={form?.source || ''} onChange={(e) => updateField('source', e.target.value)} />
              <label>原图链接</label>
              <input value={form?.image_url || ''} onChange={(e) => updateField('image_url', e.target.value)} />
              <label>关键词</label>
              <input value={form?.keywords_text || ''} onChange={(e) => updateField('keywords_text', e.target.value)} />
              <label>Asset ID</label>
              <input value={form?.titi_asset_id || ''} onChange={(e) => updateField('titi_asset_id', e.target.value)} />
              <label>World ID</label>
              <input value={form?.titi_world_id || ''} onChange={(e) => updateField('titi_world_id', e.target.value)} />
            </div>

            <div className="bio">
              <p className="bio-title">人物小传</p>
              <textarea
                value={form?.description || ''}
                onChange={(e) => updateField('description', e.target.value)}
              />
            </div>
          </div>
        </section>
      </main>

      <footer className="statusbar">
        <span>状态：{status}</span>
        <span>Bridge：{provider}</span>
        <span>条目：{items.length}</span>
      </footer>
    </div>
  )
}

export default App
