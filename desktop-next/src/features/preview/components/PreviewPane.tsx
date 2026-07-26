import { useRef, useState, type WheelEvent } from 'react'

type PreviewPaneProps = {
  busy: boolean
  itemCount: number
  previewFailed: boolean
  previewUrl: string
  selectedName: string
  selectedPath: string
  onGotoNext: () => void
  onGotoPrev: () => void
  onOpenCurrentFile: () => void
  onPreviewError: () => void
  onRevealCurrentFile: () => void
}

export function PreviewPane({
  busy,
  itemCount,
  previewFailed,
  previewUrl,
  selectedName,
  selectedPath,
  onGotoNext,
  onGotoPrev,
  onOpenCurrentFile,
  onPreviewError,
  onRevealCurrentFile,
}: PreviewPaneProps) {
  const [zoomMode, setZoomMode] = useState<'fit' | 'fill' | 'actual'>('fit')
  const lastWheelNavAtRef = useRef(0)
  const hasSelectedImage = Boolean(selectedPath)

  const onPreviewWheel = (event: WheelEvent<HTMLDivElement>): void => {
    if (busy || !selectedPath || zoomMode === 'actual') {
      return
    }
    const delta = Math.abs(event.deltaY) >= Math.abs(event.deltaX) ? event.deltaY : event.deltaX
    if (Math.abs(delta) < 12) {
      return
    }
    event.preventDefault()
    const now = window.performance.now()
    if (now - lastWheelNavAtRef.current < 420) {
      return
    }
    lastWheelNavAtRef.current = now
    if (delta > 0) {
      onGotoNext()
    } else {
      onGotoPrev()
    }
  }

  return (
    <div className={hasSelectedImage ? 'preview-pane' : 'preview-pane preview-pane-empty'}>
      {hasSelectedImage ? (
        <div className="section-head">
          <div>
            <p className="section-eyebrow">当前图片</p>
            <h2>{selectedName}</h2>
          </div>
          <span className="section-meta">{itemCount} 项</span>
        </div>
      ) : null}

      {hasSelectedImage ? (
        <div className="preview-actions">
          <button type="button" onClick={onGotoPrev} disabled={busy}>
            上一张
          </button>
          <button type="button" onClick={onGotoNext} disabled={busy}>
            下一张
          </button>
          <button type="button" onClick={onOpenCurrentFile} disabled={busy}>
            系统打开
          </button>
          <button type="button" onClick={onRevealCurrentFile} disabled={busy}>
            打开所在文件夹
          </button>
          <div className="preview-zoom" role="group" aria-label="预览缩放">
            <button type="button" className={zoomMode === 'fit' ? 'active' : ''} onClick={() => setZoomMode('fit')}>
              适应
            </button>
            <button type="button" className={zoomMode === 'fill' ? 'active' : ''} onClick={() => setZoomMode('fill')}>
              填充
            </button>
            <button type="button" className={zoomMode === 'actual' ? 'active' : ''} onClick={() => setZoomMode('actual')}>
              原始
            </button>
          </div>
        </div>
      ) : null}

      <div className="preview-stage" onWheel={onPreviewWheel}>
        <div className={`preview-canvas preview-canvas-${zoomMode}`}>
          {previewUrl && !previewFailed ? (
            <img
              key={previewUrl}
              className={`preview-image preview-image-${zoomMode}`}
              src={previewUrl}
              alt={selectedName || '预览图'}
              onError={onPreviewError}
            />
          ) : (
            <div className="placeholder">
              {selectedName ? '当前环境无法渲染该预览，元数据编辑仍可继续。' : '未选择图片'}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
