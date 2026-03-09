type PreviewPaneProps = {
  itemCount: number
  previewFailed: boolean
  previewUrl: string
  selectedName: string
  selectedPath: string
  onPreviewError: () => void
}

export function PreviewPane({
  itemCount,
  previewFailed,
  previewUrl,
  selectedName,
  selectedPath,
  onPreviewError,
}: PreviewPaneProps) {
  return (
    <>
      <div className="section-head">
        <div>
          <p className="section-eyebrow">Preview</p>
          <h2>{selectedName || '未选择图片'}</h2>
        </div>
        <span className="section-meta">{itemCount} 项</span>
      </div>

      <div className="preview-canvas">
        {previewUrl && !previewFailed ? (
          <img
            key={previewUrl}
            className="preview-image"
            src={previewUrl}
            alt={selectedName || '预览图'}
            onError={onPreviewError}
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
    </>
  )
}
