import type { MetadataItem } from '../../../types'
import { formatFileSize, formatTimestamp, type FormState } from '../../../domain/metadata'

type CurrentImageSummaryPaneProps = {
  currentItem: MetadataItem | null
  form: FormState | null
  isDirty: boolean
  keywordCount: number
  selectedName: string
  selectedPath: string
}

function metadataStatusLabel(status: string | undefined, isDirty: boolean): string {
  if (isDirty) {
    return '有未保存修改'
  }
  switch (String(status || '').toLowerCase()) {
    case 'complete':
      return '完整'
    case 'partial':
      return '部分'
    case 'none':
      return '未写入'
    default:
      return status || '未读取'
  }
}

export function CurrentImageSummaryPane({
  currentItem,
  form,
  isDirty,
  keywordCount,
  selectedName,
  selectedPath,
}: CurrentImageSummaryPaneProps) {
  const rows = [
    ['文件名', currentItem?.filename || selectedName || '-'],
    ['路径', currentItem?.filepath || selectedPath || '-'],
    ['文件大小', formatFileSize(currentItem?.filesize)],
    ['修改时间', formatTimestamp(currentItem?.modified_time)],
    ['metadata 状态', metadataStatusLabel(currentItem?.status, isDirty)],
    ['人物名', form?.person || '-'],
    ['原角色名', form?.original_role_name || '-'],
    ['标题', form?.title || '-'],
    ['关键词数量', String(keywordCount)],
    ['来源', form?.source || '-'],
    ['原图链接', form?.image_url || '-'],
  ]

  return (
    <section className="summary-pane" aria-label="当前图片摘要">
      <div className="panel-title-row">
        <p className="panel-title">当前图片摘要</p>
        <span className={isDirty ? 'chip chip-warn' : 'chip chip-ok'}>{isDirty ? '未保存' : '已同步'}</span>
      </div>
      <dl className="summary-list">
        {rows.map(([label, value]) => (
          <div key={label} className="summary-row">
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  )
}
