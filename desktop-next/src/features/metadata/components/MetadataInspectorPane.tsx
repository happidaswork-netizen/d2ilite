import type { MetadataItem } from '../../../types'
import {
  formatTimestamp,
  hasData,
  METADATA_TABS,
  parseKeywords,
  stringifyJson,
  type FormState,
  type MetadataTabKey,
} from '../../../domain/metadata'

type MetadataInspectorPaneProps = {
  activeTabPayload: unknown
  activeMetaTab: MetadataTabKey
  currentItem: MetadataItem | null
  form: FormState | null
  selectedName: string
  selectedPath: string
  onTabChange: (tab: MetadataTabKey) => void
}

const FIELD_LABELS: Record<string, string> = {
  d2i_profile: 'D2I Profile',
  role_aliases: '角色别名',
  original_role_name: '原角色名',
  name: '名称',
  title: '标题',
  person: '人物',
  gender: '性别',
  position: '职务',
  city: '城市',
  source: '来源',
  image_url: '原图链接',
  keywords: '关键词',
  description: '描述',
  titi_asset_id: 'TITI Asset ID',
  titi_world_id: 'TITI World ID',
  asset_id: 'Asset ID',
  world_id: 'World ID',
  created_at: '创建时间',
  updated_at: '更新时间',
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function isEmptyValue(value: unknown): boolean {
  if (value === null || value === undefined) {
    return true
  }
  if (typeof value === 'string') {
    return value.trim().length === 0
  }
  if (Array.isArray(value)) {
    return value.length === 0
  }
  if (isPlainRecord(value)) {
    return Object.keys(value).length === 0
  }
  return false
}

function labelOf(key: string): string {
  return FIELD_LABELS[key] || key.replace(/[_-]+/g, ' ')
}

function primitiveText(value: unknown): string {
  if (value === null || value === undefined) {
    return '-'
  }
  if (typeof value === 'boolean') {
    return value ? '是' : '否'
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : '-'
  }
  return String(value || '').trim() || '-'
}

function tabLabelOf(key: MetadataTabKey): string {
  return METADATA_TABS.find((tab) => tab.key === key)?.label || key
}

function payloadEntries(payload: unknown): Array<[string, unknown]> {
  if (Array.isArray(payload)) {
    return payload.map((value, index) => [`#${index + 1}`, value])
  }
  if (isPlainRecord(payload)) {
    return Object.entries(payload)
  }
  return hasData(payload) ? [['value', payload]] : []
}

function MetadataValue({ value }: { value: unknown }) {
  if (isEmptyValue(value)) {
    return <span className="metadata-empty-value">-</span>
  }

  if (Array.isArray(value)) {
    const primitiveItems = value.filter((item) => !isPlainRecord(item) && !Array.isArray(item))
    const complexItems = value.filter((item) => isPlainRecord(item) || Array.isArray(item))

    return (
      <div className="metadata-array">
        {primitiveItems.length > 0 ? (
          <div className="metadata-chip-list">
            {primitiveItems.map((item, index) => (
              <span key={`${primitiveText(item)}-${index}`} className="metadata-chip">
                {primitiveText(item)}
              </span>
            ))}
          </div>
        ) : null}
        {complexItems.map((item, index) => (
          <div key={index} className="metadata-nested-card">
            <MetadataValue value={item} />
          </div>
        ))}
      </div>
    )
  }

  if (isPlainRecord(value)) {
    return (
      <dl className="metadata-nested-list">
        {Object.entries(value).map(([key, nestedValue]) => (
          <div key={key} className="metadata-nested-row">
            <dt>{labelOf(key)}</dt>
            <dd>
              <MetadataValue value={nestedValue} />
            </dd>
          </div>
        ))}
      </dl>
    )
  }

  return <span className="metadata-primitive">{primitiveText(value)}</span>
}

function StructuredMetadataView({
  activeMetaTab,
  payload,
  selectedPath,
}: {
  activeMetaTab: MetadataTabKey
  payload: unknown
  selectedPath: string
}) {
  const label = tabLabelOf(activeMetaTab)
  const entries = payloadEntries(payload).filter(([, value]) => !isEmptyValue(value))

  if (!hasData(payload) || entries.length === 0) {
    return (
      <div className="metadata-structured-panel">
        <div className="json-empty">
          {selectedPath ? `${label} 暂无可展示字段` : '选择图片后显示元数据'}
        </div>
      </div>
    )
  }

  return (
    <div className="metadata-structured-panel">
      <div className="metadata-structured-head">
        <div>
          <p className="profile-card-title">{label} 结构化视图</p>
          <span className="metadata-field-count">{entries.length} 个字段</span>
        </div>
      </div>

      <dl className="metadata-field-list">
        {entries.map(([key, value]) => (
          <div key={key} className="metadata-field-row">
            <dt>{labelOf(key)}</dt>
            <dd>
              <MetadataValue value={value} />
            </dd>
          </div>
        ))}
      </dl>

      <details className="raw-json-details">
        <summary>查看原始 JSON</summary>
        <pre>{stringifyJson(payload)}</pre>
      </details>
    </div>
  )
}

export function MetadataInspectorPane({
  activeTabPayload,
  activeMetaTab,
  currentItem,
  form,
  selectedName,
  selectedPath,
  onTabChange,
}: MetadataInspectorPaneProps) {
  return (
    <div className="meta-panel">
      <div className="meta-tabs" role="tablist" aria-label="原始元数据分组">
        {METADATA_TABS.map((tab) => (
          <button
            key={tab.key}
            type="button"
            role="tab"
            className={tab.key === activeMetaTab ? 'meta-tab active' : 'meta-tab'}
            aria-selected={tab.key === activeMetaTab}
            onClick={() => onTabChange(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeMetaTab === 'profile' ? (
        <div className="profile-grid">
          <div className="profile-card">
            <p className="profile-card-title">文件信息</p>
            <dl className="profile-list">
              <div>
                <dt>文件名</dt>
                <dd>{currentItem?.filename || selectedName || '-'}</dd>
              </div>
              <div>
                <dt>路径</dt>
                <dd>{currentItem?.filepath || selectedPath || '-'}</dd>
              </div>
              <div>
                <dt>状态</dt>
                <dd>{currentItem?.status || '未提供'}</dd>
              </div>
              <div>
                <dt>修改时间</dt>
                <dd>{formatTimestamp(currentItem?.modified_time)}</dd>
              </div>
            </dl>
          </div>

          <div className="profile-card">
            <p className="profile-card-title">人物档案</p>
            <dl className="profile-list">
              <div>
                <dt>标题</dt>
                <dd>{form?.title || '-'}</dd>
              </div>
              <div>
                <dt>人物</dt>
                <dd>{form?.person || '-'}</dd>
              </div>
              <div>
                <dt>性别 / 职务</dt>
                <dd>{[form?.gender, form?.position].filter(Boolean).join(' / ') || '-'}</dd>
              </div>
              <div>
                <dt>城市</dt>
                <dd>{form?.city || '-'}</dd>
              </div>
            </dl>
          </div>

          <div className="profile-card">
            <p className="profile-card-title">来源与索引</p>
            <dl className="profile-list">
              <div>
                <dt>来源</dt>
                <dd>{form?.source || '-'}</dd>
              </div>
              <div>
                <dt>原图链接</dt>
                <dd>{form?.image_url || '-'}</dd>
              </div>
              <div>
                <dt>Asset / World</dt>
                <dd>{[form?.titi_asset_id, form?.titi_world_id].filter(Boolean).join(' / ') || '-'}</dd>
              </div>
              <div>
                <dt>关键词</dt>
                <dd>{parseKeywords(form?.keywords_text || '').join(' / ') || '-'}</dd>
              </div>
            </dl>
          </div>
        </div>
      ) : (
        <StructuredMetadataView
          activeMetaTab={activeMetaTab}
          payload={activeTabPayload}
          selectedPath={selectedPath}
        />
      )}
    </div>
  )
}
