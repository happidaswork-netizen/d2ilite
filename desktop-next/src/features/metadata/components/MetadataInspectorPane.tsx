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
        <div className="json-panel">
          {hasData(activeTabPayload) ? (
            <pre>{stringifyJson(activeTabPayload)}</pre>
          ) : (
            <div className="json-empty">
              {selectedPath
                ? `${METADATA_TABS.find((tab) => tab.key === activeMetaTab)?.label} 暂无可展示字段`
                : '选择图片后显示原始元数据'}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
