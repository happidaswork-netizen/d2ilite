import { useState } from 'react'

import type { MetadataAutofillInputMode, MetadataItem } from '../../../types'
import {
  formatFileSize,
  formatTimestamp,
  type FormState,
  type MetadataTabKey,
} from '../../../domain/metadata'
import { MetadataInspectorPane } from './MetadataInspectorPane'
import { RoleMetadataEditor } from './RoleMetadataEditor'

type MetadataEditorPaneProps = {
  activeMetaTab: MetadataTabKey
  activeTabPayload: unknown
  currentItem: MetadataItem | null
  form: FormState | null
  keywordCount: number
  metadataAiBusy: boolean
  selectedName: string
  selectedPath: string
  onAiAutofillCurrentMetadata: (inputMode: MetadataAutofillInputMode) => void
  onFieldChange: (key: keyof FormState, value: string) => void
  onGenerateBiography: () => void
  onRoleAliasAdd: () => void
  onRoleAliasRemove: (id: string) => void
  onRoleAliasChange: (id: string, key: 'name' | 'note' | 'enabled', value: string | boolean) => void
  onTabChange: (tab: MetadataTabKey) => void
}

export function MetadataEditorPane({
  activeMetaTab,
  activeTabPayload,
  currentItem,
  form,
  keywordCount,
  metadataAiBusy,
  selectedName,
  selectedPath,
  onAiAutofillCurrentMetadata,
  onFieldChange,
  onGenerateBiography,
  onRoleAliasAdd,
  onRoleAliasRemove,
  onRoleAliasChange,
  onTabChange,
}: MetadataEditorPaneProps) {
  const [aiInputMode, setAiInputMode] = useState<MetadataAutofillInputMode>('filename_metadata')

  return (
    <>
      <div className="section-head">
        <div>
          <p className="section-eyebrow">Metadata</p>
          <h2>结构化编辑</h2>
        </div>
        <span className="section-meta">{selectedPath ? '已绑定当前图片' : '等待选择条目'}</span>
      </div>

      <div className="metadata-actions">
        <label className="input-stack">
          <span>AI 补全输入</span>
          <select
            value={aiInputMode}
            onChange={(event) => setAiInputMode(event.target.value as MetadataAutofillInputMode)}
            disabled={metadataAiBusy || !selectedPath}
          >
            <option value="filename_metadata">文件名 + 元数据</option>
            <option value="filename">只读文件名</option>
            <option value="metadata">只读当前元数据</option>
          </select>
        </label>
        <button
          type="button"
          className="primary"
          onClick={() => onAiAutofillCurrentMetadata(aiInputMode)}
          disabled={metadataAiBusy || !selectedPath || !form}
        >
          {metadataAiBusy ? 'AI 补全中' : 'AI 自动补全'}
        </button>
        <button type="button" onClick={onGenerateBiography} disabled={metadataAiBusy || !selectedPath || !form}>
          AI 自动小传
        </button>
      </div>

      <div className="meta-summary-grid">
        <div className="meta-summary-card">
          <span className="meta-summary-label">状态</span>
          <strong>{currentItem?.status || '未提供'}</strong>
        </div>
        <div className="meta-summary-card">
          <span className="meta-summary-label">文件大小</span>
          <strong>{formatFileSize(currentItem?.filesize)}</strong>
        </div>
        <div className="meta-summary-card">
          <span className="meta-summary-label">修改时间</span>
          <strong>{formatTimestamp(currentItem?.modified_time)}</strong>
        </div>
        <div className="meta-summary-card">
          <span className="meta-summary-label">关键词</span>
          <strong>{keywordCount}</strong>
        </div>
      </div>

      <div className="editor-grid">
        <div className="field-grid">
          <label>标题</label>
          <input value={form?.title || ''} onChange={(event) => onFieldChange('title', event.target.value)} />
          <label>人物</label>
          <input value={form?.person || ''} onChange={(event) => onFieldChange('person', event.target.value)} />
          <label>性别</label>
          <input value={form?.gender || ''} onChange={(event) => onFieldChange('gender', event.target.value)} />
          <label>职务</label>
          <input value={form?.position || ''} onChange={(event) => onFieldChange('position', event.target.value)} />
          <label>城市</label>
          <input value={form?.city || ''} onChange={(event) => onFieldChange('city', event.target.value)} />
          <label>来源</label>
          <input value={form?.source || ''} onChange={(event) => onFieldChange('source', event.target.value)} />
          <label>原图链接</label>
          <input value={form?.image_url || ''} onChange={(event) => onFieldChange('image_url', event.target.value)} />
          <label>关键词</label>
          <input
            value={form?.keywords_text || ''}
            onChange={(event) => onFieldChange('keywords_text', event.target.value)}
          />
          <label>Asset ID</label>
          <input
            value={form?.titi_asset_id || ''}
            onChange={(event) => onFieldChange('titi_asset_id', event.target.value)}
          />
          <label>World ID</label>
          <input
            value={form?.titi_world_id || ''}
            onChange={(event) => onFieldChange('titi_world_id', event.target.value)}
          />
        </div>

        <div className="editor-side">
          <div className="bio">
            <div className="bio-head">
              <p className="bio-title">人物小传</p>
              <span>{keywordCount} 个关键词</span>
            </div>
            <textarea
              value={form?.description || ''}
              onChange={(event) => onFieldChange('description', event.target.value)}
              placeholder="这里显示或编辑描述/小传正文"
            />
          </div>

          <RoleMetadataEditor
            originalRoleName={form?.original_role_name || ''}
            roleAliases={form?.role_aliases || []}
            onOriginalRoleNameChange={(value) => onFieldChange('original_role_name', value)}
            onAddRoleAlias={onRoleAliasAdd}
            onRemoveRoleAlias={onRoleAliasRemove}
            onRoleAliasChange={onRoleAliasChange}
          />

          <MetadataInspectorPane
            activeTabPayload={activeTabPayload}
            activeMetaTab={activeMetaTab}
            currentItem={currentItem}
            form={form}
            selectedName={selectedName}
            selectedPath={selectedPath}
            onTabChange={onTabChange}
          />
        </div>
      </div>
    </>
  )
}
