import type { RoleAliasFormItem } from '../../../domain/metadata'

type RoleMetadataEditorProps = {
  originalRoleName: string
  roleAliases: RoleAliasFormItem[]
  onOriginalRoleNameChange: (value: string) => void
  onAddRoleAlias: () => void
  onRemoveRoleAlias: (id: string) => void
  onRoleAliasChange: (id: string, key: 'name' | 'note' | 'enabled', value: string | boolean) => void
}

export function RoleMetadataEditor({
  originalRoleName,
  roleAliases,
  onOriginalRoleNameChange,
  onAddRoleAlias,
  onRemoveRoleAlias,
  onRoleAliasChange,
}: RoleMetadataEditorProps) {
  return (
    <div className="role-panel">
      <div className="role-head">
        <div>
          <p className="role-title">角色元数据</p>
          <span className="role-subtitle">原角色名与扮演角色名统一走结构化编辑</span>
        </div>
        <button type="button" className="role-add-btn" onClick={onAddRoleAlias}>
          新增扮演角色
        </button>
      </div>

      <label className="input-stack">
        <span>图片原角色名</span>
        <input
          value={originalRoleName}
          onChange={(event) => onOriginalRoleNameChange(event.target.value)}
          placeholder="对应 d2i_profile.name"
        />
      </label>

      <div className="role-list">
        {roleAliases.length === 0 ? (
          <div className="role-empty">当前没有扮演角色记录。可按需新增，保存时会清空旧的 role_aliases。</div>
        ) : (
          roleAliases.map((entry) => (
            <div key={entry.id} className="role-row">
              <div className="role-row-fields">
                <input
                  value={entry.name}
                  onChange={(event) => onRoleAliasChange(entry.id, 'name', event.target.value)}
                  placeholder="扮演角色名"
                />
                <input
                  value={entry.note}
                  onChange={(event) => onRoleAliasChange(entry.id, 'note', event.target.value)}
                  placeholder="备注（可空）"
                />
              </div>
              <div className="role-row-controls">
                <label className="role-toggle">
                  <input
                    type="checkbox"
                    checked={entry.enabled}
                    onChange={(event) => onRoleAliasChange(entry.id, 'enabled', event.target.checked)}
                  />
                  <span>启用</span>
                </label>
                <button type="button" className="role-remove-btn" onClick={() => onRemoveRoleAlias(entry.id)}>
                  移除
                </button>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
