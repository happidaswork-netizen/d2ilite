import { useMemo, useState } from 'react'

import type { FormState } from '../../../domain/metadata'
import type { MetadataAutofillInputMode, NameBarOptions } from '../../../types'

type CurrentImageToolsPaneProps = {
  busy: boolean
  canSave: boolean
  form: FormState | null
  imageActionBusy: boolean
  selectedName: string
  selectedPath: string
  onAddNameBar: (options: NameBarOptions) => void
  onAiAutofillCurrentMetadata: (inputMode: MetadataAutofillInputMode) => void
  onGenerateBiography: () => void
  onPickFolderPath: (initialFolder?: string) => Promise<string>
  onRenameCurrentImage: (name: string) => void
  onSave: () => void
}

function stripExtension(fileName: string): string {
  return String(fileName || '')
    .replace(/\.[^.\\/]+$/, '')
    .replace(/[_-]+named$/i, '')
    .trim()
}

function firstText(...values: Array<string | undefined>): string {
  for (const value of values) {
    const text = String(value || '').trim()
    if (text) {
      return text
    }
  }
  return ''
}

function folderOfPath(value: string): string {
  return String(value || '').replace(/[\\/][^\\/]*$/, '')
}

type PathDraft = {
  path: string
  value: string
}

export function CurrentImageToolsPane({
  busy,
  canSave,
  form,
  imageActionBusy,
  selectedName,
  selectedPath,
  onAddNameBar,
  onAiAutofillCurrentMetadata,
  onGenerateBiography,
  onPickFolderPath,
  onRenameCurrentImage,
  onSave,
}: CurrentImageToolsPaneProps) {
  const selectedStem = useMemo(() => stripExtension(selectedName), [selectedName])
  const suggestedLabel = useMemo(
    () => firstText(form?.person, form?.original_role_name, form?.title, selectedStem),
    [form?.original_role_name, form?.person, form?.title, selectedStem],
  )

  const [nameBarLabelDraft, setNameBarLabelDraft] = useState<PathDraft>({ path: '', value: '' })
  const [nameBarOutputDir, setNameBarOutputDir] = useState<string>('')
  const [nameBarFormat, setNameBarFormat] = useState<string>('original')
  const [nameBarOutputName, setNameBarOutputName] = useState<'suffix' | 'label'>('suffix')
  const [openNameBarAfterGenerate, setOpenNameBarAfterGenerate] = useState<boolean>(true)
  const [revealNameBarAfterGenerate, setRevealNameBarAfterGenerate] = useState<boolean>(false)
  const [renameStemDraft, setRenameStemDraft] = useState<PathDraft>({ path: '', value: '' })

  const nameBarLabel = nameBarLabelDraft.path === selectedPath ? nameBarLabelDraft.value : suggestedLabel
  const renameStem = renameStemDraft.path === selectedPath ? renameStemDraft.value : suggestedLabel || selectedStem

  const actionDisabled = busy || imageActionBusy || !selectedPath
  const canAddNameBar = !actionDisabled && Boolean(nameBarLabel.trim())
  const canRename = !actionDisabled && Boolean(renameStem.trim())
  const metadataActionDisabled = busy || !selectedPath || !form

  return (
    <section className="current-tools" aria-label="当前图片操作">
      <div className="current-tools-head">
        <div>
          <p className="panel-title">当前图片操作</p>
          <span className="tool-subtitle">{selectedName || '未选择图片'}</span>
        </div>
        <span className={imageActionBusy ? 'chip chip-warn' : 'chip'}>{imageActionBusy ? '处理中' : '就绪'}</span>
      </div>

      <div className="tool-block">
        <div className="tool-block-head">
          <strong>加名字</strong>
        </div>
        <div className="tool-grid">
          <label className="input-stack tool-span-2">
            <span>名字</span>
            <input
              value={nameBarLabel}
              onChange={(event) => setNameBarLabelDraft({ path: selectedPath, value: event.target.value })}
              disabled={actionDisabled}
              placeholder="输入要加到图片上的名字"
            />
          </label>
          <label className="input-stack">
            <span>格式</span>
            <select
              value={nameBarFormat}
              onChange={(event) => setNameBarFormat(event.target.value)}
              disabled={actionDisabled}
            >
              <option value="original">同原图</option>
              <option value="png">PNG</option>
              <option value="jpg">JPG</option>
              <option value="webp">WEBP</option>
            </select>
          </label>
          <label className="input-stack">
            <span>命名</span>
            <select
              value={nameBarOutputName}
              onChange={(event) => setNameBarOutputName(event.target.value === 'label' ? 'label' : 'suffix')}
              disabled={actionDisabled}
            >
              <option value="suffix">原图名 + _named</option>
              <option value="label">直接用名字</option>
            </select>
          </label>
          <label className="input-stack tool-span-2">
            <span>输出目录（留空同目录）</span>
            <div className="path-pick-row">
              <input
                value={nameBarOutputDir}
                onChange={(event) => setNameBarOutputDir(event.target.value)}
                disabled={actionDisabled}
                placeholder="例如 D:\\图片\\已处理"
              />
              <button
                type="button"
                onClick={() => {
                  void onPickFolderPath(nameBarOutputDir || folderOfPath(selectedPath)).then((picked) => {
                    if (picked) {
                      setNameBarOutputDir(picked)
                    }
                  })
                }}
                disabled={actionDisabled}
              >
                选择
              </button>
            </div>
          </label>
        </div>
        <div className="tool-option-row">
          <label className="check-option">
            <input
              type="checkbox"
              checked={openNameBarAfterGenerate}
              onChange={(event) => setOpenNameBarAfterGenerate(event.target.checked)}
              disabled={actionDisabled}
            />
            <span>生成后预览新图</span>
          </label>
          <label className="check-option">
            <input
              type="checkbox"
              checked={revealNameBarAfterGenerate}
              onChange={(event) => setRevealNameBarAfterGenerate(event.target.checked)}
              disabled={actionDisabled}
            />
            <span>生成后打开所在文件夹</span>
          </label>
        </div>
        <div className="tool-actions">
          <button
            type="button"
            className="primary"
            onClick={() =>
              onAddNameBar({
                name: nameBarLabel,
                output_dir: nameBarOutputDir,
                output_format: nameBarFormat,
                output_name: nameBarOutputName,
                open_after_generate: openNameBarAfterGenerate,
                reveal_after_generate: revealNameBarAfterGenerate,
              })
            }
            disabled={!canAddNameBar}
          >
            生成加名字图
          </button>
        </div>
      </div>

      <div className="tool-block">
        <div className="tool-block-head">
          <strong>重命名当前图片</strong>
        </div>
        <div className="tool-grid">
          <label className="input-stack tool-span-2">
            <span>新文件名</span>
            <input
              value={renameStem}
              onChange={(event) => setRenameStemDraft({ path: selectedPath, value: event.target.value })}
              disabled={actionDisabled}
              placeholder="输入新文件名，不需要扩展名"
            />
          </label>
        </div>
        <div className="tool-actions">
          <button type="button" onClick={() => onRenameCurrentImage(renameStem)} disabled={!canRename}>
            重命名
          </button>
        </div>
      </div>

      <div className="quick-action-grid">
        <button type="button" onClick={onSave} disabled={busy || !canSave}>
          保存元数据
        </button>
        <button
          type="button"
          onClick={() => onAiAutofillCurrentMetadata('filename')}
          disabled={metadataActionDisabled}
        >
          AI 从文件名补全元数据
        </button>
        <button type="button" onClick={onGenerateBiography} disabled={metadataActionDisabled}>
          AI 生成小传
        </button>
      </div>
    </section>
  )
}
