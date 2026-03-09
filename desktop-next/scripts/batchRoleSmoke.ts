import assert from 'node:assert/strict'

import {
  applyBatchRoleOperation,
  createRoleAliasFormItem,
  type FormState,
} from '../src/features/metadata/model.ts'

function createBaseForm(): FormState {
  return {
    title: 'title',
    person: 'person',
    gender: '女',
    position: 'position',
    city: 'city',
    source: 'source',
    image_url: 'image_url',
    keywords_text: 'a, b',
    titi_asset_id: 'asset',
    titi_world_id: 'world',
    description: 'description',
    original_role_name: '旧原角色',
    role_aliases: [
      createRoleAliasFormItem({ name: '旧扮演', note: 'existing' }),
      createRoleAliasFormItem({ name: '旧扮演', note: 'duplicated' }),
    ],
  }
}

function roleNames(form: FormState): string[] {
  return form.role_aliases.map((entry) => entry.name)
}

const appended = applyBatchRoleOperation(createBaseForm(), {
  originalRoleMode: 'set',
  originalRoleName: '新原角色',
  aliasMode: 'append',
  aliasText: '旧扮演, 新扮演A, 新扮演B, 新扮演A',
})
assert.equal(appended.original_role_name, '新原角色')
assert.deepEqual(roleNames(appended), ['旧扮演', '新扮演A', '新扮演B'])

const replaced = applyBatchRoleOperation(createBaseForm(), {
  originalRoleMode: 'ignore',
  originalRoleName: '',
  aliasMode: 'replace',
  aliasText: '替换角色A, 替换角色B, 替换角色A',
})
assert.equal(replaced.original_role_name, '旧原角色')
assert.deepEqual(roleNames(replaced), ['替换角色A', '替换角色B'])

const cleared = applyBatchRoleOperation(createBaseForm(), {
  originalRoleMode: 'clear',
  originalRoleName: '',
  aliasMode: 'clear',
  aliasText: '',
})
assert.equal(cleared.original_role_name, '')
assert.deepEqual(roleNames(cleared), [])

console.log('[OK] batch role smoke passed')
