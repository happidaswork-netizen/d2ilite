# Desktop Bridge 协议（Phase 1）

脚本：`scripts/desktop_bridge_cli.py`

## 目标

为 `desktop-next`（以及未来 Tauri 壳）提供稳定的 Python 侧调用协议。

## 命令

## 1) ping

```powershell
python scripts/desktop_bridge_cli.py ping
```

返回示例：

```json
{"ok": true, "provider": "python-cli", "version": "phase1-bridge-v1"}
```

## 2) list

```powershell
python scripts/desktop_bridge_cli.py list --folder "Z:\\生成图片\\角色肖像\\警察\\原图\\公安部英烈_2021" --limit 200
```

返回：

```json
{"ok": true, "folder": "...", "count": 200, "items": ["a.jpg", "..."]}
```

## 3) read

```powershell
python scripts/desktop_bridge_cli.py read --path "Z:\\...\\龚云龙.jpg"
```

返回：

```json
{"ok": true, "item": {...ImageMetadataInfo序列化...}}
```

## 4) save

```powershell
python scripts/desktop_bridge_cli.py save --path "Z:\\...\\龚云龙.jpg" --payload-file ".\\payload.json"
```

`payload` 支持字段：

1. `title`
2. `person`
3. `gender`
4. `position`
5. `city`
6. `source`
7. `image_url`
8. `keywords`（字符串或数组）
9. `titi_asset_id`
10. `titi_world_id`
11. `description`
12. `d2i_profile`（对象）

返回：

```json
{"ok": true, "saved": true, "path": "..."}
```

失败时统一：

```json
{"ok": false, "error": "...", "detail": "..."}
```

## 备注

1. 当前是 CLI 协议，后续 Tauri 阶段可映射为 `invoke` 命令。
2. 前端先按该 JSON 结构实现桥接层，避免后续重复改 UI 逻辑。
3. 可用冒烟脚本验证协议：

```powershell
python scripts/bridge_cli_smoke.py
```
