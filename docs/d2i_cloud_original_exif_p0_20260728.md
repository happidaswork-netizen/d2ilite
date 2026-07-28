# D2I Cloud 原件仓 + 源 EXIF 审计（P0，2026-07-28）

> 对齐 Hermes 建议：配置/模板只能「尽量保原图」；可审计的拍摄时间与原件契约必须进 Cloud。  
> 本轮是**小步迭代**，不是推倒重来。

## 已有基础（本轮之前）

| 能力 | 状态 |
| --- | --- |
| `sha256` + `downloads/images/<sha[:2]>/<sha><ext>` | 经典下载路径已有 |
| `image_downloads.jsonl`：`sha256` / `saved_path` / `named_path` | 已有 |
| `extract_photo_taken_date_from_image` | 已有，但只在有值时写入 `photo_taken_at` |
| promote `place_image` hardlink/copy2 | 保字节，不重编码 |
| `images_only*` cleanup | 曾在 promote 后**删除**原件仓（与长期审计冲突） |

## 本轮交付

### 1. 共享源元数据模块

- `image_source_meta.py`（打进 NAS 包）
- `inspect_source_image` / `enrich_download_manifest_row`
- **永不编造**拍摄时间：无 EXIF → `exif_present=false`，`source_photo_taken_at=""`，`photo_taken_at_source=unknown`

### 2. 下载与 metadata 写入

- `image_downloads.jsonl` 行增加：`exif_present`、`source_photo_taken_at`、`photo_taken_at_source`、可选 `source_exif`
- `d2i_profile` / audit snapshot 同步上述字段（`photo_taken_at` 仍只在有源日期时写入）

### 3. cleanup 原件保留

- `cleanup_intermediate_outputs`：**默认保留** `downloads/images` + `image_downloads.jsonl`
- 仅当 `rules.cleanup_delete_originals=true` 才允许擦除
- 仍要求先有成功 `promote_report`（阶段 C 已做），避免 promote 前清 raw

### 4. 自检面

- API：`GET /api/v1/queues/{id}/images/audit`
- CLI：`d2i queues images-audit <q_id>`
- 门禁：`missing_sha` / `missing_path` / `path_missing_on_disk` / `sha_mismatch` / **磁盘有 EXIF 日期但清单未记录**
- `complete_ingest=true` 仅当上述问题为 0

### 5. 测试

- `tests/test_image_source_meta_p0.py`（无 scrapy 也可跑核心用例）

## 验收口径（可直接贴需求）

1. 下载后 `downloads/images/...` 存在，且与 `image_downloads.jsonl.sha256` 一致（`images-audit` rehash）
2. 源 JPEG 含 `DateTimeOriginal` → `source_photo_taken_at` 非空且 `photo_taken_at_source=source_exif`
3. 源无 EXIF 日期 → 字段空 + `exif_present=false`（**不得**用抓取时刻冒充）
4. finalize/cleanup 后默认仍能反查原件路径或 sha 仓
5. 姓名图 TITI/XMP 写入不得以「编造 DateTimeOriginal」的方式补全；本轮不改写源拍摄时间

## 明确未做 / 下轮

| 项 | 说明 |
| --- | --- |
| direct_write 双写 sha 仓 | 仍可只落姓名图；建议新队列默认非 direct 或双写 |
| promote 写入 people 的 provenance 列 | API audit 可读清单；people 表尚未加列 |
| 手工补图同一管道 | 无 upload API；rebind 仍走已有 path |
| metadata 写坏 EXIF 的 piexif.remove 路径 | 工作副本风险仍在；原件仓保留是主防线 |
| 默认 audit 仍可能把 `photo_taken_at` 当必填 | 政府模板已多改为 `["gender"]`；全局默认可再收 |

## 命令

```bash
d2i queues images-audit q_xxx
# 或
curl -sS "$D2I_CLOUD_API/api/v1/queues/q_xxx/images/audit"
```
