# D2I Lite Public Scraper Iteration Notes (2026-02-27)

This document captures the current implementation status for the public scraping workflow and related metadata changes, so future iterations can continue from a stable baseline.

## 1. Product Goal

Primary goal stays unchanged:

- Download images from public profile pages.
- Write D2I-compatible metadata into each final image.
- Keep the workflow visual, controllable, resumable, and non-black-box.

Public crawling capabilities are implementation details that serve image delivery and metadata quality.

## 2. Current Workflow (End-to-End)

### 2.1 High-level stages

1. Crawl list pages and discover detail pages.
2. Parse detail pages and extract:
   - person name
   - image URL
   - summary / full text
   - structured fields (`detail_fields`)
3. Download images (with dedupe by URL and SHA256).
4. Write metadata into final image files.
5. Produce `crawl_record.json` and optional cleanup.

### 2.2 Two download modes

- `requests_jsl` (fast mode)
  - List/detail crawl via Scrapy.
  - Image download via HTTP requests (optional JSL clearance).
  - If enabled, can auto-fallback to browser mode when fast mode fails.

- `browser` (slow/stable mode)
  - List/detail/image are all fetched via browser pipeline.
  - Optional inline image download during crawl (`download_images_during_crawl`).
  - Better for sensitive sites and anti-bot environments.

### 2.3 Fallback strategy (new)

When `rules.auto_fallback_to_browser=true` and current mode is fast:

- If crawl stage throws exception -> switch to browser mode and retry crawl stage.
- If crawl stage enters backoff -> clear backoff once and retry crawl in browser mode.
- If image stage throws exception -> switch to browser mode and retry image stage.
- If image stage enters backoff -> clear backoff once and retry image stage in browser mode.

Final run summary now records fallback metadata:

- `report.fallback.enabled`
- `report.fallback.used`
- `report.fallback.events[]`

## 3. GUI Behavior (Public Scraper Panel)

### 3.1 Setup dialog

The setup dialog now supports:

- URL or template-based startup.
- Unified randomized interval range (`min~max` seconds).
- timeout and consecutive-failure threshold.
- mode selection (`请求模式(快)` / `浏览器模式(慢稳)`).
- toggle: `快速模式失败时自动回退浏览器模式`.
- minimal output toggle (`仅保留最终图片 + 抓取记录文档`).

### 3.2 Runtime controls

- `开始抓取`
- `中止任务`
- `继续任务` (resume by existing `state/runtime_config.json`)
- live monitor with counts and row-level progress.

The row monitor labels are now explicit:

- `待处理条目（上）`
- `已完成条目（下）`

This avoids confusion with template completion states.

## 4. Template System (Selectable Presets)

### 4.1 Template directory and examples

- `scraper/templates/公安英烈墙_2021清明祭扫英烈.json`
- `scraper/templates/天同律师事务所_专业团队.json`
- `scraper/templates/海问律师事务所_专业人员.json`

Templates can include embedded `start_urls`, so selection can directly start tasks.

### 4.2 Template completion status (new)

A template run-state index is stored at:

- `scraper/state/template_run_state.json` (runtime state; ignored by git)

Template selection list is grouped as:

- `未完成 | ...` (top)
- `已完成 | ...` (bottom)

State transitions:

- task start/continue -> template marked `pending`
- task success (`exit code 0`) -> template marked `done`
- paused/failed/stopped -> stays `pending`

## 5. Output Contract

### 5.1 Final output

Target output for normal user flow:

- final images directly in selected output folder
- `crawl_record.json` in same folder

No mandatory nested `downloads/named` folder for final delivery path.

### 5.2 Intermediate data

Intermediate files can exist during run under `raw/`, `state/`, `downloads/`, `reports/`.

If `output_mode=images_only_with_record` and no unresolved failures/review items, cleanup removes intermediate artifacts automatically.

## 6. Metadata Rules and Schema

### 6.1 Gender normalization

- Unknown tokens (`unknown`, `unkonw`, `不详`, etc.) are normalized to empty value.
- Gender is not force-filled with placeholder strings.

### 6.2 Police ID support (new field)

`police_id` is now supported end-to-end:

- metadata parse/read
- metadata write
- adaptive UI display (shown when police context or field exists)
- can remain empty for later OCR/manual completion

### 6.3 Adaptive extra fields

Public scraper supports profession-specific fields:

- extracted via template `selectors.detail_fields`
- preserved under `d2i_profile.extra_fields`
- optional display labels via `detail_field_labels`

Full text remains preserved; structured extraction is additive.

## 7. Anti-Risk Controls

- randomized interval control (`interval_min_seconds` / `interval_max_seconds`)
- consecutive-failure heuristic (`suspect_block_consecutive_failures`)
- backoff file: `state/backoff_state.json`
- pause and resume supported
- browser mode option for sensitive targets
- fallback to browser mode (optional, now default true in template config)

## 8. Key Files Changed in This Iteration

- `app.py`
  - public scraper GUI setup/monitor/runtime integration
  - template state grouping and state persistence
  - adaptive metadata field display improvements
- `scraper/run_public_scraper.py`
  - browser crawl mode
  - inline metadata during download
  - backoff handling and fallback-to-browser logic
  - final report enhancements
- `scraper/public_profile_spider.py`
  - crawling and extraction improvements
- `metadata_manager.py`, `metadata_writer.py`
  - gender normalization cleanup
  - police_id parsing and write-through
- `downloader.py`, `text_parser.py`
  - download and parse support updates
- scraper configs/templates
  - generic template, MPS templates, Tiantong, Haiwen

## 9. Known Gaps / Next Iteration Backlog

1. Fast mode is still a staged pipeline by design (crawl then download), not strict single-item immediate finalization for every mode.
2. Browser driver/runtime stability still depends on local environment (Edge/Chrome/WebDriver availability).
3. Template generation is still semi-manual; a stronger auto-template assistant is needed for one-time domains.
4. Progress UI can be further improved with explicit per-item timestamps and richer retry history.
5. Add automated regression tests for:
   - template selector extraction
   - backoff/fallback transitions
   - metadata schema roundtrip (`gender`, `police_id`, `extra_fields`)

## 10. Recommended Handoff Checklist

Before each new iteration:

1. Open this document and confirm goals still match product priority (image delivery first).
2. Verify template mode and fallback toggles in GUI defaults.
3. Validate one fast-mode site and one browser-mode site.
4. Check `crawl_record.json` completeness and field quality.
5. Ensure cleanup policy matches need for resume/debug evidence.
