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
- monitor progress format now emphasizes reconciliation:
  - `总数 / 已发现 / 已完成`
  - dedicated progress bar is driven by `已发现 / 总数`
  - detail/image/metadata counters remain as supporting dimensions

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
- incremental download-index checkpointing during run to reduce duplicate downloads after forced stop/resume

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

## 11. Post-Baseline Hotfixes (after initial doc draft)

The following fixes were added after the first baseline draft and are part of current behavior:

1. Log-open freeze mitigation
   - `打开日志` now launches a separate process (`notepad.exe`) first, with directory fallback, to avoid blocking Tk main loop.

2. Window z-order behavior for preview
   - Public scraper panel no longer forces foreground dominance.
   - Clicking/opening a progress-row image explicitly brings main preview window to front and pushes scraper panel behind it.

3. Progress semantics for reconciliation
   - Main progress display now uses `总数 / 已发现 / 已完成`.
   - Progress bar follows discovery progress (`已发现 / 总数`) for completeness checks.

4. Resume duplicate-download reduction
   - Image URL/SHA indexes are checkpointed incrementally during download stage (not only at stage end).
   - This lowers repeated requests when tasks are interrupted and resumed.

5. Progress-row click freeze mitigation
   - Added selection sync guard + queued open (`after_idle`) + open re-entry lock.
   - Goal: prevent event storms when clicking names in pending/done tables.

6. Python 3.13+ compatibility fix (`imghdr` removal)
   - `downloader.py` no longer depends on stdlib `imghdr` (removed in newer Python versions).
   - Scraper browser mode can now start normally on Python versions without `imghdr`.

7. Download payload validation hardening
   - Added strict payload checks before writing files:
     - `content-type` prefix (`image/*`)
     - common image magic bytes (JPEG/PNG/GIF/WEBP/BMP/TIFF)
   - Prevents HTML/challenge/error pages from being saved as `.jpg` and later shown as corrupted images.

8. Browser crawl image rendering toggle
   - Added runtime config key: `rules.disable_page_images_during_crawl` (default `true`).
   - Purpose: when `true`, browser crawl disables page image rendering to reduce bandwidth and anti-bot noise.
   - For visual debugging, set to `false` to render page images in-browser while crawling.

9. Browser mode inline metadata sync
   - Browser inline download now also writes metadata per item during crawl.
   - GUI `元数据` column can update during run, not only after whole task finishes.

10. Manual pause / per-row retry in GUI
   - `中止任务` adjusted to `暂停任务` semantics (state kept, can resume via `继续任务`).
   - Progress table now supports right-click per-row retry marking.
   - Retry operation removes that row's detail/image/metadata result records and URL cache mapping, then re-run via `继续任务`.
