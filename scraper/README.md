# Public Scraper (Experimental)

This module adds a slow, resumable, review-first public profile scraper:

- crawl list -> detail pages with Scrapy
- `JOBDIR` resumable state
- low-speed defaults (single concurrency, delay, autothrottle)
- stop on 403/429 and write backoff state
- required-field validation with review queue
- image download with sha256 dedupe
- metadata queue output for later D2I import
- auto-write metadata into downloaded images (can disable with `--skip-metadata`)
- final images are exported into one folder with person-name filenames (`rules.named_images_dir`)

## 1) Prepare config

Copy `config.example.json` (or `config.template.generic.json`) to your own file and update:

- `start_urls`
- `allowed_domains`
- CSS/XPath selectors under `selectors`
- optional `selectors.list_fields` can extract list-card fields (merged with detail fields later)
- put profession-specific fields in `selectors.detail_fields` (saved to metadata `d2i_profile.extra_fields`)
- optional field display names can be set in `selectors.detail_field_labels` or `rules.detail_field_labels`
- optional `selectors.detail_full_text` can specify full-text selectors; if empty, crawler falls back to whole body text
- optional `rules.field_map` (or `selectors.field_map`) maps extracted fields to fixed metadata keys during parse stage
- optional unit-folder auto create: `rules.auto_unit_subdir` + `rules.unit_name` + `rules.output_subdir_pattern` + `rules.year_hint`
  - supported placeholders in `output_subdir_pattern`: `{unit}`, `{year}`, `{year_suffix}`, `{site_name}`, `{host}`
- if target uses `__jsl_clearance_s` challenge, enable `rules.jsl_clearance_enabled`
- optional fast-mode fallback: set `rules.auto_fallback_to_browser=true` to auto-switch to browser mode when request mode is blocked/failed

GUI mode (`app.py` -> "公共抓取(通用)") will auto-generate runtime config from
`config.template.generic.json` and only asks you for one start URL.
Template list in GUI is grouped by run status (`未完成` / `已完成`), tracked in `scraper/state/template_run_state.json` (runtime state file).

## 2) Run once

```powershell
cd d:\soft\gemini-business2api-workspace\d2ilite
.\.venv\Scripts\python .\scraper\run_public_scraper.py --config .\scraper\config.example.json
```

Skip metadata write if needed:

```powershell
.\.venv\Scripts\python .\scraper\run_public_scraper.py --config .\scraper\config.example.json --skip-metadata
```

Preconfigured template for `tiantonglaw.com/Team`:

```powershell
.\.venv\Scripts\python .\scraper\run_public_scraper.py --config .\scraper\config.tiantonglaw.team.json
```

## 3) Schedule nightly run

```powershell
.\.venv\Scripts\python .\scraper\run_scheduler.py --config .\scraper\config.example.json --time 02:30
```

## Output files

- `raw/list_records.jsonl`
- `raw/profiles.jsonl`
- `raw/review_queue.jsonl`
- `raw/metadata_queue.jsonl`
- `downloads/image_downloads.jsonl`
- `*.jpg` in `output_root` (default final output, one file per person name)
- `reports/crawl_report.json`
- `reports/image_download_report.json`
- `reports/reconcile_report.json`
- `reports/metadata_write_report.json`

All paths are created under `output_root` in config.
