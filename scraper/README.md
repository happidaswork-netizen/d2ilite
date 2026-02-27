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

Copy `config.example.json` to your own file and update:

- `start_urls`
- `allowed_domains`
- CSS/XPath selectors under `selectors`

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
- `downloads/named/*.jpg` (default final folder, one file per person name)
- `reports/crawl_report.json`
- `reports/image_download_report.json`
- `reports/reconcile_report.json`
- `reports/metadata_write_report.json`

All paths are created under `output_root` in config.
