---
name: d2i-cloud
description: Operate D2I Cloud queues, templates, result library, coverage graph, and finalize-to-portrait via HTTP API. Use when Hermes or another agent needs to create/start queues, inspect extraction quality, promote results into 角色肖像, or read people writeback status — without embedding new AI scrape logic into Cloud itself.
---

# D2I Cloud operator surface

## Product boundary

- **D2I Cloud** = control plane on NAS (Web + API + queue worker).
- **Hermes / AI** = operator that reads this skill and calls HTTP APIs.
- Do **not** invent private scrape pipelines. Prefer template-bound queues, then review items, then finalize.
- Final image root is only `角色肖像` (`/vol1/1001/角色肖像` host, `/runtime/portrait` in container). Never write `山东公开官员` or `__hdd_prebind`. Protect `选角/`.

## Template extract contract (read first when writing/fixing templates)

Canonical field contract: `docs/d2i_cloud_template_extract_contract.md`

**Must per person:** usable `name` (real 2–4 han person token, not nav/org/title-only) + `detail_url`.  
**Should:** avatar `image_url`, `gender`, `summary`/`full_content` (for bio + 现任 position).  
**Rules (not xpath luck):** `unit_name`, `admin.province`/`city`, output subdir / 级.  
**Final path:** `角色肖像/政府/{省}/{市}/{级}/{单位}/{性别}/{姓名}.ext`.  
**Fail the template** if list_item matches chrome or names look like `个人简历` / `要闻动态` / bare `市长`.

## Auth and base URL

- Default API: `http://127.0.0.1:8787` (Hermes host network) or `http://192.168.5.36:8787` (LAN).
- **App-layer Bearer is conditional, not absent**: `cloud/api.py` requires `Authorization: Bearer <D2I_WEB_TOKEN>` on all `/api/v1` routes **whenever env `D2I_WEB_TOKEN` is set**; only an unset token (local dev) opens the API. Check live state via `GET /api/v1/status` → `auth_enabled`. Product contract (`docs/D2I_Cloud产品契约_2026-07-25.md` §鉴权) requires the token to be **on** for any NAS/public exposure.
- Token sources when enabled: env `D2I_WEB_TOKEN`, file `/runtime/d2i-cloud-data/web_token.txt`, Web `?token=` bootstrap (stored in localStorage `d2i_cloud_token`).
- Public: `https://d2i.517411.xyz/` still sits behind **Cloudflare Access / Zero Trust** (team login). Unauthenticated browser hits get Access HTML (HTTP 200), not API JSON — finish Access sign-in once per browser, or use LAN. Passing Access does **not** waive the app-layer Bearer when `auth_enabled=true`.
- Prefer HTTP CLI on LAN/host: `d2i status|templates search|show|validate|import|outcome|queues …`

## Read-first workflow

1. `GET /api/v1/status` — `running`, `completed`, `promoted`, `desired_running` (stale desired should stay ~0).
2. Site discovery belongs to the calling model's own web-search/browser tools, not D2I. Coverage may guide the model's queries, but the model may also search broadly. Once it selects a candidate URL, call `GET /api/v1/templates/search?url=...`.
3. Never treat template-library search as public-web search.
4. `GET /api/v1/queues?limit=50` — **download/scrape** queues only (`q_*`); live runtime + `runtime.promoted` / `can_finalize`.
5. `GET /api/v1/queues/{id}` — detail; opening a completed queue may **auto-finalize once**.
6. `GET /api/v1/queues/{id}/items?limit=100` — check names, preview flags, garbage rows.
7. `GET /api/v1/library?limit=60` — cross-queue browse.
8. Coverage: `GET /api/v1/coverage/tree`; its scout proposal is only a direction hint, not internet search.
9. Vision jobs UI: open `/vision` (not scrape queue list). Page supports inventory → plan → enqueue + pump. API: `GET /api/v1/ai/vision/jobs` (`vj_*`).

## Write / control

| Action | Endpoint | Notes |
| --- | --- | --- |
| Search/show templates | `GET /api/v1/templates/search` · `GET /api/v1/templates/{id}` | Always search before making a new template |
| Validate/import template | `POST /api/v1/templates/validate` · `POST /api/v1/templates` | Imported templates persist under Cloud data, not the read-only release |
| Update/write outcome | `PATCH /api/v1/templates/{id}` · `POST /api/v1/templates/{id}/outcomes` | Record real counts and failure types; skip ceremonial test rounds |
| Create queue | `POST /api/v1/queues` | body: `template_id` or `template_path`, optional `start_url`, `speed_tier` (`safe` default), `start` |
| Start / pause / resume / retry / cancel | `POST /api/v1/queues/{id}/{action}` | |
| Finalize | `POST /api/v1/queues/{id}/finalize` | `{ "dry_run": false, "write_people": true }` → 角色肖像 + people.sqlite |
| Images audit | `GET /api/v1/queues/{id}/images/audit` | Original warehouse self-check: sha256 + path on disk + EXIF provenance (`exif_present` / `source_photo_taken_at`); CLI: `d2i queues images-audit` |
| Vision status | `GET /api/v1/ai/vision/status` | Grok runtime + `jobs` counts (queued/running/…) |
| Vision inventory | `GET /api/v1/ai/vision/inventory` | people with photo but empty `visual_gender`; path resolve |
| Vision plan | `GET /api/v1/ai/vision/plan` | split unvisioned into multi batches by 省/市 |
| Vision enqueue | `POST /api/v1/ai/vision/enqueue` | create many **vision_jobs** (not scrape queues); optional `start` |
| Vision jobs | `GET /api/v1/ai/vision/jobs` · `.../jobs/{id}` | list/show; detail items include per-person `visual_*` / status (from job snapshot + people DB) |
| Vision pump | `POST /api/v1/ai/vision/pump` | drain queued vision jobs; `max_running=1` serial 排队; `background=true` for full backfill |
| Vision requeue failed | `POST /api/v1/ai/vision/requeue-failed` | `{ "job_id"?, "policy":"retryable\|must_recrawl\|all", "start" }` → **默认只入可恢复失败**；坏图扣下；CLI: `d2i vision requeue` |
| Vision recrawl inbox | `GET /api/v1/ai/vision/recrawl-inbox` | 建议重抓收件箱（auto-route 写入）；CLI: `d2i vision recrawl-inbox` |
| Vision auto-route | `POST /api/v1/ai/vision/jobs/{id}/auto-route` | 单批跑完分流：retry-auto + 收件箱；CLI: `d2i vision auto-route <id>` |
| Vision normalize status | `POST /api/v1/ai/vision/normalize-status` | repair legacy rows: all-fail→`failed`, partial→`completed_with_errors` |
| People get | `GET /api/v1/people/{person_id}` | includes derived `workflow` label |
| People mark | `POST /api/v1/people/mark` | `{action:no_photo\|hold\|unusable\|resume, person_id\|person_ids, reason, dry_run}` gates inventory |
| People marked list | `GET /api/v1/people/workflow/marked` | list no_photo / hold / unusable |
| Vision one/batch paths | `POST /api/v1/ai/vision` | `{ "path"|"paths", "names", "person_ids", "write_people", "force" }` |
| Vision scrape-queue stage 2 | `POST /api/v1/queues/{id}/ai/run` | `{ "steps": ["vision"] }` → classify + `vision_report.json` |
| Vision report | `GET /api/v1/queues/{id}/ai/vision/report` | 漏图/错图/冲突 buckets + counts |
| Recrawl plan | `GET /api/v1/queues/{id}/ai/vision/recrawl-plan` | problem names only; `?include_review=true` optional |
| Recrawl draft | `POST /api/v1/queues/{id}/ai/vision/recrawl` | plan only by default; `create_draft` needs `confirm=true`; **never starts** |
| CLI | `d2i vision …` + `d2i people get\|mark\|marked` | photo workflow marks gate inventory |

Auto-finalize: list may promote **at most one** eligible completed queue per call; single-queue GET promotes when completed and no successful `meta.last_promote`. Opt out with queue meta `auto_finalize: false` / `skip_auto_finalize`.

### Two-stage pipeline

1. **Download / finalize** — scrape → promote to `角色肖像` + people source facts (`gender` from page).
2. **Vision** — Grok on durable portraits → stage report (`must_recrawl` / `review` / `ok`).
3. **Selective re-crawl** — report drives AI/Hermes to re-queue **problem names only**. Default: report first for confirm; never full-site auto re-scrape.

### Vision rules (Grok 4.5)

- Model via `D2I_VISION_MODEL=grok-4.5`, base `D2I_VISION_API_BASE`, key file `D2I_VISION_API_KEY_FILE` (NAS: `d2i-grok.env` + `d2i_vision_api_key`). Independent D2I secret — do not mount full Hermes profile.
- Writes only `visual_*` / `person_count` (+ optional vision meta cols). **Never** overwrite source `gender`.
- Fail-open: API/model errors leave portraits and promote results intact.
- Default skip rows that already have a non-empty `visual_gender` unless `force=true`.
- Prefer **post-promote** on durable `角色肖像` paths, not download-time sync vision.
- Severity: `must_recrawl` = missing image / no person / multi person / classify fail **+ image_too_small / image_truncated**; `review` = gender conflict / uncertain; `ok` = pass or already classified; **retryable** = rate limit / timeout / 5xx / runtime blip.
- **Auto-route on job finish:** retryable → `retry-auto` vision jobs; must_recrawl/review → daily JSONL inbox under `d2i-cloud-data/vision_followup/recrawl_inbox/`. Tiny/truncated assets are also stamped **unusable** on people so inventory stops re-picking them. `retry-auto` sources never spawn more vision children (loop guard).
- **Do not** requeue vision for 20×20 placeholders or truncated files — fix/recrawl the image first, or mark **no_photo** / **hold** (`d2i people mark …`; UI buttons on evidence panel + inbox).
- **Inventory/plan/enqueue exclude** people with workflow gates: confirmed no-photo, hold, unusable (see `cloud/people_workflow.py`).
- Artifacts: `{output_root}/reports/vision_report.json`, `vision_recrawl_candidates.json`, queue `meta.last_vision`, job `result.followup`.
- Operator one-pager: `skills/d2i-cloud/AGENT_BRIEF.md` (same directory as this skill).

### Photo workflow marks

| Action | Effect | Typical columns |
| --- | --- | --- |
| `no_photo` | Terminal: source has no usable portrait; stop crawl+vision | `source_page_image_status=no_photo_on_source`, `image_status=no_photo`, `repair_status=abandoned_no_usable_photo`, clear path |
| `hold` | Temporary pause | `repair_status=hold` |
| `unusable` | Bad local asset; skip vision, allow recrawl | `source_image_status=source_too_small_or_corrupt`, `file_status=abandoned` |
| `resume` | Clear gates | clears the above markers |

```bash
d2i people mark no_photo --person-id <id> --reason "源站暂无图片"
d2i people mark hold --person-id <id> --hold-until 2026-08-01
d2i people marked --workflow no_photo
```

## Quality gate (before trusting promote)

Reject or re-template if item names look like navigation/orgs:

- `个人简历`, `市政府领导`, `要闻动态`, `政民互动`, `魅力狮城`, `政务公开`, truncated `沧州市人`, site chrome.

Good person rows: 2–4 Chinese characters (after space collapse), real detail URL under unit path, optional avatar under article `…/images/…`.

If quality is bad:

1. Fix a local candidate, run `d2i templates validate`, then `d2i templates import --overwrite`; do not edit the read-only NAS release.
2. Create a **new** queue with the fixed template (do not assume old profiles.jsonl self-heals).
3. `finalize` only after items look clean.
4. Portrait path form: `角色肖像/政府/{省}/{市}/{级}/{单位}/{性别}/{姓名}.ext`.
5. People D-layer: `unit_name`, `primary_image_path`, `source_page_image_status` (`has_photo`/`no_photo`), `image_assets`.

## Cangzhou reference

- Template id/file: `河北省沧州市人民政府_市政府领导` / `hebei_cangzhou_gov_leadership`
- List must be `//div[contains(@class,'jobPersons')]//a[…shtml…]` — not bare `c1165` / bare `市长` text matches.
- Detail name: `ArticleTitle` meta or avatar `p`; image: avatar only.
- Gold roster (6): 刘勇、戴树胜、尹卫江、王晓玲、王少杰、姜桂海 — see `scripts/t0_cangzhou_leadership_collect.py` for offline registry path.

## AI role (do / don't)

**Do**

- Choose templates, create/start queues, poll status, review items/library, dry-run then apply finalize.
- Report bad selectors with concrete sample names/URLs.
- Suggest template xpath patches; leave Cloud core unless asked.

**Don't**

- Embed LLM extraction into the scrape hot path as the primary name source.
- Write outside `角色肖像` final root.
- Mass-start turbo without operator `allow_turbo`.
- Treat `desired_state=running` alone as live process — trust `runtime.session_running` + `runtime.status`.

## Minimal curl examples

> 以下示例假设本地开发未设 `D2I_WEB_TOKEN`。在 NAS / 任何 `auth_enabled=true` 环境,每条 curl 都必须加 `-H "Authorization: Bearer $D2I_WEB_TOKEN"`,否则一律 401。

```bash
curl -sS -H "Authorization: Bearer $D2I_WEB_TOKEN" http://127.0.0.1:8787/api/v1/status
curl -sS http://127.0.0.1:8787/api/v1/templates
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{"template_id":"河北省沧州市人民政府_市政府领导","speed_tier":"safe","start":false}' \
  http://127.0.0.1:8787/api/v1/queues
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true,"write_people":true}' \
  http://127.0.0.1:8787/api/v1/queues/q_xxx/finalize
curl -sS http://127.0.0.1:8787/api/v1/ai/vision/status
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{"steps":["vision"],"force":true,"write_people":true}' \
  http://127.0.0.1:8787/api/v1/queues/q_xxx/ai/run
curl -sS \
  http://127.0.0.1:8787/api/v1/queues/q_xxx/ai/vision/report
curl -sS \
  http://127.0.0.1:8787/api/v1/queues/q_xxx/ai/vision/recrawl-plan
# draft only after human/Hermes confirm — never auto-start
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{"create_draft":true,"confirm":true,"include_review":false}' \
  http://127.0.0.1:8787/api/v1/queues/q_xxx/ai/vision/recrawl
```
