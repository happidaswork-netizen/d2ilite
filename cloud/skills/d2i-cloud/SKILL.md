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
- Public: `https://d2i.517411.xyz/` (Cloudflare Access first, then app Bearer).
- Header: `Authorization: Bearer <D2I_WEB_TOKEN>`
- Token sources: env `D2I_WEB_TOKEN`, file `/runtime/d2i-cloud-data/web_token.txt`, or CLI `d2i` auto-read.
- Prefer HTTP CLI when available: `d2i status|templates list|queues …`

## Read-first workflow

1. `GET /api/v1/status` — `running`, `completed`, `promoted`, `desired_running` (stale desired should stay ~0).
2. `GET /api/v1/templates` — pick `id` / `path` (e.g. `河北省沧州市人民政府_市政府领导`).
3. `GET /api/v1/queues?limit=50` — live runtime + `runtime.promoted` / `can_finalize`.
4. `GET /api/v1/queues/{id}` — detail; opening a completed queue may **auto-finalize once**.
5. `GET /api/v1/queues/{id}/items?limit=100` — check names, preview flags, garbage rows.
6. `GET /api/v1/library?limit=60` — cross-queue browse.
7. Coverage: `GET /api/v1/coverage/tree` then node detail / enqueue only with a known good template.

## Write / control

| Action | Endpoint | Notes |
| --- | --- | --- |
| Create queue | `POST /api/v1/queues` | body: `template_id` or `template_path`, optional `start_url`, `speed_tier` (`safe` default), `start` |
| Start / pause / resume / retry / cancel | `POST /api/v1/queues/{id}/{action}` | |
| Finalize | `POST /api/v1/queues/{id}/finalize` | `{ "dry_run": false, "write_people": true }` → 角色肖像 + people.sqlite |
| CLI | `d2i queues finalize <id> [--dry-run] [--skip-people]` | |

Auto-finalize: list may promote **at most one** eligible completed queue per call; single-queue GET promotes when completed and no successful `meta.last_promote`. Opt out with queue meta `auto_finalize: false` / `skip_auto_finalize`.

## Quality gate (before trusting promote)

Reject or re-template if item names look like navigation/orgs:

- `个人简历`, `市政府领导`, `要闻动态`, `政民互动`, `魅力狮城`, `政务公开`, truncated `沧州市人`, site chrome.

Good person rows: 2–4 Chinese characters (after space collapse), real detail URL under unit path, optional avatar under article `…/images/…`.

If quality is bad:

1. Fix template under `scraper/templates/` (tighten `list_item`, drop broad `detail_image` site-wide `/images/` fallbacks).
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

```bash
curl -sS -H "Authorization: Bearer $D2I_WEB_TOKEN" http://127.0.0.1:8787/api/v1/status
curl -sS -H "Authorization: Bearer $D2I_WEB_TOKEN" http://127.0.0.1:8787/api/v1/templates
curl -sS -H "Authorization: Bearer $D2I_WEB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"template_id":"河北省沧州市人民政府_市政府领导","speed_tier":"safe","start":false}' \
  http://127.0.0.1:8787/api/v1/queues
curl -sS -H "Authorization: Bearer $D2I_WEB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true,"write_people":true}' \
  http://127.0.0.1:8787/api/v1/queues/q_xxx/finalize
```
