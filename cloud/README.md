# D2I Cloud (MVP-1)

NAS-oriented queue API + minimal Web board on top of existing `d2ilite` public scraper.

## Run API / Web

```bash
cd D:\bugemini\d2ilite
pip install fastapi uvicorn pydantic httpx
python -m uvicorn cloud.api:app --host 127.0.0.1 --port 8787
```

Open:

- Web 队列台: http://127.0.0.1:8787/
- **覆盖图谱（C1/C2）:** http://127.0.0.1:8787/coverage
- Health: http://127.0.0.1:8787/health
- API: http://127.0.0.1:8787/api/v1/status
- Coverage tree: http://127.0.0.1:8787/api/v1/coverage/tree
- Coverage geo: http://127.0.0.1:8787/api/v1/coverage/geo-summary
- Coverage enqueue: `POST /api/v1/coverage/nodes/{id}/enqueue`（`confirm=true` 必填）

Optional:

- `D2I_WEB_TOKEN` — if set, require `Authorization: Bearer <token>` (Web 右上角 Token 按钮可存 localStorage)
- `D2I_CLOUD_DATA_ROOT` — jobs sqlite + cloud data (default `data/cloud`)
- `D2I_CLOUD_TASKS_ROOT` — public archive task roots (default `data/public_archive`)
- `D2I_PEOPLE_DB` — people.sqlite path (default NAS `/vol4/1001/hermes-runtime/db/people.sqlite` if present, else `data/people.sqlite`)
- `D2I_COVERAGE_SNAPSHOT` — offline rollup JSON (default `data/coverage/rollup_snapshot_20260726.json`)

## CLI

```bash
python scripts/d2i_cloud_cli.py status
python scripts/d2i_cloud_cli.py templates list
python scripts/d2i_cloud_cli.py queues list
python scripts/d2i_cloud_cli.py queues create --template <id> --start-url <url> --speed-tier safe
python scripts/d2i_cloud_cli.py queues start <queue_id>
python scripts/d2i_cloud_cli.py queues pause <queue_id>
python scripts/d2i_cloud_cli.py queues show <queue_id>
```

## Smoke

```bash
python scripts/cloud_api_smoke.py
```

## Scope now

- Job DB (`jobs.sqlite`)
- Queues create/list/show/start/pause/resume/retry/cancel/finalize
- Templates list
- Speed tier mapping (`safe` / `standard` / `turbo`)
- Web 队列运行中心（TITI 浅色工作台风格：侧栏 + KPI + 队列卡 + 详情）
- **覆盖图谱 C0–C2：** 树 + ECharts 省/市热力（本地 `vendor/echarts.min.js` + 离线 GeoJSON）+ 列表备援；节点详情；AI 侦察 stub；确认后 enqueue（intent / 队列 / ban 覆盖）
- Reuses desktop scraper control + `run_public_scraper.py`
- **模版抽取合同：** [`docs/d2i_cloud_template_extract_contract.md`](../docs/d2i_cloud_template_extract_contract.md)（Cloud finalize 前必须满足的 name/url/unit 形状）
- Hermes 操作 skill：`cloud/skills/d2i-cloud/SKILL.md`

Not yet: Hermes 真侦察（C3）、vision/AI routes、flock worker service。
