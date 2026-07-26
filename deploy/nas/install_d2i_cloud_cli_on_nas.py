#!/usr/bin/env python3
"""Install thin HTTP d2i CLI into Hermes nas-main profile (assistant surface)."""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path

PROFILE = Path("/vol1/1001/hermes/profiles/nas-main")
BIN = PROFILE / "bin"
SKILL = PROFILE / "skills" / "d2i-cloud"
BACKUPS = Path("/vol1/1001/hermes/backups")
CODE = Path(os.environ.get("D2I_CLOUD_CODE_HOST", "/vol1/1001/d2i-cloud/current"))
API = os.environ.get("D2I_CLOUD_API", "http://127.0.0.1:8787")

CLI_SH = f"""#!/usr/bin/env bash
set -euo pipefail
export D2I_CLOUD_API="${{D2I_CLOUD_API:-{API}}}"
export D2I_CLOUD_CODE="${{D2I_CLOUD_CODE:-{CODE}}}"
fi
if command -v python3 >/dev/null 2>&1; then
  PY=python3
else
  PY=python
fi
# Prefer profile-local HTTP CLI (no heavy deps)
if [[ -f /data/profile/bin/d2i_cloud_http_cli.py ]]; then
  exec "$PY" /data/profile/bin/d2i_cloud_http_cli.py "$@"
fi
if [[ -f "$D2I_CLOUD_CODE/cloud/cli.py" ]]; then
  export PYTHONPATH="$D2I_CLOUD_CODE${{PYTHONPATH:+:$PYTHONPATH}}"
  exec "$PY" -m cloud.cli "$@"
fi
if [[ -f "$D2I_CLOUD_CODE/scripts/d2i_cloud_cli.py" ]]; then
  export PYTHONPATH="$D2I_CLOUD_CODE${{PYTHONPATH:+:$PYTHONPATH}}"
  exec "$PY" "$D2I_CLOUD_CODE/scripts/d2i_cloud_cli.py" "$@"
fi
echo "d2i-cloud CLI not found" >&2
exit 1
"""

SKILL_MD = f"""---
name: d2i-cloud
description: Manage D2I Cloud scrape queues via HTTP CLI (list/create/start/pause/logs). D2I Cloud is the main product; this skill is a Hermes helper only.
---

# D2I Cloud assistant

- Public Web (Tunnel + Zero Trust Access): `https://d2i.517411.xyz/`
- LAN Web: `http://192.168.5.36:8787/`
- Tunnel bridge: `http://192.168.5.36:18888/` -> `8787` (`d2i-proxy-18888`)
- API (assistant default): `{API}/api/v1`
- CLI: `/data/profile/bin/d2i` (host: `{BIN / "d2i"}`)

## Commands

```bash
d2i status
d2i templates list
d2i queues list
d2i queues create --template <id> --start-url <url> --speed-tier safe
d2i queues start <queue_id>
d2i queues pause <queue_id>
d2i queues show <queue_id>
d2i queues logs <queue_id>
```

## Rules

1. Default speed tier is `safe`; `turbo` requires explicit user confirm (`--allow-turbo`).
2. Do not mount Hermes secrets into D2I.
3. Do not write final image library on `/vol3`; Cloud tasks stay under `/runtime/d2i-cloud-tasks`.
4. Web is the primary console; Telegram/Hermes is assistant only.
5. API is open on LAN/host network (no app token).
6. Do not dual-start the same job on legacy `d2i-lite-worker` and D2I Cloud.
"""


def main() -> None:
    BACKUPS.mkdir(parents=True, exist_ok=True)
    BIN.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if SKILL.exists():
        backup = BACKUPS / f"d2i-cloud-skill-before-{stamp}"
        shutil.copytree(SKILL, backup)
        shutil.rmtree(SKILL)
        print(f"backup={backup}")
    SKILL.mkdir(parents=True, exist_ok=True)
    (SKILL / "SKILL.md").write_text(SKILL_MD, encoding="utf-8")

    # Self-contained HTTP CLI next to wrapper (works even if release path is RO/missing deps)
    http_cli_src = CODE / "cloud" / "cli.py"
    http_cli_dst = BIN / "d2i_cloud_http_cli.py"
    if http_cli_src.is_file():
        shutil.copy2(http_cli_src, http_cli_dst)
    else:
        # fallback: keep whatever is already there
        pass

    cli = BIN / "d2i"
    cli.write_text(CLI_SH, encoding="utf-8")
    cli.chmod(0o755)
    alias = BIN / "d2i-cloud"
    alias.write_text(CLI_SH, encoding="utf-8")
    alias.chmod(0o755)

    owner = PROFILE.stat()
    for path in [cli, alias, http_cli_dst, SKILL, SKILL / "SKILL.md"]:
        if not path.exists():
            continue
        try:
            os.chown(path, owner.st_uid, owner.st_gid)
        except PermissionError:
            pass
        except AttributeError:
            pass
    print(f"cli={cli}")
    print(f"http_cli={http_cli_dst if http_cli_dst.exists() else 'missing'}")
    print(f"skill={SKILL}")
    print(f"api={API}")
    

if __name__ == "__main__":
    main()
