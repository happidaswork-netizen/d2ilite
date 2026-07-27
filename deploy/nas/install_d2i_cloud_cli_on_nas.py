#!/usr/bin/env python3
"""Install thin HTTP d2i CLI + full d2i-cloud skill into Hermes nas-main profile."""

from __future__ import annotations

import os
import shutil
import stat
from datetime import datetime
from pathlib import Path

PROFILE = Path("/vol1/1001/hermes/profiles/nas-main")
BIN = PROFILE / "bin"
SKILL = PROFILE / "skills" / "d2i-cloud"
BACKUPS = Path("/vol1/1001/hermes/backups")
CODE = Path(os.environ.get("D2I_CLOUD_CODE_HOST", "/vol1/1001/d2i-cloud/current"))
API = os.environ.get("D2I_CLOUD_API", "http://127.0.0.1:8787")

# Optional: remove only clearly broken / superseded bits (never touch TITI skills).
CLEAN_BROKEN_SYMLINKS = str(os.environ.get("D2I_HERMES_CLEAN_BROKEN_LINKS", "1")).lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# d2i-image-worker is the OLD "Hermes owns vision" skill — supersede by default after Cloud took over.
ARCHIVE_LEGACY_D2I_IMAGE_WORKER = str(
    os.environ.get("D2I_HERMES_ARCHIVE_IMAGE_WORKER", "1")
).lower() in {"1", "true", "yes", "on"}

CLI_SH = f"""#!/usr/bin/env bash
set -euo pipefail
export D2I_CLOUD_API="${{D2I_CLOUD_API:-{API}}}"
export D2I_CLOUD_CODE="${{D2I_CLOUD_CODE:-{CODE}}}"
# App-layer Bearer: auto-read token when not provided (auth_enabled deployments)
if [[ -z "${{D2I_WEB_TOKEN:-}}" ]]; then
  for tf in "${{D2I_WEB_TOKEN_FILE:-}}" /runtime/d2i-cloud-data/web_token.txt /vol4/1001/hermes-runtime/d2i-cloud-data/web_token.txt; do
    if [[ -n "$tf" && -f "$tf" ]]; then
      export D2I_WEB_TOKEN="$(head -n1 "$tf" | tr -d '[:space:]')"
      break
    fi
  done
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


def _chown_like_profile(path: Path) -> None:
    try:
        owner = PROFILE.stat()
        os.chown(path, owner.st_uid, owner.st_gid)
        if path.is_dir():
            for child in path.rglob("*"):
                try:
                    os.chown(child, owner.st_uid, owner.st_gid)
                except OSError:
                    pass
    except (PermissionError, AttributeError, FileNotFoundError):
        pass


def _copy_skill_from_release() -> list[str]:
    """Copy full skill tree from current release; fall back to minimal stub only if missing."""
    notes: list[str] = []
    src_dir = CODE / "cloud" / "skills" / "d2i-cloud"
    SKILL.mkdir(parents=True, exist_ok=True)
    if src_dir.is_dir() and (src_dir / "SKILL.md").is_file():
        for item in src_dir.iterdir():
            dest = SKILL / item.name
            if item.is_file():
                shutil.copy2(item, dest)
                notes.append(f"copied {item.name} ({item.stat().st_size}B)")
            elif item.is_dir():
                if dest.exists():
                    shutil.rmtree(dest)
                shutil.copytree(item, dest)
                notes.append(f"copied dir {item.name}/")
    else:
        # Last-resort stub so profile is never left without a skill.
        stub = """---
name: d2i-cloud
description: Manage D2I Cloud via d2i CLI. Full skill missing from release — reinstall after fixing current package.
---

# D2I Cloud (stub)

Run `d2i status` then see `/vol1/1001/d2i-cloud/current/cloud/skills/d2i-cloud/SKILL.md`.
"""
        (SKILL / "SKILL.md").write_text(stub, encoding="utf-8")
        notes.append("WARN: release skill missing; wrote stub only")
    return notes


def _clean_profile() -> list[str]:
    """Safe cleanup only. Ambiguous skills are listed, not deleted."""
    actions: list[str] = []
    skills_root = PROFILE / "skills"
    if not skills_root.is_dir():
        return actions

    if CLEAN_BROKEN_SYMLINKS:
        for child in list(skills_root.iterdir()):
            if not child.is_symlink():
                continue
            try:
                target = child.resolve(strict=False)
                # broken if the link target does not exist
                if not child.exists():
                    child.unlink()
                    actions.append(f"removed broken symlink skills/{child.name}")
            except OSError as exc:
                actions.append(f"skip symlink {child.name}: {exc}")

    legacy = skills_root / "d2i-image-worker"
    if ARCHIVE_LEGACY_D2I_IMAGE_WORKER and legacy.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = BACKUPS / f"d2i-image-worker-archived-{stamp}"
        BACKUPS.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            shutil.rmtree(dest)
        shutil.move(str(legacy), str(dest))
        # Leave a one-file pointer so Hermes does not hunt a missing skill.
        pointer = skills_root / "d2i-image-worker"
        pointer.mkdir(parents=True, exist_ok=True)
        (pointer / "SKILL.md").write_text(
            """---
name: d2i-image-worker
description: DEPRECATED. Vision is owned by D2I Cloud (d2i-cloud skill + d2i vision CLI). This stub replaces the old Hermes-main-control worker skill.
---

# Deprecated

Use **d2i-cloud** / `d2i vision …` instead.

Old skill tree archived under hermes backups (`d2i-image-worker-archived-*`).
Do not invent a parallel vision pipeline.
""",
            encoding="utf-8",
        )
        _chown_like_profile(pointer)
        actions.append(f"archived legacy d2i-image-worker -> {dest}")

    # Report keep-list for operator / Hermes (do not delete).
    keep = [
        "d2i-cloud",
        "d2i-lite-template-builder",
        "titi-archive-doctor",
        "titi-cli-operator",
    ]
    actions.append("keep_skills=" + ",".join(keep))
    return actions


def main() -> None:
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False, encoding="utf-8") as fh:
        fh.write(CLI_SH)
        tmp_path = fh.name
    try:
        check = subprocess.run(["bash", "-n", tmp_path], capture_output=True, text=True)
        if check.returncode != 0:
            raise SystemExit(f"CLI_SH failed bash -n: {check.stderr.strip()}")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    BACKUPS.mkdir(parents=True, exist_ok=True)
    BIN.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if SKILL.exists():
        backup = BACKUPS / f"d2i-cloud-skill-before-{stamp}"
        if backup.exists():
            shutil.rmtree(backup)
        shutil.copytree(SKILL, backup)
        shutil.rmtree(SKILL)
        print(f"backup={backup}")

    skill_notes = _copy_skill_from_release()
    for line in skill_notes:
        print("skill:", line)

    http_cli_src = CODE / "cloud" / "cli.py"
    http_cli_dst = BIN / "d2i_cloud_http_cli.py"
    if http_cli_src.is_file():
        shutil.copy2(http_cli_src, http_cli_dst)
        print(f"http_cli_src={http_cli_src} size={http_cli_src.stat().st_size}")
    else:
        print("http_cli_src=MISSING")

    for name in ("d2i", "d2i-cloud"):
        cli = BIN / name
        cli.write_text(CLI_SH, encoding="utf-8")
        cli.chmod(cli.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    clean_notes = _clean_profile()
    for line in clean_notes:
        print("clean:", line)

    for path in [BIN / "d2i", BIN / "d2i-cloud", http_cli_dst, SKILL]:
        if path.exists():
            _chown_like_profile(path)

    print(f"cli={BIN / 'd2i'}")
    print(f"http_cli={http_cli_dst if http_cli_dst.exists() else 'missing'}")
    print(f"skill={SKILL}")
    print(f"skill_md_bytes={(SKILL / 'SKILL.md').stat().st_size if (SKILL / 'SKILL.md').is_file() else 0}")
    print(f"brief_bytes={(SKILL / 'AGENT_BRIEF.md').stat().st_size if (SKILL / 'AGENT_BRIEF.md').is_file() else 0}")
    print(f"api={API}")
    print(f"code={CODE}")


if __name__ == "__main__":
    main()
