#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Package D2I Cloud release, push to feiniu NAS, switch current, restart, smoke.

Safety gates:
  1. local import smoke (create_app) BEFORE packaging — a missing symbol never ships
  2. health + critical-API gate AFTER switch — failure auto-rolls back to the
     previous release and the script exits non-zero
  3. DEPLOY_OK is printed only when every gate passed
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

import paramiko

ROOT = Path(os.environ.get("D2I_REPO_ROOT", r"D:\bugemini\d2ilite"))
DIST = ROOT / "deploy" / "nas" / "dist"
PACKAGE_SCRIPT = ROOT / "deploy" / "nas" / "package_d2i_cloud_release.py"
CRED = Path(
    os.environ.get("D2I_NAS_CRED_FILE")
    or (
        r"C:\Users\rpy\OneDrive\个人知识库\AI备份\02_服务器部署与网络配置_重要勿删"
        r"\20_SSH密钥库_高敏感\_连接命令与密码记录\feiniu_nas_登录凭据.md"
    )
)
HOST = os.environ.get("D2I_NAS_HOST", "192.168.5.36")
USER = os.environ.get("D2I_NAS_USER", "happidas")
REMOTE_BASE = "/vol1/1001/d2i-cloud"
REMOTE_RELEASES = f"{REMOTE_BASE}/releases"
REMOTE_CURRENT = f"{REMOTE_BASE}/current"
REMOTE_COMPOSE = f"{REMOTE_BASE}/docker-compose.d2i-cloud.yml"


def load_password() -> str:
    text = CRED.read_text(encoding="utf-8")
    m = re.search(r"NAS 登录密码[：:]\s*[`\"']?([^\s`\"']+)", text)
    if not m:
        raise SystemExit("password line not found in credentials note")
    return m.group(1).strip().strip("`\"'")


def connect() -> paramiko.SSHClient:
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(
        HOST,
        username=USER,
        password=load_password(),
        timeout=25,
        allow_agent=False,
        look_for_keys=False,
        banner_timeout=40,
    )
    return c


def run(c: paramiko.SSHClient, cmd: str, timeout: int = 300) -> tuple[int, str, str]:
    stdin, stdout, stderr = c.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    code = stdout.channel.recv_exit_status()
    return code, out, err


def build_package() -> Path:
    import subprocess

    before = {p.resolve() for p in DIST.glob("d2i-cloud-*.tar.gz")}
    proc = subprocess.run(
        [sys.executable, str(PACKAGE_SCRIPT)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if (proc.stdout or "").strip():
        print(proc.stdout.strip())
    if proc.returncode != 0:
        print(proc.stderr or "")
        raise SystemExit(f"package failed rc={proc.returncode}")
    after = sorted(DIST.glob("d2i-cloud-*.tar.gz"), key=lambda p: p.stat().st_mtime)
    if not after:
        raise SystemExit("package produced no tar.gz")
    newest = after[-1]
    print("package", newest, "size", newest.stat().st_size, "new", newest.resolve() not in before)
    return newest


def http_json(url: str, token: str = "", timeout: int = 30) -> tuple[int, object]:
    headers = {}
    tok = str(token or "").strip()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = raw
        return e.code, body
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def load_web_token() -> str:
    """Local cache first, then credentials-adjacent env files — never print the value."""
    candidates = [
        DIST / ".nas_web_token",
        Path(os.environ.get("D2I_WEB_TOKEN_FILE", "") or ""),
        Path(r"C:\Users\rpy\OneDrive\个人知识库\AI备份\02_服务器部署与网络配置_重要勿删")
        / "30_服务部署记录"
        / "feiniu-nas"
        / ".d2i_web_token",
    ]
    env_tok = str(os.environ.get("D2I_WEB_TOKEN", "") or "").strip()
    if env_tok:
        return env_tok
    for path in candidates:
        if not path or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            continue
        # allow either raw token or KEY=value
        if "D2I_WEB_TOKEN=" in text:
            for line in text.splitlines():
                s = line.strip()
                if s.startswith("D2I_WEB_TOKEN="):
                    return s.split("=", 1)[1].strip().strip("`\"'")
        if text:
            return text.splitlines()[0].strip()
    return ""


def preflight_import_smoke() -> None:
    """Abort before packaging if cloud.api cannot even build the app object."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from cloud.api import create_app; app = create_app(); "
            "routes = len(app.routes); assert routes > 20, routes; "
            "print('import_smoke_ok routes', routes)",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        check=False,
    )
    if (proc.stdout or "").strip():
        print(proc.stdout.strip())
    if proc.returncode != 0:
        print(proc.stderr or "")
        raise SystemExit(f"PREFLIGHT_FAILED import smoke rc={proc.returncode} — nothing was packaged or shipped")


# Recreate the container and wait for /health; shared by deploy and rollback.
_RESTART_CMD = f"""
set -euo pipefail
cd '{REMOTE_BASE}'
if printf '%s\\n' "$NAS_SUDO_PASS" | sudo -S -p '' docker compose version >/dev/null 2>&1; then
  printf '%s\\n' "$NAS_SUDO_PASS" | sudo -S -p '' docker compose -f docker-compose.d2i-cloud.yml up -d --force-recreate
elif docker compose version >/dev/null 2>&1; then
  docker compose -f docker-compose.d2i-cloud.yml up -d --force-recreate
else
  printf '%s\\n' "$NAS_SUDO_PASS" | sudo -S -p '' docker restart d2i-cloud || true
fi
sleep 3
for i in $(seq 1 20); do
  if curl -fsS http://127.0.0.1:8787/api/v1/health >/tmp/d2i_health.json 2>/dev/null; then
    cat /tmp/d2i_health.json; echo
    exit 0
  fi
  sleep 2
done
echo 'health wait failed'
printf '%s\\n' "$NAS_SUDO_PASS" | sudo -S -p '' docker logs --tail 40 d2i-cloud || true
exit 4
"""


def restart_and_wait(c: paramiko.SSHClient) -> int:
    full = f"export NAS_SUDO_PASS={json.dumps(load_password())}; {_RESTART_CMD}"
    code, out, err = run(c, full, timeout=240)
    print("restart_out", out.strip()[-1200:])
    if err.strip():
        print("restart_err", err[:600])
    return code


def rollback(c: paramiko.SSHClient, prev_release: str, stamp: str) -> bool:
    """Point current back at prev_release and restart. True only if health recovers."""
    prev = str(prev_release or "").strip()
    if not prev or "/releases/" not in prev:
        print("ROLLBACK_SKIPPED no previous release recorded")
        return False
    print("rolling back to", prev)
    code, out, err = run(c, f"set -e; test -d '{prev}'; ln -sfn '{prev}' '{REMOTE_CURRENT}'; ls -la '{REMOTE_CURRENT}'")
    print(out.strip())
    if code != 0:
        print("ROLLBACK_FAILED symlink", err[:300])
        return False
    if restart_and_wait(c) != 0:
        print("ROLLBACK_FAILED health did not recover on", prev)
        return False
    print("ROLLBACK_OK now serving", prev, "(bad release left at releases/", stamp, ")")
    return True


def main() -> int:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("preflight import smoke")
    preflight_import_smoke()
    print("building release", stamp)
    tar_path = build_package()
    remote_tar = f"/tmp/d2i-cloud-{stamp}.tar.gz"
    remote_release = f"{REMOTE_RELEASES}/{stamp}"

    print("connecting", HOST)
    c = connect()
    try:
        code, out, err = run(c, f"hostname; ls -ld '{REMOTE_BASE}' '{REMOTE_CURRENT}' || true")
        print(out.strip())
        if err.strip():
            print("precheck_err", err.strip()[:400])
        if code != 0:
            print("precheck failed", code)
            print("DEPLOY_FAILED", stamp)
            return 2

        # Remember what we would roll back to before touching anything.
        _, prev_out, _ = run(c, f"readlink -f '{REMOTE_CURRENT}' || true")
        prev_release = prev_out.strip().splitlines()[-1] if prev_out.strip() else ""
        print("previous release:", prev_release or "(none)")

        run(c, f"mkdir -p '{REMOTE_RELEASES}' '{remote_release}'")
        sftp = c.open_sftp()
        print("upload", tar_path.name, "->", remote_tar)
        sftp.put(str(tar_path), remote_tar)
        sftp.close()

        extract_cmd = f"""
set -euo pipefail
mkdir -p '{remote_release}'
tar -xzf '{remote_tar}' -C '{remote_release}'
test -f '{remote_release}/cloud/api.py'
test -f '{remote_release}/cloud/web/library.html'
test -f '{remote_release}/cloud/web/library.js'
test -f '{remote_release}/cloud/promote_service.py'
test -f '{remote_release}/cloud/vision_service.py'
test -f '{remote_release}/visual_classifier.py'
test -f '{remote_release}/image_asset_safety.py'
# preserve previous current as backup if it is a real directory
if [ -e '{REMOTE_CURRENT}' ] && [ ! -L '{REMOTE_CURRENT}' ]; then
  if [ ! -e '{REMOTE_BASE}/current.bak-before-{stamp}' ]; then
    cp -a '{REMOTE_CURRENT}' '{REMOTE_BASE}/current.bak-before-{stamp}' || true
  fi
fi
# atomic-ish switch: new symlink then mv
ln -sfn '{remote_release}' '{REMOTE_BASE}/current.next-{stamp}'
rm -rf '{REMOTE_CURRENT}.old' || true
if [ -L '{REMOTE_CURRENT}' ] || [ -e '{REMOTE_CURRENT}' ]; then
  mv '{REMOTE_CURRENT}' '{REMOTE_CURRENT}.old' || true
fi
mv '{REMOTE_BASE}/current.next-{stamp}' '{REMOTE_CURRENT}'
rm -f '{remote_tar}'
# sync compose definition if present in package
if [ -f '{remote_release}/deploy/nas/docker-compose.d2i-cloud.yml' ]; then
  cp -f '{remote_release}/deploy/nas/docker-compose.d2i-cloud.yml' '{REMOTE_COMPOSE}' || true
fi
ls -la '{REMOTE_CURRENT}' | head
python3 - <<'PY'
from pathlib import Path
cur = Path('{REMOTE_CURRENT}')
print('api', (cur/'cloud'/'api.py').is_file())
print('library', (cur/'cloud'/'web'/'library.html').is_file())
print('library_list', 'def library_list' in (cur/'cloud'/'queue_service.py').read_text(encoding='utf-8', errors='replace'))
qs = (cur/'cloud'/'queue_service.py').read_text(encoding='utf-8', errors='replace')
print('finalize_queue', 'def finalize_queue' in qs)
print('auto_finalize', 'def _maybe_auto_finalize' in qs and 'def _reconcile_desired_state' in qs)
print('promote_service', (cur/'cloud'/'promote_service.py').is_file())
print('vision_service', (cur/'cloud'/'vision_service.py').is_file())
print('visual_classifier', (cur/'visual_classifier.py').is_file())
print('prefer_person', '_prefer_person_name' in (cur/'scraper'/'public_profile_spider.py').read_text(encoding='utf-8', errors='replace'))
print('sanitize_multilevel', 'Keep path separators' in (cur/'services'/'public_scraper_config_service.py').read_text(encoding='utf-8', errors='replace'))
comp = (cur/'deploy'/'nas'/'docker-compose.d2i-cloud.yml').read_text(encoding='utf-8', errors='replace')
print('compose_portrait', 'D2I_PORTRAIT_ROOT' in comp)
print('compose_vision', 'D2I_VISION_ENABLED' in comp and 'd2i-grok.env' in comp and 'd2i_vision_api_key' in comp)
PY
"""
        code, out, err = run(c, extract_cmd, timeout=180)
        print(out)
        if err.strip():
            print("extract_err", err[:800])
        if code != 0:
            print("extract/switch failed", code)
            print("DEPLOY_FAILED", stamp)
            return 3

        # Gate 1: container up + /api/v1/health inside 40s, else roll back.
        if restart_and_wait(c) != 0:
            print("GATE_FAILED health after switch")
            rollback(c, prev_release, stamp)
            print("DEPLOY_FAILED", stamp)
            return 4
        print("local health ok")

        # remote smoke. Critical block exits 9 on failure.
        # Token is loaded on the NAS from web_token.txt / project .env — never echoed.
        smoke_remote = r"""
python3 - <<'PY'
import json, sys, urllib.request, urllib.error
from pathlib import Path

def load_token():
    for p in (
        Path('/vol4/1001/hermes-runtime/d2i-cloud-data/web_token.txt'),
        Path('/vol1/1001/d2i-cloud/.env'),
    ):
        if not p.is_file():
            continue
        text = p.read_text(encoding='utf-8', errors='replace')
        if p.name == '.env' or 'D2I_WEB_TOKEN=' in text:
            for line in text.splitlines():
                s = line.strip()
                if s.startswith('D2I_WEB_TOKEN='):
                    return s.split('=', 1)[1].strip().strip('"\'')
        else:
            tok = text.splitlines()[0].strip() if text.strip() else ''
            if tok:
                return tok
    return ''

TOKEN = load_token()
print('smoke_token_present', bool(TOKEN), 'len', len(TOKEN))

def get(path, auth=True):
    headers = {}
    if auth and TOKEN:
        headers['Authorization'] = f'Bearer {TOKEN}'
    req = urllib.request.Request('http://127.0.0.1:8787'+path, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, json.loads(r.read().decode() or '{}')

# --- critical gate ---
try:
    st, body = get('/health', auth=False)
    assert st == 200 and body.get('ok') is True, ('health_open', st, body)
    st, body = get('/api/v1/health')
    assert st == 200 and body.get('ok') is True, ('health', st, body)
    st, body = get('/api/v1/status')
    assert st == 200 and body.get('ok') is True, ('status', st)
    # Default production posture: no app-layer Bearer (Cloudflare Access covers public).
    # If a token is deliberately set, require 401 without it; otherwise open status is fine.
    auth_on = bool(body.get('auth_enabled'))
    print('auth_enabled', auth_on)
    st, body = get('/api/v1/queues?limit=1')
    assert st == 200 and isinstance(body.get('queues'), list), ('queues', st)
    st, body = get('/api/v1/ai/vision/status')
    assert st == 200 and isinstance(body.get('jobs'), dict), ('vision_status', st)
    if auth_on:
        try:
            get('/api/v1/status', auth=False)
            raise AssertionError('unauth status must 401 when auth_enabled')
        except urllib.error.HTTPError as e:
            assert e.code == 401, ('unauth_status', e.code)
    else:
        st, body = get('/api/v1/status', auth=False)
        assert st == 200 and body.get('ok') is True, ('open_status', st)
    print('critical_gate_ok')
except Exception as exc:
    print('critical_gate_FAILED', repr(exc))
    sys.exit(9)
# --- informational smoke below (never fails the deploy) ---
for path in ['/health','/library','/coverage','/api/v1/library?limit=3','/api/v1/coverage/tree','/api/v1/ai/vision/status','/api/v1/status']:
    try:
        if path in ('/library','/coverage','/health'):
            req = urllib.request.Request('http://127.0.0.1:8787'+path)
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read(200)
                print(path, r.status, 'bytes', len(body))
        else:
            st, body = get(path)
            if path.startswith('/api/v1/library'):
                print(path, st, 'total', body.get('total'), 'previewable', body.get('previewable'), 'queues', body.get('queue_count'), 'source', body.get('source'))
            elif path.startswith('/api/v1/coverage'):
                print(path, st, 'keys', sorted(body.keys())[:8] if isinstance(body, dict) else type(body))
            elif path.endswith('/ai/vision/status') or path == '/api/v1/status':
                vision = body.get('vision') if path == '/api/v1/status' else body
                print(path, st, 'auth_enabled', body.get('auth_enabled'), 'vision_available', (vision or {}).get('available'), 'model', (vision or {}).get('model'), 'enabled', (vision or {}).get('enabled'))
            else:
                print(path, st, body)
    except Exception as e:
        print(path, 'ERR', e)
for path in [
    '/api/v1/queues/q_d95f75fd1e61/ai/vision/report',
    '/api/v1/queues/q_d95f75fd1e61/ai/vision/recrawl-plan',
]:
    try:
        st, body = get(path)
        print(path, st, 'keys', sorted(body.keys())[:8] if isinstance(body, dict) else type(body))
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8', 'replace')
        print(path, e.code, raw[:160])
    except Exception as e:
        print(path, 'ERR', e)
try:
    headers = {'Content-Type':'application/json'}
    if TOKEN:
        headers['Authorization'] = f'Bearer {TOKEN}'
    req = urllib.request.Request(
        'http://127.0.0.1:8787/api/v1/queues/q_4bc2b7985e1a/finalize',
        data=json.dumps({'dry_run': True, 'write_people': True}).encode('utf-8'),
        headers=headers,
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        body = json.loads(r.read().decode() or '{}')
        promo = body.get('promote') or {}
        print('finalize_dry', r.status, 'ok', body.get('ok'), 'counts', promo.get('counts'), 'final_base', promo.get('final_base'))
except Exception as e:
    print('finalize_dry ERR', e)
try:
    st, body = get('/api/v1/status')
    print('status', st, 'running', body.get('running'), 'completed', body.get('completed'), 'desired_running', body.get('desired_running'), 'promoted', body.get('promoted'), 'auth_enabled', body.get('auth_enabled'))
    st, body = get('/api/v1/queues?limit=20&auto_finalize=true')
    rows = body.get('queues') or []
    stuck = [q for q in rows if str(q.get('desired_state') or '') == 'running' and not (q.get('runtime') or {}).get('session_running') and str((q.get('runtime') or {}).get('status') or '') == 'completed']
    promoted = [q for q in rows if (q.get('runtime') or {}).get('promoted')]
    print('queues', st, 'count', len(rows), 'stuck_desired_running', len(stuck), 'promoted', len(promoted))
    for q in rows[:8]:
        rt = q.get('runtime') or {}
        print(' q', q.get('id'), 'desired', q.get('desired_state'), 'status', rt.get('status'), 'running', rt.get('session_running'), 'promoted', rt.get('promoted'), 'can_finalize', rt.get('can_finalize'))
except Exception as e:
    print('auto_finalize_smoke ERR', e)
print('current_release', Path('/vol1/1001/d2i-cloud/current').resolve())
comp = Path('/vol1/1001/d2i-cloud/docker-compose.d2i-cloud.yml').read_text(encoding='utf-8', errors='replace')
print('compose_has_portrait', 'D2I_PORTRAIT_ROOT' in comp, 'people_vol', '/runtime/people' in comp, 'compose_has_web_token', 'D2I_WEB_TOKEN' in comp)
PY
"""
        code, out, err = run(c, smoke_remote, timeout=120)
        print(out)
        if err.strip():
            print("smoke_err", err[:500])
        if code != 0:
            print("GATE_FAILED remote critical smoke rc", code)
            rollback(c, prev_release, stamp)
            print("DEPLOY_FAILED", stamp)
            return 5

        # LAN smoke from this machine
        print("lan smoke")
        web_token = load_web_token()
        print("lan_token_present", bool(web_token), "len", len(web_token))
        st, body = http_json(f"http://{HOST}:8787/health")
        print("lan /health", st, body)
        st, body = http_json(f"http://{HOST}:8787/api/v1/library?limit=2", token=web_token, timeout=90)
        if isinstance(body, dict):
            print(
                "lan /api/v1/library",
                st,
                "total",
                body.get("total"),
                "previewable",
                body.get("previewable"),
                "queue_count",
                body.get("queue_count"),
                "source",
                body.get("source"),
            )
        else:
            print("lan /api/v1/library", st, body)
        st, body = http_json(f"http://{HOST}:8787/api/v1/coverage/tree", token=web_token, timeout=90)
        print("lan /api/v1/coverage/tree", st, type(body).__name__, list(body)[:6] if isinstance(body, dict) else body)
        st, body = http_json(f"http://{HOST}:8787/api/v1/status", token=web_token, timeout=30)
        if isinstance(body, dict):
            print("lan /api/v1/status", st, "auth_enabled", body.get("auth_enabled"), "promoted", body.get("promoted"))
        else:
            print("lan /api/v1/status", st, body)
        # page routes
        for path in ("/library", "/coverage"):
            try:
                with urllib.request.urlopen(f"http://{HOST}:8787{path}", timeout=30) as r:
                    print("lan", path, r.status, r.headers.get("Content-Type"), "len", r.headers.get("Content-Length"))
            except Exception as e:  # noqa: BLE001
                print("lan", path, "ERR", e)

        note = DIST / f"DEPLOY-{stamp}.txt"
        note.write_text(
            "\n".join(
                [
                    f"stamp={stamp}",
                    f"package={tar_path.name}",
                    f"remote_release={remote_release}",
                    f"previous_release={prev_release}",
                    f"current={REMOTE_CURRENT}",
                    f"host={HOST}",
                    "gates=import-smoke,health,critical-api",
                    f"built_at={datetime.now().isoformat(timespec='seconds')}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print("wrote", note)
        print("DEPLOY_OK", stamp)
        return 0
    finally:
        c.close()


if __name__ == "__main__":
    # Windows consoles often default to GBK; keep deploy logs printable.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    raise SystemExit(main())
