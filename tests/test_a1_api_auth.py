# -*- coding: utf-8 -*-
"""A1-6/7 API 加固(总纲 §2.3):Bearer 常量时间比较 + CORS 收敛。

校验点:
- require_auth:未设 token 时放行;token 正确(裸串/Bearer 前缀)放行;
  错误/缺失 → HTTPException 401(内部走 hmac.compare_digest);
- create_app 的 CORS allow_origins 不含 "*",且包含生产域名与本地调试源。

纯 python assert,可被 pytest 收集,也可直接 `python tests/test_a1_api_auth.py`。
"""

import atexit
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_TMP = tempfile.mkdtemp(prefix="d2i_a1_auth_")
os.environ["D2I_SCRAPER_REGISTRY_PATH"] = os.path.join(_TMP, "registry.json")
os.environ["D2I_CLOUD_DATA_ROOT"] = os.path.join(_TMP, "cloud")
os.environ["D2I_CLOUD_JOBS_DB"] = os.path.join(_TMP, "jobs.sqlite")
os.environ["D2I_CLOUD_TASKS_ROOT"] = os.path.join(_TMP, "tasks")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)

from fastapi import HTTPException  # noqa: E402

from cloud import api as cloud_api  # noqa: E402

_TEST_TOKEN = "a1-test-token-not-a-secret"  # 仅测试用假 token,非真实凭据


def _with_token(value, fn):
    old = os.environ.get("D2I_WEB_TOKEN")
    try:
        if value is None:
            os.environ.pop("D2I_WEB_TOKEN", None)
        else:
            os.environ["D2I_WEB_TOKEN"] = value
        return fn()
    finally:
        if old is None:
            os.environ.pop("D2I_WEB_TOKEN", None)
        else:
            os.environ["D2I_WEB_TOKEN"] = old


def test_require_auth_noop_without_token():
    _with_token(None, lambda: cloud_api.require_auth(None))
    _with_token("", lambda: cloud_api.require_auth("Bearer whatever"))


def test_require_auth_accepts_correct_token():
    _with_token(_TEST_TOKEN, lambda: cloud_api.require_auth(f"Bearer {_TEST_TOKEN}"))
    _with_token(_TEST_TOKEN, lambda: cloud_api.require_auth(_TEST_TOKEN))


def test_require_auth_rejects_wrong_or_missing():
    for bad in (None, "", "Bearer nope", f"Bearer {_TEST_TOKEN}x", _TEST_TOKEN[:-1]):
        try:
            _with_token(_TEST_TOKEN, lambda: cloud_api.require_auth(bad))
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError(f"authorization={bad!r} must be rejected")


def test_cors_origins_converged():
    app = cloud_api.create_app()
    cors = next(
        (mw for mw in app.user_middleware if "CORSMiddleware" in str(mw.cls)),
        None,
    )
    assert cors is not None, "CORSMiddleware missing"
    origins = list(cors.kwargs.get("allow_origins") or [])
    assert "*" not in origins, "wildcard origin must be gone"
    assert "https://d2i.517411.xyz" in origins
    assert "http://127.0.0.1:8787" in origins
    assert "http://localhost:8791" in origins


if __name__ == "__main__":
    for fn in (
        test_require_auth_noop_without_token,
        test_require_auth_accepts_correct_token,
        test_require_auth_rejects_wrong_or_missing,
        test_cors_origins_converged,
    ):
        fn()
        print(f"PASS {fn.__name__}")
