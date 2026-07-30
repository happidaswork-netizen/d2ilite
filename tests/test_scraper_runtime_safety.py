"""Focused runtime safety checks for public scraper downloads.

Runs with plain Python; no live network and no crawler startup.
"""

from pathlib import Path
import os
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SCRAPER = ROOT / "scraper"
for item in (str(ROOT), str(SCRAPER)):
    if item not in sys.path:
        sys.path.insert(0, item)

import run_public_scraper as runner


class _Response:
    status_code = 200
    text = ""


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response()


def test_temp_dir_uses_writable_runtime_root():
    previous = os.environ.get("D2I_SCRAPER_TEMP_ROOT")
    with tempfile.TemporaryDirectory() as tmp:
        try:
            os.environ["D2I_SCRAPER_TEMP_ROOT"] = tmp
            actual = runner.scoped_temp_dir("_tmp_browser_downloads", "queue/demo")
            expected = Path(tmp) / "d2i-public-scraper" / "_tmp_browser_downloads"
            assert actual.is_dir()
            assert actual.is_relative_to(expected)
        finally:
            if previous is None:
                os.environ.pop("D2I_SCRAPER_TEMP_ROOT", None)
            else:
                os.environ["D2I_SCRAPER_TEMP_ROOT"] = previous


def test_request_honors_template_scoped_tls_setting():
    session = _Session()
    runner._request_with_optional_jsl(
        session=session,
        url="https://example.invalid/image.png",
        headers={"User-Agent": "test"},
        timeout_seconds=5,
        enable_jsl=False,
        jsl_max_retries=1,
        verify_tls=False,
    )
    assert session.calls[0][1]["verify"] is False


if __name__ == "__main__":
    test_temp_dir_uses_writable_runtime_root()
    test_request_honors_template_scoped_tls_setting()
    print("test_scraper_runtime_safety: PASS")
